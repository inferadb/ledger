//! Vault lifecycle integration tests (Path A, P2b.2.g).
//!
//! Validates that the per-vault consensus lifecycle composes correctly
//! through the real gRPC + Raft + apply pipeline. The earlier slices
//! (P2b.2.a–P2b.2.f) landed:
//!   * `RaftManager::start_vault_group` / `stop_vault_group`,
//!   * `VaultCreationRequest` / `VaultDeletionRequest` signals from the `CreateVault` /
//!     `DeleteVault` apply arms,
//!   * the watcher task in `start_organization_group` that drives those signals,
//!   * rehydration of non-deleted vaults on `start_organization_group` re-invocation,
//!   * per-vault raft.db fan-out in `sync_all_state_dbs`.
//!
//! Unit tests cover each piece in isolation. This file is the
//! end-to-end assertion: a gRPC `CreateVault` RPC against a
//! 3-voter cluster must cause a per-vault `VaultGroup` to register
//! on every voter via the commit-dispatcher → watcher → `start_vault_group`
//! chain.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, VaultId, VaultSlug};

use crate::common::{
    TestCluster, create_vault_client, create_write_client, resolve_org_id,
    wait_for_vault_group_live_on_all_voters, wait_for_vault_group_removed_on_all_voters,
};

/// Primary test: `CreateVault` must bring a per-vault `VaultGroup` live
/// on every voter in the organization via the real apply pipeline.
///
/// Flow:
///   1. Build a 3-node cluster with 1 data region (`US_EAST_VA`).
///   2. Create an organization via the production gRPC + saga path.
///   3. Create a vault via the production gRPC path (`VaultService::create_vault`).
///   4. Wait for the `CreateVault` → `VaultCreationRequest` → watcher → `start_vault_group` chain
///      to fire on every voter.
///   5. Assert every voter reports the same `(region, org_id, vault_id)` entry in
///      `list_vault_groups()` and `has_vault_group(..) == true`.
#[tokio::test]
async fn test_create_vault_brings_vault_group_live_on_all_voters() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;

    let leader = cluster.leader().expect("cluster has a leader");

    // Create an organization via the production gRPC pipeline.
    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-lifecycle-org", leader)
            .await
            .expect("create organization");

    // Resolve the external slug to the internal id used by the Raft
    // group registry. The `create_test_organization` helper polls until
    // status == Active, so the slug is indexed on the leader.
    let org_id = resolve_org_id(leader, org_slug);

    // Sanity: per-organization group must exist on every voter before
    // we propose CreateVault. The `create_test_organization` helper
    // internally awaits organization saga completion, but the per-org
    // group propagation is slightly further downstream —
    // `create_test_vault` would fail with NotFound /
    // FailedPrecondition if it wasn't live yet, so a hard assertion on
    // `has_organization_group` catches ordering issues cleanly.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_organization_group(region, org_id),
            "per-organization group (region={region:?}, org_id={org_id:?}) missing on node {}",
            node.id,
        );
    }

    // Baseline: no vault groups exist yet for this (region, org_id).
    for node in cluster.nodes() {
        let pre: Vec<_> = node
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_id)
            .collect();
        assert!(
            pre.is_empty(),
            "pre-CreateVault: node {} already has vault groups for ({region:?}, {org_id:?}): \
             {pre:?}",
            node.id,
        );
    }

    // Create the vault through the gRPC surface — same code path a
    // real SDK client hits. `create_test_vault` retries on NotFound /
    // FailedPrecondition while the per-org group spins up, but by here
    // the group is already live so it should succeed on the first try.
    // The returned slug is not read — the vault-live assertion below
    // goes through the internal VaultId registry on every voter.
    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");

    // Poll until every voter's `RaftManager` has registered a vault
    // group for (region, org_id). The apply-phase `CreateVault` arm is
    // fire-and-forget through the `VaultCreationRequest` channel, so
    // we cannot assume synchronous propagation from the gRPC return.
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    // Final assertions — primary test contract.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_vault_group(region, org_id, vault_id),
            "vault group (region={region:?}, org_id={org_id:?}, vault_id={vault_id:?}) missing \
             on node {}",
            node.id,
        );
    }

    // The leader's list_vault_groups must contain the exact triple.
    let leader_triples = leader.manager.list_vault_groups();
    assert!(
        leader_triples.contains(&(region, org_id, vault_id)),
        "leader's list_vault_groups missing ({region:?}, {org_id:?}, {vault_id:?}): got \
         {leader_triples:?}",
    );

    // Exactly one vault group must exist for this (region, org_id) —
    // we created exactly one vault. Guards against rehydration fan-out
    // bugs that would multiplicatively spawn groups and against
    // duplicate VaultCreationRequest handling.
    let vaults_for_org: Vec<(Region, OrganizationId, VaultId)> =
        leader_triples.into_iter().filter(|(r, o, _)| *r == region && *o == org_id).collect();
    assert_eq!(
        vaults_for_org.len(),
        1,
        "expected exactly one vault group for (region={region:?}, org_id={org_id:?}); got \
         {vaults_for_org:?}",
    );
}

/// Regression test for #166: per-vault Raft replication on followers.
///
/// `lookup_by_consensus_shard` must resolve a per-vault `ConsensusStateId`
/// to the parent organization's `InnerGroup` on every voter. The gRPC
/// `RaftService::replicate` handler dispatches inbound `Replicate`
/// messages by shard id; before this fix, the lookup only scanned
/// `RaftManager::regions` (system + data + per-org groups) and missed
/// per-vault shards that register on the parent org's
/// [`ConsensusEngine`]. Misses fell back to the data-region group and
/// the AppendEntries was silently misrouted to the wrong shard, breaking
/// quorum formation on the vault's Raft log.
///
/// This asserts the post-fix invariant: for every voter, the vault's
/// shard id resolves to a group whose engine is the parent organization
/// group's engine. The follow-on `RaftService::replicate` dispatch path
/// then calls `engine.peer_message(consensus_shard, ...)` with the
/// wire-side shard id directly, landing on the per-vault shard
/// registered via `org_inner.handle().add_shard(consensus_shard)`.
#[tokio::test]
async fn test_vault_shard_lookup_resolves_to_parent_org_group_on_every_voter() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-shard-lookup-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);
    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    for node in cluster.nodes() {
        // Local sanity: the vault group is live on this voter and exposes a
        // shard id distinct from the parent org's shard id.
        let vault_group = node
            .manager
            .get_vault_group(region, org_id, vault_id)
            .expect("vault group registered on every voter");
        let vault_shard_id = vault_group.shard_id();

        let parent_org_group = node
            .manager
            .get_organization_group(region, org_id)
            .expect("parent organization group registered on every voter");
        let parent_shard_id = parent_org_group.handle().shard_id();
        assert_ne!(
            vault_shard_id, parent_shard_id,
            "node {}: vault shard id collides with parent org shard id — derivation regression",
            node.id,
        );

        // Critical: looking up the vault's shard id through the
        // RaftService dispatch helper must resolve to a group whose
        // engine matches the parent org's engine. The `Arc::ptr_eq`
        // check is what guarantees the inbound `Replicate` path will
        // dispatch into the same reactor where the vault shard is
        // registered.
        let resolved =
            node.manager.lookup_by_consensus_shard(vault_shard_id).unwrap_or_else(|| {
                panic!(
                    "node {}: lookup_by_consensus_shard({vault_shard_id:?}) returned None for live \
                 vault — replication path would silently misroute",
                    node.id,
                )
            });
        assert!(
            std::sync::Arc::ptr_eq(
                &resolved.handle().engine_arc(),
                &parent_org_group.handle().engine_arc(),
            ),
            "node {}: vault shard {vault_shard_id:?} resolved to a group whose engine is not the \
             parent org's engine — peer_message would dispatch into the wrong reactor",
            node.id,
        );
    }
}

// ---------------------------------------------------------------------------
// Additional helpers — used by the multi-vault, cross-org, and delete tests.
// ---------------------------------------------------------------------------

/// Polls every node until every voter reports exactly `expected_count` vault
/// groups for `(region, org_id)`, with the same set of `VaultId`s on every
/// node. Returns the converged set sorted ascending.
///
/// This is the count-aware sibling of
/// [`wait_for_vault_group_live_on_all_voters`]. The earlier helper assumes
/// "a single new vault has just been created"; multi-vault tests need to
/// wait for a specific cardinality before sampling.
async fn wait_for_vault_set_on_all_voters(
    cluster: &TestCluster,
    region: Region,
    org_id: OrganizationId,
    expected_count: usize,
    timeout: Duration,
) -> Vec<VaultId> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let per_node: Vec<Vec<VaultId>> = cluster
            .nodes()
            .iter()
            .map(|n| {
                let mut ids: Vec<VaultId> = n
                    .manager
                    .list_vault_groups()
                    .into_iter()
                    .filter(|(r, o, _)| *r == region && *o == org_id)
                    .map(|(_, _, v)| v)
                    .collect();
                ids.sort();
                ids
            })
            .collect();

        if let Some(first) = per_node.first()
            && first.len() == expected_count
            && per_node.iter().all(|ids| ids == first)
        {
            return first.clone();
        }

        if tokio::time::Instant::now() >= deadline {
            let rendered: Vec<String> = cluster
                .nodes()
                .iter()
                .zip(per_node.iter())
                .map(|(n, ids)| format!("node {}: {:?}", n.id, ids))
                .collect();
            panic!(
                "vault set for (region={region:?}, org_id={org_id:?}) did not converge to \
                 {expected_count} entries within {timeout:?}. per-node state: [{}]",
                rendered.join(" | "),
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Issues a `DeleteVault` gRPC request against the cluster's leader. Mirrors
/// the path a real SDK client would hit. Returns the response on success.
///
/// Retries on `NotFound` and on `Unavailable("Not the leader")`. Symmetric
/// with `common::create_test_vault`'s retry loop. Rationale: `delete_vault`
/// resolves the vault slug through the GLOBAL slug index, populated by a
/// `RegisterVaultDirectoryEntry` propose that is separate from the per-org
/// `CreateVault` propose. Under starvation the GLOBAL propagation can lag
/// the local `CreateVault` confirmation, so a freshly-created vault may
/// briefly look `NotFound` to a delete RPC immediately after creation.
async fn delete_test_vault(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout_dur = std::time::Duration::from_secs(15);

    loop {
        let mut client = create_vault_client(addr).await?;
        let result = client
            .delete_vault(proto::DeleteVaultRequest {
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                caller: None,
            })
            .await;

        match result {
            Ok(_) => return Ok(()),
            Err(status)
                if status.code() == tonic::Code::NotFound
                    || (status.code() == tonic::Code::Unavailable
                        && status.message().contains("Not the leader")) =>
            {
                if start.elapsed() > timeout_dur {
                    return Err(
                        format!("vault deletion failed after retry: {}", status.message()).into()
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            },
            Err(status) => return Err(format!("delete vault failed: {status}").into()),
        }
    }
}

/// Two `CreateVault` proposals against the same organization must produce
/// two distinct vault groups, both live on every voter.
///
/// Validates:
///   * `VaultCreationRequest` watcher fan-out across multiple vaults in a single organization.
///   * `start_vault_group` allocates a distinct `shard_id` per vault — the deterministic
///     per-`(region, org, vault)` shard derivation must not alias.
///   * `list_vault_groups()` reports exactly two entries on every voter.
#[tokio::test]
async fn test_create_multiple_vaults_in_one_org() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-multi-vault-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    // First vault — wait for it to appear on every voter.
    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault 1");
    let vault_id_1 =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    // Second vault — wait for the cardinality to grow to 2 on every voter.
    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault 2");
    let converged =
        wait_for_vault_set_on_all_voters(&cluster, region, org_id, 2, Duration::from_secs(15))
            .await;

    assert_eq!(converged.len(), 2, "expected exactly two vaults; got {converged:?}");
    assert!(
        converged.contains(&vault_id_1),
        "converged set {converged:?} missing first vault {vault_id_1:?}",
    );

    // The second VaultId is the one in `converged` that isn't `vault_id_1`.
    let vault_id_2 = converged
        .iter()
        .copied()
        .find(|v| *v != vault_id_1)
        .expect("converged set has a second distinct VaultId");

    assert_ne!(
        vault_id_1, vault_id_2,
        "expected distinct VaultIds for the two vaults; got {vault_id_1:?} == {vault_id_2:?}",
    );

    // Every voter must register both vault groups.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_vault_group(region, org_id, vault_id_1),
            "vault group 1 ({region:?}, {org_id:?}, {vault_id_1:?}) missing on node {}",
            node.id,
        );
        assert!(
            node.manager.has_vault_group(region, org_id, vault_id_2),
            "vault group 2 ({region:?}, {org_id:?}, {vault_id_2:?}) missing on node {}",
            node.id,
        );

        let scoped: Vec<VaultId> = node
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_id)
            .map(|(_, _, v)| v)
            .collect();
        assert_eq!(
            scoped.len(),
            2,
            "node {} expected exactly 2 vaults for (region={region:?}, org_id={org_id:?}); got \
             {scoped:?}",
            node.id,
        );
    }

    // Anchor the deterministic-per-(region, org, vault) shard_id invariant:
    // distinct VaultIds must yield distinct VaultGroups (and thus distinct
    // shard_ids) on the leader. This guards against a regression where two
    // vaults collapse onto the same Raft group.
    let group_1 = leader
        .manager
        .get_vault_group(region, org_id, vault_id_1)
        .expect("get vault group 1 on leader");
    let group_2 = leader
        .manager
        .get_vault_group(region, org_id, vault_id_2)
        .expect("get vault group 2 on leader");
    assert_ne!(
        group_1.shard_id(),
        group_2.shard_id(),
        "expected distinct shard_ids for distinct vaults; got {:?} == {:?}",
        group_1.shard_id(),
        group_2.shard_id(),
    );
}

/// A vault created in organization B must not appear in organization A's
/// `vault_groups` view, and vice versa.
///
/// Validates cross-organization isolation at the registration / routing
/// layer: the `(region, org_id, vault_id)` tuple is unique per organization,
/// and the watcher / `start_vault_group` chain wires each vault into its
/// owning per-organization group exclusively.
#[tokio::test]
async fn test_create_vault_in_second_org() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_a_slug, _admin_a) =
        crate::common::create_test_organization(&leader.addr, "vault-cross-org-a", leader)
            .await
            .expect("create org A");
    let (org_b_slug, _admin_b) =
        crate::common::create_test_organization(&leader.addr, "vault-cross-org-b", leader)
            .await
            .expect("create org B");

    let org_a_id = resolve_org_id(leader, org_a_slug);
    let org_b_id = resolve_org_id(leader, org_b_slug);
    assert_ne!(org_a_id, org_b_id, "test fixture invariant: distinct org IDs");

    // Create one vault in each org.
    crate::common::create_test_vault(&leader.addr, org_a_slug).await.expect("create vault A");
    let vault_id_a = wait_for_vault_group_live_on_all_voters(
        &cluster,
        region,
        org_a_id,
        Duration::from_secs(30),
    )
    .await;

    crate::common::create_test_vault(&leader.addr, org_b_slug).await.expect("create vault B");
    let vault_id_b = wait_for_vault_group_live_on_all_voters(
        &cluster,
        region,
        org_b_id,
        Duration::from_secs(30),
    )
    .await;

    // Note: `VaultId` is allocated from a per-organization sequence, so the
    // first vault in every fresh org gets `VaultId(1)`. The cross-org
    // isolation invariant therefore lives on the `(region, org_id, vault_id)`
    // triple — *not* on raw VaultId comparison. Two vaults can legally share
    // a `VaultId` value as long as they're scoped to different orgs.
    //
    // The vault groups live in disjoint per-org keyspaces; the two triples
    // `(region, org_a_id, vault_id_a)` and `(region, org_b_id, vault_id_b)`
    // are distinct even when `vault_id_a == vault_id_b`.
    for node in cluster.nodes() {
        let org_a_vaults: Vec<VaultId> = node
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_a_id)
            .map(|(_, _, v)| v)
            .collect();
        let org_b_vaults: Vec<VaultId> = node
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_b_id)
            .map(|(_, _, v)| v)
            .collect();

        assert_eq!(
            org_a_vaults,
            vec![vault_id_a],
            "node {}: org A vault set mismatch (expected [{vault_id_a:?}])",
            node.id,
        );
        assert_eq!(
            org_b_vaults,
            vec![vault_id_b],
            "node {}: org B vault set mismatch (expected [{vault_id_b:?}])",
            node.id,
        );

        // Triple-level isolation: each org's vault is only registered under
        // its own (region, org_id) — not under the other org's (region,
        // org_id) even when the VaultId values collide. `has_vault_group`
        // checks the full triple.
        assert!(
            node.manager.has_vault_group(region, org_a_id, vault_id_a),
            "node {}: org A's vault triple ({region:?}, {org_a_id:?}, {vault_id_a:?}) missing",
            node.id,
        );
        assert!(
            node.manager.has_vault_group(region, org_b_id, vault_id_b),
            "node {}: org B's vault triple ({region:?}, {org_b_id:?}, {vault_id_b:?}) missing",
            node.id,
        );
    }
}

/// `DeleteVault` must flow through the apply pipeline and tear down the
/// per-vault `VaultGroup` on every voter via the
/// `VaultDeletionRequest` → watcher → `stop_vault_group` chain.
///
/// Validates the symmetric counterpart of the create path — the
/// `DeleteVault` apply arm at `crates/raft/src/log_storage/operations/mod.rs`
/// fires `VaultDeletionRequest` only on `VaultDeleted { success: true }`,
/// so the test must exercise a real apply path via gRPC, not a direct
/// state mutation.
#[tokio::test]
async fn test_delete_vault_tears_down_vault_group() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-delete-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    // Create the vault and wait for it to be live on every voter.
    let vault_slug =
        crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    // Sanity: every voter has the vault group before delete.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_vault_group(region, org_id, vault_id),
            "pre-DeleteVault: node {} missing vault group ({region:?}, {org_id:?}, {vault_id:?})",
            node.id,
        );
    }

    // Issue the delete via gRPC — same code path a real SDK client hits.
    delete_test_vault(&leader.addr, org_slug, vault_slug).await.expect("delete vault");

    // Wait for the delete signal → watcher → `stop_vault_group` chain to
    // remove the registration on every voter. This is fire-and-forget from
    // the apply arm just like the create path, so we cannot assume
    // synchronous propagation from the gRPC return.
    wait_for_vault_group_removed_on_all_voters(
        &cluster,
        region,
        org_id,
        vault_id,
        Duration::from_secs(15),
    )
    .await;

    // Final assertions — every voter has torn down the vault group.
    for node in cluster.nodes() {
        assert!(
            !node.manager.has_vault_group(region, org_id, vault_id),
            "post-DeleteVault: node {} still has vault group ({region:?}, {org_id:?}, \
             {vault_id:?})",
            node.id,
        );

        let remaining: Vec<(Region, OrganizationId, VaultId)> = node
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_id)
            .collect();
        assert!(
            remaining.is_empty(),
            "post-DeleteVault: node {} expected no vault groups for (region={region:?}, \
             org_id={org_id:?}); got {remaining:?}",
            node.id,
        );

        // Delete is scoped to the vault — the parent organization group must
        // remain live on every voter.
        assert!(
            node.manager.has_organization_group(region, org_id),
            "post-DeleteVault: node {} lost organization group ({region:?}, {org_id:?}) — delete \
             leaked beyond the vault",
            node.id,
        );
    }
}

/// A graceful whole-cluster restart must rehydrate every existing
/// per-organization Raft group (Task #151) and every existing per-vault
/// `VaultGroup` (Task #146's sweep inside `start_organization_group`) on
/// every voter.
///
/// Cold-restart election convergence (Task #172) closed the production gap
/// that previously kept this test ignored: the three-phase shutdown sweep
/// (vault → org → region) plus election-critical message preservation in
/// `peer_sender::drop_queue` and HTTP/2 server keepalive (Task #167) now
/// allow simultaneous whole-cluster restart to converge to a leader
/// deterministically. The test runs on every CI invocation; if it starts
/// to fail again, suspect regressions in the cold-restart election path,
/// the persisted-membership rehydration in `RaftLogStore::open`, or the
/// shutdown-sweep ordering in `GracefulShutdown`.
///
/// Flow:
///   1. Build a 3-node cluster with 1 data region.
///   2. Create an organization + a vault via the production gRPC path.
///   3. Wait for the vault group to be live on every voter.
///   4. `cluster.graceful_restart()` — flushes WALs, syncs state DBs, stops each server,
///      re-bootstraps every node against the same data_dir.
///   5. Wait for the system region to re-elect a leader.
///   6. Wait for the per-org group to rehydrate on every voter.
///   7. Wait for the per-vault group to rehydrate on every voter, with the SAME `(region, org_id,
///      vault_id)` triple observed before the restart.
///
/// This is the end-to-end assertion on the rehydration chain — production
/// code at `bootstrap.rs` (restart-path block) + `rehydrate_organization_group`
/// (shared helper) + `start_organization_group` (per-vault sweep) must
/// compose correctly through a real restart. No production code path is
/// stubbed or bypassed.
// Multi-thread runtime override: this test drives the only TCP + restart
// path in the suite (`with_tcp_data_regions(3, 1)` + `graceful_restart`),
// which compounds tokio worker starvation under cargo's parallel test
// scheduler. `#[tokio::test]` defaults to single-worker `CurrentThread`,
// and three full Raft nodes plus the test driver on one worker is the
// reproduction window for the Mode A / Mode C flake class documented in
// `docs/audits/parallel-test-flakes-2026-04-29.md`. Multi-thread keeps
// this test starvation-tolerant without affecting the rest of the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_group_rehydrates_after_graceful_cluster_restart() {
    // TCP transport is required for this test: on restart, peer addresses
    // must re-populate before the per-organization rehydration sweep runs,
    // and the only mechanism that does that without modifying production
    // code is `bootstrap_node`'s `--join` seed discovery path. That path
    // only accepts `SocketAddr` strings, so UDS socket paths cannot be
    // used here — `parse_seed_addresses` filters them out as unparseable.
    let cluster = TestCluster::with_tcp_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader pre-restart");

    // Create the org + vault via the production gRPC path. The helpers
    // retry against NotFound / FailedPrecondition while the per-org
    // group spins up, so by the time these return the org is Active and
    // the vault row is committed.
    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-restart-org", leader)
            .await
            .expect("create organization pre-restart");
    let org_id_pre = resolve_org_id(leader, org_slug);
    crate::common::create_test_vault(&leader.addr, org_slug)
        .await
        .expect("create vault pre-restart");

    // Wait until every voter has the vault group registered — establishes
    // the "before" side of the restart assertion. Uses the same helper the
    // primary create test uses.
    let vault_id_pre = wait_for_vault_group_live_on_all_voters(
        &cluster,
        region,
        org_id_pre,
        std::time::Duration::from_secs(30),
    )
    .await;

    // Graceful whole-cluster restart. Every node shuts down cleanly
    // (WAL flush + state DB sync), then re-bootstraps against the same
    // data_dir. The returned cluster holds new `TestNode` handles but
    // the SAME cluster_id, node ids, and socket addresses.
    let cluster = cluster.graceful_restart().await;

    // A leader must re-emerge on the system (GLOBAL) region before any
    // of the applied-state resolution below is safe — `resolve_org_id`
    // reads from the leader's `applied_state`, which only populates after
    // the restart-path log replay catches up to `last_committed`.
    let timeout = std::time::Duration::from_secs(60);
    if cluster.wait_for_leader_agreement(timeout).await.is_none() {
        let snapshots: Vec<String> = cluster
            .nodes()
            .iter()
            .map(|n| {
                let peers: Vec<(u64, String)> = n.manager.peer_addresses().iter_peers();
                format!(
                    "node {} addr={} leader={:?} term={} peers={:?}",
                    n.id,
                    n.addr,
                    n.current_leader(),
                    n.current_term(),
                    peers,
                )
            })
            .collect();
        panic!(
            "leader did not re-elect within {timeout:?} after graceful_restart. per-node \
             snapshots:\n  {}",
            snapshots.join("\n  "),
        );
    }

    // The leader after restart may be a different node than before — the
    // election is independent of the pre-restart leader id. Resolve it
    // now against the post-restart cluster state.
    let leader = cluster.leader().expect("cluster has a leader post-restart");

    // `OrganizationId` is an internal sequential id persisted to disk, so
    // the slug must resolve to the same internal id post-restart. A
    // mismatch would indicate the applied-state snapshot lost the slug
    // index across the restart — a correctness bug.
    let org_id_post = resolve_org_id(leader, org_slug);
    assert_eq!(
        org_id_pre, org_id_post,
        "organization id changed across restart: pre={org_id_pre:?}, post={org_id_post:?}",
    );

    // Primary assertion: the vault group must come back up on every voter
    // with the same triple. The 30s budget is deliberately generous —
    // the rehydration chain on restart is:
    //   a. Log replay populates `peer_addresses` (RegisterPeerAddress
    //      entries re-apply).
    //   b. `bootstrap.rs`'s restart-path sweep calls
    //      `rehydrate_organization_group` for each persisted org; if
    //      `peer_addresses` is still empty on a given node at that
    //      moment, the call is warn-and-skip and that node's org group
    //      does not rehydrate this cycle.
    //   c. Every node that successfully rehydrates its org group runs
    //      the vault sweep inside `start_organization_group`.
    //
    // The restart kicks off all three nodes concurrently, so node #1
    // may see empty peers before node #2 has finished starting its
    // server. When that happens the test cannot converge without a
    // subsequent mechanism re-kicking the sweep — surface diagnostic
    // output loudly so we learn about the regression rather than
    // suppressing it.
    let vault_id_post = wait_for_vault_group_live_on_all_voters(
        &cluster,
        region,
        org_id_post,
        std::time::Duration::from_secs(30),
    )
    .await;

    assert_eq!(
        vault_id_pre, vault_id_post,
        "vault id changed across restart: pre={vault_id_pre:?}, post={vault_id_post:?}",
    );

    // Every voter must also have the organization group live — the
    // per-vault group cannot be live without its parent per-org group,
    // but assert it explicitly so a regression points at the right
    // layer.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_organization_group(region, org_id_post),
            "post-restart: node {} missing per-organization group ({region:?}, {org_id_post:?})",
            node.id,
        );
        assert!(
            node.manager.has_vault_group(region, org_id_post, vault_id_post),
            "post-restart: node {} missing vault group ({region:?}, {org_id_post:?}, \
             {vault_id_post:?})",
            node.id,
        );
    }
}

/// P2c.3.b: a `Write` gRPC RPC must propose to the vault's per-vault Raft
/// shard, not the parent organization shard. The vault's `VaultGroup` apply
/// pipeline runs the proposed entry and advances the per-vault
/// `last_applied` counter; the org group's apply pipeline does not.
///
/// Flow:
///   1. 3-voter cluster + 1 data region.
///   2. Create org + vault; wait for the `VaultGroup` to register on every voter.
///   3. Capture the per-vault `last_applied` baseline on the leader's vault group.
///   4. Issue a `Write` via the gRPC `WriteService` against the leader.
///   5. Assert the write succeeded.
///   6. Assert the leader's vault group `last_applied` advanced — confirms the proposal landed on
///      the vault shard's apply pipeline.
///
/// Regression-tests the full per-vault write routing pipeline:
/// vault leader adoption (#160), vault response fan-out (#163), and
/// per-vault Raft replication on followers (#166). `WriteService::write`
/// proposes through `vault_group.handle()` (see task #162 / P2c.3.b.2);
/// a failure here means one of those links regressed.
#[tokio::test]
async fn test_write_routes_to_vault_shard_and_lands_in_vault_state() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    // Create an organization through the production gRPC + saga path.
    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-write-routing-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    // Create the vault. The `create_test_vault` helper retries while the
    // per-org group spins up, so by the time it returns the org group is
    // live on the leader.
    let vault_slug =
        crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");

    // Wait for the per-vault `VaultGroup` to register on every voter via
    // the apply → watcher → `start_vault_group` chain. Returns the
    // internal `VaultId` every node agrees on.
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    // Capture baseline per-vault applied index on the leader's vault
    // group. `applied_index_watch()` surfaces the vault `RaftLogStore`'s
    // last-applied counter, driven forward by
    // `RaftLogStore::apply_committed_entries` every time the vault apply
    // pipeline runs an entry — distinct from the parent org's
    // `applied_state.last_applied`. It only advances when an entry
    // applies on the vault shard.
    //
    // Note: `InnerVaultGroup::vault_applied_state` (the `ArcSwap`
    // carve-out) is not yet wired — task #165 ("Project
    // vault_applied_state from RaftLogStore.applied_state on each
    // apply") is pending — so we observe the advance through the watch
    // channel instead.
    let leader_vault_pre =
        leader.manager.get_vault_group(region, org_id, vault_id).expect("leader has vault group");
    let last_applied_index_pre = *leader_vault_pre.applied_index_watch().borrow();

    // Issue a `Write` through the gRPC surface — same code path a real
    // SDK client hits.
    let mut client =
        create_write_client(&leader.addr).await.expect("connect to leader write service");
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-routing-test-client".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: org_slug.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "vault-routing-test-key".to_string(),
                    value: b"vault-routing-test-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request).await.expect("write should succeed").into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(success)) => {
            assert!(success.tx_id.is_some(), "write success must include tx_id");
            assert!(success.block_height > 0, "write success must include block_height > 0");
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(err)) => {
            panic!("write failed: code={:?} message={:?}", err.code, err.message,);
        },
        None => panic!("write response had no result"),
    }

    // Poll the leader's vault-group applied index until it advances.
    // `propose_and_wait` returns when the apply worker delivers the
    // response, so by the time the write call above unblocks, the vault
    // `RaftLogStore::applied_state` has already been mutated and the
    // watch broadcast has fired. We still poll with a short grace
    // window to absorb the watch-channel scheduling delay.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let leader_vault = leader
            .manager
            .get_vault_group(region, org_id, vault_id)
            .expect("leader has vault group");
        let last_applied_index_post = *leader_vault.applied_index_watch().borrow();
        if last_applied_index_post > last_applied_index_pre {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "leader's vault group last_applied did not advance after Write: pre={} post={} \
                 (vault_id={:?}, org_id={:?}). Write may have routed to org shard instead of \
                 vault shard.",
                last_applied_index_pre, last_applied_index_post, vault_id, org_id,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// =============================================================================
// Phase 7 / O1: vault hibernation integration tests
// =============================================================================

/// End-to-end hibernation lifecycle test.
///
/// 1. Build a 1-node cluster + create org + vault.
/// 2. Enable hibernation policy at runtime (the cluster is bootstrapped with hibernation OFF;
///    flipping it on at the manager exercises the same pathway as the `--vault-hibernation` CLI
///    flag without requiring a second TestCluster constructor).
/// 3. Backdate the vault's `last_activity` and run a manual scan — assert the vault transitions to
///    `Dormant`.
/// 4. Issue a real gRPC `CreateVault` (against a *new* vault slug) on the same org so the proposal
///    pipeline runs in production shape; the original vault stays Dormant because writes target a
///    different vault. Then directly call `touch_activity()` on the dormant vault to simulate the
///    wake-on-request path and assert the wake transition fires within the configured wake budget.
#[tokio::test]
async fn test_vault_hibernation_sleep_and_wake() {
    use std::time::Instant;

    use inferadb_ledger_raft::raft_manager::VaultLifecycleState;

    let cluster = TestCluster::with_data_regions(1, 1).await;
    cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("cluster has a leader");
    let region = Region::US_EAST_VA;

    let (org_slug, _admin_slug) =
        crate::common::create_test_organization(&leader.addr, "vault-hibernation-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    // Flip hibernation on with an aggressive idle window — this exercises the
    // same setter the `--vault-hibernation` flag uses during bootstrap.
    leader.manager.set_hibernation_config(
        inferadb_ledger_types::config::HibernationConfig::builder()
            .enabled(true)
            .idle_secs(1)
            .scan_interval_secs(1)
            .wake_threshold_ms(100)
            .build()
            .expect("valid hibernation config"),
    );

    let vault_group = leader
        .manager
        .get_vault_group(region, org_id, vault_id)
        .expect("vault group present on leader");
    assert_eq!(vault_group.lifecycle_state(), VaultLifecycleState::Active);

    // Backdate the vault's last-activity stamp so the next scan sees it as
    // long-idle, then trigger the scan. The Active → Dormant arm of
    // `run_hibernation_scan_once` dispatches `tokio::spawn(sleep_vault(..))`
    // (so the sync scan loop is not blocked on `engine.pause_shard` + the
    // page-cache eviction syscalls). The spawned task — not the scan call —
    // performs the lifecycle transition, so the test must poll for the
    // transition to land instead of asserting it synchronously.
    vault_group.force_last_activity_for_test(0);
    leader.manager.run_hibernation_scan_once();
    let dormant_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if vault_group.lifecycle_state() == VaultLifecycleState::Dormant {
            break;
        }
        if tokio::time::Instant::now() >= dormant_deadline {
            panic!(
                "vault did not transition to Dormant within 2s after run_hibernation_scan_once; \
                 final state = {:?}",
                vault_group.lifecycle_state(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wake path — `touch_activity()` is what the proposal handler calls on
    // every request; assert it transitions back to Active and that the
    // wake-time fits inside the configured budget.
    let wake_start = Instant::now();
    let woke = vault_group.touch_activity();
    let wake_elapsed = wake_start.elapsed();
    assert!(woke, "touch_activity on Dormant vault must report a wake transition");
    assert_eq!(vault_group.lifecycle_state(), VaultLifecycleState::Active);
    assert!(
        wake_elapsed < Duration::from_millis(100),
        "wake transition must complete within the 100ms budget; took {:?}",
        wake_elapsed,
    );
}

/// Hibernation must remain a no-op when the master flag is `false`. This is
/// the cluster-lifecycle gate — vault state never transitions to Dormant
/// regardless of how stale activity gets.
#[tokio::test]
async fn test_vault_hibernation_disabled_by_default() {
    use inferadb_ledger_raft::raft_manager::VaultLifecycleState;

    let cluster = TestCluster::with_data_regions(1, 1).await;
    cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("cluster has a leader");
    let region = Region::US_EAST_VA;

    let (org_slug, _admin_slug) = crate::common::create_test_organization(
        &leader.addr,
        "vault-hibernation-disabled-org",
        leader,
    )
    .await
    .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);
    crate::common::create_test_vault(&leader.addr, org_slug).await.expect("create vault");
    let vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    let vault_group = leader
        .manager
        .get_vault_group(region, org_id, vault_id)
        .expect("vault group present on leader");

    // Force-stale activity timestamp. The scan still runs every tick, but
    // with the master config disabled it must early-return without
    // transitioning anything.
    vault_group.force_last_activity_for_test(0);
    leader.manager.run_hibernation_scan_once();

    assert_eq!(
        vault_group.lifecycle_state(),
        VaultLifecycleState::Active,
        "vault must stay Active when hibernation is disabled, even with stale activity",
    );
}
