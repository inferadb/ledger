//! Vault lifecycle integration tests (Path A, P2b.2.g).
//!
//! Validates that the per-vault consensus lifecycle composes correctly
//! through the real gRPC + Raft + apply pipeline. The earlier slices
//! (P2b.2.a–P2b.2.f) landed:
//!   * `RaftManager::start_vault_group` / `stop_vault_group`,
//!   * `VaultCreationRequest` / `VaultDeletionRequest` signals from the
//!     `CreateVault` / `DeleteVault` apply arms,
//!   * the watcher task in `start_organization_group` that drives those
//!     signals,
//!   * rehydration of non-deleted vaults on `start_organization_group`
//!     re-invocation,
//!   * per-vault raft.db fan-out in `sync_all_state_dbs`.
//!
//! Unit tests cover each piece in isolation. This file is the
//! end-to-end assertion: a gRPC `CreateVault` RPC against a
//! 3-voter cluster must cause a per-vault `VaultGroup` to register
//! on every voter via the commit-dispatcher → watcher → `start_vault_group`
//! chain.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, VaultId};

use crate::common::{TestCluster, TestNode};

/// Resolves an external `OrganizationSlug` to its internal `OrganizationId`
/// via the GLOBAL applied-state accessor. Panics if the slug is not yet
/// indexed — callers must have awaited organization activation.
fn resolve_org_id(node: &TestNode, slug: OrganizationSlug) -> OrganizationId {
    node.manager
        .system_region()
        .expect("system region running")
        .applied_state()
        .resolve_slug_to_id(slug)
        .expect("organization slug resolves after CreateOrganization commits")
}

/// Polls every node until the cluster agrees on a single registered
/// `(region, org_id, *)` triple, or `timeout` elapses. Returns the
/// `VaultId` every node observes.
///
/// This is the primary vault-live assertion and it intentionally
/// does NOT depend on the GLOBAL vault-slug index — the index write
/// (`SystemRequest::RegisterVaultDirectoryEntry`) is a separate propose
/// from the per-org `CreateVault` and their apply order on followers
/// is not coupled to the vault group start. The test contract —
/// "`CreateVault` brings a vault group live on every voter" — is
/// observable directly on `RaftManager::list_vault_groups`.
async fn wait_for_vault_group_live_on_all_voters(
    cluster: &TestCluster,
    region: Region,
    org_id: OrganizationId,
    timeout: Duration,
) -> VaultId {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Snapshot per-node triples filtered to (region, org_id).
        let per_node: Vec<Vec<VaultId>> = cluster
            .nodes()
            .iter()
            .map(|n| {
                n.manager
                    .list_vault_groups()
                    .into_iter()
                    .filter(|(r, o, _)| *r == region && *o == org_id)
                    .map(|(_, _, v)| v)
                    .collect()
            })
            .collect();

        // Every node must report a single VaultId and every node must
        // agree on the SAME VaultId. That is the "vault group is live
        // on all voters" contract.
        if let Some(first) = per_node.first()
            && first.len() == 1
            && per_node.iter().all(|ids| ids == first)
        {
            return first[0];
        }

        if tokio::time::Instant::now() >= deadline {
            let rendered: Vec<String> = cluster
                .nodes()
                .iter()
                .zip(per_node.iter())
                .map(|(n, ids)| format!("node {}: {:?}", n.id, ids))
                .collect();
            panic!(
                "vault group for (region={region:?}, org_id={org_id:?}) did not converge across \
                 voters within {timeout:?}. per-node state: [{}]",
                rendered.join(" | "),
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Primary test: `CreateVault` must bring a per-vault `VaultGroup` live
/// on every voter in the organization via the real apply pipeline.
///
/// Flow:
///   1. Build a 3-node cluster with 1 data region (`US_EAST_VA`).
///   2. Create an organization via the production gRPC + saga path.
///   3. Create a vault via the production gRPC path (`VaultService::create_vault`).
///   4. Wait for the `CreateVault` → `VaultCreationRequest` → watcher →
///      `start_vault_group` chain to fire on every voter.
///   5. Assert every voter reports the same `(region, org_id, vault_id)`
///      entry in `list_vault_groups()` and `has_vault_group(..) == true`.
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
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(15))
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
    let vaults_for_org: Vec<(Region, OrganizationId, VaultId)> = leader_triples
        .into_iter()
        .filter(|(r, o, _)| *r == region && *o == org_id)
        .collect();
    assert_eq!(
        vaults_for_org.len(),
        1,
        "expected exactly one vault group for (region={region:?}, org_id={org_id:?}); got \
         {vaults_for_org:?}",
    );
}
