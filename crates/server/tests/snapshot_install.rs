//! End-to-end snapshot install tests (Stage 4 of the snapshot install path).
//!
//! Closes the loop opened by Stages 1–3:
//!
//! - Stage 1b: bifurcated builder + install paths (`SnapshotScope::{Org, Vault}`).
//! - Stage 2: [`SnapshotPersister`](inferadb_ledger_raft::snapshot_persister::SnapshotPersister) +
//!   AES-256-GCM envelope + retention.
//! - Stage 3: streaming RPC (`InstallSnapshotStream`) + leader-side `SnapshotSender`.
//! - Stage 4: receiver-side `SnapshotInstaller` — staged file → decrypt → apply-command →
//!   `RaftLogStore::install_snapshot` → engine notify → prune.
//!
//! These tests exercise the full chain `persist_snapshot` → reactor → streamer
//! → receiver → installer for both `Org` and `Vault` scopes against a real
//! multi-node cluster. The reactor's `Action::SendSnapshot` /
//! `Action::InstallSnapshot` emissions are simulated via the `*_for_test`
//! helpers on `RaftManager` — same dispatch surface the reactor uses, just
//! invoked synchronously by the test instead of by a long-running Raft
//! protocol scenario (which would require pinning down log compaction +
//! deliberate follower-lag induction).
//!
//! What's covered:
//!   * Org-scope: `persist_snapshot` on the leader, stream to a follower, install on the follower,
//!     observe the staged file land + the install prune it.
//!   * Vault-scope: same, but for a per-vault `VaultGroup`. Validates the per-vault apply-command
//!     channel + commit-pump install arm wired in this dispatch.
//!
//! Failure modes (`StagedNotFound`, `Decrypt`, scope-mismatch, etc.) are
//! exercised by the unit tests in `crates/raft/src/snapshot_installer.rs` and
//! `crates/raft/src/snapshot_receiver.rs` — they don't require a full
//! cluster.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_consensus::types::NodeId as ConsensusNodeId;
use inferadb_ledger_raft::snapshot::SnapshotScope;
use inferadb_ledger_types::Region;

use crate::common::{
    TestCluster, create_test_organization, create_test_vault, resolve_org_id,
    wait_for_org_vault_group_live,
};

/// Polls until the persister reports a staged file at `index` for the given
/// scope, or panics on timeout.
async fn wait_for_staged_file(
    manager: &std::sync::Arc<inferadb_ledger_raft::RaftManager>,
    region: Region,
    scope: SnapshotScope,
    index: u64,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let staged = manager
            .snapshot_persister()
            .list_staged(region, scope)
            .await
            .expect("list_staged must succeed");
        if staged.iter().any(|m| m.index == index) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "staged file at index {index} for scope {scope:?} did not land within \
                 {timeout:?}; staged = {staged:?}",
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until no staged file remains at `index` for the given scope —
/// the install pruning path.
async fn wait_for_staged_file_removed(
    manager: &std::sync::Arc<inferadb_ledger_raft::RaftManager>,
    region: Region,
    scope: SnapshotScope,
    index: u64,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let staged = manager
            .snapshot_persister()
            .list_staged(region, scope)
            .await
            .expect("list_staged must succeed");
        if !staged.iter().any(|m| m.index == index) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "staged file at index {index} for scope {scope:?} was not pruned within \
                 {timeout:?}; staged = {staged:?}",
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Org-scope end-to-end install.
///
/// 1. Build a 3-node TCP cluster with one data region.
/// 2. Create an organization (drives commits through the org's apply pipeline).
/// 3. Persist an org-scope snapshot on the leader.
/// 4. Drive `send_snapshot_for_test` from the leader to a follower — exercises Stage 3 streaming.
/// 5. Wait for the staged file to land on the follower.
/// 6. Drive `install_snapshot_for_test` on the follower — exercises Stage 4: staged → decrypt →
///    apply-command channel → `RaftLogStore::install_snapshot` (org variant) → engine notify →
///    prune.
/// 7. Assert: the staged file is pruned (success path) and the manager hasn't panicked.
#[tokio::test]
async fn snapshot_install_end_to_end_org_scope() {
    let cluster = TestCluster::with_tcp_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    // Drive enough commits through the org's apply pipeline to populate
    // applied state. `create_test_organization` runs the full saga
    // (CreateUser + RegisterEmailHash + CreateOrganization + activation),
    // which lands several committed entries in the system + region group's
    // raft.db.
    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "snapshot-install-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    // Wait for the per-organization group to spin up on every voter.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if cluster.nodes().iter().all(|n| n.manager.has_organization_group(region, org_id)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for node in cluster.nodes() {
        assert!(
            node.manager.has_organization_group(region, org_id),
            "per-organization group missing on node {} after timeout",
            node.id,
        );
    }

    let leader_org = leader
        .manager
        .get_organization_group(region, org_id)
        .expect("leader must have the per-organization group");
    let org_shard_id = leader_org.handle().shard_id();
    let org_term = leader_org.handle().shard_state().term;

    // Persist an org-scope snapshot on the leader.
    let persisted = leader
        .manager
        .persist_snapshot(region, org_id, None, org_shard_id, org_term)
        .await
        .expect("persist_snapshot org scope");
    assert_eq!(persisted.scope, SnapshotScope::Org { organization_id: org_id });
    assert!(
        persisted.size_bytes > 0,
        "persisted snapshot must be non-empty (got {} bytes)",
        persisted.size_bytes
    );

    // Pick a follower whose vote we'll install onto.
    let followers = cluster.followers();
    assert!(!followers.is_empty(), "expected at least one follower");
    let follower = followers[0];

    // Stage 3: stream the snapshot from the leader to the follower.
    leader.manager.send_snapshot_for_test(
        org_shard_id,
        ConsensusNodeId(follower.id),
        persisted.index,
    );

    // Wait for the receiver-side staging file to land. The streaming RPC is
    // asynchronous (`tokio::spawn`'d); polling `list_staged` covers the
    // race the production install path also handles via the
    // `DEFAULT_STAGED_WAIT` poll loop.
    wait_for_staged_file(
        &follower.manager,
        region,
        SnapshotScope::Org { organization_id: org_id },
        persisted.index,
        Duration::from_secs(10),
    )
    .await;

    // Stage 4: drive the install on the follower.
    follower.manager.install_snapshot_for_test(
        org_shard_id,
        org_term,
        persisted.index,
        persisted.term.unwrap_or(org_term),
    );

    // The install path prunes the staged file on success — this is the
    // observable signal that the full chain landed (decrypt + apply-command
    // dispatch + `RaftLogStore::install_snapshot` + engine notify + prune).
    wait_for_staged_file_removed(
        &follower.manager,
        region,
        SnapshotScope::Org { organization_id: org_id },
        persisted.index,
        Duration::from_secs(15),
    )
    .await;
}

/// Vault-scope end-to-end install.
///
/// 1. Build a 3-node TCP cluster with one data region.
/// 2. Create an organization + a vault.
/// 3. Persist a vault-scope snapshot on the leader.
/// 4. Stream + install on a follower.
/// 5. Assert: the staged file is pruned (success path).
///
/// This is the first integration assertion that the per-vault install
/// path — wired in Stage 4b — drives `RaftLogStore::install_snapshot`
/// through the per-vault commit pump's apply-command arm. Before this
/// dispatch, the per-vault install errored with
/// `InstallError::VaultPathNotWired`; the install would never prune the
/// staged file, so this test would hang in `wait_for_staged_file_removed`.
#[tokio::test]
async fn snapshot_install_end_to_end_vault_scope() {
    let cluster = TestCluster::with_tcp_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "snapshot-install-vault", leader)
            .await
            .expect("create organization");
    create_test_vault(&leader.addr, org_slug).await.expect("create vault");

    let (region, vault_id) =
        wait_for_org_vault_group_live(&cluster, org_slug, Duration::from_secs(15)).await;
    let org_id = resolve_org_id(leader, org_slug);

    // Sanity: every voter has the vault group; pick a follower for the
    // install side.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_vault_group(region, org_id, vault_id),
            "vault group missing on node {} for ({region:?}, {org_id:?}, {vault_id:?})",
            node.id,
        );
    }

    let leader_vault = leader
        .manager
        .get_vault_group(region, org_id, vault_id)
        .expect("leader must have the vault group");
    let vault_shard_id = leader_vault.shard_id();
    let vault_term = leader_vault.handle().shard_state().term;

    let persisted = leader
        .manager
        .persist_snapshot(region, org_id, Some(vault_id), vault_shard_id, vault_term)
        .await
        .expect("persist_snapshot vault scope");
    assert_eq!(persisted.scope, SnapshotScope::Vault { organization_id: org_id, vault_id });
    assert!(
        persisted.size_bytes > 0,
        "persisted vault snapshot must be non-empty (got {} bytes)",
        persisted.size_bytes
    );

    let followers = cluster.followers();
    assert!(!followers.is_empty(), "expected at least one follower");
    let follower = followers[0];

    leader.manager.send_snapshot_for_test(
        vault_shard_id,
        ConsensusNodeId(follower.id),
        persisted.index,
    );

    let scope = SnapshotScope::Vault { organization_id: org_id, vault_id };
    wait_for_staged_file(
        &follower.manager,
        region,
        scope,
        persisted.index,
        Duration::from_secs(10),
    )
    .await;

    follower.manager.install_snapshot_for_test(
        vault_shard_id,
        vault_term,
        persisted.index,
        persisted.term.unwrap_or(vault_term),
    );

    wait_for_staged_file_removed(
        &follower.manager,
        region,
        scope,
        persisted.index,
        Duration::from_secs(15),
    )
    .await;
}
