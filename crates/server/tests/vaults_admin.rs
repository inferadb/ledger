//! Per-vault admin RPC integration tests.
//!
//! Validates the `AdminListVaults`, `ShowVault`, and `RepairVault` RPCs
//! end-to-end through the real wire-protocol + Raft + apply pipeline.
//! These RPCs back the `ledger vaults {list, show, repair}` CLI subcommands.
//!
//! F.1.f.2.Stage1e Wave 2: migrated from the legacy tonic helpers
//! (`create_test_organization` / `create_test_vault` / `create_admin_client`)
//! to their wire-protocol siblings (`wire_create_test_organization` /
//! `wire_create_test_vault` / `wire_admin_client`). The setup helpers
//! `wait_for_vault_group_live_on_all_voters` and `resolve_org_id` are
//! transport-agnostic infrastructure (they read directly from
//! `RaftManager` / `system_region().applied_state()`), so they work
//! identically on both transports.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::Region;
use inferadb_ledger_wire::services::admin as w;

use crate::common::{
    TestCluster, resolve_org_id, wait_for_vault_group_live_on_all_voters, wire_admin_client,
    wire_create_test_organization, wire_create_test_vault,
};

/// `AdminListVaults` against an organization with three created vaults
/// must return exactly three `VaultInfo` entries — one per vault — with
/// status `"active"` (no hibernation enabled in this test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_list_vaults_returns_one_entry_per_created_vault() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        wire_create_test_organization(&cluster, leader.id, "vaults-admin-list-org")
            .await
            .expect("create organization");

    let org_id = resolve_org_id(leader, org_slug);

    // Create three vaults.
    for _ in 0..3 {
        wire_create_test_vault(&cluster, leader.id, org_slug).await.expect("create vault");
    }

    // Poll until the leader sees all three vault groups. CreateVault
    // proposes through Raft; the per-org watcher fans out to
    // `start_vault_group` async, so the RPC return does not imply the
    // vault group is registered on this node yet.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let count = leader
            .manager
            .list_vault_groups()
            .into_iter()
            .filter(|(r, o, _)| *r == region && *o == org_id)
            .count();
        if count >= 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("leader did not register 3 vault groups within 15s; saw {count}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Hit `AdminListVaults` against the leader.
    let admin = wire_admin_client(&cluster, leader.id);
    let resp = admin
        .admin_list_vaults(
            w::AdminListVaultsRequest { organization: Some(org_slug) },
            // request_id =
            leader.id as u128,
        )
        .await
        .expect("admin_list_vaults RPC");

    assert_eq!(
        resp.vaults.len(),
        3,
        "expected 3 vault info entries, got {}: {:?}",
        resp.vaults.len(),
        resp.vaults,
    );

    for v in &resp.vaults {
        assert_eq!(v.status, "active", "vault {:?} should be active, got {:?}", v.slug, v.status);
        assert!(v.voter_count > 0, "vault {:?} has zero voters: {:?}", v.slug, v);
        assert!(v.slug.is_some_and(|s| s.value() != 0), "vault {:?} has no slug", v.slug);
    }
}

/// `ShowVault` against a freshly-created vault must return the vault's
/// internal IDs, voter set, lifecycle state, and apply progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_vault_returns_membership_and_lifecycle() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        wire_create_test_organization(&cluster, leader.id, "vaults-admin-show-org")
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    let vault_slug =
        wire_create_test_vault(&cluster, leader.id, org_slug).await.expect("create vault");

    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    let admin = wire_admin_client(&cluster, leader.id);
    let resp = admin
        .show_vault(
            w::ShowVaultRequest { organization: Some(org_slug), vault: Some(vault_slug) },
            // request_id =
            leader.id as u128,
        )
        .await
        .expect("show_vault RPC");

    let info = resp.info.as_ref().expect("info populated");
    assert_eq!(info.status, "active");
    assert_eq!(info.slug.map(|s| s.value()), Some(vault_slug.value()));
    assert_eq!(resp.region, region.as_str());
    assert!(resp.organization_id > 0, "organization_id should be non-zero");
    assert!(resp.vault_id > 0, "vault_id should be non-zero");
    assert!(!resp.voters.is_empty(), "vault must have at least one voter");
    assert_eq!(resp.lifecycle_state, "active");
}

/// `RepairVault` against a healthy vault must return `status = "noop"`
/// (the placeholder behaviour pending consensus-engine repair APIs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_vault_returns_noop_for_healthy_vault() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        wire_create_test_organization(&cluster, leader.id, "vaults-admin-repair-org")
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    let vault_slug =
        wire_create_test_vault(&cluster, leader.id, org_slug).await.expect("create vault");

    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    let admin = wire_admin_client(&cluster, leader.id);
    let resp = admin
        .repair_vault(
            w::RepairVaultRequest { organization: Some(org_slug), vault: Some(vault_slug) },
            // request_id =
            leader.id as u128,
        )
        .await
        .expect("repair_vault RPC");

    assert_eq!(resp.status, "noop", "expected noop, got: {} ({})", resp.status, resp.message);
    assert!(!resp.message.is_empty(), "repair message should explain the noop reason");
}

/// `ShowVault` against a non-existent vault must return a `NotFound`
/// `WireError`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_vault_returns_not_found_for_unknown_vault() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        wire_create_test_organization(&cluster, leader.id, "vaults-admin-notfound-org")
            .await
            .expect("create organization");

    let admin = wire_admin_client(&cluster, leader.id);

    // Use a clearly-unregistered vault slug.
    let bogus_vault_slug = inferadb_ledger_types::VaultSlug::new(0xDEAD_BEEF_DEAD_BEEF);

    let err = admin
        .show_vault(
            w::ShowVaultRequest { organization: Some(org_slug), vault: Some(bogus_vault_slug) },
            // request_id =
            leader.id as u128,
        )
        .await
        .expect_err("show_vault against unknown vault should fail");

    match err {
        inferadb_ledger_wire_transport::RpcError::WireError(wire_err) => {
            assert_eq!(
                wire_err.code,
                inferadb_ledger_wire::error::ErrorCode::NotFound,
                "expected NotFound, got {:?}: {}",
                wire_err.code,
                wire_err.message,
            );
        },
        other => panic!("expected WireError(NotFound), got: {other:?}"),
    }
}
