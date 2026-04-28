//! Per-vault admin RPC integration tests (Phase 7 / O4).
//!
//! Validates the `AdminListVaults`, `ShowVault`, and `RepairVault` RPCs
//! end-to-end through the real gRPC + Raft + apply pipeline. These RPCs
//! back the `ledger vaults {list, show, repair}` CLI subcommands.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::Region;

use crate::common::{
    TestCluster, create_admin_client, create_test_organization, create_test_vault, resolve_org_id,
    wait_for_vault_group_live_on_all_voters,
};

/// `AdminListVaults` against an organization with three created vaults
/// must return exactly three `VaultInfo` entries — one per vault — with
/// status `"active"` (no hibernation enabled in this test).
#[tokio::test]
async fn admin_list_vaults_returns_one_entry_per_created_vault() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "vaults-admin-list-org", leader)
            .await
            .expect("create organization");

    let org_id = resolve_org_id(leader, org_slug);

    // Create three vaults.
    for _ in 0..3 {
        create_test_vault(&leader.addr, org_slug).await.expect("create vault");
    }

    // Poll until the leader sees all three vault groups. CreateVault
    // proposes through Raft; the per-org watcher fans out to
    // `start_vault_group` async, so the gRPC return does not imply the
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
    let mut admin = create_admin_client(&leader.addr).await.expect("admin client");
    let resp = admin
        .admin_list_vaults(proto::AdminListVaultsRequest {
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
        })
        .await
        .expect("admin_list_vaults RPC")
        .into_inner();

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
        assert!(v.slug.as_ref().is_some_and(|s| s.slug != 0), "vault {:?} has no slug", v.slug,);
    }
}

/// `ShowVault` against a freshly-created vault must return the vault's
/// internal IDs, voter set, lifecycle state, and apply progress.
#[tokio::test]
async fn show_vault_returns_membership_and_lifecycle() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "vaults-admin-show-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    let vault_slug = create_test_vault(&leader.addr, org_slug).await.expect("create vault");

    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(15))
            .await;

    let mut admin = create_admin_client(&leader.addr).await.expect("admin client");
    let resp = admin
        .show_vault(proto::ShowVaultRequest {
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            vault: Some(proto::VaultSlug { slug: vault_slug.value() }),
        })
        .await
        .expect("show_vault RPC")
        .into_inner();

    let info = resp.info.as_ref().expect("info populated");
    assert_eq!(info.status, "active");
    assert_eq!(info.slug.as_ref().map(|s| s.slug), Some(vault_slug.value()));
    assert_eq!(resp.region, region.as_str());
    assert!(resp.organization_id > 0, "organization_id should be non-zero");
    assert!(resp.vault_id > 0, "vault_id should be non-zero");
    assert!(!resp.voters.is_empty(), "vault must have at least one voter");
    assert_eq!(resp.lifecycle_state, "active");
}

/// `RepairVault` against a healthy vault must return `status = "noop"`
/// (the placeholder behaviour pending consensus-engine repair APIs).
#[tokio::test]
async fn repair_vault_returns_noop_for_healthy_vault() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let region = Region::US_EAST_VA;
    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "vaults-admin-repair-org", leader)
            .await
            .expect("create organization");
    let org_id = resolve_org_id(leader, org_slug);

    let vault_slug = create_test_vault(&leader.addr, org_slug).await.expect("create vault");

    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(15))
            .await;

    let mut admin = create_admin_client(&leader.addr).await.expect("admin client");
    let resp = admin
        .repair_vault(proto::RepairVaultRequest {
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            vault: Some(proto::VaultSlug { slug: vault_slug.value() }),
        })
        .await
        .expect("repair_vault RPC")
        .into_inner();

    assert_eq!(resp.status, "noop", "expected noop, got: {} ({})", resp.status, resp.message);
    assert!(!resp.message.is_empty(), "repair message should explain the noop reason");
}

/// `ShowVault` against a non-existent vault must return `NOT_FOUND`.
#[tokio::test]
async fn show_vault_returns_not_found_for_unknown_vault() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("cluster has a leader");

    let (org_slug, _admin_slug) =
        create_test_organization(&leader.addr, "vaults-admin-notfound-org", leader)
            .await
            .expect("create organization");

    let mut admin = create_admin_client(&leader.addr).await.expect("admin client");

    // Use a clearly-unregistered vault slug.
    let bogus_vault_slug: u64 = 0xDEAD_BEEF_DEAD_BEEF;

    let err = admin
        .show_vault(proto::ShowVaultRequest {
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            vault: Some(proto::VaultSlug { slug: bogus_vault_slug }),
        })
        .await
        .expect_err("show_vault against unknown vault should fail");

    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "expected NotFound, got {:?}: {}",
        err.code(),
        err.message(),
    );
}
