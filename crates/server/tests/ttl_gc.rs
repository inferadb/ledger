//! TTL garbage collection integration tests.
//!
//! Tests that expired entities are properly cleaned up by the GC.
//!
//! - Expired entities remain in state until garbage collection
//! - GC runs only on leader
//! - Uses ExpireEntity operation (distinct from DeleteEntity)
//! - Actor recorded as "system:gc" for audit trail
//!
//! F.1.f.2.Stage1e Wave 5: migrated from the legacy tonic helpers
//! (`create_admin_client` / `create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their wire-protocol
//! siblings (`wire_admin_client` / `wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::services::{admin as wa, read as wr, shared as ws, write as ww};

use crate::common::{
    TestCluster, wire_admin_client, wire_create_test_organization, wire_create_test_vault,
    wire_read_client, wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    cluster: &TestCluster,
    node_id: u64,
    name: &str,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = wire_create_test_organization(cluster, node_id, name).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    wire_create_test_vault(cluster, node_id, organization).await
}

/// Writes an entity with optional TTL.
async fn write_entity_with_ttl(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
    expires_at: Option<u64>,
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        organization: Some(organization),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: client_id.to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                condition: None,
                expires_at,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await?;

    match response.result {
        Some(ww::WriteResponseResult::Success(s)) => Ok(s.block_height),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("Write error: {:?}", e).into()),
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from a vault.
async fn read_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let client = wire_read_client(cluster, node_id);

    let request = wr::ReadRequest {
        organization: Some(organization),
        vault: Some(vault),
        key: key.to_string(),
        consistency: ws::ReadConsistency::Eventual,
        caller: None,
    };

    let response = client.read(request, rand::random::<u128>()).await?;
    Ok(response.value.map(|b| b.to_vec()))
}

/// Force a GC cycle via admin RPC.
async fn force_gc(
    cluster: &TestCluster,
    node_id: u64,
    organization: Option<OrganizationSlug>,
    vault: Option<VaultSlug>,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let client = wire_admin_client(cluster, node_id);

    let request = wa::ForceGcRequest { organization, vault };

    let response = client.force_gc(request, rand::random::<u128>()).await?;

    if response.success {
        Ok((response.expired_count, response.vaults_scanned))
    } else {
        Err(format!("GC failed: {}", response.message).into())
    }
}

// ============================================================================
// TTL GC Tests
// ============================================================================

/// Tests that ForceGc removes expired entities.
///
/// Expired entities remain in state until garbage collection.
#[tokio::test]
async fn test_force_gc_removes_expired_entities() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let org =
        create_organization(&cluster, leader.id, "ttl-gc-test").await.expect("create organization");
    let vault = create_vault(&cluster, leader.id, org).await.expect("create vault");

    // Get current time
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    // Write an entity that expired 1 hour ago
    let expired_at = now - 3600;
    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault,
        "expired-key",
        b"expired-value",
        Some(expired_at),
        "gc-test-client",
    )
    .await
    .expect("write expired entity");

    // Write a non-expiring entity
    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault,
        "permanent-key",
        b"permanent-value",
        None,
        "gc-test-client",
    )
    .await
    .expect("write permanent entity");

    // Write an entity that expires in the future
    let future_expires = now + 3600;
    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault,
        "future-key",
        b"future-value",
        Some(future_expires),
        "gc-test-client",
    )
    .await
    .expect("write future entity");

    // Before GC: All entities should be readable (expired entities are lazy-filtered)
    // Note: Read filters out expired entities by default, but they're still in state
    let _expired_before = read_entity(&cluster, leader.id, org, vault, "expired-key")
        .await
        .expect("read expired key");
    // Read may return None for expired entities due to lazy filtering
    // The key here is that the entity is still IN state storage

    let permanent_before = read_entity(&cluster, leader.id, org, vault, "permanent-key")
        .await
        .expect("read permanent key");
    assert!(permanent_before.is_some(), "permanent entity should be readable before GC");

    let future_before =
        read_entity(&cluster, leader.id, org, vault, "future-key").await.expect("read future key");
    assert!(future_before.is_some(), "future entity should be readable before GC");

    // Run GC
    let (expired_count, vaults_scanned) =
        force_gc(&cluster, leader.id, Some(org), Some(vault)).await.expect("force gc");

    assert_eq!(expired_count, 1, "should have expired 1 entity");
    assert_eq!(vaults_scanned, 1, "should have scanned 1 vault");

    // After GC: Permanent and future entities should still be readable
    let permanent_after = read_entity(&cluster, leader.id, org, vault, "permanent-key")
        .await
        .expect("read permanent key after gc");
    assert!(permanent_after.is_some(), "permanent entity should still exist after GC");

    let future_after = read_entity(&cluster, leader.id, org, vault, "future-key")
        .await
        .expect("read future key after gc");
    assert!(future_after.is_some(), "future entity should still exist after GC");
}

/// Tests that GC on empty vault succeeds.
#[tokio::test]
async fn test_force_gc_empty_vault() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let org = create_organization(&cluster, leader.id, "empty-gc-test")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, org).await.expect("create vault");

    // Run GC on empty vault
    let (expired_count, vaults_scanned) = force_gc(&cluster, leader.id, Some(org), Some(vault))
        .await
        .expect("force gc on empty vault");

    assert_eq!(expired_count, 0, "should have no expired entities");
    assert_eq!(vaults_scanned, 1, "should have scanned 1 vault");
}

/// Tests that GC runs on all vaults when no filter specified.
#[tokio::test]
async fn test_force_gc_all_vaults() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and multiple vaults
    let org = create_organization(&cluster, leader.id, "multi-vault-gc-test")
        .await
        .expect("create organization");
    let vault1 = create_vault(&cluster, leader.id, org).await.expect("create vault 1");
    let vault2 = create_vault(&cluster, leader.id, org).await.expect("create vault 2");

    // Write expired entities to both vaults
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let expired_at = now - 3600;

    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault1,
        "expired-1",
        b"value-1",
        Some(expired_at),
        "gc-multi-client",
    )
    .await
    .expect("write to vault 1");

    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault2,
        "expired-2",
        b"value-2",
        Some(expired_at),
        "gc-multi-client",
    )
    .await
    .expect("write to vault 2");

    // Run GC on all vaults (no filter)
    let (expired_count, vaults_scanned) =
        force_gc(&cluster, leader.id, None, None).await.expect("force gc all vaults");

    assert_eq!(expired_count, 2, "should have expired 2 entities");
    assert!(vaults_scanned >= 2, "should have scanned at least 2 vaults");
}

/// Tests that multiple entities with different TTLs are handled correctly.
///
/// Only entities whose TTL has passed should be collected; entities with
/// future TTLs should remain in storage.
#[tokio::test]
async fn test_force_gc_multiple_ttl_timings() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org = create_organization(&cluster, leader.id, "multi-ttl-test")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, org).await.expect("create vault");

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    // Write 5 entities: 3 expired at different times in the past, 2 with future TTL
    let expired_1h = now - 3600;
    let expired_1m = now - 60;
    let expired_1s = now - 1;
    let future_1h = now + 3600;
    let future_1d = now + 86400;

    for (key, ttl) in [
        ("expired-1h", Some(expired_1h)),
        ("expired-1m", Some(expired_1m)),
        ("expired-1s", Some(expired_1s)),
        ("future-1h", Some(future_1h)),
        ("future-1d", Some(future_1d)),
    ] {
        write_entity_with_ttl(&cluster, leader.id, org, vault, key, b"data", ttl, "ttl-client")
            .await
            .expect("write entity");
    }

    // Run GC
    let (expired_count, vaults_scanned) =
        force_gc(&cluster, leader.id, Some(org), Some(vault)).await.expect("force gc");

    assert_eq!(expired_count, 3, "should expire exactly 3 entities");
    assert_eq!(vaults_scanned, 1, "should scan 1 vault");

    // Verify future entities still exist
    let future_1h_val =
        read_entity(&cluster, leader.id, org, vault, "future-1h").await.expect("read future-1h");
    assert!(future_1h_val.is_some(), "future-1h entity should still exist");

    let future_1d_val =
        read_entity(&cluster, leader.id, org, vault, "future-1d").await.expect("read future-1d");
    assert!(future_1d_val.is_some(), "future-1d entity should still exist");
}

/// Tests that GC is idempotent — running twice produces zero expirations the second time.
///
/// After all expired entities are collected, subsequent GC runs should find
/// nothing to expire.
#[tokio::test]
async fn test_force_gc_idempotent() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org = create_organization(&cluster, leader.id, "idempotent-gc-test")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, org).await.expect("create vault");

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    // Write expired entity
    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault,
        "expired-once",
        b"value",
        Some(now - 3600),
        "gc-idem-client",
    )
    .await
    .expect("write expired");

    // First GC run
    let (count1, _) =
        force_gc(&cluster, leader.id, Some(org), Some(vault)).await.expect("first gc");
    assert_eq!(count1, 1, "first GC should expire 1 entity");

    // Second GC run — nothing to expire
    let (count2, scanned2) =
        force_gc(&cluster, leader.id, Some(org), Some(vault)).await.expect("second gc");
    assert_eq!(count2, 0, "second GC should expire 0 entities");
    assert_eq!(scanned2, 1, "should still scan the vault");
}

/// Tests that expired entities are filtered from read responses.
///
/// Expired entities remain in state until GC runs, but reads
/// should filter them out based on current time vs expires_at.
#[tokio::test]
async fn test_expired_entity_not_returned_by_read() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org = create_organization(&cluster, leader.id, "read-filter-test")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, org).await.expect("create vault");

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    // Write entity that is already expired (1 hour ago)
    write_entity_with_ttl(
        &cluster,
        leader.id,
        org,
        vault,
        "already-expired",
        b"ghost",
        Some(now - 3600),
        "read-filter-client",
    )
    .await
    .expect("write expired entity");

    // Read should NOT return the expired entity (lazy filtering)
    let result = read_entity(&cluster, leader.id, org, vault, "already-expired")
        .await
        .expect("read expired entity");

    assert!(result.is_none(), "expired entity should not be returned by read (lazy TTL filtering)");
}

/// Tests that GC fails on follower node.
///
/// Only the leader can run garbage collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_force_gc_fails_on_follower() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    // Find a follower
    let followers = cluster.followers();
    assert!(!followers.is_empty(), "should have followers");
    let follower = &followers[0];

    // Try to run GC on follower
    let result = force_gc(&cluster, follower.id, None, None).await;

    assert!(result.is_err(), "GC should fail on follower: {:?}", result);

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("leader") || err.contains("precondition"),
        "Error should mention leader requirement: {}",
        err
    );
}
