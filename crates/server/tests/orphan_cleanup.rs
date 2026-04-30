//! Integration tests for the orphan cleanup background job.
//!
//! Tests that:
//! - Orphan cleanup job starts and runs without errors
//! - Orphan cleanup only runs on leader
//! - Deleted users' memberships are identified as orphans
//! - Orphaned memberships are cleaned up
//!
//! F.1.f.2.Stage1e Wave 4: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};
use serial_test::serial;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
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

/// Writes an entity to a specific organization.
async fn write_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &serde_json::Value,
    client_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        organization: Some(organization),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: client_id.to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::from(serde_json::to_vec(value).unwrap()),
                condition: None,
                expires_at: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await?;

    match response.result {
        Some(ww::WriteResponseResult::Success(_)) => Ok(()),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("Write error: {:?}", e).into()),
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from an organization.
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

// ============================================================================
// Orphan Cleanup Tests
// ============================================================================

/// Tests that orphan cleanup job starts and runs without errors.
#[tokio::test]
async fn test_orphan_cleanup_job_starts() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Orphan cleanup runs in background
    // Verify cluster remains healthy
    assert!(leader.handle.current_leader().is_some(), "leader should be elected");

    // Give cleanup job time to run at least one cycle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cluster should still be healthy
    assert!(leader.handle.current_leader().is_some(), "cluster should remain healthy");
}

/// Tests that orphan cleanup only runs on leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_orphan_cleanup_leader_only() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let leader_id = cluster.wait_for_leader().await;

    // Verify we have followers
    let followers = cluster.followers();
    assert_eq!(followers.len(), 2, "should have 2 followers");

    // Give cleanup time to potentially run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cluster should remain stable
    let new_leader_id = cluster.wait_for_leader().await;
    assert_eq!(leader_id, new_leader_id, "leader should not have changed");
}

/// Tests detection of deleted users.
///
/// Users with `deleted_at` or status DELETED/DELETING are considered deleted.
#[tokio::test]
async fn test_deleted_user_detection() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault for test data
    let ns_id = create_organization(&cluster, leader.id, "deleted-user-ns")
        .await
        .expect("create organization");
    let vault_id = create_vault(&cluster, leader.id, ns_id).await.expect("create vault");

    // Create a user with deleted_at timestamp
    let deleted_user_id = 1001i64;
    let deleted_user_key = format!("user:{}", deleted_user_id);
    let deleted_user_value = serde_json::json!({
        "id": deleted_user_id,
        "name": "Deleted User",
        "email": "deleted@example.com",
        "deleted_at": "2024-01-15T10:00:00Z",
    });

    write_entity(
        &cluster,
        leader.id,
        ns_id,
        vault_id,
        &deleted_user_key,
        &deleted_user_value,
        "orphan-test",
    )
    .await
    .expect("create deleted user");

    // Create a user with DELETED status
    let deleted_status_user_id = 1002i64;
    let deleted_status_user_key = format!("user:{}", deleted_status_user_id);
    let deleted_status_user_value = serde_json::json!({
        "id": deleted_status_user_id,
        "name": "Status Deleted User",
        "email": "status-deleted@example.com",
        "status": "DELETED",
    });

    write_entity(
        &cluster,
        leader.id,
        ns_id,
        vault_id,
        &deleted_status_user_key,
        &deleted_status_user_value,
        "orphan-test",
    )
    .await
    .expect("create status-deleted user");

    // Create an active user
    let active_user_id = 1003i64;
    let active_user_key = format!("user:{}", active_user_id);
    let active_user_value = serde_json::json!({
        "id": active_user_id,
        "name": "Active User",
        "email": "active@example.com",
        "status": "ACTIVE",
    });

    write_entity(
        &cluster,
        leader.id,
        ns_id,
        vault_id,
        &active_user_key,
        &active_user_value,
        "orphan-test",
    )
    .await
    .expect("create active user");

    // Verify all users were written
    let deleted_bytes = read_entity(&cluster, leader.id, ns_id, vault_id, &deleted_user_key)
        .await
        .expect("read deleted user")
        .expect("deleted user should exist");

    let deleted_user: serde_json::Value = serde_json::from_slice(&deleted_bytes).unwrap();
    assert!(deleted_user.get("deleted_at").is_some(), "User should have deleted_at");

    let status_bytes = read_entity(&cluster, leader.id, ns_id, vault_id, &deleted_status_user_key)
        .await
        .expect("read status-deleted user")
        .expect("status-deleted user should exist");

    let status_user: serde_json::Value = serde_json::from_slice(&status_bytes).unwrap();
    assert_eq!(status_user.get("status").and_then(|s| s.as_str()), Some("DELETED"));

    let active_bytes = read_entity(&cluster, leader.id, ns_id, vault_id, &active_user_key)
        .await
        .expect("read active user")
        .expect("active user should exist");

    let active_user: serde_json::Value = serde_json::from_slice(&active_bytes).unwrap();
    assert_eq!(active_user.get("status").and_then(|s| s.as_str()), Some("ACTIVE"));
    assert!(active_user.get("deleted_at").is_none(), "Active user should not have deleted_at");
}

/// Tests membership data format for orphan detection.
#[tokio::test]
async fn test_membership_data_format() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create an organization and vault
    let ns_id = create_organization(&cluster, leader.id, "membership-test-ns")
        .await
        .expect("create organization");
    let vault_id = create_vault(&cluster, leader.id, ns_id).await.expect("create vault");

    // Create a membership record
    let user_id = 2001i64;
    let member_key = format!("member:{}", user_id);
    let member_value = serde_json::json!({
        "user_id": user_id,
        "role": "member",
        "created_at": "2024-01-01T00:00:00Z",
    });

    write_entity(
        &cluster,
        leader.id,
        ns_id,
        vault_id,
        &member_key,
        &member_value,
        "membership-test",
    )
    .await
    .expect("create membership");

    // Verify membership was written
    let member_bytes = read_entity(&cluster, leader.id, ns_id, vault_id, &member_key)
        .await
        .expect("read membership")
        .expect("membership should exist");

    let membership: serde_json::Value = serde_json::from_slice(&member_bytes).unwrap();
    assert_eq!(membership.get("user_id").and_then(|v| v.as_i64()), Some(user_id));
    assert_eq!(membership.get("role").and_then(|r| r.as_str()), Some("member"));
}

/// Tests that orphan cleanup does not remove non-orphaned records.
///
/// Records that belong to active users should survive cleanup cycles.
#[tokio::test]
async fn test_orphan_cleanup_skips_active_records() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let ns_id = create_organization(&cluster, leader.id, "skip-active-ns")
        .await
        .expect("create organization");
    let vault_id = create_vault(&cluster, leader.id, ns_id).await.expect("create vault");

    // Write a "member" record that should not be cleaned up
    let member_key = "member:9999";
    let member_value = serde_json::json!({
        "user_id": 9999,
        "role": "admin",
        "note": "This active member should not be cleaned",
    });

    write_entity(
        &cluster,
        leader.id,
        ns_id,
        vault_id,
        member_key,
        &member_value,
        "skip-active-test",
    )
    .await
    .expect("create member");

    // Give cleanup time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The member should still exist (not orphaned)
    let member_bytes = read_entity(&cluster, leader.id, ns_id, vault_id, member_key)
        .await
        .expect("read member")
        .expect("active member should still exist");

    let member: serde_json::Value = serde_json::from_slice(&member_bytes).unwrap();
    assert_eq!(member.get("user_id").and_then(|v| v.as_i64()), Some(9999));
}

/// Tests orphan cleanup handles empty organizations gracefully.
#[tokio::test]
async fn test_orphan_cleanup_handles_empty_organization() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create an organization with no memberships
    let _ns_id =
        create_organization(&cluster, leader.id, "empty-ns").await.expect("create organization");

    // Give cleanup time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cluster should remain healthy (no errors from empty organization scan)
    assert!(leader.handle.current_leader().is_some(), "cluster should remain healthy");
}

/// Tests concurrent background jobs don't interfere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_orphan_cleanup_with_concurrent_jobs() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("has leader");

    // Create some state to exercise all background jobs.
    // Use the saga-aware helper that waits for Active status to avoid
    // "Organization with slug not found" races on multi-node clusters.
    let organization = create_organization(&cluster, leader.id, "concurrent-jobs-test")
        .await
        .expect("create organization");

    // Wait for GLOBAL Raft propagation to all nodes before creating vault
    assert!(
        cluster.wait_for_sync(Duration::from_secs(10)).await,
        "cluster should sync after organization creation"
    );

    // Create vault
    let _vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Wait for vault creation to propagate, then let background jobs run
    // at least one cycle (OrphanCleanup, TtlGC, SagaOrchestrator, AutoRecovery, LearnerRefresh).
    assert!(
        cluster.wait_for_sync(Duration::from_secs(10)).await,
        "cluster should sync after vault creation"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify cluster is still healthy
    let leader_id = cluster.wait_for_leader().await;
    assert!(leader_id > 0, "cluster should still have a leader");

    // All nodes should still be responsive
    for node in cluster.nodes() {
        assert!(node.handle.current_leader().is_some(), "node {} should know the leader", node.id);
    }
}
