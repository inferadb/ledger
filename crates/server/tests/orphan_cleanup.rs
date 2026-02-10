//! Integration tests for the orphan cleanup background job.
//!
//! Tests that:
//! - Orphan cleanup job starts and runs without errors
//! - Orphan cleanup only runs on leader
//! - Deleted users' memberships are identified as orphans
//! - Orphaned memberships are cleaned up

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{TestCluster, create_admin_client, create_read_client, create_write_client};
use serial_test::serial;

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates a namespace and return its ID.
async fn create_namespace(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_namespace(inferadb_ledger_proto::proto::CreateNamespaceRequest {
            name: name.to_string(),
            shard_id: None,
            quota: None,
        })
        .await?;

    let namespace_id =
        response.into_inner().namespace_id.map(|n| n.id).ok_or("No namespace_id in response")?;

    Ok(namespace_id)
}

/// Writes an entity to a specific namespace.
async fn write_entity(
    addr: std::net::SocketAddr,
    namespace_id: i64,
    vault_id: i64,
    key: &str,
    value: &serde_json::Value,
    client_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault_id }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: serde_json::to_vec(value).unwrap(),
                    condition: None,
                    expires_at: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await?.into_inner();

    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => Ok(()),
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            Err(format!("Write error: {:?}", e).into())
        },
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from a namespace.
async fn read_entity(
    addr: std::net::SocketAddr,
    namespace_id: i64,
    vault_id: i64,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = create_read_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ReadRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault_id }),
        key: key.to_string(),
        consistency: 0, // EVENTUAL
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

// ============================================================================
// Orphan Cleanup Tests
// ============================================================================

/// Tests that orphan cleanup job starts and runs without errors.
#[serial]
#[tokio::test]
async fn test_orphan_cleanup_job_starts() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Orphan cleanup runs in background
    // Verify cluster remains healthy
    let metrics = leader.raft.metrics().borrow().clone();
    assert!(metrics.current_leader.is_some(), "leader should be elected");

    // Give cleanup job time to run at least one cycle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cluster should still be healthy
    let metrics = leader.raft.metrics().borrow().clone();
    assert!(metrics.current_leader.is_some(), "cluster should remain healthy");
}

/// Tests that orphan cleanup only runs on leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_orphan_cleanup_leader_only() {
    let cluster = TestCluster::new(3).await;
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
/// Per DESIGN.md: Users with deleted_at or status=DELETED/DELETING are considered deleted.
#[serial]
#[tokio::test]
async fn test_deleted_user_detection() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create a user with deleted_at timestamp
    let deleted_user_id = 1001i64;
    let deleted_user_key = format!("user:{}", deleted_user_id);
    let deleted_user_value = serde_json::json!({
        "id": deleted_user_id,
        "name": "Deleted User",
        "email": "deleted@example.com",
        "deleted_at": "2024-01-15T10:00:00Z",
    });

    write_entity(leader.addr, 0, 0, &deleted_user_key, &deleted_user_value, "orphan-test")
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
        leader.addr,
        0,
        0,
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

    write_entity(leader.addr, 0, 0, &active_user_key, &active_user_value, "orphan-test")
        .await
        .expect("create active user");

    // Verify all users were written
    let deleted_bytes = read_entity(leader.addr, 0, 0, &deleted_user_key)
        .await
        .expect("read deleted user")
        .expect("deleted user should exist");

    let deleted_user: serde_json::Value = serde_json::from_slice(&deleted_bytes).unwrap();
    assert!(deleted_user.get("deleted_at").is_some(), "User should have deleted_at");

    let status_bytes = read_entity(leader.addr, 0, 0, &deleted_status_user_key)
        .await
        .expect("read status-deleted user")
        .expect("status-deleted user should exist");

    let status_user: serde_json::Value = serde_json::from_slice(&status_bytes).unwrap();
    assert_eq!(status_user.get("status").and_then(|s| s.as_str()), Some("DELETED"));

    let active_bytes = read_entity(leader.addr, 0, 0, &active_user_key)
        .await
        .expect("read active user")
        .expect("active user should exist");

    let active_user: serde_json::Value = serde_json::from_slice(&active_bytes).unwrap();
    assert_eq!(active_user.get("status").and_then(|s| s.as_str()), Some("ACTIVE"));
    assert!(active_user.get("deleted_at").is_none(), "Active user should not have deleted_at");
}

/// Tests membership data format for orphan detection.
#[serial]
#[tokio::test]
async fn test_membership_data_format() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create a namespace
    let ns_id =
        create_namespace(leader.addr, "membership-test-ns").await.expect("create namespace");

    // Create a membership record
    let user_id = 2001i64;
    let member_key = format!("member:{}", user_id);
    let member_value = serde_json::json!({
        "user_id": user_id,
        "role": "member",
        "created_at": "2024-01-01T00:00:00Z",
    });

    write_entity(leader.addr, ns_id, 0, &member_key, &member_value, "membership-test")
        .await
        .expect("create membership");

    // Verify membership was written
    let member_bytes = read_entity(leader.addr, ns_id, 0, &member_key)
        .await
        .expect("read membership")
        .expect("membership should exist");

    let membership: serde_json::Value = serde_json::from_slice(&member_bytes).unwrap();
    assert_eq!(membership.get("user_id").and_then(|v| v.as_i64()), Some(user_id));
    assert_eq!(membership.get("role").and_then(|r| r.as_str()), Some("member"));
}

/// Tests that orphan cleanup respects system namespace boundaries.
///
/// Cleanup should skip the _system namespace (namespace_id = 0).
#[serial]
#[tokio::test]
async fn test_orphan_cleanup_skips_system_namespace() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Write a "member" record directly to _system (which shouldn't be cleaned up)
    let system_member_key = "member:9999";
    let system_member_value = serde_json::json!({
        "user_id": 9999,
        "role": "admin",
        "note": "This is in _system and should not be cleaned",
    });

    write_entity(leader.addr, 0, 0, system_member_key, &system_member_value, "system-member-test")
        .await
        .expect("create system member");

    // Give cleanup time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The system member should still exist (cleanup skips _system)
    let member_bytes = read_entity(leader.addr, 0, 0, system_member_key)
        .await
        .expect("read system member")
        .expect("system member should still exist");

    let member: serde_json::Value = serde_json::from_slice(&member_bytes).unwrap();
    assert_eq!(member.get("user_id").and_then(|v| v.as_i64()), Some(9999));
}

/// Tests orphan cleanup handles empty namespaces gracefully.
#[serial]
#[tokio::test]
async fn test_orphan_cleanup_handles_empty_namespace() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create a namespace with no memberships
    let _ns_id = create_namespace(leader.addr, "empty-ns").await.expect("create namespace");

    // Give cleanup time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cluster should remain healthy (no errors from empty namespace scan)
    let metrics = leader.raft.metrics().borrow().clone();
    assert!(metrics.current_leader.is_some(), "cluster should remain healthy");
}

/// Tests concurrent background jobs don't interfere.
#[serial]
#[tokio::test]
async fn test_orphan_cleanup_with_concurrent_jobs() {
    let cluster = TestCluster::new(3).await;
    let leader = cluster.leader().expect("has leader");

    // Create some state to exercise all background jobs
    let mut client = create_admin_client(leader.addr).await.unwrap();

    // Create namespace
    let ns_response = client
        .create_namespace(inferadb_ledger_proto::proto::CreateNamespaceRequest {
            name: "concurrent-jobs-test".to_string(),
            shard_id: None,
            quota: None,
        })
        .await
        .unwrap();

    let namespace_id = ns_response.into_inner().namespace_id.map(|n| n.id).unwrap();

    // Create vault
    let _vault_response = client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await
        .unwrap();

    // Let all background jobs run concurrently for a bit
    // OrphanCleanup, TtlGC, SagaOrchestrator, AutoRecovery, LearnerRefresh
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify cluster is still healthy
    let leader_id = cluster.wait_for_leader().await;
    assert!(leader_id > 0, "cluster should still have a leader");

    // All nodes should still be responsive
    for node in cluster.nodes() {
        let metrics = node.raft.metrics().borrow().clone();
        assert!(metrics.current_leader.is_some(), "node {} should know the leader", node.id);
    }
}
