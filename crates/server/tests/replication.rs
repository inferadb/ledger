//! Replication integration tests.
//!
//! Tests that writes replicate correctly across cluster nodes.
//!
//! F.1.f.2.Stage1e Wave 3: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};
use serial_test::serial;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Helper: read an entity value (used by isolation assertions).
// ============================================================================

async fn read_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Option<Vec<u8>> {
    let client = wire_read_client(cluster, node_id);
    let response = client
        .read(
            wr::ReadRequest {
                organization: Some(organization),
                vault: Some(vault),
                key: key.to_string(),
                consistency: ws::ReadConsistency::Unspecified,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .ok()?;
    response.value.map(|b| b.to_vec())
}

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

// ============================================================================
// Replication Tests
// ============================================================================

/// Tests that multiple writes replicate in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_ordered_replication() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives writes, beta stays empty.
    let org_alpha = create_organization(&cluster, leader.id, "ordered-repl-ns-a")
        .await
        .expect("create org alpha");
    let vault_alpha =
        create_vault(&cluster, leader.id, org_alpha).await.expect("create vault alpha");
    let org_beta = create_organization(&cluster, leader.id, "ordered-repl-ns-b")
        .await
        .expect("create org beta");
    let vault_beta = create_vault(&cluster, leader.id, org_beta).await.expect("create vault beta");

    let client = wire_write_client(&cluster, leader.id);

    // Submit multiple writes (alpha only)
    for i in 0..5u64 {
        let request = ww::WriteRequest {
            client_id: Some(ws::ClientIdMessage { id: "ordered-test".to_string() }),
            idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            organization: Some(org_alpha),
            vault: Some(vault_alpha),
            operations: vec![ws::Operation {
                op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                    key: format!("key-{}", i),
                    value: Bytes::from(format!("value-{}", i).into_bytes()),
                    expires_at: None,
                    condition: None,
                })),
            }],
            include_tx_proof: false,
            caller: None,
        };

        let response =
            client.write(request, rand::random::<u128>()).await.expect("write should succeed");
        match response.result {
            Some(ww::WriteResponseResult::Success(_)) => {},
            _ => panic!("write {} should succeed", i),
        }
    }

    // Wait for replication
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "all nodes should sync");
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;

    // All nodes should have same last applied
    let leader_applied = leader.last_applied();
    assert!(leader_applied >= 5, "leader should have applied at least 5 entries");

    for follower in cluster.followers() {
        assert_eq!(
            follower.last_applied(),
            leader_applied,
            "follower {} should match leader",
            follower.id
        );
    }

    // Isolation: beta must not see alpha's writes on any node.
    for node in cluster.nodes() {
        let value = read_entity(&cluster, node.id, org_beta, vault_beta, "key-0").await;
        assert_eq!(value, None, "node {} beta must not see alpha's key-0", node.id);
    }
}

/// Tests that followers have consistent state after writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_follower_state_consistency() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives the batch, beta stays empty.
    let org_alpha = create_organization(&cluster, leader.id, "follower-consistency-ns-a")
        .await
        .expect("create org alpha");
    let vault_alpha =
        create_vault(&cluster, leader.id, org_alpha).await.expect("create vault alpha");
    let org_beta = create_organization(&cluster, leader.id, "follower-consistency-ns-b")
        .await
        .expect("create org beta");
    let vault_beta = create_vault(&cluster, leader.id, org_beta).await.expect("create vault beta");

    let client = wire_write_client(&cluster, leader.id);

    // Submit per-vault writes sequentially (alpha only). A5 migrated this
    // from `BatchWrite` to per-op `Write` after Phase 6 of the per-vault
    // consensus migration deprecated cross-vault batches.
    let mut last_height: u64 = 0;
    for i in 0..10 {
        let request = ww::WriteRequest {
            client_id: Some(ws::ClientIdMessage { id: "batch-test".to_string() }),
            idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            organization: Some(org_alpha),
            vault: Some(vault_alpha),
            operations: vec![ws::Operation {
                op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                    key: format!("batch-key-{}", i),
                    value: Bytes::from(format!("batch-value-{}", i).into_bytes()),
                    expires_at: None,
                    condition: None,
                })),
            }],
            include_tx_proof: false,
            caller: None,
        };
        let response =
            client.write(request, rand::random::<u128>()).await.expect("write should succeed");
        match response.result {
            Some(ww::WriteResponseResult::Success(s)) => {
                assert!(
                    s.block_height > last_height,
                    "block height must be monotonically increasing: got {} after {}",
                    s.block_height,
                    last_height
                );
                last_height = s.block_height;
            },
            _ => panic!("write should succeed"),
        }
    }

    // Wait for sync
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "all nodes should sync after batch write");
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;

    // Verify all nodes are on same term
    let leader_term = leader.current_term();
    for follower in cluster.followers() {
        assert_eq!(
            follower.current_term(),
            leader_term,
            "follower {} should be on same term as leader",
            follower.id
        );
    }

    // Isolation: beta must not see alpha's batch writes on any node.
    for node in cluster.nodes() {
        let value = read_entity(&cluster, node.id, org_beta, vault_beta, "batch-key-0").await;
        assert_eq!(value, None, "node {} beta must not see alpha's batch-key-0", node.id);
    }
}

/// Tests that replication succeeds when writes are spaced apart in time.
///
/// Verifies that the replication pipeline handles non-contiguous writes
/// (with idle time between them) without losing data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_replication_with_idle_gap_between_writes() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives writes, beta stays empty.
    let org_alpha = create_organization(&cluster, leader.id, "delay-repl-ns-a")
        .await
        .expect("create org alpha");
    let vault_alpha =
        create_vault(&cluster, leader.id, org_alpha).await.expect("create vault alpha");
    let org_beta =
        create_organization(&cluster, leader.id, "delay-repl-ns-b").await.expect("create org beta");
    let vault_beta = create_vault(&cluster, leader.id, org_beta).await.expect("create vault beta");

    let client = wire_write_client(&cluster, leader.id);

    // Write some data (alpha only)
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "delay-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(org_alpha),
        vault: Some(vault_alpha),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "delay-key".to_string(),
                value: Bytes::from_static(b"delay-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    // Small delay to simulate network latency
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write more data (alpha only)
    let request2 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "delay-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(org_alpha),
        vault: Some(vault_alpha),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "delay-key-2".to_string(),
                value: Bytes::from_static(b"delay-value-2"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    client.write(request2, rand::random::<u128>()).await.expect("second write should succeed");

    // Should still sync
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "cluster should sync after delay");
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;

    // Isolation: beta must not see alpha's delayed writes on any node.
    for node in cluster.nodes() {
        assert_eq!(
            read_entity(&cluster, node.id, org_beta, vault_beta, "delay-key").await,
            None,
            "node {} beta must not see alpha delay-key",
            node.id
        );
        assert_eq!(
            read_entity(&cluster, node.id, org_beta, vault_beta, "delay-key-2").await,
            None,
            "node {} beta must not see alpha delay-key-2",
            node.id
        );
    }
}
