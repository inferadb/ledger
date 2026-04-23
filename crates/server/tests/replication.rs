//! Replication integration tests.
//!
//! Tests that writes replicate correctly across cluster nodes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
use serial_test::serial;

use crate::common::{TestCluster, TestNode, create_read_client, create_write_client};

// ============================================================================
// Helper: read an entity value (used by isolation assertions).
// ============================================================================

async fn read_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Option<Vec<u8>> {
    let mut client = create_read_client(addr).await.ok()?;
    let response = client
        .read(inferadb_ledger_proto::proto::ReadRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
            key: key.to_string(),
            consistency: 0,
            caller: None,
        })
        .await
        .ok()?;
    response.into_inner().value
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: &str,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, organization).await
}

// ============================================================================
// Replication Tests
// ============================================================================

/// Tests that multiple writes replicate in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_ordered_replication() {
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives writes, beta stays empty.
    let org_alpha = create_organization(&leader.addr, "ordered-repl-ns-a", leader)
        .await
        .expect("create org alpha");
    let vault_alpha = create_vault(&leader.addr, org_alpha).await.expect("create vault alpha");
    let org_beta = create_organization(&leader.addr, "ordered-repl-ns-b", leader)
        .await
        .expect("create org beta");
    let vault_beta = create_vault(&leader.addr, org_beta).await.expect("create vault beta");

    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Submit multiple writes (alpha only)
    for i in 0..5u64 {
        let request = inferadb_ledger_proto::proto::WriteRequest {
            client_id: Some(inferadb_ledger_proto::proto::ClientId {
                id: "ordered-test".to_string(),
            }),
            idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: org_alpha.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_alpha.value() }),
            operations: vec![inferadb_ledger_proto::proto::Operation {
                op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                    inferadb_ledger_proto::proto::SetEntity {
                        key: format!("key-{}", i),
                        value: format!("value-{}", i).into_bytes(),
                        expires_at: None,
                        condition: None,
                    },
                )),
            }],
            include_tx_proof: false,
            caller: None,
        };

        let response = client.write(request).await.expect("write should succeed");
        match response.into_inner().result {
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
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
        let value = read_entity(&node.addr, org_beta, vault_beta, "key-0").await;
        assert_eq!(value, None, "node {} beta must not see alpha's key-0", node.id);
    }
}

/// Tests that followers have consistent state after writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_follower_state_consistency() {
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives the batch, beta stays empty.
    let org_alpha = create_organization(&leader.addr, "follower-consistency-ns-a", leader)
        .await
        .expect("create org alpha");
    let vault_alpha = create_vault(&leader.addr, org_alpha).await.expect("create vault alpha");
    let org_beta = create_organization(&leader.addr, "follower-consistency-ns-b", leader)
        .await
        .expect("create org beta");
    let vault_beta = create_vault(&leader.addr, org_beta).await.expect("create vault beta");

    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Submit a batch of writes (alpha only)
    let batch_request = inferadb_ledger_proto::proto::BatchWriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "batch-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: org_alpha.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_alpha.value() }),
        operations: (0..10)
            .map(|i| inferadb_ledger_proto::proto::BatchWriteOperation {
                operations: vec![inferadb_ledger_proto::proto::Operation {
                    op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                        inferadb_ledger_proto::proto::SetEntity {
                            key: format!("batch-key-{}", i),
                            value: format!("batch-value-{}", i).into_bytes(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                }],
            })
            .collect(),
        include_tx_proofs: false,
        caller: None,
    };

    let response = client.batch_write(batch_request).await.expect("batch write should succeed");

    match response.into_inner().result {
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Success(_)) => {},
        _ => panic!("batch write should succeed"),
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
        let value = read_entity(&node.addr, org_beta, vault_beta, "batch-key-0").await;
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
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // ORG-ISOLATION: alpha receives writes, beta stays empty.
    let org_alpha = create_organization(&leader.addr, "delay-repl-ns-a", leader)
        .await
        .expect("create org alpha");
    let vault_alpha = create_vault(&leader.addr, org_alpha).await.expect("create vault alpha");
    let org_beta = create_organization(&leader.addr, "delay-repl-ns-b", leader)
        .await
        .expect("create org beta");
    let vault_beta = create_vault(&leader.addr, org_beta).await.expect("create vault beta");

    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Write some data (alpha only)
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "delay-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: org_alpha.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_alpha.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "delay-key".to_string(),
                    value: b"delay-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    client.write(request).await.expect("write should succeed");

    // Small delay to simulate network latency
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write more data (alpha only)
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "delay-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: org_alpha.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_alpha.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "delay-key-2".to_string(),
                    value: b"delay-value-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    client.write(request2).await.expect("second write should succeed");

    // Should still sync
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "cluster should sync after delay");
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;

    // Isolation: beta must not see alpha's delayed writes on any node.
    for node in cluster.nodes() {
        assert_eq!(
            read_entity(&node.addr, org_beta, vault_beta, "delay-key").await,
            None,
            "node {} beta must not see alpha delay-key",
            node.id
        );
        assert_eq!(
            read_entity(&node.addr, org_beta, vault_beta, "delay-key-2").await,
            None,
            "node {} beta must not see alpha delay-key-2",
            node.id
        );
    }
}
