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

mod common;

use std::time::Duration;

use common::TestCluster;
use serial_test::serial;

/// Tests that multiple writes replicate in order.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ordered_replication() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit multiple writes
    for i in 0..5u64 {
        let request = inferadb_ledger_proto::proto::WriteRequest {
            client_id: Some(inferadb_ledger_proto::proto::ClientId {
                id: "ordered-test".to_string(),
            }),
            idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 1 }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
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
}

/// Tests that followers have consistent state after writes.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follower_state_consistency() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit a batch of writes
    let batch_request = inferadb_ledger_proto::proto::BatchWriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "batch-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 1 }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
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
    };

    let response = client.batch_write(batch_request).await.expect("batch write should succeed");

    match response.into_inner().result {
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Success(_)) => {},
        _ => panic!("batch write should succeed"),
    }

    // Wait for sync
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "all nodes should sync after batch write");

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
}

/// Tests replication continues after a brief network delay.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_replication_after_delay() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write some data
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "delay-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 1 }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
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
    };

    client.write(request).await.expect("write should succeed");

    // Small delay to simulate network latency
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write more data
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "delay-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 1 }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
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
    };

    client.write(request2).await.expect("second write should succeed");

    // Should still sync
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "cluster should sync after delay");
}
