//! Write/read integration tests.
//!
//! Tests single-node and multi-node write/read cycles through Raft consensus.

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

/// Test single-node cluster bootstrap and leader election.
#[tokio::test]
async fn test_single_node_bootstrap() {
    let cluster = TestCluster::new(1).await;

    // Should elect itself as leader
    let leader_id = cluster.wait_for_leader().await;
    assert_eq!(leader_id, 1, "single node should be leader");

    // Verify leader state
    let leader = cluster.leader().expect("should have leader");
    assert!(leader.is_leader(), "node should report itself as leader");
}

/// Test single-node write and read cycle.
#[tokio::test]
async fn test_single_node_write_read() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create a write client
    let mut client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Submit a write request using SetEntity operation
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "test-client".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "test-key".to_string(),
                    value: b"test-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await;
    assert!(
        response.is_ok(),
        "write should succeed: {:?}",
        response.err()
    );

    let response = response.unwrap().into_inner();
    match response.result {
        Some(ledger_raft::proto::write_response::Result::Success(success)) => {
            assert!(success.tx_id.is_some(), "should have tx_id");
            assert!(
                success.block_height > 0,
                "should have non-zero block height"
            );
        }
        Some(ledger_raft::proto::write_response::Result::Error(err)) => {
            panic!("write failed: {:?}", err);
        }
        None => {
            panic!("no result in response");
        }
    }
}

/// Test write idempotency - same client_id + sequence should return cached result.
#[tokio::test]
async fn test_write_idempotency() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "idempotent-client".to_string(),
        }),
        sequence: 42,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "idempotent-key".to_string(),
                    value: b"idempotent-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    // First write
    let response1 = client
        .write(request.clone())
        .await
        .expect("first write should succeed")
        .into_inner();

    // Second write with same client_id + sequence
    let response2 = client
        .write(request)
        .await
        .expect("second write should succeed")
        .into_inner();

    // Both should return the same result
    match (response1.result, response2.result) {
        (
            Some(ledger_raft::proto::write_response::Result::Success(s1)),
            Some(ledger_raft::proto::write_response::Result::Success(s2)),
        ) => {
            assert_eq!(s1.tx_id, s2.tx_id, "tx_id should match");
            assert_eq!(
                s1.block_height, s2.block_height,
                "block_height should match"
            );
        }
        _ => panic!("both writes should succeed"),
    }
}

/// Test three-node cluster formation and leader election.
/// Note: This test requires proper cluster joining which is not yet implemented.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_three_node_cluster_formation() {
    let cluster = TestCluster::new(3).await;

    // Wait for leader election
    let leader_id = cluster.wait_for_leader().await;
    assert!(
        leader_id >= 1 && leader_id <= 3,
        "leader should be one of the nodes"
    );

    // Should have exactly one leader
    let leaders: Vec<_> = cluster.nodes().iter().filter(|n| n.is_leader()).collect();
    assert_eq!(leaders.len(), 1, "should have exactly one leader");

    // Should have two followers
    let followers = cluster.followers();
    assert_eq!(followers.len(), 2, "should have two followers");
}

/// Test write to leader replicates to followers.
/// Note: This test requires proper cluster joining which is not yet implemented.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_three_node_write_replication() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Submit a write
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "replication-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "replicated-key".to_string(),
                    value: b"replicated-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await.expect("write should succeed");
    let response = response.into_inner();

    // Verify write succeeded
    match response.result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write should succeed"),
    }

    // Wait for replication to complete
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "all nodes should sync");

    // Verify all nodes have the same last applied index
    let leader_applied = leader.last_applied();
    for follower in cluster.followers() {
        assert_eq!(
            follower.last_applied(),
            leader_applied,
            "follower {} should have same last_applied as leader",
            follower.id
        );
    }
}
