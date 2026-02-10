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
use serial_test::serial;

/// Test single-node cluster bootstrap and leader election.
#[serial]
#[tokio::test]
async fn test_single_node_bootstrap() {
    let cluster = TestCluster::new(1).await;

    // Get the actual node ID (Snowflake ID)
    let expected_leader_id = cluster.nodes()[0].id;

    // Should elect itself as leader
    let leader_id = cluster.wait_for_leader().await;
    assert_eq!(leader_id, expected_leader_id, "single node should be leader");

    // Verify leader state
    let leader = cluster.leader().expect("should have leader");
    assert!(leader.is_leader(), "node should report itself as leader");
}

/// Test single-node write and read cycle.
#[serial]
#[tokio::test]
async fn test_single_node_write_read() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create a write client
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit a write request using SetEntity operation
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "test-client".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
    assert!(response.is_ok(), "write should succeed: {:?}", response.err());

    let response = response.unwrap().into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(success)) => {
            assert!(success.tx_id.is_some(), "should have tx_id");
            assert!(success.block_height > 0, "should have non-zero block height");
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(err)) => {
            panic!("write failed: {:?}", err);
        },
        None => {
            panic!("no result in response");
        },
    }
}

/// Test write idempotency - same client_id + idempotency_key should return cached result.
#[serial]
#[tokio::test]
async fn test_write_idempotency() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Use a fixed idempotency key for both writes to test deduplication
    let idempotency_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "idempotent-client".to_string(),
        }),
        idempotency_key: idempotency_key.clone(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
    let response1 =
        client.write(request.clone()).await.expect("first write should succeed").into_inner();

    // Second write with same client_id + idempotency_key
    let response2 = client.write(request).await.expect("second write should succeed").into_inner();

    // Both should return the same result
    match (response1.result, response2.result) {
        (
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s1)),
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s2)),
        ) => {
            assert_eq!(s1.tx_id, s2.tx_id, "tx_id should match");
            assert_eq!(s1.block_height, s2.block_height, "block_height should match");
        },
        _ => panic!("both writes should succeed"),
    }
}

/// Test two-node cluster formation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_cluster_formation() {
    let cluster = TestCluster::new(2).await;

    // Get the actual node IDs (Snowflake IDs)
    let node_ids: Vec<u64> = cluster.nodes().iter().map(|n| n.id).collect();

    // Wait for leader election
    let leader_id = cluster.wait_for_leader().await;
    assert!(
        node_ids.contains(&leader_id),
        "leader {} should be one of the nodes {:?}",
        leader_id,
        node_ids
    );

    // Should have exactly one leader
    let leaders: Vec<_> = cluster.nodes().iter().filter(|n| n.is_leader()).collect();
    assert_eq!(leaders.len(), 1, "should have exactly one leader");

    // Should have one follower
    let followers = cluster.followers();
    assert_eq!(followers.len(), 1, "should have one follower");
}

/// Test three-node cluster formation and leader election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_cluster_formation() {
    let cluster = TestCluster::new(3).await;

    // Get the actual node IDs (Snowflake IDs)
    let node_ids: Vec<u64> = cluster.nodes().iter().map(|n| n.id).collect();

    // Wait for leader election
    let leader_id = cluster.wait_for_leader().await;
    assert!(
        node_ids.contains(&leader_id),
        "leader {} should be one of the nodes {:?}",
        leader_id,
        node_ids
    );

    // Should have exactly one leader
    let leaders: Vec<_> = cluster.nodes().iter().filter(|n| n.is_leader()).collect();
    assert_eq!(leaders.len(), 1, "should have exactly one leader");

    // Should have two followers
    let followers = cluster.followers();
    assert_eq!(followers.len(), 2, "should have two followers");
}

/// Test that writes create blocks that can be retrieved via GetBlock.
///
/// DESIGN.md §3.2.1: State root is computed after applying transactions.
/// DESIGN.md §7.1: GetBlock returns stored block with header and transactions.
#[serial]
#[tokio::test]
async fn test_write_creates_retrievable_block() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create write and read clients
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");
    let mut read_client =
        common::create_read_client(leader.addr).await.expect("connect to leader for reads");

    // Submit a write
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "block-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "block-key".to_string(),
                    value: b"block-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = write_client.write(request).await.expect("write should succeed").into_inner();

    let block_height = match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => s.block_height,
        _ => panic!("write should succeed"),
    };

    assert!(block_height > 0, "block_height should be > 0");

    // Retrieve the block via GetBlock
    let get_block_request = inferadb_ledger_proto::proto::GetBlockRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        height: block_height,
    };

    let block_response = read_client
        .get_block(get_block_request)
        .await
        .expect("get_block should succeed")
        .into_inner();

    let block = block_response.block.expect("should have block");
    let header = block.header.expect("block should have header");

    // Verify block header
    assert_eq!(header.height, block_height, "block height should match");
    assert!(header.state_root.is_some(), "block should have state_root");
    assert!(header.tx_merkle_root.is_some(), "block should have tx_merkle_root");
    assert!(header.previous_hash.is_some(), "block should have previous_hash");

    // Verify state_root is non-zero (we applied operations, so state changed)
    let state_root = header.state_root.unwrap();
    assert!(
        !state_root.value.iter().all(|&b| b == 0),
        "state_root should not be all zeros after write"
    );

    // Verify block contains the transaction
    assert_eq!(block.transactions.len(), 1, "block should have 1 transaction");
    let tx = &block.transactions[0];
    assert_eq!(tx.operations.len(), 1, "transaction should have 1 operation");
}

/// Test write to leader replicates to followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_write_replication() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit a write
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "replication-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
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
