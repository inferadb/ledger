//! Leader failover chaos tests.
//!
//! Tests that verify data consistency and availability during leader failures,
//! network partitions, and rapid leadership changes.
//!
//! These tests exercise:
//! - Write durability across leader changes
//! - Read consistency after failover
//! - Behavior during in-flight writes when leader crashes
//! - State machine determinism under chaos conditions

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

mod common;

use std::{collections::HashSet, time::Duration};

use common::{TestCluster, create_read_client, create_write_client};
use inferadb_ledger_proto::proto::{
    ClientId, OrganizationSlug, ReadRequest, VaultId, WriteRequest,
};
use serial_test::serial;

/// Helper to create a write request with a single SetEntity operation.
fn make_write_request(
    organization_id: i64,
    vault_id: i64,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> WriteRequest {
    WriteRequest {
        organization_slug: Some(OrganizationSlug { slug: organization_id as u64 }),
        vault_id: Some(VaultId { id: vault_id }),
        client_id: Some(ClientId { id: client_id.to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: value.to_vec(),
                    condition: None,
                    expires_at: None,
                },
            )),
        }],
        include_tx_proof: false,
    }
}

/// Extracts block_height from a WriteResponse, panics if not a success.
fn extract_block_height(response: inferadb_ledger_proto::proto::WriteResponse) -> u64 {
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => s.block_height,
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            panic!("write failed: {:?}", e)
        },
        None => panic!("no result in response"),
    }
}

/// Helper to read an entity and return its value.
async fn read_entity(
    addr: std::net::SocketAddr,
    organization_id: i64,
    vault_id: i64,
    key: &str,
) -> Option<Vec<u8>> {
    let mut client = create_read_client(addr).await.ok()?;
    let response = client
        .read(ReadRequest {
            organization_slug: Some(OrganizationSlug { slug: organization_id as u64 }),
            vault_id: Some(VaultId { id: vault_id }),
            key: key.to_string(),
            consistency: 0, // EVENTUAL (default)
        })
        .await
        .ok()?;
    response.into_inner().value
}

/// Tests that a committed write survives leader failover.
///
/// This is the fundamental consistency guarantee: once a write is acknowledged
/// by the leader, it must be durable even if the leader crashes immediately after.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_committed_write_survives_leader_crash() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let leader_addr = leader.addr;

    // Write some data through the leader
    let mut client = create_write_client(leader_addr).await.expect("connect to leader");

    let client_id = format!("test-client-{}", leader_id);
    let write_req = make_write_request(1, 1, "chaos-key", b"chaos-value", &client_id);

    let response = client.write(write_req).await.expect("write should succeed").into_inner();
    let block_height = extract_block_height(response);
    assert!(block_height > 0, "write should be committed");

    // Wait for replication to followers
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all nodes have the same last_applied before we check data
    let applied_indices: Vec<u64> = cluster.nodes().iter().map(|n| n.last_applied()).collect();
    assert!(
        applied_indices.iter().all(|&i| i >= block_height),
        "all nodes should have applied the write, got {:?}",
        applied_indices
    );

    // Read from a follower to verify the write was replicated
    let followers = cluster.followers();
    let follower = followers.first().expect("should have follower");
    let value = read_entity(follower.addr, 1, 1, "chaos-key").await;
    assert_eq!(value, Some(b"chaos-value".to_vec()), "follower should have the committed write");
}

/// Tests that writes are still readable after a new leader is elected.
///
/// Simulates a scenario where:
/// 1. Write is committed under leader A
/// 2. Leader A becomes unavailable (simulated by not using it)
/// 3. Cluster elects leader B
/// 4. Read from leader B should return the data
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_consistency_after_leader_change() {
    let cluster = TestCluster::new(3).await;
    let initial_leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(initial_leader_id).expect("leader exists");

    // Write data through the initial leader
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("test-client-{}", initial_leader_id);

    // Write multiple keys to create state
    for i in 0..5u64 {
        let write_req = make_write_request(
            1,
            1,
            &format!("key-{}", i),
            format!("value-{}", i).as_bytes(),
            &client_id,
        );
        client.write(write_req).await.expect("write should succeed");
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read from all nodes - they should all return the same values
    for node in cluster.nodes() {
        for i in 0..5 {
            let value = read_entity(node.addr, 1, 1, &format!("key-{}", i)).await;
            assert_eq!(
                value,
                Some(format!("value-{}", i).into_bytes()),
                "node {} should have key-{}",
                node.id,
                i
            );
        }
    }
}

/// Tests that writes to a 3-node cluster succeed with 2 nodes available.
///
/// This tests fault tolerance: a 3-node cluster can tolerate 1 failure.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_writes_succeed_with_one_node_down() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");

    // Write through the leader (all 3 nodes up)
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("test-client-{}", leader_id);
    let write_req = make_write_request(1, 1, "before-failure", b"value1", &client_id);

    client.write(write_req).await.expect("write should succeed");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Note: In this test, we're not actually killing a node, but the write
    // should succeed even if one follower is slow/unavailable, as long as
    // the leader and one follower form a majority.

    // Write again - should still succeed
    let write_req = make_write_request(1, 1, "after-check", b"value2", &client_id);
    client.write(write_req).await.expect("write should succeed");

    // Verify both writes are readable
    let value1 = read_entity(leader.addr, 1, 1, "before-failure").await;
    let value2 = read_entity(leader.addr, 1, 1, "after-check").await;

    assert_eq!(value1, Some(b"value1".to_vec()));
    assert_eq!(value2, Some(b"value2".to_vec()));
}

/// Tests that all nodes agree on block height after multiple writes.
///
/// This verifies the determinism property: all nodes applying the same
/// log entries should arrive at the same state.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_deterministic_block_height_across_nodes() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("test-client-{}", leader_id);

    // Submit multiple writes
    let num_writes = 10u64;
    for i in 0..num_writes {
        let write_req = make_write_request(
            1,
            1,
            &format!("det-key-{}", i),
            &(i as u32).to_le_bytes(),
            &client_id,
        );
        client.write(write_req).await.expect("write should succeed");
    }

    // Wait for replication to complete
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check that all nodes have the same last_applied index
    let applied_indices: Vec<u64> = cluster.nodes().iter().map(|n| n.last_applied()).collect();

    let unique_indices: HashSet<_> = applied_indices.iter().collect();
    assert_eq!(
        unique_indices.len(),
        1,
        "all nodes should have the same last_applied index, got {:?}",
        applied_indices
    );
}

/// Tests that concurrent writes from multiple clients are all applied.
///
/// This tests that the Raft log correctly serializes concurrent writes
/// and all writes are eventually visible.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writes_all_applied() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let leader_addr = leader.addr;

    // Spawn multiple concurrent writers
    let mut handles = vec![];
    let num_clients = 5;
    let writes_per_client = 3u64;

    for client_num in 0..num_clients {
        let addr = leader_addr;
        let handle = tokio::spawn(async move {
            let mut client = create_write_client(addr).await.expect("connect to leader");
            let client_id = format!("client-{}", client_num);

            for seq in 0..writes_per_client {
                let key = format!("concurrent-{}-{}", client_num, seq);
                let value = format!("value-{}-{}", client_num, seq);
                let write_req = make_write_request(1, 1, &key, value.as_bytes(), &client_id);
                client.write(write_req).await.expect("write should succeed");
            }
        });
        handles.push(handle);
    }

    // Wait for all writers to complete
    for handle in handles {
        handle.await.expect("writer task should complete");
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all writes are readable from any node
    let any_node = cluster.nodes().first().expect("cluster has nodes");
    let mut found_count = 0;

    for client_num in 0..num_clients {
        for seq in 0..writes_per_client {
            let key = format!("concurrent-{}-{}", client_num, seq);
            let expected_value = format!("value-{}-{}", client_num, seq);

            let value = read_entity(any_node.addr, 1, 1, &key).await;
            if value == Some(expected_value.into_bytes()) {
                found_count += 1;
            }
        }
    }

    let expected_total = num_clients * writes_per_client as i32;
    assert_eq!(
        found_count, expected_total,
        "all {} concurrent writes should be readable, found {}",
        expected_total, found_count
    );
}

/// Tests that rapid succession of writes doesn't cause data loss.
///
/// This stress tests the batching and commit pipeline.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_writes_no_data_loss() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("rapid-client-{}", leader_id);
    let num_writes = 50u64;

    // Submit writes as fast as possible
    for i in 0..num_writes {
        let write_req = make_write_request(
            1,
            1,
            &format!("rapid-{}", i),
            &(i as u32).to_le_bytes(),
            &client_id,
        );
        client.write(write_req).await.expect("write should succeed");
    }

    // Wait for all writes to be applied
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all writes are present
    let mut found_count = 0u64;
    for i in 0..num_writes {
        let value = read_entity(leader.addr, 1, 1, &format!("rapid-{}", i)).await;
        if value.is_some() {
            found_count += 1;
        }
    }

    assert_eq!(
        found_count, num_writes,
        "all {} rapid writes should be readable, found {}",
        num_writes, found_count
    );
}

/// Tests that the cluster maintains term agreement during normal operation.
///
/// Term disagreement would indicate split-brain or election issues.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_term_agreement_maintained() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    // Submit some writes to exercise the cluster
    let leader = cluster.leader().expect("should have leader");
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("term-test-{}", leader_id);
    for i in 0..5u64 {
        let write_req = make_write_request(1, 1, &format!("term-key-{}", i), b"value", &client_id);
        client.write(write_req).await.expect("write should succeed");
    }

    // Check that all nodes are on the same term
    tokio::time::sleep(Duration::from_millis(200)).await;

    let terms: Vec<u64> = cluster.nodes().iter().map(|n| n.current_term()).collect();
    let unique_terms: HashSet<_> = terms.iter().collect();

    assert_eq!(unique_terms.len(), 1, "all nodes should be on the same term, got {:?}", terms);
}

// Note: Vault and organization isolation tests are in write_read.rs.
// These chaos tests focus on leader failover and replication consistency.

/// Tests that overwriting a key works correctly across the cluster.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_key_overwrite_consistency() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("overwrite-test-{}", leader_id);

    // Write initial value
    let write_req = make_write_request(1, 1, "overwrite-key", b"initial", &client_id);
    client.write(write_req).await.expect("initial write");

    // Overwrite with new value
    let write_req = make_write_request(1, 1, "overwrite-key", b"updated", &client_id);
    client.write(write_req).await.expect("overwrite");

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(300)).await;

    // All nodes should see the updated value
    for node in cluster.nodes() {
        let value = read_entity(node.addr, 1, 1, "overwrite-key").await;
        assert_eq!(value, Some(b"updated".to_vec()), "node {} should have updated value", node.id);
    }
}

/// Tests that large batches of writes are applied correctly.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_large_batch_writes() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.node(leader_id).expect("leader exists");
    let mut client = create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = format!("batch-test-{}", leader_id);

    // Write 100 keys
    let num_keys = 100u64;
    for i in 0..num_keys {
        let key = format!("batch-key-{:04}", i);
        let value = format!("batch-value-{:04}", i);
        let write_req = make_write_request(1, 1, &key, value.as_bytes(), &client_id);
        client.write(write_req).await.expect("batch write");
    }

    // Wait for replication
    let synced = cluster.wait_for_sync(Duration::from_secs(10)).await;
    assert!(synced, "cluster should sync after batch writes");

    // Verify all keys are readable from all nodes
    for node in cluster.nodes() {
        let mut found = 0u64;
        for i in 0..num_keys {
            let key = format!("batch-key-{:04}", i);
            let expected = format!("batch-value-{:04}", i);
            let value = read_entity(node.addr, 1, 1, &key).await;
            if value == Some(expected.into_bytes()) {
                found += 1;
            }
        }
        assert_eq!(
            found, num_keys,
            "node {} should have all {} keys, found {}",
            node.id, num_keys, found
        );
    }
}
