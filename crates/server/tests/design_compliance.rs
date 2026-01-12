//! DESIGN.md compliance integration tests.
//!
//! These tests verify that the implementation adheres to the invariants and
//! behaviors specified in DESIGN.md.

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

// =============================================================================
// Multi-Vault Failure Isolation Tests
// DESIGN.md Invariants 34-37: Vault failures are isolated
// =============================================================================

/// DESIGN.md Invariant 34: "A diverged vault does not affect other vaults."
///
/// This test verifies that when one vault's state root diverges (perhaps due to
/// a bug or corruption), other vaults continue to operate normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_divergence_does_not_affect_other_vaults() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write to vault 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "vault1-key".to_string(),
                    value: b"vault1-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response1 = write_client.write(request1).await.expect("write to vault 1");
    match response1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write to vault 1 should succeed"),
    }

    // Write to vault 2
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        sequence: 2,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "vault2-key".to_string(),
                    value: b"vault2-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response2 = write_client.write(request2).await.expect("write to vault 2");
    match response2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write to vault 2 should succeed"),
    }

    // Wait for replication to complete
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault 1 divergence using the admin API
    let mut admin_client = common::create_admin_client(leader.addr)
        .await
        .expect("connect to admin service");

    let divergence_request = ledger_raft::proto::SimulateDivergenceRequest {
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        expected_state_root: Some(ledger_raft::proto::Hash {
            value: vec![1u8; 32], // Fake expected root
        }),
        computed_state_root: Some(ledger_raft::proto::Hash {
            value: vec![2u8; 32], // Different computed root
        }),
        at_height: 1,
    };

    let sim_response = admin_client
        .simulate_divergence(divergence_request)
        .await
        .expect("simulate divergence should succeed");
    assert!(sim_response.into_inner().success, "divergence simulation should succeed");

    // Wait for the health status update to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify vault 1 is now UNAVAILABLE (diverged vaults report as unavailable)
    let mut health_client = common::create_health_client(leader.addr)
        .await
        .expect("connect to health service");

    let vault1_health = health_client
        .check(ledger_raft::proto::HealthCheckRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
            vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        })
        .await
        .expect("health check for vault 1");

    assert_eq!(
        vault1_health.into_inner().status(),
        ledger_raft::proto::HealthStatus::Unavailable,
        "Vault 1 should be marked as UNAVAILABLE (diverged)"
    );

    // Verify vault 2 is still HEALTHY - this is the key invariant
    let vault2_health = health_client
        .check(ledger_raft::proto::HealthCheckRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
            vault_id: Some(ledger_raft::proto::VaultId { id: 2 }),
        })
        .await
        .expect("health check for vault 2");

    assert_eq!(
        vault2_health.into_inner().status(),
        ledger_raft::proto::HealthStatus::Healthy,
        "Vault 2 should still be HEALTHY despite vault 1 divergence"
    );

    // Verify vault 2 is still writable
    let request3 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        sequence: 3,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "vault2-key-2".to_string(),
                    value: b"vault2-value-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response3 = write_client.write(request3).await.expect("write to vault 2 after divergence");
    match response3.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write to vault 2 should still succeed after vault 1 divergence"),
    }
}

/// DESIGN.md Invariant 35: "Diverged vault returns UNAVAILABLE for reads."
///
/// When a vault's computed state root doesn't match the expected root,
/// the vault should be marked as diverged and return UNAVAILABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_diverged_vault_returns_unavailable() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write some data to establish a vault
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "divergence-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "divergence-key".to_string(),
                    value: b"divergence-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    write_client
        .write(request)
        .await
        .expect("write should succeed");

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault divergence using the admin API
    let mut admin_client = common::create_admin_client(leader.addr)
        .await
        .expect("connect to admin service");

    let divergence_request = ledger_raft::proto::SimulateDivergenceRequest {
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        expected_state_root: Some(ledger_raft::proto::Hash {
            value: vec![1u8; 32], // Fake expected root
        }),
        computed_state_root: Some(ledger_raft::proto::Hash {
            value: vec![2u8; 32], // Different computed root
        }),
        at_height: 1,
    };

    let sim_response = admin_client
        .simulate_divergence(divergence_request)
        .await
        .expect("simulate divergence should succeed");
    assert!(sim_response.into_inner().success, "divergence simulation should succeed");

    // Wait for the health status update to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify health check returns UNAVAILABLE (diverged vaults report as unavailable)
    let mut health_client = common::create_health_client(leader.addr)
        .await
        .expect("connect to health service");

    let health_response = health_client
        .check(ledger_raft::proto::HealthCheckRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
            vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        })
        .await
        .expect("health check should succeed");

    assert_eq!(
        health_response.into_inner().status(),
        ledger_raft::proto::HealthStatus::Unavailable,
        "Vault should be marked as UNAVAILABLE (diverged)"
    );

    // Attempt to read from the diverged vault - should return UNAVAILABLE
    let mut read_client = common::create_read_client(leader.addr)
        .await
        .expect("connect to read service");

    let read_request = ledger_raft::proto::ReadRequest {
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        key: "divergence-key".to_string(),
        consistency: ledger_raft::proto::ReadConsistency::Eventual.into(),
    };

    let read_result = read_client.read(read_request).await;

    // The read should fail with UNAVAILABLE status
    match read_result {
        Err(status) => {
            assert_eq!(
                status.code(),
                tonic::Code::Unavailable,
                "Read from diverged vault should return UNAVAILABLE, got: {:?}",
                status
            );
        }
        Ok(_) => {
            panic!("Read from diverged vault should fail with UNAVAILABLE");
        }
    }
}

// =============================================================================
// State Root Verification Tests
// DESIGN.md §3.2.1: State Commitment and Bucket-Based Verification
// =============================================================================

/// DESIGN.md: State root must be consistent after node restart.
///
/// After a node restarts, its computed state root should match what was
/// persisted, ensuring durability of the merkle tree state.
///
/// Implementation needs:
/// - TestCluster.restart_node(node_id) capability
/// - API to query current state root for a vault
#[tokio::test]
#[ignore = "requires node restart capability in TestCluster"]
async fn test_state_root_consistency_after_restart() {
    // This test would:
    // 1. Create a cluster and write data
    // 2. Record the state root
    // 3. Restart a node
    // 4. Verify the state root matches after restart
}

/// DESIGN.md Invariant 28: "Followers verify state roots match leader."
///
/// When a follower applies log entries, it must verify that its computed
/// state root matches the state root included in the log entry from the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follower_state_root_verification() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Submit a write that will replicate to followers
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "state-root-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "verification-key".to_string(),
                    value: b"verification-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    client.write(request).await.expect("write should succeed");

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Verify all nodes have matching healthy status for this vault
    // If state roots didn't match, the vault would be marked as Diverged
    for node in cluster.nodes() {
        let mut health_client = common::create_health_client(node.addr)
            .await
            .expect("connect to node for health check");

        let health_req = ledger_raft::proto::HealthCheckRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
            vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        };

        let response = health_client
            .check(health_req)
            .await
            .expect("health check should succeed");
        let status = response.into_inner().status();

        assert_eq!(
            status,
            ledger_raft::proto::HealthStatus::Healthy,
            "Node {} vault should be healthy after replication",
            node.id
        );
    }
}

// =============================================================================
// Consensus Edge Case Tests
// DESIGN.md §2.2: Raft Consensus with OpenRaft
// =============================================================================

/// DESIGN.md Invariant 9: "Sequence numbers must be monotonically increasing."
///
/// This test verifies that client sequence tracking survives leader failover.
/// After a new leader is elected, previously processed sequences should still
/// be detected as duplicates.
///
/// Implementation needs:
/// - TestCluster.stop_node(node_id) to trigger failover
/// - Leader election wait with timeout
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires leader failover triggering capability"]
async fn test_sequence_survives_leader_failover() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Submit a write with sequence 1
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "failover-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "failover-key".to_string(),
                    value: b"failover-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response1 = client.write(request.clone()).await.expect("first write");
    match response1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(s)) => {
            // Store the tx_id for later comparison
            let _tx_id = s.tx_id;
        }
        _ => panic!("first write should succeed"),
    }

    // TODO: Trigger leader failover
    // TODO: Connect to new leader
    // TODO: Retry same request - should get cached result
    let _ = leader_id;
}

/// DESIGN.md: Log compaction preserves applied state.
///
/// After log compaction (snapshot creation), the log entries before the
/// snapshot can be discarded, but the state must be preserved.
///
/// Implementation needs:
/// - API to trigger snapshot creation
/// - API to trigger log compaction
/// - Configuration for snapshot frequency
#[tokio::test]
#[ignore = "requires snapshot/compaction trigger API"]
async fn test_log_compaction_after_snapshot() {
    // This test would:
    // 1. Write enough data to trigger snapshot creation
    // 2. Verify snapshot is created
    // 3. Trigger log compaction
    // 4. Verify state is still accessible
}

/// DESIGN.md: Followers can install snapshots from leader.
///
/// When a follower is too far behind (e.g., after being offline),
/// the leader should send a snapshot instead of individual log entries.
///
/// Implementation needs:
/// - Turmoil integration for network partitions (see network_simulation.rs)
/// - Snapshot trigger API
/// - API to verify InstallSnapshot RPC was received
#[tokio::test]
#[ignore = "requires turmoil integration with snapshot triggering"]
async fn test_snapshot_install_from_leader() {
    // This test would:
    // 1. Create a 3-node cluster
    // 2. Partition one node
    // 3. Write enough data to trigger snapshot
    // 4. Heal the partition
    // 5. Verify the partitioned node installs snapshot
}

/// DESIGN.md: Writes during network partition are handled correctly.
///
/// During a network partition, writes to the majority partition should
/// succeed, while writes to the minority partition should fail or timeout.
///
/// This functionality is tested in `network_simulation.rs` using turmoil:
/// - `test_network_partition_blocks_communication` - verifies partitions block RPCs
/// - `test_majority_partition_continues_operating` - verifies majority continues while minority fails
#[tokio::test]
#[ignore = "covered by turmoil tests in network_simulation.rs"]
async fn test_network_partition_during_write() {
    // See network_simulation.rs for turmoil-based partition tests
}
