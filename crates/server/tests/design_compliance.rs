//! DESIGN.md compliance integration tests.
//!
//! These tests verify that the implementation adheres to the invariants and
//! behaviors specified in DESIGN.md. Many tests are marked as #[ignore] because
//! they require multi-node cluster infrastructure that is not yet implemented.

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
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_vault_divergence_does_not_affect_other_vaults() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = common::create_write_client(leader.addr)
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

    let response1 = client.write(request1).await.expect("write to vault 1");
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

    let response2 = client.write(request2).await.expect("write to vault 2");
    match response2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write to vault 2 should succeed"),
    }

    // TODO: Simulate vault 1 divergence (requires internal state manipulation)
    // TODO: Verify vault 2 is still accessible and healthy
}

/// DESIGN.md Invariant 35: "Diverged vault returns UNAVAILABLE for reads."
///
/// When a vault's computed state root doesn't match the expected root,
/// the vault should be marked as diverged and return UNAVAILABLE.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
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

    write_client.write(request).await.expect("write should succeed");

    // TODO: Corrupt the vault's state to cause divergence
    // TODO: Attempt to read from the diverged vault
    // TODO: Verify UNAVAILABLE error is returned
}

// =============================================================================
// State Root Verification Tests
// DESIGN.md §3.2.1: State Commitment and Bucket-Based Verification
// =============================================================================

/// DESIGN.md: State root must be consistent after node restart.
///
/// After a node restarts, its computed state root should match what was
/// persisted, ensuring durability of the merkle tree state.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_state_root_consistency_after_restart() {
    // This test would:
    // 1. Create a cluster and write data
    // 2. Record the state root
    // 3. Restart a node
    // 4. Verify the state root matches after restart
    //
    // Requires restart capability in TestCluster
}

/// DESIGN.md Invariant 28: "Followers verify state roots match leader."
///
/// When a follower applies log entries, it must verify that its computed
/// state root matches the state root included in the log entry from the leader.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
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

    // TODO: Verify all followers have matching state roots
    // This requires exposing vault health status through the API
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
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
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
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_log_compaction_after_snapshot() {
    // This test would:
    // 1. Write enough data to trigger snapshot creation
    // 2. Verify snapshot is created
    // 3. Trigger log compaction
    // 4. Verify state is still accessible
    //
    // Requires snapshot/compaction trigger capability
}

/// DESIGN.md: Followers can install snapshots from leader.
///
/// When a follower is too far behind (e.g., after being offline),
/// the leader should send a snapshot instead of individual log entries.
#[tokio::test]
#[ignore = "multi-node cluster joining not yet implemented"]
async fn test_snapshot_install_from_leader() {
    // This test would:
    // 1. Create a 3-node cluster
    // 2. Partition one node
    // 3. Write enough data to trigger snapshot
    // 4. Heal the partition
    // 5. Verify the partitioned node installs snapshot
    //
    // Requires network partition simulation (turmoil)
}

/// DESIGN.md: Writes during network partition are handled correctly.
///
/// During a network partition, writes to the majority partition should
/// succeed, while writes to the minority partition should fail or timeout.
#[tokio::test]
#[ignore = "requires turmoil for network partition simulation"]
async fn test_network_partition_during_write() {
    // This test would:
    // 1. Create a 3-node cluster
    // 2. Partition one node from the others
    // 3. Submit writes to majority partition - should succeed
    // 4. Submit writes to minority partition - should fail
    // 5. Heal partition
    // 6. Verify cluster converges
    //
    // Requires turmoil for deterministic network simulation
}
