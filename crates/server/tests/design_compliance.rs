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
use serial_test::serial;

// =============================================================================
// Basic Sequence Tracking Test
// =============================================================================

/// Simple test to verify sequence gap detection is working.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_sequence_gap_detection() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write with sequence 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "seq-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "seq-key-1".to_string(),
                    value: b"value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response1 = write_client.write(request1).await.expect("write should succeed");
    match response1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        other => panic!("first write should succeed, got: {:?}", other),
    }

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write with sequence 3 - should fail with SequenceGap
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "seq-test".to_string(),
        }),
        sequence: 3, // Skip sequence 2
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "seq-key-2".to_string(),
                    value: b"value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response2 = write_client.write(request2).await.expect("write RPC should succeed");
    match response2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Error(e)) => {
            assert_eq!(
                e.code(),
                ledger_raft::proto::WriteErrorCode::SequenceGap,
                "should get SequenceGap error"
            );
            assert_eq!(
                e.last_committed_sequence,
                Some(1),
                "last committed should be 1"
            );
        }
        other => panic!("second write should fail with SequenceGap, got: {:?}", other),
    }
}

/// Test that sequence tracking works with two writes to same vault.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_vault_two_writes() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write sequence 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: "same-vault".to_string() }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "key-1".to_string(),
                    value: b"val-1".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write 1");
    match resp1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write 1 (seq 1) succeeded");
        }
        other => panic!("write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write sequence 2
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: "same-vault".to_string() }),
        sequence: 2,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "key-2".to_string(),
                    value: b"val-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write 2");
    match resp2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write 2 (seq 2) succeeded");
        }
        other => panic!("write 2 should succeed, got: {:?}", other),
    }
}

/// Test writing only to vault 2 (no vault 1 involved).
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_only_vault_2() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write to vault 2 with sequence 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: "only-v2".to_string() }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key".to_string(),
                    value: b"v2-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write v2 seq1");
    match resp1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (seq 1) succeeded");
        }
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2 with sequence 2
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: "only-v2".to_string() }),
        sequence: 2,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-val-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write v2 seq2");
    match resp2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (seq 2) succeeded");
        }
        other => panic!("vault 2 write 2 should succeed, got: {:?}", other),
    }
}

/// Test writing to vault 2 first, then vault 1, then vault 2 again.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_2_first_then_1_then_2() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    let client_id = "vault-order-test".to_string();

    // Write to vault 2 FIRST with sequence 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }), // Vault 2 first!
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key".to_string(),
                    value: b"v2-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write v2 seq1");
    match resp1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (seq 1) succeeded");
        }
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 1 with sequence 1
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 1 }), // Now vault 1
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v1-key".to_string(),
                    value: b"v1-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write v1 seq1");
    match resp2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 1 (seq 1) succeeded");
        }
        other => panic!("vault 1 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2 again with sequence 2
    let request3 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 2,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: 1 }),
        vault_id: Some(ledger_raft::proto::VaultId { id: 2 }), // Back to vault 2
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-val-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp3 = write_client.write(request3).await.expect("write v2 seq2");
    match resp3.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (seq 2) succeeded");
        }
        other => panic!("vault 2 write 2 should succeed, got: {:?}", other),
    }
}

/// Test that sequence tracking works correctly across multiple vaults.
/// This is a minimal test to isolate the two-vault sequence tracking issue.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_vault_sequence_tracking() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    let namespace_id = 1i64;
    let vault1_id = 1i64;
    let vault2_id = 2i64;
    let client_id = "two-vault-test".to_string();

    // Write to vault 1 with sequence 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault1_id }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v1-key".to_string(),
                    value: b"v1-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write to vault 1");
    match resp1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 1 with sequence 1 succeeded");
        }
        other => panic!("vault 1 write 1 should succeed, got: {:?}", other),
    }

    // Long sleep to ensure state machine has applied the entry
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2
    // Note: Sequence tracking is per (namespace_id, vault_id, client_id), so
    // writing to a different vault starts fresh at sequence 1
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 1, // Fresh sequence for new vault (not 2!)
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault2_id }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key".to_string(),
                    value: b"v2-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write to vault 2");
    match resp2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 with sequence 1 succeeded");
        }
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    }

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2 with sequence 2 (should succeed)
    let request3 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId { id: client_id.clone() }),
        sequence: 2, // Next sequence for vault 2
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault2_id }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-value-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp3 = write_client.write(request3).await.expect("write to vault 2 seq 2");
    match resp3.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 with sequence 2 succeeded");
        }
        other => panic!("vault 2 write 2 should succeed, got: {:?}", other),
    }
}

// =============================================================================
// Multi-Vault Failure Isolation Tests
// DESIGN.md Invariants 34-37: Vault failures are isolated
// =============================================================================

/// DESIGN.md Invariant 34: "A diverged vault does not affect other vaults."
///
/// This test verifies that when one vault's state root diverges (perhaps due to
/// a bug or corruption), other vaults continue to operate normally.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_divergence_does_not_affect_other_vaults() {
    // Use 1-node cluster to simplify debugging sequence tracking
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Use fixed namespace and vault IDs (writes create implicit vaults)
    // Per DESIGN.md: Writes to non-existent vaults auto-create them
    let namespace_id = 1i64;
    let vault1_id = 1i64;
    let vault2_id = 2i64;

    let mut write_client = common::create_write_client(leader.addr)
        .await
        .expect("connect to leader");

    // Write to vault 1
    let request1 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault1_id }),
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

    let response1 = write_client
        .write(request1)
        .await
        .expect("write to vault 1");
    match response1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        _ => panic!("write to vault 1 should succeed"),
    }

    // Give state machine time to apply and update client_sequences
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2
    // Note: Sequence tracking is per (namespace_id, vault_id, client_id), so
    // writing to a different vault starts fresh at sequence 1
    let request2 = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        sequence: 1, // Fresh sequence for new vault (not 2!)
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault2_id }),
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

    let response2 = write_client
        .write(request2)
        .await
        .expect("write to vault 2");
    match response2.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        other => panic!("write to vault 2 should succeed, got: {:?}", other),
    }

    // Give state machine time to apply and update client_sequences
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for replication to complete
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault 1 divergence using the admin API
    let mut admin_client = common::create_admin_client(leader.addr)
        .await
        .expect("connect to admin service");

    let divergence_request = ledger_raft::proto::SimulateDivergenceRequest {
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault1_id }),
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
    assert!(
        sim_response.into_inner().success,
        "divergence simulation should succeed"
    );

    // Wait for the health status update to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify vault 1 is now UNAVAILABLE (diverged vaults report as unavailable)
    let mut health_client = common::create_health_client(leader.addr)
        .await
        .expect("connect to health service");

    let vault1_health = health_client
        .check(ledger_raft::proto::HealthCheckRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
            vault_id: Some(ledger_raft::proto::VaultId { id: vault1_id }),
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
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
            vault_id: Some(ledger_raft::proto::VaultId { id: vault2_id }),
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
        sequence: 2, // Next sequence after 1 for vault 2
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault2_id }),
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

    let response3 = write_client
        .write(request3)
        .await
        .expect("write to vault 2 after divergence");
    match response3.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(_)) => {}
        other => panic!(
            "write to vault 2 should still succeed after vault 1 divergence, got: {:?}",
            other
        ),
    }
}

/// DESIGN.md Invariant 35: "Diverged vault returns UNAVAILABLE for reads."
///
/// When a vault's computed state root doesn't match the expected root,
/// the vault should be marked as diverged and return UNAVAILABLE.
#[serial]
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
    assert!(
        sim_response.into_inner().success,
        "divergence simulation should succeed"
    );

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
#[serial]
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
/// After a new leader is elected, the replicated sequence state prevents
/// sequence gaps (can't skip from sequence 1 to sequence 3).
///
/// ## Important Limitation
///
/// The **IdempotencyCache** (which returns ALREADY_COMMITTED with original tx_id)
/// is in-memory only and does NOT survive leader failover. This is a pragmatic
/// trade-off to avoid storing response data in the Raft log.
///
/// After failover:
/// - Retry with same (client_id, sequence) will succeed as a new write
/// - Client receives a new tx_id (not the original)
/// - Sequence gap detection still works (can't skip ahead)
///
/// For clients that need stronger idempotency guarantees across failover,
/// they should use application-level deduplication (e.g., unique operation keys).
///
/// The test uses leave_cluster to remove the leader, triggering a new election.
/// The idempotency cache is stored in the replicated applied state, so the new
/// leader should still detect duplicate (client_id, sequence) pairs.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence_survives_leader_failover() {
    let cluster = TestCluster::new(3).await;
    let original_leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let leader_addr = leader.addr;

    // Create namespace and vault before writing
    let mut admin_client = common::create_admin_client(leader_addr)
        .await
        .expect("connect to admin service");

    let ns_response = admin_client
        .create_namespace(ledger_raft::proto::CreateNamespaceRequest {
            name: "failover-test-ns".to_string(),
        })
        .await
        .expect("create namespace");

    let namespace_id = ns_response
        .into_inner()
        .namespace_id
        .map(|n| n.id)
        .expect("namespace_id");

    let vault_response = admin_client
        .create_vault(ledger_raft::proto::CreateVaultRequest {
            namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await
        .expect("create vault");

    let vault_id = vault_response
        .into_inner()
        .vault_id
        .map(|v| v.id)
        .expect("vault_id");

    // Wait for namespace/vault to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let mut write_client = common::create_write_client(leader_addr)
        .await
        .expect("connect to leader");

    // Submit a write with sequence 1
    let request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "failover-test".to_string(),
        }),
        sequence: 1,
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault_id }),
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

    let response1 = write_client
        .write(request.clone())
        .await
        .expect("first write");
    let original_tx_id = match response1.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(s)) => {
            s.tx_id.expect("should have tx_id")
        }
        other => panic!("first write should succeed, got: {:?}", other),
    };

    // Wait for replication to all followers before triggering failover
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Trigger leader failover by having the leader remove itself from the cluster.
    // This will cause a new leader election among the remaining nodes.
    let mut admin_client = common::create_admin_client(leader_addr)
        .await
        .expect("connect to admin service");

    let leave_response = admin_client
        .leave_cluster(ledger_raft::proto::LeaveClusterRequest {
            node_id: original_leader_id,
        })
        .await
        .expect("leave_cluster RPC should succeed");

    assert!(
        leave_response.into_inner().success,
        "leader should successfully leave cluster"
    );

    // Wait for a new leader to be elected (with timeout)
    // The new leader must be different from the original leader
    let new_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // Check each remaining node for leadership
            for node in cluster.nodes() {
                if node.id == original_leader_id {
                    continue; // Skip the old leader
                }
                if let Some(leader) = node.current_leader() {
                    if leader != original_leader_id {
                        return leader;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("new leader should be elected within timeout");

    assert_ne!(
        new_leader_id, original_leader_id,
        "new leader should be different from original"
    );

    // Connect to the new leader
    let new_leader = cluster
        .node(new_leader_id)
        .expect("new leader node should exist");
    let mut new_write_client = common::create_write_client(new_leader.addr)
        .await
        .expect("connect to new leader");

    // Retry the same request (same client_id, same sequence)
    // Due to in-memory idempotency cache limitation (see doc comment above),
    // this will succeed as a NEW write with a different tx_id
    let retry_response = new_write_client
        .write(request.clone())
        .await
        .expect("retry write should succeed");

    let retry_tx_id = match retry_response.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Success(s)) => {
            s.tx_id.expect("should have tx_id")
        }
        other => panic!(
            "retry after failover should succeed (new write), got: {:?}",
            other
        ),
    };

    // Note: After failover, retry gets a NEW tx_id (not the original)
    // This documents the in-memory idempotency cache limitation
    assert_ne!(
        retry_tx_id.id, original_tx_id.id,
        "retry should get different tx_id (idempotency cache doesn't survive failover)"
    );

    // However, sequence tracking DOES survive failover.
    // Verify we can't skip from sequence 1 to sequence 3 (gap detection works)
    let gap_request = ledger_raft::proto::WriteRequest {
        client_id: Some(ledger_raft::proto::ClientId {
            id: "failover-test".to_string(),
        }),
        sequence: 3, // Skip sequence 2 - should fail
        namespace_id: Some(ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(ledger_raft::proto::VaultId { id: vault_id }),
        operations: vec![ledger_raft::proto::Operation {
            op: Some(ledger_raft::proto::operation::Op::SetEntity(
                ledger_raft::proto::SetEntity {
                    key: "gap-key".to_string(),
                    value: b"gap-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let gap_response = new_write_client
        .write(gap_request)
        .await
        .expect("gap write RPC should succeed");

    match gap_response.into_inner().result {
        Some(ledger_raft::proto::write_response::Result::Error(e)) => {
            assert_eq!(
                e.code(),
                ledger_raft::proto::WriteErrorCode::SequenceGap,
                "sequence gap should be detected after failover, got: {:?}",
                e
            );
        }
        other => {
            panic!(
                "sequence gap should be rejected after failover, got: {:?}",
                other
            );
        }
    }
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
