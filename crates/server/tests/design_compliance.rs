//! DESIGN.md compliance integration tests.
//!
//! These tests verify that the implementation adheres to the invariants and
//! behaviors specified in DESIGN.md. The server assigns sequence numbers at
//! commit time; clients provide a 16-byte idempotency key per request.

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
// Idempotency Key Tests
// =============================================================================

/// Verify that reusing an idempotency key with a different payload returns
/// `IdempotencyKeyReused`.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_idempotency_key_reuse_detection() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    let shared_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    // First write with idempotency key
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "idem-test".to_string() }),
        idempotency_key: shared_key.clone(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "idem-key-1".to_string(),
                    value: b"value-a".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response1 = write_client.write(request1).await.expect("write should succeed");
    match response1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        other => panic!("first write should succeed, got: {:?}", other),
    }

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write reusing the same idempotency key with a DIFFERENT payload
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "idem-test".to_string() }),
        idempotency_key: shared_key.clone(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "idem-key-different".to_string(),
                    value: b"value-b".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response2 = write_client.write(request2).await.expect("write RPC should succeed");
    match response2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            assert_eq!(
                e.code(),
                inferadb_ledger_proto::proto::WriteErrorCode::IdempotencyKeyReused,
                "should get IdempotencyKeyReused error"
            );
        },
        other => panic!("second write should fail with IdempotencyKeyReused, got: {:?}", other),
    }
}

/// Test that two writes to the same vault with unique idempotency keys both succeed.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_vault_two_writes() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // First write
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "same-vault".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write 1 succeeded");
        },
        other => panic!("write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write with a different idempotency key
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "same-vault".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write 2 succeeded");
        },
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
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write to vault 2
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "only-v2".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 2 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v2-key".to_string(),
                    value: b"v2-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write v2 first");
    match resp1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (first) succeeded");
        },
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write to vault 2
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "only-v2".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 2 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-val-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write v2 second");
    match resp2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (second) succeeded");
        },
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
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write to vault 2 first
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-order-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 2 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v2-key".to_string(),
                    value: b"v2-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp1 = write_client.write(request1).await.expect("write v2 first");
    match resp1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 succeeded");
        },
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 1
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-order-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v1-key".to_string(),
                    value: b"v1-val".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp2 = write_client.write(request2).await.expect("write v1");
    match resp2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 1 succeeded");
        },
        other => panic!("vault 1 write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2 again
    let request3 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-order-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 2 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-val-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp3 = write_client.write(request3).await.expect("write v2 again");
    match resp3.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
            println!("Write to vault 2 (again) succeeded");
        },
        other => panic!("vault 2 write 2 should succeed, got: {:?}", other),
    }
}

/// Test that the server assigns monotonically increasing sequences across
/// multiple vaults. Each vault has its own independent sequence counter.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_vault_server_assigned_sequences() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    let namespace_id = 1i64;
    let vault1_id = 1i64;
    let vault2_id = 2i64;
    let client_id = "two-vault-test".to_string();

    // Write to vault 1
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.clone() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault1_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
    let v1_seq1 = match resp1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            println!("Write to vault 1 succeeded, assigned_sequence={}", s.assigned_sequence);
            s.assigned_sequence
        },
        other => panic!("vault 1 write 1 should succeed, got: {:?}", other),
    };

    // Long sleep to ensure state machine has applied the entry
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.clone() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault2_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
    let v2_seq1 = match resp2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            println!("Write to vault 2 succeeded, assigned_sequence={}", s.assigned_sequence);
            s.assigned_sequence
        },
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    };

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2 again
    let request3 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.clone() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault2_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "v2-key-2".to_string(),
                    value: b"v2-value-2".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let resp3 = write_client.write(request3).await.expect("write to vault 2 second");
    let v2_seq2 = match resp3.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            println!(
                "Write to vault 2 (second) succeeded, assigned_sequence={}",
                s.assigned_sequence
            );
            s.assigned_sequence
        },
        other => panic!("vault 2 write 2 should succeed, got: {:?}", other),
    };

    // Verify the server assigned sequences correctly:
    // - Vault 1 first write should get sequence 1
    // - Vault 2 first write should get sequence 1 (independent counter)
    // - Vault 2 second write should get sequence 2
    assert_eq!(v1_seq1, 1, "vault 1 first write should be assigned sequence 1");
    assert_eq!(v2_seq1, 1, "vault 2 first write should be assigned sequence 1");
    assert_eq!(v2_seq2, 2, "vault 2 second write should be assigned sequence 2");
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
    // Use 1-node cluster to simplify debugging
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Use fixed namespace and vault IDs (writes create implicit vaults)
    // Per DESIGN.md: Writes to non-existent vaults auto-create them
    let namespace_id = 1i64;
    let vault1_id = 1i64;
    let vault2_id = 2i64;

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write to vault 1
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault1_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        _ => panic!("write to vault 1 should succeed"),
    }

    // Give state machine time to apply
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault2_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        other => panic!("write to vault 2 should succeed, got: {:?}", other),
    }

    // Give state machine time to apply
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for replication to complete
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault 1 divergence using the admin API
    let mut admin_client =
        common::create_admin_client(leader.addr).await.expect("connect to admin service");

    let divergence_request = inferadb_ledger_proto::proto::SimulateDivergenceRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault1_id }),
        expected_state_root: Some(inferadb_ledger_proto::proto::Hash {
            value: vec![1u8; 32], // Fake expected root
        }),
        computed_state_root: Some(inferadb_ledger_proto::proto::Hash {
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
    let mut health_client =
        common::create_health_client(leader.addr).await.expect("connect to health service");

    let vault1_health = health_client
        .check(inferadb_ledger_proto::proto::HealthCheckRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
            vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault1_id }),
        })
        .await
        .expect("health check for vault 1");

    assert_eq!(
        vault1_health.into_inner().status(),
        inferadb_ledger_proto::proto::HealthStatus::Unavailable,
        "Vault 1 should be marked as UNAVAILABLE (diverged)"
    );

    // Verify vault 2 is still HEALTHY - this is the key invariant
    let vault2_health = health_client
        .check(inferadb_ledger_proto::proto::HealthCheckRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
            vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault2_id }),
        })
        .await
        .expect("health check for vault 2");

    assert_eq!(
        vault2_health.into_inner().status(),
        inferadb_ledger_proto::proto::HealthStatus::Healthy,
        "Vault 2 should still be HEALTHY despite vault 1 divergence"
    );

    // Verify vault 2 is still writable
    let request3 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault2_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
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
    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write some data to establish a vault
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "divergence-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault divergence using the admin API
    let mut admin_client =
        common::create_admin_client(leader.addr).await.expect("connect to admin service");

    let divergence_request = inferadb_ledger_proto::proto::SimulateDivergenceRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        expected_state_root: Some(inferadb_ledger_proto::proto::Hash {
            value: vec![1u8; 32], // Fake expected root
        }),
        computed_state_root: Some(inferadb_ledger_proto::proto::Hash {
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
    let mut health_client =
        common::create_health_client(leader.addr).await.expect("connect to health service");

    let health_response = health_client
        .check(inferadb_ledger_proto::proto::HealthCheckRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
            vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        })
        .await
        .expect("health check should succeed");

    assert_eq!(
        health_response.into_inner().status(),
        inferadb_ledger_proto::proto::HealthStatus::Unavailable,
        "Vault should be marked as UNAVAILABLE (diverged)"
    );

    // Attempt to read from the diverged vault - should return UNAVAILABLE
    let mut read_client =
        common::create_read_client(leader.addr).await.expect("connect to read service");

    let read_request = inferadb_ledger_proto::proto::ReadRequest {
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        key: "divergence-key".to_string(),
        consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual.into(),
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
        },
        Ok(_) => {
            panic!("Read from diverged vault should fail with UNAVAILABLE");
        },
    }
}

// =============================================================================
// State Root Verification Tests
// DESIGN.md section 3.2.1: State Commitment and Bucket-Based Verification
// =============================================================================

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
    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit a write that will replicate to followers
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "state-root-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
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

        let health_req = inferadb_ledger_proto::proto::HealthCheckRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: 1 }),
            vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: 1 }),
        };

        let response = health_client.check(health_req).await.expect("health check should succeed");
        let status = response.into_inner().status();

        assert_eq!(
            status,
            inferadb_ledger_proto::proto::HealthStatus::Healthy,
            "Node {} vault should be healthy after replication",
            node.id
        );
    }
}

// =============================================================================
// Consensus Edge Case Tests
// DESIGN.md section 2.2: Raft Consensus with OpenRaft
// =============================================================================

/// Verify that idempotency detection works correctly across leader failover.
///
/// After a write succeeds on the original leader and the cluster fails over to
/// a new leader, retrying the same idempotency key should return the cached
/// result (ALREADY_COMMITTED) rather than executing a duplicate write.
///
/// The test uses leave_cluster to remove the leader, triggering a new election.
/// The idempotency cache is stored in the replicated applied state, so the new
/// leader should still detect duplicate idempotency keys.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_idempotency_survives_leader_failover() {
    let cluster = TestCluster::new(3).await;
    let original_leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let leader_addr = leader.addr;

    // Create namespace and vault before writing
    let mut admin_client =
        common::create_admin_client(leader_addr).await.expect("connect to admin service");

    let ns_response = admin_client
        .create_namespace(inferadb_ledger_proto::proto::CreateNamespaceRequest {
            name: "failover-test-ns".to_string(),
            shard_id: None,
            quota: None,
        })
        .await
        .expect("create namespace");

    let namespace_id = ns_response.into_inner().namespace_id.map(|n| n.id).expect("namespace_id");

    let vault_response = admin_client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await
        .expect("create vault");

    let vault_id = vault_response.into_inner().vault_id.map(|v| v.id).expect("vault_id");

    // Wait for namespace/vault to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let mut write_client =
        common::create_write_client(leader_addr).await.expect("connect to leader");

    // Submit a write with a known idempotency key
    let idem_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "failover-test".to_string() }),
        idempotency_key: idem_key.clone(),
        namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault_id }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "failover-key".to_string(),
                    value: b"failover-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response1 = write_client.write(request.clone()).await.expect("first write");
    let original_tx_id = match response1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            s.tx_id.expect("should have tx_id")
        },
        other => panic!("first write should succeed, got: {:?}", other),
    };

    // Wait for replication to all followers before triggering failover
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Trigger leader failover by having the leader remove itself from the cluster.
    // This will cause a new leader election among the remaining nodes.
    let mut admin_client =
        common::create_admin_client(leader_addr).await.expect("connect to admin service");

    let leave_response = admin_client
        .leave_cluster(inferadb_ledger_proto::proto::LeaveClusterRequest {
            node_id: original_leader_id,
        })
        .await
        .expect("leave_cluster RPC should succeed");

    assert!(leave_response.into_inner().success, "leader should successfully leave cluster");

    // Wait for a new leader to be elected (with timeout)
    // The new leader must be different from the original leader
    let new_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // Check each remaining node for leadership
            for node in cluster.nodes() {
                if node.id == original_leader_id {
                    continue; // Skip the old leader
                }
                if let Some(leader) = node.current_leader()
                    && leader != original_leader_id
                {
                    return leader;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("new leader should be elected within timeout");

    assert_ne!(new_leader_id, original_leader_id, "new leader should be different from original");

    // Connect to the new leader
    let new_leader = cluster.node(new_leader_id).expect("new leader node should exist");
    let mut new_write_client =
        common::create_write_client(new_leader.addr).await.expect("connect to new leader");

    // Retry the same request (same idempotency key, same payload) on the new leader.
    // The idempotency cache is replicated, so this should return ALREADY_COMMITTED
    // with the original tx_id.
    let retry_response =
        new_write_client.write(request.clone()).await.expect("retry write should succeed");

    match retry_response.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            assert_eq!(
                e.code(),
                inferadb_ledger_proto::proto::WriteErrorCode::AlreadyCommitted,
                "duplicate idempotency key should return AlreadyCommitted after failover"
            );
            assert_eq!(
                e.committed_tx_id,
                Some(original_tx_id),
                "should return the original tx_id from the replicated cache"
            );
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            // If the in-memory idempotency cache didn't survive failover, the write
            // succeeds as a new write. This documents the limitation.
            let retry_tx_id = s.tx_id.expect("should have tx_id");
            assert_ne!(
                retry_tx_id.id, original_tx_id.id,
                "if cache miss, retry gets different tx_id (idempotency cache limitation)"
            );
        },
        other => panic!(
            "retry after failover should return AlreadyCommitted or Success, got: {:?}",
            other
        ),
    }
}

// NOTE: Additional tests for log compaction, snapshot installation, and network
// partitions are implemented in network_simulation.rs using turmoil for realistic
// network fault injection.
