//! Design compliance integration tests.
//!
//! These tests verify that the implementation adheres to the invariants and
//! behaviors specified in the design specification. The server assigns sequence numbers at
//! commit time; clients provide a 16-byte idempotency key per request.
//!
//! F.1.f.2.Stage1e Wave 6: migrated from legacy tonic helpers to wire-protocol
//! siblings. Diverged-vault read failures now return
//! `RpcError::WireError(StaleRouting)` (the wire mapping of the legacy
//! `tonic::Code::Unavailable`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{admin as wa, health as wh, read as wr, shared as ws, vault as wv, write as ww},
};
use inferadb_ledger_wire_transport::RpcError;
use serial_test::serial;

use crate::common::{
    TestCluster, wire_admin_client, wire_create_test_organization, wire_create_test_vault,
    wire_health_client, wire_read_client, wire_vault_client, wire_write_client,
};

// =============================================================================
// Test Helpers
// =============================================================================

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

// =============================================================================
// Idempotency Key Tests
// =============================================================================

/// Verifies that reusing an idempotency key with a different payload returns
/// `IdempotencyKeyReused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_idempotency_key_reuse_detection() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "idem-reuse-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    let write_client = wire_write_client(&cluster, leader.id);

    let shared_key = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());

    // First write with idempotency key
    let request1 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "idem-test".to_string() }),
        idempotency_key: shared_key.clone(),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "idem-key-1".to_string(),
                value: Bytes::from_static(b"value-a"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response1 =
        write_client.write(request1, rand::random::<u128>()).await.expect("write should succeed");
    match response1.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        other => panic!("first write should succeed, got: {:?}", other),
    }

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write reusing the same idempotency key with a DIFFERENT payload
    let request2 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "idem-test".to_string() }),
        idempotency_key: shared_key.clone(),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "idem-key-different".to_string(),
                value: Bytes::from_static(b"value-b"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response2 = write_client
        .write(request2, rand::random::<u128>())
        .await
        .expect("write RPC should succeed");
    match response2.result {
        Some(ww::WriteResponseResult::Error(e)) => {
            assert_eq!(
                e.code,
                ww::WriteErrorCode::IdempotencyKeyReused,
                "should get IdempotencyKeyReused error"
            );
        },
        other => panic!("second write should fail with IdempotencyKeyReused, got: {:?}", other),
    }
}

/// Tests that two writes to the same vault with unique idempotency keys both succeed.
///
/// Verifies that the server accepts multiple writes to the same vault when
/// each uses a distinct idempotency key (no false-positive dedup detection).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distinct_idempotency_keys_both_succeed() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "same-vault-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    let write_client = wire_write_client(&cluster, leader.id);

    // First write
    let request1 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "same-vault".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "key-1".to_string(),
                value: Bytes::from_static(b"val-1"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let resp1 = write_client.write(request1, rand::random::<u128>()).await.expect("write 1");
    match resp1.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        other => panic!("write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write with a different idempotency key
    let request2 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "same-vault".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "key-2".to_string(),
                value: Bytes::from_static(b"val-2"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let resp2 = write_client.write(request2, rand::random::<u128>()).await.expect("write 2");
    match resp2.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        other => panic!("write 2 should succeed, got: {:?}", other),
    }
}

/// Tests that the server assigns monotonically increasing sequences across
/// multiple vaults. Each vault has its own independent sequence counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_vault_server_assigned_sequences() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and two vaults
    let organization = create_organization(&cluster, leader.id, "two-vault-seq-ns")
        .await
        .expect("create organization");
    let vault1 = create_vault(&cluster, leader.id, organization).await.expect("create vault 1");
    let vault2 = create_vault(&cluster, leader.id, organization).await.expect("create vault 2");

    let write_client = wire_write_client(&cluster, leader.id);

    let client_id = "two-vault-test".to_string();

    // Write to vault 1
    let request1 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: client_id.clone() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault1),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "v1-key".to_string(),
                value: Bytes::from_static(b"v1-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let resp1 =
        write_client.write(request1, rand::random::<u128>()).await.expect("write to vault 1");
    let v1_seq1 = match resp1.result {
        Some(ww::WriteResponseResult::Success(s)) => s.assigned_sequence,
        other => panic!("vault 1 write 1 should succeed, got: {:?}", other),
    };

    // Long sleep to ensure state machine has applied the entry
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2
    let request2 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: client_id.clone() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault2),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "v2-key".to_string(),
                value: Bytes::from_static(b"v2-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let resp2 =
        write_client.write(request2, rand::random::<u128>()).await.expect("write to vault 2");
    let v2_seq1 = match resp2.result {
        Some(ww::WriteResponseResult::Success(s)) => s.assigned_sequence,
        other => panic!("vault 2 write 1 should succeed, got: {:?}", other),
    };

    // Wait for state to be applied
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write to vault 2 again
    let request3 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: client_id.clone() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault2),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "v2-key-2".to_string(),
                value: Bytes::from_static(b"v2-value-2"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let resp3 = write_client
        .write(request3, rand::random::<u128>())
        .await
        .expect("write to vault 2 second");
    let v2_seq2 = match resp3.result {
        Some(ww::WriteResponseResult::Success(s)) => s.assigned_sequence,
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
// Invariants 34-37: Vault failures are isolated
// =============================================================================

/// Invariant 34: A diverged vault does not affect other vaults.
///
/// This test verifies that when one vault's state root diverges (perhaps due to
/// a bug or corruption), other vaults continue to operate normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_divergence_does_not_affect_other_vaults() {
    // Use 1-node cluster to simplify debugging
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and two vaults
    let organization = create_organization(&cluster, leader.id, "vault-isolation-ns")
        .await
        .expect("create organization");
    let vault1 = create_vault(&cluster, leader.id, organization).await.expect("create vault 1");
    let vault2 = create_vault(&cluster, leader.id, organization).await.expect("create vault 2");

    let write_client = wire_write_client(&cluster, leader.id);

    // Write to vault 1
    let request1 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "vault-isolation-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault1),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "vault1-key".to_string(),
                value: Bytes::from_static(b"vault1-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response1 =
        write_client.write(request1, rand::random::<u128>()).await.expect("write to vault 1");
    match response1.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        _ => panic!("write to vault 1 should succeed"),
    }

    // Give state machine time to apply
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write to vault 2
    let request2 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "vault-isolation-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault2),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "vault2-key".to_string(),
                value: Bytes::from_static(b"vault2-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response2 =
        write_client.write(request2, rand::random::<u128>()).await.expect("write to vault 2");
    match response2.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        other => panic!("write to vault 2 should succeed, got: {:?}", other),
    }

    // Give state machine time to apply
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for replication to complete
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault 1 divergence using the admin API
    let admin_client = wire_admin_client(&cluster, leader.id);

    let divergence_request = wa::SimulateDivergenceRequest {
        organization: Some(organization),
        vault: Some(vault1),
        expected_state_root: Some(ws::Hash {
            value: Bytes::from(vec![1u8; 32]), // Fake expected root
        }),
        computed_state_root: Some(ws::Hash {
            value: Bytes::from(vec![2u8; 32]), // Different computed root
        }),
        at_height: 1,
    };

    let sim_response = admin_client
        .simulate_divergence(divergence_request, rand::random::<u128>())
        .await
        .expect("simulate divergence should succeed");
    assert!(sim_response.success, "divergence simulation should succeed");

    // Wait for the health status update to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify vault 1 is now UNAVAILABLE (diverged vaults report as unavailable)
    let health_client = wire_health_client(&cluster, leader.id);

    let vault1_health = health_client
        .check(
            wh::HealthCheckRequest { organization: Some(organization), vault: Some(vault1) },
            rand::random::<u128>(),
        )
        .await
        .expect("health check for vault 1");

    assert_eq!(
        vault1_health.status,
        ws::HealthStatus::Unavailable,
        "Vault 1 should be marked as UNAVAILABLE (diverged)"
    );

    // Verify vault 2 is still HEALTHY - this is the key invariant
    let vault2_health = health_client
        .check(
            wh::HealthCheckRequest { organization: Some(organization), vault: Some(vault2) },
            rand::random::<u128>(),
        )
        .await
        .expect("health check for vault 2");

    assert_eq!(
        vault2_health.status,
        ws::HealthStatus::Healthy,
        "Vault 2 should still be HEALTHY despite vault 1 divergence"
    );

    // Verify vault 2 is still writable
    let request3 = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "vault-isolation-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault2),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "vault2-key-2".to_string(),
                value: Bytes::from_static(b"vault2-value-2"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response3 = write_client
        .write(request3, rand::random::<u128>())
        .await
        .expect("write to vault 2 after divergence");
    match response3.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        other => panic!(
            "write to vault 2 should still succeed after vault 1 divergence, got: {:?}",
            other
        ),
    }
}

/// Invariant 35: Diverged vault returns UNAVAILABLE for reads.
///
/// When a vault's computed state root doesn't match the expected root,
/// the vault should be marked as diverged and return UNAVAILABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_diverged_vault_returns_unavailable() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "divergence-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    let write_client = wire_write_client(&cluster, leader.id);

    // Write some data to establish a vault
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "divergence-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "divergence-key".to_string(),
                value: Bytes::from_static(b"divergence-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    write_client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    // Wait for replication (global + data region)
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Simulate vault divergence using the admin API
    let admin_client = wire_admin_client(&cluster, leader.id);

    let divergence_request = wa::SimulateDivergenceRequest {
        organization: Some(organization),
        vault: Some(vault),
        expected_state_root: Some(ws::Hash {
            value: Bytes::from(vec![1u8; 32]), // Fake expected root
        }),
        computed_state_root: Some(ws::Hash {
            value: Bytes::from(vec![2u8; 32]), // Different computed root
        }),
        at_height: 1,
    };

    let sim_response = admin_client
        .simulate_divergence(divergence_request, rand::random::<u128>())
        .await
        .expect("simulate divergence should succeed");
    assert!(sim_response.success, "divergence simulation should succeed");

    // Wait for the health status update to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify health check returns UNAVAILABLE (diverged vaults report as unavailable)
    let health_client = wire_health_client(&cluster, leader.id);

    let health_response = health_client
        .check(
            wh::HealthCheckRequest { organization: Some(organization), vault: Some(vault) },
            rand::random::<u128>(),
        )
        .await
        .expect("health check should succeed");

    assert_eq!(
        health_response.status,
        ws::HealthStatus::Unavailable,
        "Vault should be marked as UNAVAILABLE (diverged)"
    );

    // Attempt to read from the diverged vault - should return StaleRouting
    // (the wire mapping of the legacy `tonic::Code::Unavailable`).
    let read_client = wire_read_client(&cluster, leader.id);

    let read_request = wr::ReadRequest {
        organization: Some(organization),
        vault: Some(vault),
        key: "divergence-key".to_string(),
        consistency: ws::ReadConsistency::Eventual,
        caller: None,
    };

    let read_result = read_client.read(read_request, rand::random::<u128>()).await;

    match read_result {
        Err(RpcError::WireError(wire_err)) => {
            assert_eq!(
                wire_err.code,
                ErrorCode::StaleRouting,
                "Read from diverged vault should return StaleRouting (wire mapping of tonic Unavailable), got: {:?}",
                wire_err,
            );
        },
        Err(other) => {
            panic!(
                "Read from diverged vault should fail with WireError(StaleRouting), got: {other:?}"
            );
        },
        Ok(_) => {
            panic!("Read from diverged vault should fail with WireError(StaleRouting)");
        },
    }
}

// =============================================================================
// State Root Verification Tests
// State commitment and bucket-based verification
// =============================================================================

/// Invariant 28: Followers verify state roots match leader.
///
/// When a follower applies log entries, it must verify that its computed
/// state root matches the state root included in the log entry from the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_follower_state_root_verification() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "state-root-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Wait for org/vault creation to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let client = wire_write_client(&cluster, leader.id);

    // Submit a write that will replicate to followers
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "state-root-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "verification-key".to_string(),
                value: Bytes::from_static(b"verification-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Verify all nodes have matching healthy status for this vault
    // If state roots didn't match, the vault would be marked as Diverged
    for node in cluster.nodes() {
        let health_client = wire_health_client(&cluster, node.id);

        let health_req =
            wh::HealthCheckRequest { organization: Some(organization), vault: Some(vault) };

        let response = health_client
            .check(health_req, rand::random::<u128>())
            .await
            .expect("health check should succeed");

        assert_eq!(
            response.status,
            ws::HealthStatus::Healthy,
            "Node {} vault should be healthy after replication",
            node.id
        );
    }
}

// =============================================================================
// Consensus Edge Case Tests
// Raft consensus
// =============================================================================

/// Verifies that idempotency detection works correctly across leader failover.
///
/// After a write succeeds on the original leader and the cluster fails over to
/// a new leader, retrying the same idempotency key should return the cached
/// result (ALREADY_COMMITTED) rather than executing a duplicate write.
///
/// The test uses leave_cluster to remove the leader, triggering a new election.
/// The idempotency cache is stored in the replicated applied state, so the new
/// leader should still detect duplicate idempotency keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_idempotency_survives_leader_failover() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let original_leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault before writing
    let organization = create_organization(&cluster, leader.id, "failover-test-ns")
        .await
        .expect("create organization");

    let vault_client = wire_vault_client(&cluster, leader.id);

    let vault_slug =
        inferadb_ledger_types::snowflake::generate_vault_slug().expect("generate vault slug");
    let vault_response = vault_client
        .create_vault(
            wv::CreateVaultRequest {
                organization: Some(organization),
                replication_factor: 0,
                initial_nodes: vec![],
                retention_policy: None,
                caller: None,
                slug: Some(vault_slug),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create vault");

    let vault = vault_response.vault.expect("vault");

    // Wait for organization/vault to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let write_client = wire_write_client(&cluster, leader.id);

    // Submit a write with a known idempotency key
    let idem_key = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());

    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "failover-test".to_string() }),
        idempotency_key: idem_key.clone(),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "failover-key".to_string(),
                value: Bytes::from_static(b"failover-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response1 =
        write_client.write(request.clone(), rand::random::<u128>()).await.expect("first write");
    let _original_tx_id = match response1.result {
        Some(ww::WriteResponseResult::Success(s)) => s.tx_id.expect("should have tx_id"),
        other => panic!("first write should succeed, got: {:?}", other),
    };

    // Wait for replication to all followers before triggering failover (global + data region)
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Trigger leader failover: transfer GLOBAL leadership to a follower first,
    // then have the new leader remove the old node via LeaveCluster. This avoids
    // the self-removal path (where the leader removes itself and remaining nodes
    // must hold an unassisted re-election).
    let follower = cluster
        .nodes()
        .iter()
        .find(|n| n.id != original_leader_id)
        .expect("should have a follower");
    let transfer_target = follower.id;

    // Transfer GLOBAL leadership via the consensus handle (no gRPC RPC for this).
    leader
        .handle
        .transfer_leader(transfer_target)
        .await
        .expect("GLOBAL leader transfer should succeed");

    // Wait for the new leader to be elected after transfer.
    let new_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for node in cluster.nodes() {
                if node.id == original_leader_id {
                    continue;
                }
                if let Some(l) = node.current_leader()
                    && l != original_leader_id
                {
                    return l;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("new leader should be elected after transfer");

    // Now have the NEW leader remove the old node from the cluster.
    let new_admin_client = wire_admin_client(&cluster, new_leader_id);

    let leave_response = new_admin_client
        .leave_cluster(
            wa::LeaveClusterRequest { node_id: original_leader_id },
            rand::random::<u128>(),
        )
        .await
        .expect("leave_cluster RPC should succeed");

    assert!(leave_response.success, "old leader should successfully leave cluster");

    assert_ne!(new_leader_id, original_leader_id, "new leader should be different from original");

    // Wait for data regions to elect new leaders after the old node departed.
    // The departed node was the data region leader — remaining nodes need to
    // hold a new election (election timeout ~150-300ms in test config).
    cluster.wait_for_leaders(Duration::from_secs(10)).await;
    cluster.wait_for_data_region_sync(Duration::from_secs(10)).await;

    // Writes route through the data region that owns the vault (not GLOBAL).
    // After failover the GLOBAL leader and data-region leader may be different
    // nodes, so connect to whichever remaining node currently leads the first
    // data region (the one `TestCluster::with_wire_transport_and_size` provisions).
    let data_region = inferadb_ledger_types::ALL_REGIONS[1];
    let regional_leader_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for node in cluster.nodes() {
                if node.id == original_leader_id {
                    continue;
                }
                if let Some(rg) = node.region_group(data_region)
                    && rg.handle().current_leader() == Some(node.id)
                {
                    return node.id;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a remaining node should lead the data region after failover");

    let new_write_client = wire_write_client(&cluster, regional_leader_id);

    // Retry the same request (same idempotency key, same payload) on the new leader.
    // The idempotency cache is replicated, so this should return ALREADY_COMMITTED
    // with the original tx_id.
    let retry_response = new_write_client
        .write(request.clone(), rand::random::<u128>())
        .await
        .expect("retry write should succeed");

    match retry_response.result {
        Some(ww::WriteResponseResult::Error(e)) => {
            assert_eq!(
                e.code,
                ww::WriteErrorCode::AlreadyCommitted,
                "duplicate idempotency key should return AlreadyCommitted after failover"
            );
            // The replicated fallback path returns committed_tx_id = None because
            // only the moka cache stores the full WriteSuccess. The replicated
            // ClientSequenceEntry stores enough to detect duplicates (sequence +
            // idempotency_key + request_hash) but not the full response.
            assert_eq!(
                e.committed_tx_id, None,
                "replicated fallback does not carry tx_id (only moka cache has it)"
            );
            assert!(
                e.assigned_sequence.is_some(),
                "replicated fallback should return the committed sequence number"
            );
        },
        Some(ww::WriteResponseResult::Success(_s)) => {
            // This branch should no longer be reached because cross-failover
            // deduplication catches the duplicate via the replicated state.
            panic!("cross-failover dedup should catch duplicate — Success is unexpected");
        },
        other => panic!("retry after failover should return AlreadyCommitted, got: {:?}", other),
    }
}

// NOTE: Additional tests for log compaction, snapshot installation, and network
// partitions are implemented in network_simulation.rs using turmoil for realistic
// network fault injection.
