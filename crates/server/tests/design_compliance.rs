//! Design compliance integration tests.
//!
//! These tests verify that the implementation adheres to the invariants and
//! behaviors specified in the design specification. The server assigns sequence numbers at
//! commit time; clients provide a 16-byte idempotency key per request.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::{
    common,
    common::{TestCluster, TestNode, create_health_client, create_read_client},
};

// =============================================================================
// Test Helpers
// =============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: std::net::SocketAddr,
    name: &str,
    node: &TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: std::net::SocketAddr,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, organization).await
}

// =============================================================================
// Idempotency Key Tests
// =============================================================================

/// Verifies that reusing an idempotency key with a different payload returns
/// `IdempotencyKeyReused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_idempotency_key_reuse_detection() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(leader.addr, "idem-reuse-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    let shared_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    // First write with idempotency key
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "idem-test".to_string() }),
        idempotency_key: shared_key.clone(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
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

/// Tests that two writes to the same vault with unique idempotency keys both succeed.
///
/// Verifies that the server accepts multiple writes to the same vault when
/// each uses a distinct idempotency key (no false-positive dedup detection).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distinct_idempotency_keys_both_succeed() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(leader.addr, "same-vault-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // First write
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "same-vault".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
    };

    let resp1 = write_client.write(request1).await.expect("write 1");
    match resp1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        other => panic!("write 1 should succeed, got: {:?}", other),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second write with a different idempotency key
    let request2 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "same-vault".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
    };

    let resp2 = write_client.write(request2).await.expect("write 2");
    match resp2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        other => panic!("write 2 should succeed, got: {:?}", other),
    }
}

/// Tests that the server assigns monotonically increasing sequences across
/// multiple vaults. Each vault has its own independent sequence counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_vault_server_assigned_sequences() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and two vaults
    let organization = create_organization(leader.addr, "two-vault-seq-ns", leader)
        .await
        .expect("create organization");
    let vault1 = create_vault(leader.addr, organization).await.expect("create vault 1");
    let vault2 = create_vault(leader.addr, organization).await.expect("create vault 2");

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    let client_id = "two-vault-test".to_string();

    // Write to vault 1
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.clone() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault1.value() }),
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
        caller: None,
    };

    let resp1 = write_client.write(request1).await.expect("write to vault 1");
    let v1_seq1 = match resp1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            // assigned_sequence verified in assertions below
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault2.value() }),
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
        caller: None,
    };

    let resp2 = write_client.write(request2).await.expect("write to vault 2");
    let v2_seq1 = match resp2.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            // assigned_sequence verified in assertions below
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault2.value() }),
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
        caller: None,
    };

    let resp3 = write_client.write(request3).await.expect("write to vault 2 second");
    let v2_seq2 = match resp3.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            // assigned_sequence verified in assertions below
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
// Invariants 34-37: Vault failures are isolated
// =============================================================================

/// Invariant 34: A diverged vault does not affect other vaults.
///
/// This test verifies that when one vault's state root diverges (perhaps due to
/// a bug or corruption), other vaults continue to operate normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_divergence_does_not_affect_other_vaults() {
    // Use 1-node cluster to simplify debugging
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and two vaults
    let organization = create_organization(leader.addr, "vault-isolation-ns", leader)
        .await
        .expect("create organization");
    let vault1 = create_vault(leader.addr, organization).await.expect("create vault 1");
    let vault2 = create_vault(leader.addr, organization).await.expect("create vault 2");

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write to vault 1
    let request1 = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "vault-isolation-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault1.value() }),
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
        caller: None,
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault2.value() }),
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
        caller: None,
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault1.value() }),
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
        create_health_client(leader.addr).await.expect("connect to health service");

    let vault1_health = health_client
        .check(inferadb_ledger_proto::proto::HealthCheckRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault1.value() }),
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
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault2.value() }),
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
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault2.value() }),
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
        caller: None,
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

/// Invariant 35: Diverged vault returns UNAVAILABLE for reads.
///
/// When a vault's computed state root doesn't match the expected root,
/// the vault should be marked as diverged and return UNAVAILABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_diverged_vault_returns_unavailable() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(leader.addr, "divergence-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    let mut write_client =
        common::create_write_client(leader.addr).await.expect("connect to leader");

    // Write some data to establish a vault
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "divergence-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
    };

    write_client.write(request).await.expect("write should succeed");

    // Wait for replication (global + data region)
    cluster.wait_for_sync(Duration::from_secs(5)).await;
    cluster.wait_for_data_region_sync(Duration::from_secs(5)).await;

    // Simulate vault divergence using the admin API
    let mut admin_client =
        common::create_admin_client(leader.addr).await.expect("connect to admin service");

    let divergence_request = inferadb_ledger_proto::proto::SimulateDivergenceRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        create_health_client(leader.addr).await.expect("connect to health service");

    let health_response = health_client
        .check(inferadb_ledger_proto::proto::HealthCheckRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        })
        .await
        .expect("health check should succeed");

    assert_eq!(
        health_response.into_inner().status(),
        inferadb_ledger_proto::proto::HealthStatus::Unavailable,
        "Vault should be marked as UNAVAILABLE (diverged)"
    );

    // Attempt to read from the diverged vault - should return UNAVAILABLE
    let mut read_client = create_read_client(leader.addr).await.expect("connect to read service");

    let read_request = inferadb_ledger_proto::proto::ReadRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        key: "divergence-key".to_string(),
        consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual.into(),
        caller: None,
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
// State commitment and bucket-based verification
// =============================================================================

/// Invariant 28: Followers verify state roots match leader.
///
/// When a follower applies log entries, it must verify that its computed
/// state root matches the state root included in the log entry from the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follower_state_root_verification() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(leader.addr, "state-root-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    // Wait for org/vault creation to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let mut client = common::create_write_client(leader.addr).await.expect("connect to leader");

    // Submit a write that will replicate to followers
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "state-root-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
    };

    client.write(request).await.expect("write should succeed");

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Verify all nodes have matching healthy status for this vault
    // If state roots didn't match, the vault would be marked as Diverged
    for node in cluster.nodes() {
        let mut health_client =
            create_health_client(node.addr).await.expect("connect to node for health check");

        let health_req = inferadb_ledger_proto::proto::HealthCheckRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
// Raft consensus with OpenRaft
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
async fn test_idempotency_survives_leader_failover() {
    let cluster = TestCluster::new(3).await;
    let original_leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let leader_addr = leader.addr;

    // Create organization and vault before writing
    let organization = create_organization(leader_addr, "failover-test-ns", leader)
        .await
        .expect("create organization");

    let mut vault_client =
        common::create_vault_client(leader_addr).await.expect("connect to vault service");

    let vault_response = vault_client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
            caller: None,
        })
        .await
        .expect("create vault");

    let vault = vault_response.into_inner().vault.map(|v| VaultSlug::new(v.slug)).expect("vault");

    // Wait for organization/vault to replicate
    cluster.wait_for_sync(Duration::from_secs(2)).await;

    let mut write_client =
        common::create_write_client(leader_addr).await.expect("connect to leader");

    // Submit a write with a known idempotency key
    let idem_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "failover-test".to_string() }),
        idempotency_key: idem_key.clone(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
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
        caller: None,
    };

    let response1 = write_client.write(request.clone()).await.expect("first write");
    let _original_tx_id = match response1.into_inner().result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            s.tx_id.expect("should have tx_id")
        },
        other => panic!("first write should succeed, got: {:?}", other),
    };

    // Wait for replication to all followers before triggering failover (global + data region)
    cluster.wait_for_sync(Duration::from_secs(5)).await;
    cluster.wait_for_data_region_sync(Duration::from_secs(5)).await;

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

    // Wait for a new leader to be elected (with timeout).
    // Standard Raft (single-term-leader) may need extra election rounds on
    // split votes, and CI contention can delay timers — 20s is generous.
    let new_leader_id = tokio::time::timeout(Duration::from_secs(20), async {
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

    // Wait for data regions to elect new leaders after the old node departed.
    // The departed node was the data region leader — remaining nodes need to
    // hold a new election (election timeout ~150-300ms in test config).
    cluster.wait_for_leaders(Duration::from_secs(10)).await;
    cluster.wait_for_data_region_sync(Duration::from_secs(10)).await;

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
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_s)) => {
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
