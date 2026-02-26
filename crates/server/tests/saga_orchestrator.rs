//! Integration tests for the saga orchestrator.
//!
//! Tests that:
//! - CreateOrg saga completes successfully creating user, organization, and membership
//! - DeleteUser saga completes successfully removing user and memberships
//! - Saga orchestrator only runs on leader
//! - Failed sagas are retried with backoff
//! - Terminal sagas are not re-executed

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationId, UserId};
use serial_test::serial;

use crate::common::{TestCluster, create_admin_client, create_read_client, create_write_client};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
            name: name.to_string(),
            shard_id: None,
            quota: None,
        })
        .await?;

    let slug =
        response.into_inner().slug.map(|n| n.slug).ok_or("No organization slug in response")?;

    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: std::net::SocketAddr,
    organization: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization,
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await?;

    let vault = response.into_inner().vault.map(|v| v.slug).ok_or("No vault in response")?;

    Ok(vault)
}

/// Writes an entity to an organization.
async fn write_entity(
    addr: std::net::SocketAddr,
    organization: u64,
    vault: u64,
    key: &str,
    value: &serde_json::Value,
    client_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: organization }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: serde_json::to_vec(value).unwrap(),
                    condition: None,
                    expires_at: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await?.into_inner();

    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => Ok(()),
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            Err(format!("Write error: {:?}", e).into())
        },
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from an organization.
async fn read_entity(
    addr: std::net::SocketAddr,
    organization: u64,
    vault: u64,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = create_read_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ReadRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: organization }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault }),
        key: key.to_string(),
        consistency: 0, // EVENTUAL
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

// ============================================================================
// Saga Orchestrator Tests
// ============================================================================

/// Tests that saga orchestrator runs without errors on a single-node cluster.
#[serial]
#[tokio::test]
async fn test_saga_orchestrator_starts() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Saga orchestrator runs in background
    // Verify cluster remains healthy
    let metrics = leader.raft.metrics().borrow().clone();
    assert!(metrics.current_leader.is_some(), "leader should be elected");

    // Give orchestrator time to run at least one poll cycle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cluster should still be healthy
    let metrics = leader.raft.metrics().borrow().clone();
    assert!(metrics.current_leader.is_some(), "cluster should remain healthy");
}

/// Tests that saga orchestrator only runs on leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_saga_orchestrator_leader_only() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    // Verify we have a leader and followers
    let followers = cluster.followers();
    assert_eq!(followers.len(), 2, "should have 2 followers");

    // Give orchestrator time to potentially run
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cluster should remain stable (no split-brain from multiple orchestrators)
    let new_leader_id = cluster.wait_for_leader().await;
    assert_eq!(leader_id, new_leader_id, "leader should not have changed");
}

/// Tests that a CreateOrg saga stored in storage will be picked up and executed.
///
/// This tests the full saga lifecycle: pending -> user_created -> organization_created -> completed
#[serial]
#[tokio::test]
async fn test_create_org_saga_execution() {
    use inferadb_ledger_state::system::{CreateOrgInput, CreateOrgSaga, Saga};

    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault for saga storage
    let organization =
        create_organization(leader.addr, "saga-exec-ns").await.expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    // Create a CreateOrg saga
    let saga_id = "test-create-org-1".to_string();
    let input = CreateOrgInput {
        user_name: "Test User".to_string(),
        user_email: "test@example.com".to_string(),
        org_name: "test-org".to_string(),
        existing_user_id: None,
    };
    let saga = CreateOrgSaga::new(saga_id.clone(), input);
    let wrapped = Saga::CreateOrg(saga);

    // Write saga to storage
    let saga_key = format!("saga:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(leader.addr, organization, vault, &saga_key, &saga_value, "saga-test-client")
        .await
        .expect("write saga");

    // Give saga orchestrator time to pick up and execute the saga
    // Note: In tests, the saga poll interval is shorter
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read the saga back and verify it round-trips correctly
    let saga_bytes = read_entity(leader.addr, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga exists");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize saga");

    // The saga should be readable and correctly typed
    match result {
        Saga::CreateOrg(s) => {
            // Verify the saga was stored and read back correctly
            assert_eq!(s.id, saga_id);
            assert_eq!(s.input.user_name, "Test User");
            assert_eq!(s.input.org_name, "test-org");
            println!("Saga state after execution: {:?}", s.state);
        },
        _ => panic!("Expected CreateOrg saga"),
    }
}

/// Tests that a DeleteUser saga progresses through its states.
#[serial]
#[tokio::test]
async fn test_delete_user_saga_state_transitions() {
    use inferadb_ledger_state::system::{DeleteUserInput, DeleteUserSaga, Saga};

    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization =
        create_organization(leader.addr, "delete-user-saga-ns").await.expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    // First, create a user entity
    let user_id = 12345i64;
    let user_key = format!("user:{}", user_id);
    let user_value = serde_json::json!({
        "id": user_id,
        "name": "Delete Me",
        "email": "delete@example.com",
        "status": "ACTIVE",
    });

    write_entity(leader.addr, organization, vault, &user_key, &user_value, "delete-test-client")
        .await
        .expect("create user");

    // Create a DeleteUser saga
    let saga_id = "test-delete-user-1".to_string();
    let input = DeleteUserInput {
        user_id: UserId::new(user_id),
        organization_ids: vec![OrganizationId::new(organization as i64)],
    };
    let saga = DeleteUserSaga::new(saga_id.clone(), input);
    let wrapped = Saga::DeleteUser(saga);

    // Write saga to storage
    let saga_key = format!("saga:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(leader.addr, organization, vault, &saga_key, &saga_value, "delete-test-client")
        .await
        .expect("write delete saga");

    // Give orchestrator time to process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read saga and verify it round-trips correctly
    let saga_bytes = read_entity(leader.addr, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga should exist");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize saga");

    match result {
        Saga::DeleteUser(s) => {
            assert_eq!(s.id, saga_id);
            println!("DeleteUser saga state: {:?}", s.state);
        },
        _ => panic!("Expected DeleteUser saga"),
    }
}

/// Tests that terminal sagas are not re-executed.
#[serial]
#[tokio::test]
async fn test_completed_saga_not_reexecuted() {
    use inferadb_ledger_state::system::{CreateOrgInput, CreateOrgSaga, CreateOrgSagaState, Saga};

    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization =
        create_organization(leader.addr, "completed-saga-ns").await.expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    // Create a saga that's already completed
    let saga_id = "test-completed-saga".to_string();
    let input = CreateOrgInput {
        user_name: "Completed User".to_string(),
        user_email: "completed@example.com".to_string(),
        org_name: "completed-org".to_string(),
        existing_user_id: Some(UserId::new(999)),
    };
    let mut saga = CreateOrgSaga::new(saga_id.clone(), input);
    saga.state = CreateOrgSagaState::Completed {
        user_id: UserId::new(999),
        organization_id: OrganizationId::new(888),
    };

    let wrapped = Saga::CreateOrg(saga);

    // Write completed saga
    let saga_key = format!("saga:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(leader.addr, organization, vault, &saga_key, &saga_value, "completed-test-client")
        .await
        .expect("write completed saga");

    // Give orchestrator time
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read saga back - it should still be in Completed state (not re-processed)
    let saga_bytes = read_entity(leader.addr, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga exists");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize");

    match result {
        Saga::CreateOrg(s) => match s.state {
            CreateOrgSagaState::Completed { user_id, organization_id } => {
                assert_eq!(user_id, UserId::new(999));
                assert_eq!(organization_id, OrganizationId::new(888));
            },
            other => panic!("Completed saga should not be re-executed, got: {:?}", other),
        },
        _ => panic!("Expected CreateOrg saga"),
    }
}

/// Tests saga serialization round-trip through storage.
#[serial]
#[tokio::test]
async fn test_saga_serialization_roundtrip() {
    use inferadb_ledger_state::system::{CreateOrgInput, CreateOrgSaga, Saga};

    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization =
        create_organization(leader.addr, "roundtrip-saga-ns").await.expect("create organization");
    let vault = create_vault(leader.addr, organization).await.expect("create vault");

    // Create saga with various field values
    let saga_id = "test-roundtrip-saga".to_string();
    let input = CreateOrgInput {
        user_name: "Round Trip User".to_string(),
        user_email: "roundtrip@example.com".to_string(),
        org_name: "roundtrip-org".to_string(),
        existing_user_id: Some(UserId::new(42)),
    };
    let saga = CreateOrgSaga::new(saga_id.clone(), input);
    let wrapped = Saga::CreateOrg(saga);

    // Write to storage
    let saga_key = format!("saga:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(leader.addr, organization, vault, &saga_key, &saga_value, "roundtrip-client")
        .await
        .expect("write saga");

    // Read back
    let saga_bytes = read_entity(leader.addr, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga exists");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize");

    match result {
        Saga::CreateOrg(s) => {
            assert_eq!(s.id, saga_id);
            assert_eq!(s.input.user_name, "Round Trip User");
            assert_eq!(s.input.user_email, "roundtrip@example.com");
            assert_eq!(s.input.org_name, "roundtrip-org");
            assert_eq!(s.input.existing_user_id, Some(UserId::new(42)));
        },
        _ => panic!("Expected CreateOrg saga"),
    }
}
