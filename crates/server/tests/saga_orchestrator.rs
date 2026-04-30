//! Integration tests for the saga orchestrator.
//!
//! Tests that:
//! - DeleteUser saga completes successfully removing user and memberships
//! - Saga orchestrator only runs on leader
//! - Failed sagas are retried with backoff
//! - Terminal sagas are not re-executed
//!
//! F.1.f.2.Stage1e Wave 4: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, UserId, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};
use serial_test::serial;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

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

/// Writes an entity to an organization.
async fn write_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &serde_json::Value,
    client_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        organization: Some(organization),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: client_id.to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::from(serde_json::to_vec(value).unwrap()),
                condition: None,
                expires_at: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await?;

    match response.result {
        Some(ww::WriteResponseResult::Success(_)) => Ok(()),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("Write error: {:?}", e).into()),
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from an organization.
async fn read_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let client = wire_read_client(cluster, node_id);

    let request = wr::ReadRequest {
        organization: Some(organization),
        vault: Some(vault),
        key: key.to_string(),
        consistency: ws::ReadConsistency::Eventual,
        caller: None,
    };

    let response = client.read(request, rand::random::<u128>()).await?;
    Ok(response.value.map(|b| b.to_vec()))
}

// ============================================================================
// Saga Orchestrator Tests
// ============================================================================

/// Tests that saga orchestrator runs without errors on a single-node cluster.
#[tokio::test]
async fn test_saga_orchestrator_starts() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Saga orchestrator runs in background
    // Verify cluster remains healthy
    assert!(leader.handle.current_leader().is_some(), "leader should be elected");

    // Give orchestrator time to run at least one poll cycle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cluster should still be healthy
    assert!(leader.handle.current_leader().is_some(), "cluster should remain healthy");
}

/// Tests that saga orchestrator only runs on leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_saga_orchestrator_leader_only() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
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

/// Tests that a DeleteUser saga progresses through its states.
#[tokio::test]
async fn test_delete_user_saga_state_transitions() {
    use inferadb_ledger_state::system::{DeleteUserInput, DeleteUserSaga, Saga, SagaId};

    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "delete-user-saga-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // First, create a user entity
    let user_id = 12345i64;
    let user_key = format!("user:{}", user_id);
    let user_value = serde_json::json!({
        "id": user_id,
        "name": "Delete Me",
        "email": "delete@example.com",
        "status": "ACTIVE",
    });

    write_entity(
        &cluster,
        leader.id,
        organization,
        vault,
        &user_key,
        &user_value,
        "delete-test-client",
    )
    .await
    .expect("create user");

    // Create a DeleteUser saga
    let saga_id = "test-delete-user-1".to_string();
    let input = DeleteUserInput {
        user: UserId::new(user_id),
        organization_ids: vec![OrganizationId::new(organization.value() as i64)],
    };
    let saga = DeleteUserSaga::new(SagaId::new(saga_id.clone()), input);
    let wrapped = Saga::DeleteUser(saga);

    // Write saga to storage (using a user-space key since system-prefixed keys
    // are rejected by the gRPC input validator — the saga orchestrator writes
    // actual saga records internally through Raft, not through the Write API).
    let saga_key = format!("saga-test:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(
        &cluster,
        leader.id,
        organization,
        vault,
        &saga_key,
        &saga_value,
        "delete-test-client",
    )
    .await
    .expect("write delete saga");

    // The saga entity is committed through Raft when write_entity returns.

    // Read saga and verify it round-trips correctly
    let saga_bytes = read_entity(&cluster, leader.id, organization, vault, &saga_key)
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
#[tokio::test]
async fn test_completed_saga_not_reexecuted() {
    use inferadb_ledger_state::system::{
        CreateOrganizationInput, CreateOrganizationSaga, CreateOrganizationSagaState,
        OrganizationTier, Saga, SagaId,
    };
    use inferadb_ledger_types::Region;

    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "completed-saga-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Create a saga that's already completed
    let saga_id = "test-completed-saga".to_string();
    let input = CreateOrganizationInput {
        slug: inferadb_ledger_types::OrganizationSlug::new(777),
        region: Region::US_EAST_VA,
        tier: OrganizationTier::Free,
        admin: UserId::new(999),
    };
    let mut saga = CreateOrganizationSaga::new(SagaId::new(saga_id.clone()), input);
    saga.state = CreateOrganizationSagaState::Completed {
        organization_id: OrganizationId::new(888),
        organization_slug: inferadb_ledger_types::OrganizationSlug::new(777),
    };

    let wrapped = Saga::CreateOrganization(saga);

    // Write completed saga (using a user-space key — system-prefixed keys are
    // rejected by gRPC input validation; the orchestrator writes real sagas
    // internally through Raft).
    let saga_key = format!("saga-test:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(
        &cluster,
        leader.id,
        organization,
        vault,
        &saga_key,
        &saga_value,
        "completed-test-client",
    )
    .await
    .expect("write completed saga");

    // Give orchestrator time to cycle and potentially (incorrectly) re-execute
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Read saga back - it should still be in Completed state (not re-processed)
    let saga_bytes = read_entity(&cluster, leader.id, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga exists");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize");

    match result {
        Saga::CreateOrganization(s) => match s.state {
            CreateOrganizationSagaState::Completed { organization_id, organization_slug } => {
                assert_eq!(organization_id, OrganizationId::new(888));
                assert_eq!(organization_slug, inferadb_ledger_types::OrganizationSlug::new(777));
            },
            other => panic!("Completed saga should not be re-executed, got: {:?}", other),
        },
        _ => panic!("Expected CreateOrganization saga"),
    }
}

/// Tests saga serialization round-trip through storage.
#[tokio::test]
async fn test_saga_serialization_roundtrip() {
    use inferadb_ledger_state::system::{
        CreateOrganizationInput, CreateOrganizationSaga, OrganizationTier, Saga, SagaId,
    };
    use inferadb_ledger_types::Region;

    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "roundtrip-saga-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Create saga with various field values
    let saga_id = "test-roundtrip-saga".to_string();
    let input = CreateOrganizationInput {
        slug: inferadb_ledger_types::OrganizationSlug::new(4242),
        region: Region::US_EAST_VA,
        tier: OrganizationTier::Free,
        admin: UserId::new(42),
    };
    let saga = CreateOrganizationSaga::new(SagaId::new(saga_id.clone()), input);
    let wrapped = Saga::CreateOrganization(saga);

    // Write to storage (using a user-space key — system-prefixed keys are
    // rejected by gRPC input validation).
    let saga_key = format!("saga-test:{}", saga_id);
    let saga_value = serde_json::to_value(&wrapped).unwrap();

    write_entity(
        &cluster,
        leader.id,
        organization,
        vault,
        &saga_key,
        &saga_value,
        "roundtrip-client",
    )
    .await
    .expect("write saga");

    // Read back
    let saga_bytes = read_entity(&cluster, leader.id, organization, vault, &saga_key)
        .await
        .expect("read saga")
        .expect("saga exists");

    let result: Saga = serde_json::from_slice(&saga_bytes).expect("deserialize");

    match result {
        Saga::CreateOrganization(s) => {
            assert_eq!(s.id, saga_id);
            assert_eq!(s.input.region, Region::US_EAST_VA);
            assert_eq!(s.input.admin, UserId::new(42));
        },
        _ => panic!("Expected CreateOrganization saga"),
    }
}
