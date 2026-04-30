//! Integration tests for the `CheckRelationship` RPC on `ReadService`.
//!
//! Covers the core existence/absence paths and input validation. Follower-
//! redirect tests are omitted — there is no `follower_client` helper in the
//! common harness and the SDK's `RegionLeaderCache` transparent-redirect path
//! is already exercised by `redirect_routing.rs`.
//!
//! F.1.f.2.Stage1e Wave 3: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use bytes::Bytes;
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{read as wr, shared as ws, write as ww},
};
use inferadb_ledger_wire_transport::RpcError;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Helpers
// ============================================================================

/// Seeds a single `CreateRelationship` operation into `vault` via the wire
/// write service and waits for the block to commit.
async fn seed_relationship(
    cluster: &TestCluster,
    node_id: u64,
    organization: inferadb_ledger_types::OrganizationSlug,
    vault: inferadb_ledger_types::VaultSlug,
    resource: &str,
    relation: &str,
    subject: &str,
) {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "check-rel-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::CreateRelationship(ws::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response =
        client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    match response.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        Some(ww::WriteResponseResult::Error(err)) => {
            panic!("seed_relationship write failed: {:?}", err);
        },
        None => panic!("seed_relationship: no result in response"),
    }
}

// ============================================================================
// Tests
// ============================================================================

/// `CheckRelationship` returns `exists = true` and `checked_at_height > 0`
/// when the tuple has been written to the vault.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_relationship_returns_true_for_existing_tuple() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = wire_create_test_organization(&cluster, leader.id, "check-rel-exists-org")
        .await
        .expect("create organization")
        .0;
    let vault =
        wire_create_test_vault(&cluster, leader.id, organization).await.expect("create vault");

    seed_relationship(&cluster, leader.id, organization, vault, "doc:1", "viewer", "user:42").await;

    let read_client = wire_read_client(&cluster, leader.id);

    let response = read_client
        .check_relationship(
            wr::CheckRelationshipRequest {
                organization: Some(organization),
                vault: Some(vault),
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:42".to_string(),
                consistency: ws::ReadConsistency::Eventual,
                caller: Some(ws::UserSlug::new(1)),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("check_relationship should succeed");

    assert!(response.exists, "exists should be true for seeded tuple");
    assert!(response.checked_at_height > 0, "checked_at_height should be > 0 after a write");
}

/// `CheckRelationship` returns `exists = false` when no tuple has been written
/// for the given triple.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_relationship_returns_false_for_missing_tuple() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = wire_create_test_organization(&cluster, leader.id, "check-rel-missing-org")
        .await
        .expect("create organization")
        .0;
    let vault =
        wire_create_test_vault(&cluster, leader.id, organization).await.expect("create vault");

    // No tuples seeded.
    let read_client = wire_read_client(&cluster, leader.id);

    let response = read_client
        .check_relationship(
            wr::CheckRelationshipRequest {
                organization: Some(organization),
                vault: Some(vault),
                resource: "doc:nonexistent".to_string(),
                relation: "viewer".to_string(),
                subject: "user:42".to_string(),
                consistency: ws::ReadConsistency::Eventual,
                caller: Some(ws::UserSlug::new(1)),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("check_relationship should succeed");

    assert!(!response.exists, "exists should be false when no tuple has been written");
}

/// `CheckRelationship` respects tuple exactness: a stored `(doc:1, viewer,
/// user:42)` tuple does NOT satisfy a check for `(doc:1, editor, user:42)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_relationship_partial_match_returns_false() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = wire_create_test_organization(&cluster, leader.id, "check-rel-partial-org")
        .await
        .expect("create organization")
        .0;
    let vault =
        wire_create_test_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Seed only the "viewer" relation — not "editor".
    seed_relationship(&cluster, leader.id, organization, vault, "doc:1", "viewer", "user:42").await;

    let read_client = wire_read_client(&cluster, leader.id);

    let response = read_client
        .check_relationship(
            wr::CheckRelationshipRequest {
                organization: Some(organization),
                vault: Some(vault),
                resource: "doc:1".to_string(),
                relation: "editor".to_string(), // different relation — should not match
                subject: "user:42".to_string(),
                consistency: ws::ReadConsistency::Eventual,
                caller: Some(ws::UserSlug::new(1)),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("check_relationship should succeed");

    assert!(!response.exists, "exists should be false when only a different relation is stored");
}

/// `CheckRelationship` rejects a subject string that exceeds the 1 024-byte
/// `max_relationship_string_bytes` limit with `INVALID_ARGUMENT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_relationship_rejects_oversize_subject() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = wire_create_test_organization(&cluster, leader.id, "check-rel-oversize-org")
        .await
        .expect("create organization")
        .0;
    let vault =
        wire_create_test_vault(&cluster, leader.id, organization).await.expect("create vault");

    let oversize_subject = format!("u:{}", "x".repeat(1025));

    let read_client = wire_read_client(&cluster, leader.id);

    let result = read_client
        .check_relationship(
            wr::CheckRelationshipRequest {
                organization: Some(organization),
                vault: Some(vault),
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: oversize_subject,
                consistency: ws::ReadConsistency::Eventual,
                caller: Some(ws::UserSlug::new(1)),
            },
            rand::random::<u128>(),
        )
        .await;

    let err = result.expect_err("oversize subject should be rejected");
    match err {
        RpcError::WireError(wire_err) => {
            assert_eq!(
                wire_err.code,
                ErrorCode::InvalidArgument,
                "expected InvalidArgument for oversize subject, got {:?}: {}",
                wire_err.code,
                wire_err.message,
            );
        },
        other => panic!("expected WireError(InvalidArgument), got: {other:?}"),
    }
}
