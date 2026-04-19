//! Integration tests for the `CheckRelationship` RPC on `ReadService`.
//!
//! Covers the core existence/absence paths and input validation. Follower-
//! redirect tests are omitted — there is no `follower_client` helper in the
//! common harness and the SDK's `RegionLeaderCache` transparent-redirect path
//! is already exercised by `redirect_routing.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use crate::common::{
    TestCluster, create_read_client, create_test_organization, create_test_vault,
    create_write_client,
};

// ============================================================================
// Helpers
// ============================================================================

/// Seeds a single `CreateRelationship` operation into `vault` via the write
/// service and waits for the block to commit.
async fn seed_relationship(
    addr: &str,
    organization: inferadb_ledger_types::OrganizationSlug,
    vault: inferadb_ledger_types::VaultSlug,
    resource: &str,
    relation: &str,
    subject: &str,
) {
    let mut client = create_write_client(addr).await.expect("connect write client");

    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "check-rel-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::CreateRelationship(
                inferadb_ledger_proto::proto::CreateRelationship {
                    resource: resource.to_string(),
                    relation: relation.to_string(),
                    subject: subject.to_string(),
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request).await.expect("write should succeed").into_inner();

    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(err)) => {
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
#[tokio::test]
async fn check_relationship_returns_true_for_existing_tuple() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = create_test_organization(&leader.addr, "check-rel-exists-org", leader)
        .await
        .expect("create organization")
        .0;
    let vault = create_test_vault(&leader.addr, organization).await.expect("create vault");

    seed_relationship(&leader.addr, organization, vault, "doc:1", "viewer", "user:42").await;

    let mut read_client = create_read_client(&leader.addr).await.expect("connect read client");

    let response = read_client
        .check_relationship(tonic::Request::new(
            inferadb_ledger_proto::proto::CheckRelationshipRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:42".to_string(),
                consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual as i32,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: 1 }),
            },
        ))
        .await
        .expect("check_relationship should succeed")
        .into_inner();

    assert!(response.exists, "exists should be true for seeded tuple");
    assert!(response.checked_at_height > 0, "checked_at_height should be > 0 after a write");
}

/// `CheckRelationship` returns `exists = false` when no tuple has been written
/// for the given triple.
#[tokio::test]
async fn check_relationship_returns_false_for_missing_tuple() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = create_test_organization(&leader.addr, "check-rel-missing-org", leader)
        .await
        .expect("create organization")
        .0;
    let vault = create_test_vault(&leader.addr, organization).await.expect("create vault");

    // No tuples seeded.
    let mut read_client = create_read_client(&leader.addr).await.expect("connect read client");

    let response = read_client
        .check_relationship(tonic::Request::new(
            inferadb_ledger_proto::proto::CheckRelationshipRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
                resource: "doc:nonexistent".to_string(),
                relation: "viewer".to_string(),
                subject: "user:42".to_string(),
                consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual as i32,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: 1 }),
            },
        ))
        .await
        .expect("check_relationship should succeed")
        .into_inner();

    assert!(!response.exists, "exists should be false when no tuple has been written");
}

/// `CheckRelationship` respects tuple exactness: a stored `(doc:1, viewer,
/// user:42)` tuple does NOT satisfy a check for `(doc:1, editor, user:42)`.
#[tokio::test]
async fn check_relationship_partial_match_returns_false() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = create_test_organization(&leader.addr, "check-rel-partial-org", leader)
        .await
        .expect("create organization")
        .0;
    let vault = create_test_vault(&leader.addr, organization).await.expect("create vault");

    // Seed only the "viewer" relation — not "editor".
    seed_relationship(&leader.addr, organization, vault, "doc:1", "viewer", "user:42").await;

    let mut read_client = create_read_client(&leader.addr).await.expect("connect read client");

    let response = read_client
        .check_relationship(tonic::Request::new(
            inferadb_ledger_proto::proto::CheckRelationshipRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
                resource: "doc:1".to_string(),
                relation: "editor".to_string(), // different relation — should not match
                subject: "user:42".to_string(),
                consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual as i32,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: 1 }),
            },
        ))
        .await
        .expect("check_relationship should succeed")
        .into_inner();

    assert!(!response.exists, "exists should be false when only a different relation is stored");
}

/// `CheckRelationship` rejects a subject string that exceeds the 1 024-byte
/// `max_relationship_string_bytes` limit with `INVALID_ARGUMENT`.
#[tokio::test]
async fn check_relationship_rejects_oversize_subject() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let organization = create_test_organization(&leader.addr, "check-rel-oversize-org", leader)
        .await
        .expect("create organization")
        .0;
    let vault = create_test_vault(&leader.addr, organization).await.expect("create vault");

    let oversize_subject = format!("u:{}", "x".repeat(1025));

    let mut read_client = create_read_client(&leader.addr).await.expect("connect read client");

    let result = read_client
        .check_relationship(tonic::Request::new(
            inferadb_ledger_proto::proto::CheckRelationshipRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: oversize_subject,
                consistency: inferadb_ledger_proto::proto::ReadConsistency::Eventual as i32,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: 1 }),
            },
        ))
        .await;

    let status = result.expect_err("oversize subject should be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "expected INVALID_ARGUMENT for oversize subject, got {:?}: {}",
        status.code(),
        status.message()
    );
}
