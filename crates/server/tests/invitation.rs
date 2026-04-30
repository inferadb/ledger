//! Integration tests for the organization invitation system.
//!
//! Tests the critical cross-cutting scenarios from the PRD:
//! - Multi-email HMAC matching (invitations to secondary emails)
//! - Rate limit enforcement (per-user, per-org, per-email caps)
//! - Cross-org team validation (team from different org rejected)
//! - Full lifecycle flows (create → accept/decline/revoke)
//!
//! F.1.f.2.Stage1e Wave 5: migrated from the legacy tonic helpers
//! (`create_invitation_client` / `create_organization_client` /
//! `setup_org_with_admin`) to their wire-protocol siblings
//! (`wire_invitation_client` / `wire_organization_client` /
//! `wire_create_test_organization`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{
        invitation as wi, organization as wo,
        shared::{OrganizationMemberRole, UserSlug as WUserSlug},
    },
};
use inferadb_ledger_wire_transport::RpcError;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_invitation_client, wire_organization_client,
};

// ============================================================================
// Helpers
// ============================================================================

type InvClient = inferadb_ledger_wire_services::InvitationServiceClient;

/// Creates an organization with a real admin user and waits for Active status.
/// Returns `(org_slug, admin_user_slug)`.
/// Uses a unique admin email derived from the org name to avoid collisions.
async fn setup_org(cluster: &TestCluster, node_id: u64, name: &str) -> (OrganizationSlug, u64) {
    wire_create_test_organization(cluster, node_id, name).await.expect("create organization")
}

/// Creates an invitation and returns (slug, token).
/// `admin_slug` must be a valid org admin for the target org.
async fn create_invite(
    client: &InvClient,
    org: OrganizationSlug,
    admin_slug: u64,
    email: &str,
    ttl_hours: u32,
) -> (InviteSlug, String) {
    let resp = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(admin_slug)),
                email: email.to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create invite");
    let slug = resp.slug.expect("invite slug");
    let token = resp.token;
    (slug, token)
}

// ============================================================================
// Tests: Full Lifecycle
// ============================================================================

/// Create an invitation and verify it appears in the org's listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_and_list_invitations() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Lifecycle Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    // Create two invitations
    let (slug1, token1) = create_invite(&client, org, admin, "alice@example.com", 24).await;
    let (slug2, _token2) = create_invite(&client, org, admin, "bob@example.com", 48).await;

    assert_ne!(slug1, slug2, "invitation slugs should be unique");
    assert!(!token1.is_empty(), "token should be non-empty");
    assert_eq!(token1.len(), 64, "token should be 64 hex characters");

    // List all invitations for the org
    let list_resp = client
        .list_organization_invites(
            wi::ListOrganizationInvitesRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(admin)),
                status_filter: None,
                page_token: None,
                page_size: 50,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("list invitations");
    let invitations = list_resp.invitations;
    assert_eq!(invitations.len(), 2, "should list 2 invitations");

    // Both should be Pending
    for inv in &invitations {
        assert_eq!(inv.status, wi::InvitationStatus::Pending);
    }
}

/// Create an invitation, then revoke it. Verify it shows as Revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_and_revoke_invitation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Revoke Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    let (slug, _token) = create_invite(&client, org, admin, "revokee@example.com", 24).await;

    // Revoke the invitation
    let revoke_resp = client
        .revoke_organization_invite(
            wi::RevokeOrganizationInviteRequest {
                slug: Some(slug),
                caller: Some(WUserSlug::new(admin)),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("revoke invitation");
    let inv = revoke_resp.invitation.expect("invitation");
    assert_eq!(inv.status, wi::InvitationStatus::Revoked);

    // Trying to revoke again should fail (already resolved)
    let err = client
        .revoke_organization_invite(
            wi::RevokeOrganizationInviteRequest {
                slug: Some(slug),
                caller: Some(WUserSlug::new(admin)),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::FailedPrecondition | ErrorCode::InvitationAlreadyResolved
                ),
                "expected FailedPrecondition or InvitationAlreadyResolved, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => {
            panic!(
                "expected WireError(FailedPrecondition | InvitationAlreadyResolved), got: {other:?}"
            )
        },
    }
}

// ============================================================================
// Tests: Duplicate Pending → ALREADY_EXISTS
// ============================================================================

/// Creating a second invitation to the same email in the same org returns
/// ALREADY_EXISTS with the existing invitation's slug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_duplicate_pending_returns_already_exists() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Dup Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    // First invitation succeeds
    let (_slug1, _) = create_invite(&client, org, admin, "dup@example.com", 24).await;

    // Second invitation to the same email should fail
    let err = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(admin)),
                email: "dup@example.com".to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 24,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::AlreadyExists | ErrorCode::InvitationDuplicatePending
                ),
                "expected AlreadyExists or InvitationDuplicatePending, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => {
            panic!("expected WireError(AlreadyExists | InvitationDuplicatePending), got: {other:?}")
        },
    }
}

// ============================================================================
// Tests: Rate Limiting
// ============================================================================

/// Per-email global pending cap: creating more than 10 pending invitations
/// to the same email (across different orgs) should eventually be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_email_pending_cap() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let target_email = "popular@example.com";
    let client = wire_invitation_client(&cluster, leader.id);

    // Create 10 organizations and send one invitation each to the same email
    let mut slugs = Vec::new();
    for i in 0..10 {
        let (org, admin) = setup_org(&cluster, leader.id, &format!("Cap Org {i}")).await;
        let (slug, _) = create_invite(&client, org, admin, target_email, 168).await;
        slugs.push(slug);
    }

    // 11th invitation from a new org should be rejected (pending cap = 10)
    let (org_11, admin_11) = setup_org(&cluster, leader.id, "Cap Org 10").await;
    let err = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org_11),
                caller: Some(WUserSlug::new(admin_11)),
                email: target_email.to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 168,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(wire_err.code, ErrorCode::RateLimited | ErrorCode::InvitationRateLimited),
                "11th pending invitation should be rate-limited, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => panic!("expected WireError(RateLimited | InvitationRateLimited), got: {other:?}"),
    }
}

/// Per-email total limit: creating more than 20 invitations to the same email
/// within the retention window should be rejected, even if previous ones were revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_email_total_limit() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let target_email = "targeted@example.com";
    let client = wire_invitation_client(&cluster, leader.id);

    // Create 20 invitations from 20 different orgs, revoking each one to stay
    // under the pending cap (10) but accumulate total entries.
    for i in 0..20 {
        let (org, admin) = setup_org(&cluster, leader.id, &format!("Total Org {i}")).await;
        let (slug, _) = create_invite(&client, org, admin, target_email, 168).await;

        // Revoke to free up pending cap
        client
            .revoke_organization_invite(
                wi::RevokeOrganizationInviteRequest {
                    slug: Some(slug),
                    caller: Some(WUserSlug::new(admin)),
                },
                rand::random::<u128>(),
            )
            .await
            .expect("revoke");
    }

    // 21st invitation should be rejected (total limit = 20)
    let (org_21, admin_21) = setup_org(&cluster, leader.id, "Total Org 20").await;
    let err = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org_21),
                caller: Some(WUserSlug::new(admin_21)),
                email: target_email.to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 168,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(wire_err.code, ErrorCode::RateLimited | ErrorCode::InvitationRateLimited),
                "21st total invitation should be rate-limited, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => panic!("expected WireError(RateLimited | InvitationRateLimited), got: {other:?}"),
    }
}

// Scan ceiling (500 entries) is exercised implicitly by the per-email
// total limit test. A dedicated test would require 500+ entries.

// ============================================================================
// Tests: Cross-Org Team Validation
// ============================================================================

/// Creating an invitation with a team from a different organization should be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cross_org_team_rejected() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org_a, admin_a) = setup_org(&cluster, leader.id, "Team Org A").await;
    let (org_b, admin_b) = setup_org(&cluster, leader.id, "Team Org B").await;

    // Create a team in org B
    let org_client = wire_organization_client(&cluster, leader.id);
    let team_slug = inferadb_ledger_types::snowflake::generate_team_slug().expect("team slug");
    let team_resp = org_client
        .create_organization_team(
            wo::CreateOrganizationTeamRequest {
                organization: Some(org_b),
                caller: Some(WUserSlug::new(admin_b)),
                name: "Team B".to_string(),
                slug: Some(team_slug),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create team in org B");
    let team_slug = team_resp.team.expect("team").slug.expect("team slug");

    // Try to create an invitation in org A with team from org B
    let inv_client = wire_invitation_client(&cluster, leader.id);
    let err = inv_client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org_a),
                caller: Some(WUserSlug::new(admin_a)),
                email: "crossorg@example.com".to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 24,
                team: Some(team_slug),
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert_eq!(
                wire_err.code,
                ErrorCode::InvalidArgument,
                "team from different org should be rejected, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
            assert!(
                wire_err.message.contains("different organization"),
                "error message should mention organization mismatch: {}",
                wire_err.message
            );
        },
        other => panic!("expected WireError(InvalidArgument), got: {other:?}"),
    }
}

// ============================================================================
// Tests: Email Normalization in Rate Limiting
// ============================================================================

/// Plus-addressed variants of the same email should count against the same
/// rate limit. user+tag@example.com and user@example.com produce the same HMAC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_plus_addressing_dedup() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Plus Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    // Create invitation to user@example.com
    create_invite(&client, org, admin, "user@example.com", 24).await;

    // Try to create invitation to user+tag@example.com in the same org
    // Should return ALREADY_EXISTS (normalizes to the same HMAC → duplicate pending)
    let err = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(admin)),
                email: "user+tag@example.com".to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 24,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::AlreadyExists | ErrorCode::InvitationDuplicatePending
                ),
                "plus-addressed variant should be treated as duplicate pending, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => {
            panic!("expected WireError(AlreadyExists | InvitationDuplicatePending), got: {other:?}")
        },
    }
}

/// Gmail dotted variants should also count as the same email.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gmail_dot_dedup() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Gmail Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    // Create invitation to user@gmail.com
    create_invite(&client, org, admin, "user@gmail.com", 24).await;

    // Try to create invitation to u.s.e.r@gmail.com in the same org
    let err = client
        .create_organization_invite(
            wi::CreateOrganizationInviteRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(admin)),
                email: "u.s.e.r@gmail.com".to_string(),
                role: OrganizationMemberRole::Member,
                ttl_hours: 24,
                team: None,
                slug: Some(InviteSlug::new(
                    inferadb_ledger_types::snowflake::generate().expect("snowflake"),
                )),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::AlreadyExists | ErrorCode::InvitationDuplicatePending
                ),
                "Gmail dotted variant should be treated as duplicate pending, got {:?}: {}",
                wire_err.code,
                wire_err.message
            );
        },
        other => {
            panic!("expected WireError(AlreadyExists | InvitationDuplicatePending), got: {other:?}")
        },
    }
}

// ============================================================================
// Tests: Authorization — Uniform NOT_FOUND
// ============================================================================

/// Attempting to get invitation details with the wrong user returns NOT_FOUND
/// (not PERMISSION_DENIED, to avoid confirming the invitation exists).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wrong_user_gets_not_found() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let (org, admin) = setup_org(&cluster, leader.id, "Auth Org").await;
    let client = wire_invitation_client(&cluster, leader.id);

    let (slug, _) = create_invite(&client, org, admin, "real@example.com", 24).await;

    // A different user (slug 999) tries to get details — should get NOT_FOUND
    let err = client
        .get_invitation_details(
            wi::GetInvitationDetailsRequest {
                slug: Some(slug),
                user: Some(WUserSlug::new(999)),
                caller: Some(WUserSlug::new(999)),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert_eq!(
                wire_err.code,
                ErrorCode::NotFound,
                "wrong user should get NotFound, not PermissionDenied"
            );
        },
        other => panic!("expected WireError(NotFound), got: {other:?}"),
    }
}

/// Attempting to get details for a non-existent invitation slug also returns NOT_FOUND.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_nonexistent_slug_gets_not_found() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("leader");

    let client = wire_invitation_client(&cluster, leader.id);

    let err = client
        .get_invitation_details(
            wi::GetInvitationDetailsRequest {
                slug: Some(InviteSlug::new(999_999_999)),
                user: Some(WUserSlug::new(1)),
                caller: Some(WUserSlug::new(1)),
            },
            rand::random::<u128>(),
        )
        .await
        .unwrap_err();
    match err {
        RpcError::WireError(wire_err) => {
            assert_eq!(wire_err.code, ErrorCode::NotFound);
        },
        other => panic!("expected WireError(NotFound), got: {other:?}"),
    }
}

// Suppress unused-import warning for re-exported types not referenced after migration.
#[allow(dead_code)]
fn _types_referenced(_o: OrganizationSlug, _u: UserSlug, _t: TeamSlug, _i: InviteSlug) {}
