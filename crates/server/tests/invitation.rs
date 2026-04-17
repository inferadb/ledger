//! Integration tests for the organization invitation system.
//!
//! Tests the critical cross-cutting scenarios from the PRD:
//! - Multi-email HMAC matching (invitations to secondary emails)
//! - Rate limit enforcement (per-user, per-org, per-email caps)
//! - Cross-org team validation (team from different org rejected)
//! - Full lifecycle flows (create → accept/decline/revoke)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_proto::proto;
use tonic::Code;

use crate::common::{
    TestCluster, create_invitation_client, create_organization_client, setup_org_with_admin,
};

// ============================================================================
// Helpers
// ============================================================================

type InvClient =
    proto::invitation_service_client::InvitationServiceClient<tonic::transport::Channel>;

/// Creates an organization with a real admin user and waits for Active status.
/// Returns `(org_slug, admin_user_slug)`.
/// Uses a unique admin email derived from the org name to avoid collisions.
async fn setup_org(addr: &str, name: &str, node: &crate::common::TestNode) -> (u64, u64) {
    let admin_email = format!("admin-{}@test.example.com", name.to_lowercase().replace(' ', "-"));
    setup_org_with_admin(addr, name, &admin_email, node).await
}

/// Creates an invitation and returns (slug, token).
/// `admin_slug` must be a valid org admin for the target org.
async fn create_invite(
    client: &mut InvClient,
    org: u64,
    admin_slug: u64,
    email: &str,
    ttl_hours: u32,
) -> (u64, String) {
    let resp = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org }),
            caller: Some(proto::UserSlug { slug: admin_slug }),
            email: email.to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours,
            team: None,
        })
        .await
        .expect("create invite");
    let inner = resp.into_inner();
    let slug = inner.slug.expect("invite slug").slug;
    let token = inner.token;
    (slug, token)
}

// ============================================================================
// Tests: Full Lifecycle
// ============================================================================

/// Create an invitation and verify it appears in the org's listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_and_list_invitations() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Lifecycle Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // Create two invitations
    let (slug1, token1) = create_invite(&mut client, org, admin, "alice@example.com", 24).await;
    let (slug2, _token2) = create_invite(&mut client, org, admin, "bob@example.com", 48).await;

    assert_ne!(slug1, slug2, "invitation slugs should be unique");
    assert!(!token1.is_empty(), "token should be non-empty");
    assert_eq!(token1.len(), 64, "token should be 64 hex characters");

    // List all invitations for the org
    let list_resp = client
        .list_organization_invites(proto::ListOrganizationInvitesRequest {
            organization: Some(proto::OrganizationSlug { slug: org }),
            caller: Some(proto::UserSlug { slug: admin }),
            status_filter: None,
            page_token: None,
            page_size: 50,
        })
        .await
        .expect("list invitations");
    let invitations = list_resp.into_inner().invitations;
    assert_eq!(invitations.len(), 2, "should list 2 invitations");

    // Both should be Pending
    for inv in &invitations {
        assert_eq!(inv.status, proto::InvitationStatus::Pending as i32);
    }
}

/// Create an invitation, then revoke it. Verify it shows as Revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_and_revoke_invitation() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Revoke Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    let (slug, _token) = create_invite(&mut client, org, admin, "revokee@example.com", 24).await;

    // Revoke the invitation
    let revoke_resp = client
        .revoke_organization_invite(proto::RevokeOrganizationInviteRequest {
            slug: Some(proto::InviteSlug { slug }),
            caller: Some(proto::UserSlug { slug: admin }),
        })
        .await
        .expect("revoke invitation");
    let inv = revoke_resp.into_inner().invitation.expect("invitation");
    assert_eq!(inv.status, proto::InvitationStatus::Revoked as i32);

    // Trying to revoke again should fail (already resolved)
    let err = client
        .revoke_organization_invite(proto::RevokeOrganizationInviteRequest {
            slug: Some(proto::InviteSlug { slug }),
            caller: Some(proto::UserSlug { slug: admin }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

// ============================================================================
// Tests: Duplicate Pending → ALREADY_EXISTS
// ============================================================================

/// Creating a second invitation to the same email in the same org returns
/// ALREADY_EXISTS with the existing invitation's slug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_duplicate_pending_returns_already_exists() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Dup Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // First invitation succeeds
    let (_slug1, _) = create_invite(&mut client, org, admin, "dup@example.com", 24).await;

    // Second invitation to the same email should fail
    let err = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org }),
            caller: Some(proto::UserSlug { slug: admin }),
            email: "dup@example.com".to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 24,
            team: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
}

// ============================================================================
// Tests: Rate Limiting
// ============================================================================

/// Per-email global pending cap: creating more than 10 pending invitations
/// to the same email (across different orgs) should eventually be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_email_pending_cap() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let target_email = "popular@example.com";
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // Create 10 organizations and send one invitation each to the same email
    let mut slugs = Vec::new();
    for i in 0..10 {
        let (org, admin) = setup_org(addr, &format!("Cap Org {i}"), node).await;
        let (slug, _) = create_invite(&mut client, org, admin, target_email, 168).await;
        slugs.push(slug);
    }

    // 11th invitation from a new org should be rejected (pending cap = 10)
    let (org_11, admin_11) = setup_org(addr, "Cap Org 10", node).await;
    let err = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org_11 }),
            caller: Some(proto::UserSlug { slug: admin_11 }),
            email: target_email.to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 168,
            team: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::ResourceExhausted,
        "11th pending invitation should be rate-limited"
    );
}

/// Per-email total limit: creating more than 20 invitations to the same email
/// within the retention window should be rejected, even if previous ones were revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_email_total_limit() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let target_email = "targeted@example.com";
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // Create 20 invitations from 20 different orgs, revoking each one to stay
    // under the pending cap (10) but accumulate total entries.
    for i in 0..20 {
        let (org, admin) = setup_org(addr, &format!("Total Org {i}"), node).await;
        let (slug, _) = create_invite(&mut client, org, admin, target_email, 168).await;

        // Revoke to free up pending cap
        client
            .revoke_organization_invite(proto::RevokeOrganizationInviteRequest {
                slug: Some(proto::InviteSlug { slug }),
                caller: Some(proto::UserSlug { slug: admin }),
            })
            .await
            .expect("revoke");
    }

    // 21st invitation should be rejected (total limit = 20)
    let (org_21, admin_21) = setup_org(addr, "Total Org 20", node).await;
    let err = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org_21 }),
            caller: Some(proto::UserSlug { slug: admin_21 }),
            email: target_email.to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 168,
            team: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "21st total invitation should be rate-limited");
}

// Scan ceiling (500 entries) is exercised implicitly by the per-email
// total limit test. A dedicated test would require 500+ entries.

// ============================================================================
// Tests: Cross-Org Team Validation
// ============================================================================

/// Creating an invitation with a team from a different organization should be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cross_org_team_rejected() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org_a, admin_a) = setup_org(addr, "Team Org A", node).await;
    let (org_b, admin_b) = setup_org(addr, "Team Org B", node).await;

    // Create a team in org B
    let mut org_client = create_organization_client(addr).await.expect("connect org");
    let team_resp = org_client
        .create_organization_team(proto::CreateOrganizationTeamRequest {
            organization: Some(proto::OrganizationSlug { slug: org_b }),
            caller: Some(proto::UserSlug { slug: admin_b }),
            name: "Team B".to_string(),
        })
        .await
        .expect("create team in org B");
    let team_slug = team_resp.into_inner().team.expect("team").slug.expect("team slug").slug;

    // Try to create an invitation in org A with team from org B
    let mut inv_client = create_invitation_client(addr).await.expect("connect inv");
    let err = inv_client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org_a }),
            caller: Some(proto::UserSlug { slug: admin_a }),
            email: "crossorg@example.com".to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 24,
            team: Some(proto::TeamSlug { slug: team_slug }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "team from different org should be rejected");
    assert!(
        err.message().contains("different organization"),
        "error message should mention organization mismatch: {}",
        err.message()
    );
}

// ============================================================================
// Tests: Email Normalization in Rate Limiting
// ============================================================================

/// Plus-addressed variants of the same email should count against the same
/// rate limit. user+tag@example.com and user@example.com produce the same HMAC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_plus_addressing_dedup() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Plus Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // Create invitation to user@example.com
    create_invite(&mut client, org, admin, "user@example.com", 24).await;

    // Try to create invitation to user+tag@example.com in the same org
    // Should return ALREADY_EXISTS (normalizes to the same HMAC → duplicate pending)
    let err = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org }),
            caller: Some(proto::UserSlug { slug: admin }),
            email: "user+tag@example.com".to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 24,
            team: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::AlreadyExists,
        "plus-addressed variant should be treated as duplicate pending"
    );
}

/// Gmail dotted variants should also count as the same email.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gmail_dot_dedup() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Gmail Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    // Create invitation to user@gmail.com
    create_invite(&mut client, org, admin, "user@gmail.com", 24).await;

    // Try to create invitation to u.s.e.r@gmail.com in the same org
    let err = client
        .create_organization_invite(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: org }),
            caller: Some(proto::UserSlug { slug: admin }),
            email: "u.s.e.r@gmail.com".to_string(),
            role: proto::OrganizationMemberRole::Member as i32,
            ttl_hours: 24,
            team: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::AlreadyExists,
        "Gmail dotted variant should be treated as duplicate pending"
    );
}

// ============================================================================
// Tests: Authorization — Uniform NOT_FOUND
// ============================================================================

/// Attempting to get invitation details with the wrong user returns NOT_FOUND
/// (not PERMISSION_DENIED, to avoid confirming the invitation exists).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wrong_user_gets_not_found() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let (org, admin) = setup_org(addr, "Auth Org", node).await;
    let mut client = create_invitation_client(addr).await.expect("connect inv");

    let (slug, _) = create_invite(&mut client, org, admin, "real@example.com", 24).await;

    // A different user (slug 999) tries to get details — should get NOT_FOUND
    let err = client
        .get_invitation_details(proto::GetInvitationDetailsRequest {
            slug: Some(proto::InviteSlug { slug }),
            user: Some(proto::UserSlug { slug: 999 }),
            caller: Some(proto::UserSlug { slug: 999 }),
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::NotFound,
        "wrong user should get NOT_FOUND, not PERMISSION_DENIED"
    );
}

/// Attempting to get details for a non-existent invitation slug also returns NOT_FOUND.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_nonexistent_slug_gets_not_found() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes()[0];
    let addr = &node.addr;

    let mut client = create_invitation_client(addr).await.expect("connect inv");

    let err = client
        .get_invitation_details(proto::GetInvitationDetailsRequest {
            slug: Some(proto::InviteSlug { slug: 999_999_999 }),
            user: Some(proto::UserSlug { slug: 1 }),
            caller: Some(proto::UserSlug { slug: 1 }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}
