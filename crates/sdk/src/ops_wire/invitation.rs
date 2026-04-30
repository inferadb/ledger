//! Wire-based organization-invitation ops.
//!
//! Mirrors [`super::super::ops::invitation`]: same returned domain types
//! ([`InvitationCreated`], [`InvitationInfo`], [`InvitationPage`],
//! [`ReceivedInvitationInfo`], [`ReceivedInvitationPage`]), same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`InvitationServiceClient`](inferadb_ledger_wire_services::InvitationServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (status / role enums, page envelopes,
//! optional `Invitation` unwrap) in isolation. The eight per-RPC entry
//! points themselves are not exercised yet — [`LedgerClient`](crate::LedgerClient)
//! keeps dispatching through the tonic `ops::invitation` path until F.1
//! flips the connection pool.

use std::sync::Arc;

use inferadb_ledger_types::{InviteSlug, OrganizationSlug, TeamSlug, UserSlug};
use inferadb_ledger_wire::services::{invitation as w, shared as ws};
use inferadb_ledger_wire_services::InvitationServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    types::{
        admin::OrganizationMemberRole,
        invitation::{
            InvitationCreated, InvitationInfo, InvitationPage, InvitationStatus,
            ReceivedInvitationInfo, ReceivedInvitationPage,
        },
    },
};

// ---------------------------------------------------------------------------
// Admin operations
// ---------------------------------------------------------------------------

/// Issue a `CreateOrganizationInvite` call via the wire transport.
///
/// Mirrors [`LedgerClient::create_organization_invite`](crate::LedgerClient::create_organization_invite)
/// at the dispatch layer. The caller is responsible for generating the
/// `invite_slug` once outside any retry loop so retries hit the server's
/// apply-side idempotency path rather than creating duplicate invitations
/// on response-lost-in-flight.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's retry
/// loop is responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_organization_invite(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    caller: UserSlug,
    email: String,
    role: OrganizationMemberRole,
    ttl_hours: u32,
    team: Option<TeamSlug>,
    invite_slug: u64,
) -> Result<InvitationCreated> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::CreateOrganizationInviteRequest {
        organization: Some(organization),
        caller: Some(caller),
        email,
        role: domain_role_to_wire(role),
        ttl_hours,
        team,
        slug: Some(InviteSlug::new(invite_slug)),
    };
    let response = client
        .create_organization_invite(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(domain_invitation_created_from_wire(response))
}

/// Issue a `ListOrganizationInvites` call via the wire transport.
///
/// Mirrors [`LedgerClient::list_organization_invites`](crate::LedgerClient::list_organization_invites).
/// Returns admin-view records (invitee email visible, organization name
/// empty).
///
/// # Errors
///
/// Same shape as [`create_organization_invite`].
pub(crate) async fn list_organization_invites(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    caller: UserSlug,
    status_filter: Option<InvitationStatus>,
    page_token: Option<Vec<u8>>,
    page_size: u32,
) -> Result<InvitationPage> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::ListOrganizationInvitesRequest {
        organization: Some(organization),
        caller: Some(caller),
        status_filter: status_filter.map(domain_status_to_wire),
        page_token: page_token.map(bytes::Bytes::from),
        page_size,
    };
    let response = client
        .list_organization_invites(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(domain_invitation_page_from_wire(response))
}

/// Issue a `GetOrganizationInvite` call via the wire transport.
///
/// Mirrors [`LedgerClient::get_organization_invite`](crate::LedgerClient::get_organization_invite).
/// Returns the admin-view record.
///
/// # Errors
///
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) if the response envelope is
/// missing the `invitation` field; otherwise same as [`create_organization_invite`].
pub(crate) async fn get_organization_invite(
    wire_client: Arc<WireClient>,
    request_id: u128,
    slug: InviteSlug,
    caller: UserSlug,
) -> Result<InvitationInfo> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::GetOrganizationInviteRequest { slug: Some(slug), caller: Some(caller) };
    let response = client
        .get_organization_invite(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    response
        .invitation
        .map(domain_invitation_info_from_wire)
        .ok_or_else(|| missing_invitation_field("GetOrganizationInviteResponse"))
}

/// Issue a `RevokeOrganizationInvite` call via the wire transport.
///
/// Mirrors [`LedgerClient::revoke_organization_invite`](crate::LedgerClient::revoke_organization_invite).
/// Returns the admin-view record reflecting the post-revoke status.
///
/// # Errors
///
/// Same shape as [`get_organization_invite`].
pub(crate) async fn revoke_organization_invite(
    wire_client: Arc<WireClient>,
    request_id: u128,
    slug: InviteSlug,
    caller: UserSlug,
) -> Result<InvitationInfo> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::RevokeOrganizationInviteRequest { slug: Some(slug), caller: Some(caller) };
    let response = client
        .revoke_organization_invite(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    response
        .invitation
        .map(domain_invitation_info_from_wire)
        .ok_or_else(|| missing_invitation_field("RevokeOrganizationInviteResponse"))
}

// ---------------------------------------------------------------------------
// User operations
// ---------------------------------------------------------------------------

/// Issue a `ListReceivedInvitations` call via the wire transport.
///
/// Mirrors [`LedgerClient::list_received_invitations`](crate::LedgerClient::list_received_invitations).
/// Returns user-view records (organization name visible, invitee email
/// empty).
///
/// # Errors
///
/// Same shape as [`create_organization_invite`].
pub(crate) async fn list_received_invitations(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    status_filter: Option<InvitationStatus>,
    page_token: Option<Vec<u8>>,
    page_size: u32,
) -> Result<ReceivedInvitationPage> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::ListReceivedInvitationsRequest {
        user: Some(user),
        status_filter: status_filter.map(domain_status_to_wire),
        page_token: page_token.map(bytes::Bytes::from),
        page_size,
        caller: Some(user),
    };
    let response = client
        .list_received_invitations(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(domain_received_invitation_page_from_wire(response))
}

/// Issue a `GetInvitationDetails` call via the wire transport.
///
/// Mirrors [`LedgerClient::get_invitation_details`](crate::LedgerClient::get_invitation_details).
/// Returns the user-view record for the authenticated user.
///
/// # Errors
///
/// Same shape as [`get_organization_invite`].
pub(crate) async fn get_invitation_details(
    wire_client: Arc<WireClient>,
    request_id: u128,
    slug: InviteSlug,
    user: UserSlug,
) -> Result<ReceivedInvitationInfo> {
    let client = InvitationServiceClient::new(wire_client);
    let request =
        w::GetInvitationDetailsRequest { slug: Some(slug), user: Some(user), caller: Some(user) };
    let response =
        client.get_invitation_details(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .invitation
        .map(domain_received_invitation_info_from_wire)
        .ok_or_else(|| missing_invitation_field("GetInvitationDetailsResponse"))
}

/// Issue an `AcceptInvitation` call via the wire transport.
///
/// Mirrors [`LedgerClient::accept_invitation`](crate::LedgerClient::accept_invitation).
/// On success, the user is added as an organization member with the
/// designated role and optional team membership; the returned record is
/// the user-view post-accept snapshot.
///
/// # Errors
///
/// Same shape as [`get_organization_invite`].
pub(crate) async fn accept_invitation(
    wire_client: Arc<WireClient>,
    request_id: u128,
    slug: InviteSlug,
    caller: UserSlug,
) -> Result<ReceivedInvitationInfo> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::AcceptInvitationRequest { slug: Some(slug), caller: Some(caller) };
    let response =
        client.accept_invitation(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .invitation
        .map(domain_received_invitation_info_from_wire)
        .ok_or_else(|| missing_invitation_field("AcceptInvitationResponse"))
}

/// Issue a `DeclineInvitation` call via the wire transport.
///
/// Mirrors [`LedgerClient::decline_invitation`](crate::LedgerClient::decline_invitation).
///
/// # Errors
///
/// Same shape as [`get_organization_invite`].
pub(crate) async fn decline_invitation(
    wire_client: Arc<WireClient>,
    request_id: u128,
    slug: InviteSlug,
    caller: UserSlug,
) -> Result<ReceivedInvitationInfo> {
    let client = InvitationServiceClient::new(wire_client);
    let request = w::DeclineInvitationRequest { slug: Some(slug), caller: Some(caller) };
    let response =
        client.decline_invitation(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .invitation
        .map(domain_received_invitation_info_from_wire)
        .ok_or_else(|| missing_invitation_field("DeclineInvitationResponse"))
}

// ---------------------------------------------------------------------------
// Wire → domain mappers
// ---------------------------------------------------------------------------

/// Maps a wire `CreateOrganizationInviteResponse` into the SDK's domain
/// [`InvitationCreated`].
///
/// `slug` defaults to `0` if the wire envelope omits it (matches the proto
/// path's `s.slug` default). `created_at` / `expires_at` are UNIX
/// nanoseconds on the wire and convert to `SystemTime` via
/// [`nanos_to_system_time`].
fn domain_invitation_created_from_wire(
    resp: w::CreateOrganizationInviteResponse,
) -> InvitationCreated {
    InvitationCreated {
        slug: resp.slug.unwrap_or_else(|| InviteSlug::new(0)),
        status: wire_invitation_status_to_domain(resp.status),
        created_at: nanos_to_system_time(resp.created_at),
        expires_at: nanos_to_system_time(resp.expires_at),
        token: resp.token,
    }
}

/// Maps a wire `ListOrganizationInvitesResponse` into the SDK's domain
/// [`InvitationPage`].
fn domain_invitation_page_from_wire(resp: w::ListOrganizationInvitesResponse) -> InvitationPage {
    InvitationPage {
        invitations: resp.invitations.into_iter().map(domain_invitation_info_from_wire).collect(),
        next_page_token: resp.next_page_token.map(|b| b.to_vec()),
    }
}

/// Maps a wire `ListReceivedInvitationsResponse` into the SDK's domain
/// [`ReceivedInvitationPage`].
fn domain_received_invitation_page_from_wire(
    resp: w::ListReceivedInvitationsResponse,
) -> ReceivedInvitationPage {
    ReceivedInvitationPage {
        invitations: resp
            .invitations
            .into_iter()
            .map(domain_received_invitation_info_from_wire)
            .collect(),
        next_page_token: resp.next_page_token.map(|b| b.to_vec()),
    }
}

/// Maps a wire `Invitation` into the SDK's admin-view [`InvitationInfo`].
///
/// Optional slug fields (`slug`, `organization`, `inviter`) default to the
/// zero-id newtype on absence — mirrors the `as_ref().map_or(0, ...)`
/// pattern in the proto path.
fn domain_invitation_info_from_wire(inv: w::Invitation) -> InvitationInfo {
    InvitationInfo {
        slug: inv.slug.unwrap_or_else(|| InviteSlug::new(0)),
        organization: inv.organization.unwrap_or_else(|| OrganizationSlug::new(0)),
        inviter: inv.inviter.unwrap_or_else(|| UserSlug::new(0)),
        invitee_email: inv.invitee_email,
        role: wire_role_to_domain(inv.role),
        team: inv.team,
        status: wire_invitation_status_to_domain(inv.status),
        created_at: nanos_to_system_time(inv.created_at),
        expires_at: nanos_to_system_time(inv.expires_at),
        resolved_at: inv.resolved_at.and_then(nanos_to_system_time),
    }
}

/// Maps a wire `Invitation` into the SDK's user-view [`ReceivedInvitationInfo`].
fn domain_received_invitation_info_from_wire(inv: w::Invitation) -> ReceivedInvitationInfo {
    ReceivedInvitationInfo {
        slug: inv.slug.unwrap_or_else(|| InviteSlug::new(0)),
        organization: inv.organization.unwrap_or_else(|| OrganizationSlug::new(0)),
        organization_name: inv.organization_name,
        role: wire_role_to_domain(inv.role),
        team: inv.team,
        status: wire_invitation_status_to_domain(inv.status),
        created_at: nanos_to_system_time(inv.created_at),
        expires_at: nanos_to_system_time(inv.expires_at),
    }
}

/// Numeric-tag mapping from the wire `InvitationStatus` to the domain
/// enum.
///
/// Wire enum is `#[repr(i32)]` with proto-matching tags; the wire-side
/// `Unspecified` collapses to `Pending` in the domain layer to match the
/// proto-path behavior in
/// [`InvitationStatus::from_proto`](crate::types::invitation::InvitationStatus).
fn wire_invitation_status_to_domain(status: w::InvitationStatus) -> InvitationStatus {
    match status {
        w::InvitationStatus::Accepted => InvitationStatus::Accepted,
        w::InvitationStatus::Declined => InvitationStatus::Declined,
        w::InvitationStatus::Expired => InvitationStatus::Expired,
        w::InvitationStatus::Revoked => InvitationStatus::Revoked,
        // Unspecified collapses to Pending — matches proto path.
        w::InvitationStatus::Unspecified | w::InvitationStatus::Pending => {
            InvitationStatus::Pending
        },
    }
}

/// Numeric-tag mapping from the domain `InvitationStatus` to the wire
/// enum, used when forwarding `status_filter` on list calls.
fn domain_status_to_wire(status: InvitationStatus) -> w::InvitationStatus {
    match status {
        InvitationStatus::Pending => w::InvitationStatus::Pending,
        InvitationStatus::Accepted => w::InvitationStatus::Accepted,
        InvitationStatus::Declined => w::InvitationStatus::Declined,
        InvitationStatus::Expired => w::InvitationStatus::Expired,
        InvitationStatus::Revoked => w::InvitationStatus::Revoked,
    }
}

/// Numeric-tag mapping from the wire `OrganizationMemberRole` to the
/// domain enum. The wire `Unspecified` and `Member` variants both collapse
/// to `Member` in the domain — matches the proto path.
fn wire_role_to_domain(role: ws::OrganizationMemberRole) -> OrganizationMemberRole {
    match role {
        ws::OrganizationMemberRole::Admin => OrganizationMemberRole::Admin,
        // Unspecified + Member both surface as Member — mirrors proto path
        // in [`crate::types::admin::OrganizationMemberRole::from_proto`].
        ws::OrganizationMemberRole::Member | ws::OrganizationMemberRole::Unspecified => {
            OrganizationMemberRole::Member
        },
    }
}

/// Numeric-tag mapping from the domain `OrganizationMemberRole` to the
/// wire enum, used on outgoing requests.
fn domain_role_to_wire(role: OrganizationMemberRole) -> ws::OrganizationMemberRole {
    match role {
        OrganizationMemberRole::Admin => ws::OrganizationMemberRole::Admin,
        OrganizationMemberRole::Member => ws::OrganizationMemberRole::Member,
    }
}

/// Converts a UNIX-nanoseconds wire timestamp into `SystemTime`.
///
/// Returns `None` for `0` (the wire convention for "unset") and for values
/// that overflow `Duration`. This mirrors the proto path's
/// `proto_timestamp_to_system_time` which returns `None` on absence /
/// invalid inputs.
fn nanos_to_system_time(nanos: u64) -> Option<std::time::SystemTime> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, subsec_nanos))
}

/// Builds the "missing invitation field" `SdkError` shared by every
/// single-record wire response (`Get` / `Revoke` / `Accept` / `Decline` /
/// `GetInvitationDetails`). Mirrors `proto_util::missing_response_field`
/// at the dispatch layer — same `wire ErrorCode::Internal` shape so SDK
/// consumers see one error shape across both transports.
fn missing_invitation_field(response_type: &str) -> SdkError {
    SdkError::Rpc {
        code: inferadb_ledger_wire::ErrorCode::Internal,
        message: format!("Missing invitation in {response_type}"),
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the wire-response → domain-type mapping plus the
    //! enum-tag projections in both directions. End-to-end tests against a
    //! real `WireClient` / `WireServer` are deferred to E.7 (TestCluster
    //! migration) — at that point the eight per-RPC entry points get
    //! exercised in full.

    use super::*;

    // ----- InvitationStatus: wire → domain -----

    #[test]
    fn wire_invitation_status_each_variant_maps_correctly() {
        // Pin per-variant mapping so a future addition to the wire enum
        // doesn't silently fall through to Pending without a matching
        // domain variant.
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Pending),
            InvitationStatus::Pending,
        );
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Accepted),
            InvitationStatus::Accepted,
        );
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Declined),
            InvitationStatus::Declined,
        );
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Expired),
            InvitationStatus::Expired,
        );
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Revoked),
            InvitationStatus::Revoked,
        );
        // Unspecified collapses to Pending — matches proto path.
        assert_eq!(
            wire_invitation_status_to_domain(w::InvitationStatus::Unspecified),
            InvitationStatus::Pending,
        );
    }

    // ----- InvitationStatus: domain → wire -----

    #[test]
    fn domain_invitation_status_to_wire_each_variant() {
        assert_eq!(domain_status_to_wire(InvitationStatus::Pending), w::InvitationStatus::Pending);
        assert_eq!(
            domain_status_to_wire(InvitationStatus::Accepted),
            w::InvitationStatus::Accepted
        );
        assert_eq!(
            domain_status_to_wire(InvitationStatus::Declined),
            w::InvitationStatus::Declined
        );
        assert_eq!(domain_status_to_wire(InvitationStatus::Expired), w::InvitationStatus::Expired);
        assert_eq!(domain_status_to_wire(InvitationStatus::Revoked), w::InvitationStatus::Revoked);
    }

    // ----- OrganizationMemberRole: wire → domain -----

    #[test]
    fn wire_role_each_variant_maps_correctly() {
        assert_eq!(
            wire_role_to_domain(ws::OrganizationMemberRole::Admin),
            OrganizationMemberRole::Admin,
        );
        assert_eq!(
            wire_role_to_domain(ws::OrganizationMemberRole::Member),
            OrganizationMemberRole::Member,
        );
        // Unspecified collapses to Member — matches proto path in
        // OrganizationMemberRole::from_proto.
        assert_eq!(
            wire_role_to_domain(ws::OrganizationMemberRole::Unspecified),
            OrganizationMemberRole::Member,
        );
    }

    // ----- OrganizationMemberRole: domain → wire -----

    #[test]
    fn domain_role_to_wire_each_variant() {
        assert_eq!(
            domain_role_to_wire(OrganizationMemberRole::Admin),
            ws::OrganizationMemberRole::Admin
        );
        assert_eq!(
            domain_role_to_wire(OrganizationMemberRole::Member),
            ws::OrganizationMemberRole::Member,
        );
    }

    // ----- Repr-i32 numeric tags pinned -----

    #[test]
    fn wire_invitation_status_numeric_tags_match_proto() {
        // Wire enum is `#[repr(i32)]`; tags must match proto so the
        // numeric tag flows uninterpreted through the dispatch layer.
        assert_eq!(w::InvitationStatus::Unspecified as i32, 0);
        assert_eq!(w::InvitationStatus::Pending as i32, 1);
        assert_eq!(w::InvitationStatus::Accepted as i32, 2);
        assert_eq!(w::InvitationStatus::Declined as i32, 3);
        assert_eq!(w::InvitationStatus::Expired as i32, 4);
        assert_eq!(w::InvitationStatus::Revoked as i32, 5);
    }

    #[test]
    fn wire_org_member_role_numeric_tags_match_proto() {
        assert_eq!(ws::OrganizationMemberRole::Unspecified as i32, 0);
        assert_eq!(ws::OrganizationMemberRole::Admin as i32, 1);
        assert_eq!(ws::OrganizationMemberRole::Member as i32, 2);
    }

    // ----- nanos_to_system_time -----

    #[test]
    fn nanos_to_system_time_zero_returns_none() {
        // 0 ns is the wire convention for "unset"; matches proto's absent
        // `Timestamp` message.
        assert!(nanos_to_system_time(0).is_none());
    }

    #[test]
    fn nanos_to_system_time_round_trips_known_value() {
        // 1_700_000_000_000_000_000 ns = 1_700_000_000 s + 0 ns past
        // UNIX epoch. Round-trip through SystemTime → Duration to confirm
        // both halves of the `secs` / `subsec_nanos` split are correct.
        let nanos = 1_700_000_000_500_000_000u64;
        let st = nanos_to_system_time(nanos).expect("known value should convert");
        let d = st.duration_since(std::time::UNIX_EPOCH).expect("after epoch");
        assert_eq!(d.as_secs(), 1_700_000_000);
        assert_eq!(d.subsec_nanos(), 500_000_000);
    }

    // ----- domain_invitation_created_from_wire -----

    #[test]
    fn invitation_created_from_wire_preserves_fields() {
        let resp = w::CreateOrganizationInviteResponse {
            slug: Some(InviteSlug::new(42)),
            status: w::InvitationStatus::Pending,
            created_at: 0, // unset
            expires_at: 1_700_000_000_000_000_000,
            token: "abc123".to_owned(),
        };
        let domain = domain_invitation_created_from_wire(resp);
        assert_eq!(domain.slug, InviteSlug::new(42));
        assert_eq!(domain.status, InvitationStatus::Pending);
        assert!(domain.created_at.is_none());
        assert!(domain.expires_at.is_some());
        assert_eq!(domain.token, "abc123");
    }

    #[test]
    fn invitation_created_missing_slug_defaults_to_zero() {
        // Mirrors the proto path's `s.slug.as_ref().map_or(0, ...)`
        // fallback. A future server bug omitting the slug should not
        // panic — same behavior as the tonic dispatch path.
        let resp = w::CreateOrganizationInviteResponse {
            slug: None,
            status: w::InvitationStatus::Pending,
            created_at: 0,
            expires_at: 0,
            token: String::new(),
        };
        let domain = domain_invitation_created_from_wire(resp);
        assert_eq!(domain.slug, InviteSlug::new(0));
    }

    // ----- domain_invitation_info_from_wire -----

    #[test]
    fn invitation_info_from_wire_admin_view() {
        let inv = w::Invitation {
            slug: Some(InviteSlug::new(10)),
            organization: Some(OrganizationSlug::new(20)),
            inviter: Some(UserSlug::new(30)),
            invitee_email: "test@example.com".to_owned(),
            organization_name: String::new(),
            role: ws::OrganizationMemberRole::Member,
            team: Some(TeamSlug::new(40)),
            status: w::InvitationStatus::Accepted,
            created_at: 1_700_000_000_000_000_000,
            expires_at: 1_700_000_000_000_000_000,
            resolved_at: Some(1_700_000_001_000_000_000),
        };
        let info = domain_invitation_info_from_wire(inv);
        assert_eq!(info.slug, InviteSlug::new(10));
        assert_eq!(info.organization, OrganizationSlug::new(20));
        assert_eq!(info.inviter, UserSlug::new(30));
        assert_eq!(info.invitee_email, "test@example.com");
        assert_eq!(info.role, OrganizationMemberRole::Member);
        assert_eq!(info.team, Some(TeamSlug::new(40)));
        assert_eq!(info.status, InvitationStatus::Accepted);
        assert!(info.created_at.is_some());
        assert!(info.expires_at.is_some());
        assert!(info.resolved_at.is_some());
    }

    #[test]
    fn invitation_info_resolved_at_none_when_unresolved() {
        let inv = w::Invitation {
            slug: Some(InviteSlug::new(1)),
            organization: Some(OrganizationSlug::new(2)),
            inviter: Some(UserSlug::new(3)),
            invitee_email: "x@y".to_owned(),
            organization_name: String::new(),
            role: ws::OrganizationMemberRole::Member,
            team: None,
            status: w::InvitationStatus::Pending,
            created_at: 1_700_000_000_000_000_000,
            expires_at: 1_700_000_000_000_000_000,
            resolved_at: None,
        };
        let info = domain_invitation_info_from_wire(inv);
        assert!(info.resolved_at.is_none());
    }

    // ----- domain_received_invitation_info_from_wire -----

    #[test]
    fn received_invitation_info_from_wire_user_view() {
        let inv = w::Invitation {
            slug: Some(InviteSlug::new(50)),
            organization: Some(OrganizationSlug::new(60)),
            inviter: Some(UserSlug::new(70)),
            invitee_email: String::new(),
            organization_name: "Acme Corp".to_owned(),
            role: ws::OrganizationMemberRole::Admin,
            team: None,
            status: w::InvitationStatus::Pending,
            created_at: 0,
            expires_at: 0,
            resolved_at: None,
        };
        let info = domain_received_invitation_info_from_wire(inv);
        assert_eq!(info.slug, InviteSlug::new(50));
        assert_eq!(info.organization, OrganizationSlug::new(60));
        assert_eq!(info.organization_name, "Acme Corp");
        assert_eq!(info.role, OrganizationMemberRole::Admin);
        assert!(info.team.is_none());
        assert_eq!(info.status, InvitationStatus::Pending);
    }

    // ----- Page envelopes -----

    #[test]
    fn invitation_page_from_wire_round_trips_token_and_records() {
        let inv = w::Invitation {
            slug: Some(InviteSlug::new(1)),
            organization: Some(OrganizationSlug::new(2)),
            inviter: Some(UserSlug::new(3)),
            invitee_email: "a@b".to_owned(),
            organization_name: String::new(),
            role: ws::OrganizationMemberRole::Member,
            team: None,
            status: w::InvitationStatus::Pending,
            created_at: 0,
            expires_at: 0,
            resolved_at: None,
        };
        let resp = w::ListOrganizationInvitesResponse {
            invitations: vec![inv],
            next_page_token: Some(bytes::Bytes::from_static(b"\x01\x02\x03")),
        };
        let page = domain_invitation_page_from_wire(resp);
        assert_eq!(page.invitations.len(), 1);
        assert_eq!(page.next_page_token.as_deref(), Some(&[0x01u8, 0x02, 0x03][..]));
    }

    #[test]
    fn invitation_page_no_next_token_round_trips_none() {
        let resp =
            w::ListOrganizationInvitesResponse { invitations: vec![], next_page_token: None };
        let page = domain_invitation_page_from_wire(resp);
        assert!(page.invitations.is_empty());
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn received_invitation_page_from_wire_round_trips() {
        let inv = w::Invitation {
            slug: Some(InviteSlug::new(11)),
            organization: Some(OrganizationSlug::new(22)),
            inviter: Some(UserSlug::new(33)),
            invitee_email: String::new(),
            organization_name: "Org".to_owned(),
            role: ws::OrganizationMemberRole::Member,
            team: None,
            status: w::InvitationStatus::Pending,
            created_at: 0,
            expires_at: 0,
            resolved_at: None,
        };
        let resp =
            w::ListReceivedInvitationsResponse { invitations: vec![inv], next_page_token: None };
        let page = domain_received_invitation_page_from_wire(resp);
        assert_eq!(page.invitations.len(), 1);
        assert!(page.next_page_token.is_none());
    }

    // ----- missing_invitation_field error shape -----

    #[test]
    fn missing_invitation_field_returns_internal_rpc_error() {
        // Mirror `proto_util::missing_response_field` so SDK consumers
        // see one error shape across both transports.
        let err = missing_invitation_field("AcceptInvitationResponse");
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(message.contains("AcceptInvitationResponse"), "got: {message}");
                assert!(message.contains("Missing invitation"), "got: {message}");
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }
}
