//! Wire types for `InvitationService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    InviteSlug, OrganizationMemberRole, OrganizationSlug, TeamSlug, UserSlug,
};

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InvitationStatus {
    Unspecified = 0,
    Pending = 1,
    Accepted = 2,
    Declined = 3,
    Expired = 4,
    Revoked = 5,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Invitation {
    pub slug: Option<InviteSlug>,
    pub organization: Option<OrganizationSlug>,
    pub inviter: Option<UserSlug>,
    pub invitee_email: String,
    pub organization_name: String,
    pub role: OrganizationMemberRole,
    pub team: Option<TeamSlug>,
    pub status: InvitationStatus,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    /// UNIX nanoseconds.
    pub resolved_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationInviteRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub email: String,
    pub role: OrganizationMemberRole,
    pub ttl_hours: u32,
    pub team: Option<TeamSlug>,
    pub slug: Option<InviteSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationInviteResponse {
    pub slug: Option<InviteSlug>,
    pub status: InvitationStatus,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationInvitesRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub status_filter: Option<InvitationStatus>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationInvitesResponse {
    pub invitations: Vec<Invitation>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationInviteRequest {
    pub slug: Option<InviteSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationInviteResponse {
    pub invitation: Option<Invitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeOrganizationInviteRequest {
    pub slug: Option<InviteSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeOrganizationInviteResponse {
    pub invitation: Option<Invitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListReceivedInvitationsRequest {
    pub user: Option<UserSlug>,
    pub status_filter: Option<InvitationStatus>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListReceivedInvitationsResponse {
    pub invitations: Vec<Invitation>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetInvitationDetailsRequest {
    pub slug: Option<InviteSlug>,
    pub user: Option<UserSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetInvitationDetailsResponse {
    pub invitation: Option<Invitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AcceptInvitationRequest {
    pub slug: Option<InviteSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AcceptInvitationResponse {
    pub invitation: Option<Invitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeclineInvitationRequest {
    pub slug: Option<InviteSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeclineInvitationResponse {
    pub invitation: Option<Invitation>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn create_invite_request_roundtrips_via_postcard() {
        let req = CreateOrganizationInviteRequest {
            organization: Some(OrganizationSlug::new(1)),
            caller: Some(UserSlug::new(42)),
            email: "user@example.com".to_owned(),
            role: OrganizationMemberRole::Member,
            ttl_hours: 168,
            team: Some(TeamSlug::new(7)),
            slug: Some(InviteSlug::new(0xABC)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: CreateOrganizationInviteRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
