//! Wire types for `OrganizationService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    NodeId, OrganizationMember, OrganizationMemberRole, OrganizationSlug, OrganizationStatus,
    OrganizationTeam, OrganizationTeamMemberRole, OrganizationTier, TeamSlug, UserSlug,
};

// ---------------------------------------------------------------------------
// Create / Get / Delete / List Organization
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationRequest {
    pub name: String,
    pub region: String,
    pub tier: Option<OrganizationTier>,
    pub caller: Option<UserSlug>,
    pub slug: Option<OrganizationSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationResponse {
    pub slug: Option<OrganizationSlug>,
    pub name: String,
    pub region: String,
    pub member_nodes: Vec<NodeId>,
    pub status: OrganizationStatus,
    pub config_version: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
    pub tier: OrganizationTier,
    pub members: Vec<OrganizationMember>,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteOrganizationRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteOrganizationResponse {
    /// UNIX nanoseconds.
    pub deleted_at: u64,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationResponse {
    pub slug: Option<OrganizationSlug>,
    pub name: String,
    pub region: String,
    pub member_nodes: Vec<NodeId>,
    pub status: OrganizationStatus,
    pub config_version: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
    pub tier: OrganizationTier,
    pub members: Vec<OrganizationMember>,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationsRequest {
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationsResponse {
    pub organizations: Vec<GetOrganizationResponse>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrateOrganizationRequest {
    pub slug: Option<OrganizationSlug>,
    pub target_region: String,
    pub acknowledge_residency_downgrade: bool,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrateOrganizationResponse {
    pub slug: Option<OrganizationSlug>,
    pub source_region: String,
    pub target_region: String,
    pub status: OrganizationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationResponse {
    pub slug: Option<OrganizationSlug>,
    pub name: String,
    pub region: String,
    pub member_nodes: Vec<NodeId>,
    pub status: OrganizationStatus,
    pub config_version: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
    pub tier: OrganizationTier,
    pub members: Vec<OrganizationMember>,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

// ---------------------------------------------------------------------------
// Organization members
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationMembersRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationMembersResponse {
    pub members: Vec<OrganizationMember>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveOrganizationMemberRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub target: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveOrganizationMemberResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationMemberRoleRequest {
    pub slug: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub target: Option<UserSlug>,
    pub role: OrganizationMemberRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationMemberRoleResponse {
    pub member: Option<OrganizationMember>,
}

// ---------------------------------------------------------------------------
// Organization teams
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationTeamsRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListOrganizationTeamsResponse {
    pub teams: Vec<OrganizationTeam>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationTeamRequest {
    pub organization: Option<OrganizationSlug>,
    pub name: String,
    pub caller: Option<UserSlug>,
    pub slug: Option<TeamSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateOrganizationTeamResponse {
    pub team: Option<OrganizationTeam>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteOrganizationTeamRequest {
    pub slug: Option<TeamSlug>,
    pub move_members_to: Option<TeamSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteOrganizationTeamResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationTeamRequest {
    pub slug: Option<TeamSlug>,
    pub caller: Option<UserSlug>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateOrganizationTeamResponse {
    pub team: Option<OrganizationTeam>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationTeamRequest {
    pub slug: Option<TeamSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetOrganizationTeamResponse {
    pub team: Option<OrganizationTeam>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddTeamMemberRequest {
    pub team: Option<TeamSlug>,
    pub user: Option<UserSlug>,
    pub role: OrganizationTeamMemberRole,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddTeamMemberResponse {
    pub team: Option<OrganizationTeam>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveTeamMemberRequest {
    pub team: Option<TeamSlug>,
    pub user: Option<UserSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveTeamMemberResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateTeamMemberRoleRequest {
    pub team: Option<TeamSlug>,
    pub user: Option<UserSlug>,
    pub role: OrganizationTeamMemberRole,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateTeamMemberRoleResponse {
    pub team: Option<OrganizationTeam>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn create_organization_request_roundtrips_via_postcard() {
        let req = CreateOrganizationRequest {
            name: "acme_corp".to_owned(),
            region: "us-east-va".to_owned(),
            tier: Some(OrganizationTier::Launch),
            caller: Some(UserSlug::new(42)),
            slug: Some(OrganizationSlug::new(0xDEADBEEF)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: CreateOrganizationRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
