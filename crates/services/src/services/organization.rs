//! Organization service implementation.
//!
//! Handles organization lifecycle: creation, deletion, retrieval, listing,
//! and region migration. Write operations flow through Raft for consistency;
//! read operations hit the local applied state directly.
//!
//! Organization creation uses a fire-and-forget saga that separates GLOBAL
//! directory metadata from regional PII (organization name). Region migration
//! also uses a saga for the orchestrator to drive.

use inferadb_ledger_raft::logging::RequestContext;
use inferadb_ledger_state::system::{
    Organization, OrganizationMember as DomainOrganizationMember,
    OrganizationMemberRole as DomainMemberRole, OrganizationProfile, OrganizationRegistry,
    OrganizationStatus as DomainOrganizationStatus, OrganizationTier as DomainOrganizationTier,
    SYSTEM_VAULT_ID, SystemKeys, SystemOrganizationService, Team,
    TeamMemberRole as DomainTeamMemberRole,
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    OrganizationId as DomainOrganizationId, OrganizationSlug as DomainOrganizationSlug, TeamId,
    decode,
};
use inferadb_ledger_wire::services::{organization as w, shared as ws};
use tonic::Status;

use super::{
    error_classify, service_infra::ServiceContext, slug_resolver::SlugResolver,
    wire_helpers::wire_error_to_tonic_status,
};

/// gRPC handler for organization lifecycle operations.
pub struct OrganizationService {
    /// Shared service infrastructure. Promoted to `pub(super)` so the
    /// inlined wire-trait impl in [`super::organization_wire`] can drive
    /// the same proposal / state-layer surface as the tonic impl below.
    pub(super) ctx: ServiceContext,
}

impl OrganizationService {
    /// Creates a new `OrganizationService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Validates that a protected region has sufficient in-region nodes for quorum.
    ///
    /// Returns `Ok(())` if the region is non-protected or has at least 3
    /// in-region nodes. Returns a `FailedPrecondition` status with
    /// structured error details otherwise. The residency contract is
    /// resolved from the GLOBAL region directory; an unknown region (no
    /// directory entry) is treated as non-protected — production paths
    /// always write the directory entry before any organization can
    /// reference a region, so a missing entry means the region was
    /// never registered and there is nothing to enforce against.
    pub(super) fn validate_region_nodes(
        &self,
        region: inferadb_ledger_types::Region,
    ) -> Result<(), Status> {
        let residency =
            match inferadb_ledger_state::system::lookup_region_residency(&self.ctx.state, region) {
                Ok(Some(r)) => r,
                Ok(None) | Err(_) => return Ok(()),
            };
        if !residency.requires_residency {
            return Ok(());
        }
        let sys_svc = self.ctx.system_service();
        let nodes = sys_svc
            .list_nodes()
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;
        let in_region_count = nodes.iter().filter(|n| n.region == region).count();
        if in_region_count >= 3 {
            return Ok(());
        }
        let mut context = std::collections::HashMap::new();
        context.insert("region".to_string(), region.as_str().to_string());
        context.insert("available_nodes".to_string(), in_region_count.to_string());
        context.insert("required_nodes".to_string(), "3".to_string());
        let mut wire_err = super::error_details::build_error_details(
            inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes.as_u16(),
            false,
            None,
            context,
            Some(
                inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes
                    .suggested_action(),
            ),
        );
        wire_err.code = inferadb_ledger_wire::ErrorCode::FailedPrecondition;
        wire_err.message = format!(
            "Insufficient in-region nodes for protected region {}: \
             {in_region_count} available, 3 required",
            region.as_str()
        );
        Err(super::wire_helpers::wire_error_to_tonic_status(wire_err))
    }

    /// Reads the `Organization` skeleton from the GLOBAL state layer and overlays
    /// PII (name) from the REGIONAL `OrganizationProfile`.
    ///
    /// Returns the merged view. When REGIONAL is unavailable, the skeleton
    /// is returned with `name: ""` (graceful degradation).
    pub(super) fn read_organization(&self, org_id: DomainOrganizationId) -> Option<Organization> {
        let key = SystemKeys::organization_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        let mut org = decode::<Organization>(&entity.value)
            .inspect_err(|e| {
                tracing::warn!(
                    organization = org_id.value(),
                    error = ?e,
                    "corrupt Organization skeleton, skipping"
                );
            })
            .ok()?;
        self.overlay_org_profile(&mut org, org_id);
        Some(org)
    }

    /// Overlays PII fields from a REGIONAL `OrganizationProfile` onto a GLOBAL
    /// `Organization` skeleton. Matching the `overlay_app_profile()` pattern.
    fn overlay_org_profile(&self, org: &mut Organization, org_id: DomainOrganizationId) {
        let regional = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .and_then(|meta| self.ctx.regional_state(meta.region).ok());
        let Some(state) = regional else { return };
        let key = SystemKeys::organization_profile_key(org_id);
        let Ok(Some(entity)) = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) else {
            return;
        };
        if let Ok(profile) = decode::<OrganizationProfile>(&entity.value) {
            org.name = profile.name;
        }
    }

    /// Reads the organization registry from the StateLayer.
    pub(super) fn read_org_registry(
        &self,
        org_id: DomainOrganizationId,
    ) -> Option<OrganizationRegistry> {
        let key = SystemKeys::organization_registry_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<OrganizationRegistry>(&entity.value).ok()
    }

    /// Reads a team record from the REGIONAL state layer.
    pub(super) fn read_team(&self, org_id: DomainOrganizationId, team_id: TeamId) -> Option<Team> {
        let org_meta = self.ctx.applied_state.get_organization(org_id)?;
        let state = self.ctx.regional_state(org_meta.region).ok()?;
        let key = SystemKeys::team_key(org_id, team_id);
        let entity = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<Team>(&entity.value)
            .inspect_err(|e| {
                tracing::warn!(
                    organization = org_id.value(),
                    team = team_id.value(),
                    error = ?e,
                    "corrupt team record, skipping"
                );
            })
            .ok()
    }

    /// Lists all team records for an organization from the REGIONAL state layer.
    pub(super) fn list_teams(&self, org_id: DomainOrganizationId) -> Vec<Team> {
        /// Maximum number of team records to load per organization.
        const MAX_TEAMS: usize = 1_000;

        let org_meta = match self.ctx.applied_state.get_organization(org_id) {
            Some(meta) => meta,
            None => return Vec::new(),
        };
        let state = match self.ctx.regional_state(org_meta.region) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let prefix = format!("{}{}:", SystemKeys::TEAM_PREFIX, org_id.value());
        let entities = match state.list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, MAX_TEAMS) {
            Ok(entities) => entities,
            Err(_) => return Vec::new(),
        };
        entities
            .iter()
            .filter_map(|e| {
                decode::<Team>(&e.value)
                    .inspect_err(|err| {
                        tracing::warn!(
                            organization = org_id.value(),
                            key = %String::from_utf8_lossy(&e.key),
                            error = %err,
                            "corrupt team record, skipping"
                        );
                    })
                    .ok()
            })
            .collect()
    }

    /// Converts a domain `Team` to its wire representation.
    pub(super) fn team_to_wire(
        sys_svc: &SystemOrganizationService<FileBackend>,
        team: &Team,
        org_slug: DomainOrganizationSlug,
    ) -> ws::OrganizationTeam {
        let members = team
            .members
            .iter()
            .filter_map(|m| {
                sys_svc.get_user(m.user_id).ok().flatten().map(|user| {
                    let role = match m.role {
                        DomainTeamMemberRole::Manager => ws::OrganizationTeamMemberRole::Manager,
                        DomainTeamMemberRole::Member => ws::OrganizationTeamMemberRole::Member,
                    };
                    ws::OrganizationTeamMember {
                        user: Some(user.slug),
                        role,
                        joined_at: m.joined_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
                    }
                })
            })
            .collect();

        ws::OrganizationTeam {
            slug: Some(team.slug),
            organization: Some(org_slug),
            name: team.name.clone(),
            members,
            created_at: team.created_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
            updated_at: team.updated_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
        }
    }

    /// Resolves a user slug to a `UserId` and loads the non-deleted organization.
    ///
    /// Shared precondition check for `validate_org_admin`, `validate_org_member`,
    /// and `validate_org_admin_or_team_manager`.
    fn resolve_user_and_organization(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        user_slug: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(inferadb_ledger_types::UserId, Organization), Status> {
        let user_id = slug_resolver
            .extract_and_resolve_user(user_slug)
            .inspect_err(|status| {
                ctx.set_error("InvalidArgument", &status.message);
            })
            .map_err(wire_error_to_tonic_status)?;

        if self
            .read_org_registry(organization_id)
            .is_some_and(|r| r.status == DomainOrganizationStatus::Deleted)
        {
            ctx.set_error("NotFound", "Organization not found");
            return Err(Status::not_found("Organization not found"));
        }

        let org = self.read_organization(organization_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Organization not found");
            Status::not_found("Organization not found")
        })?;

        Ok((user_id, org))
    }

    /// Validates that the initiator is an administrator of the given organization.
    ///
    /// Returns the resolved `UserId` on success.
    pub(super) fn validate_org_admin(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        initiator_slug: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<inferadb_ledger_types::UserId, Status> {
        let (initiator_id, org) = self.resolve_user_and_organization(
            slug_resolver,
            organization_id,
            initiator_slug,
            ctx,
        )?;

        let is_admin = org
            .members
            .iter()
            .any(|m| m.user_id == initiator_id && m.role == DomainMemberRole::Admin);
        if !is_admin {
            ctx.set_error("PermissionDenied", "User is not an organization administrator");
            return Err(Status::permission_denied("User is not an organization administrator"));
        }

        Ok(initiator_id)
    }

    /// Validates that the caller is a member of the given organization (any role).
    ///
    /// Returns the resolved `UserId` and organization on success.
    pub(super) fn validate_org_member(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        caller: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(inferadb_ledger_types::UserId, Organization), Status> {
        let (caller_id, org) =
            self.resolve_user_and_organization(slug_resolver, organization_id, caller, ctx)?;

        let is_member = org.members.iter().any(|m| m.user_id == caller_id);
        if !is_member {
            ctx.set_error("NotFound", "Organization not found");
            return Err(Status::not_found("Organization not found"));
        }

        Ok((caller_id, org))
    }

    /// Validates that the initiator is an org admin or team manager.
    ///
    /// Returns the resolved `UserId` on success.
    pub(super) fn validate_org_admin_or_team_manager(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        team_id: TeamId,
        initiator_slug: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<inferadb_ledger_types::UserId, Status> {
        let (initiator_id, org) = self.resolve_user_and_organization(
            slug_resolver,
            organization_id,
            initiator_slug,
            ctx,
        )?;

        let is_org_admin = org
            .members
            .iter()
            .any(|m| m.user_id == initiator_id && m.role == DomainMemberRole::Admin);
        let is_team_manager = self.read_team(organization_id, team_id).is_some_and(|t| {
            t.members.iter().any(|m| {
                m.user_id == initiator_id
                    && m.role == inferadb_ledger_state::system::TeamMemberRole::Manager
            })
        });
        if !is_org_admin && !is_team_manager {
            ctx.set_error("PermissionDenied", "Must be org admin or team manager");
            return Err(Status::permission_denied(
                "Must be an organization administrator or team manager",
            ));
        }

        Ok(initiator_id)
    }

    /// Converts a domain `OrganizationMember` to its wire representation.
    ///
    /// Returns `None` if the user cannot be resolved (e.g. deleted user).
    pub(super) fn member_to_wire(
        sys_svc: &SystemOrganizationService<FileBackend>,
        member: &DomainOrganizationMember,
    ) -> Option<ws::OrganizationMember> {
        sys_svc.get_user(member.user_id).ok().flatten().map(|user| {
            let role = match member.role {
                DomainMemberRole::Admin => ws::OrganizationMemberRole::Admin,
                DomainMemberRole::Member => ws::OrganizationMemberRole::Member,
            };
            ws::OrganizationMember {
                user: Some(user.slug),
                role,
                joined_at: member.joined_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64,
            }
        })
    }

    /// Builds a `GetOrganizationResponse` from Raft state + StateLayer data.
    ///
    /// Accepts an optional pre-loaded `Organization` to avoid a redundant
    /// StateLayer read when the caller already has it (e.g. from `validate_org_member`).
    pub(super) fn build_org_response(
        &self,
        org_meta: inferadb_ledger_raft::log_storage::OrganizationMeta,
        resolved_slug: DomainOrganizationSlug,
        cached_org: Option<Organization>,
    ) -> w::GetOrganizationResponse {
        let org = cached_org.or_else(|| self.read_organization(org_meta.organization));
        let registry = self.read_org_registry(org_meta.organization);

        // Domain → wire enum mapping (numeric tags differ between proto and
        // the wire enum's reordered ordinals, so map by name).
        let status = match org_meta.status {
            DomainOrganizationStatus::Active => ws::OrganizationStatus::Active,
            DomainOrganizationStatus::Provisioning => ws::OrganizationStatus::Provisioning,
            DomainOrganizationStatus::Migrating => ws::OrganizationStatus::Migrating,
            DomainOrganizationStatus::Suspended => ws::OrganizationStatus::Suspended,
            DomainOrganizationStatus::Deleted => ws::OrganizationStatus::Deleted,
        };
        let tier = match org_meta.tier {
            DomainOrganizationTier::Free => ws::OrganizationTier::Free,
            DomainOrganizationTier::Launch => ws::OrganizationTier::Launch,
            DomainOrganizationTier::Scale => ws::OrganizationTier::Scale,
        };

        let name = org.as_ref().map_or(String::new(), |o| o.name.clone());
        let member_nodes = registry.as_ref().map_or(vec![], |r| r.member_nodes.to_vec());
        let config_version = registry.as_ref().map_or(0, |r| r.config_version);
        let created_at = registry
            .as_ref()
            .map(|r| r.created_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64)
            .unwrap_or(0);

        let members = org.as_ref().map_or(vec![], |o| {
            let sys_svc = self.ctx.system_service();
            o.members.iter().filter_map(|m| Self::member_to_wire(&sys_svc, m)).collect()
        });
        let updated_at = org
            .as_ref()
            .map(|o| o.updated_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64)
            .unwrap_or(0);

        w::GetOrganizationResponse {
            slug: Some(resolved_slug),
            name,
            region: org_meta.region.as_str().to_string(),
            member_nodes,
            status,
            config_version,
            created_at,
            tier,
            members,
            updated_at,
        }
    }
}
