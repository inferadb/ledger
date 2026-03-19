//! Organization service implementation.
//!
//! Handles organization lifecycle: creation, deletion, retrieval, listing,
//! and region migration. Write operations flow through Raft for consistency;
//! read operations hit the local applied state directly.
//!
//! Organization creation uses a fire-and-forget saga that separates GLOBAL
//! directory metadata from regional PII (organization name). Region migration
//! also uses a saga for the orchestrator to drive.

use inferadb_ledger_proto::proto::{
    self, AddTeamMemberRequest, AddTeamMemberResponse, CreateOrganizationRequest,
    CreateOrganizationResponse, CreateOrganizationTeamRequest, CreateOrganizationTeamResponse,
    DeleteOrganizationRequest, DeleteOrganizationResponse, DeleteOrganizationTeamRequest,
    DeleteOrganizationTeamResponse, GetOrganizationRequest, GetOrganizationResponse,
    GetOrganizationTeamRequest, GetOrganizationTeamResponse, ListOrganizationMembersRequest,
    ListOrganizationMembersResponse, ListOrganizationTeamsRequest, ListOrganizationTeamsResponse,
    ListOrganizationsRequest, ListOrganizationsResponse, MigrateOrganizationRequest,
    MigrateOrganizationResponse, NodeId, OrganizationSlug,
    OrganizationStatus as ProtoOrganizationStatus, Region as ProtoRegion,
    RemoveOrganizationMemberRequest, RemoveOrganizationMemberResponse, RemoveTeamMemberRequest,
    RemoveTeamMemberResponse, UpdateOrganizationMemberRoleRequest,
    UpdateOrganizationMemberRoleResponse, UpdateOrganizationRequest, UpdateOrganizationResponse,
    UpdateOrganizationTeamRequest, UpdateOrganizationTeamResponse,
};
use inferadb_ledger_raft::{
    error::ServiceError,
    logging::RequestContext,
    metrics, trace_context,
    types::{LedgerRequest, LedgerResponse},
};
use inferadb_ledger_state::system::{
    Organization, OrganizationMember as DomainOrganizationMember,
    OrganizationMemberRole as DomainMemberRole, OrganizationProfile, OrganizationRegistry,
    OrganizationStatus as DomainOrganizationStatus, SYSTEM_VAULT_ID, SystemKeys,
    SystemOrganizationService, Team,
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    OrganizationId as DomainOrganizationId, OrganizationSlug as DomainOrganizationSlug, TeamId,
    decode,
    events::{EventAction, EventOutcome as EventOutcomeType},
    validation,
};
use tonic::{Request, Response, Status};

use super::{service_infra::ServiceContext, slug_resolver::SlugResolver};

/// Organization lifecycle: creation, deletion, retrieval, listing, and region migration.
pub struct OrganizationService {
    ctx: ServiceContext,
}

impl OrganizationService {
    /// Creates a new `OrganizationService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Validates that a protected region has sufficient in-region nodes for quorum.
    ///
    /// Returns `Ok(())` if the region is non-protected or has at least 3 in-region nodes.
    /// Returns a `FailedPrecondition` status with structured error details otherwise.
    fn validate_region_nodes(&self, region: inferadb_ledger_types::Region) -> Result<(), Status> {
        if !region.requires_residency() {
            return Ok(());
        }
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let nodes = sys_svc.list_nodes().map_err(|e| {
            tracing::error!(error = %e, "Failed to list nodes for region validation");
            Status::internal("Internal error")
        })?;
        let in_region_count = nodes.iter().filter(|n| n.region == region).count();
        if in_region_count >= 3 {
            return Ok(());
        }
        let mut context = std::collections::HashMap::new();
        context.insert("region".to_string(), region.as_str().to_string());
        context.insert("available_nodes".to_string(), in_region_count.to_string());
        context.insert("required_nodes".to_string(), "3".to_string());
        let details = super::error_details::build_error_details(
            inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes.as_u16(),
            false,
            None,
            context,
            Some(
                inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes
                    .suggested_action(),
            ),
        );
        let encoded = prost::Message::encode_to_vec(&details);
        Err(Status::with_details(
            tonic::Code::FailedPrecondition,
            format!(
                "Insufficient in-region nodes for protected region {}: \
                 {in_region_count} available, 3 required",
                region.as_str()
            ),
            encoded.into(),
        ))
    }

    /// Reads the `Organization` skeleton from the GLOBAL state layer and overlays
    /// PII (name) from the REGIONAL `OrganizationProfile`.
    ///
    /// Returns the merged view. When REGIONAL is unavailable, the skeleton
    /// is returned with `name: ""` (graceful degradation).
    fn read_organization(&self, org_id: DomainOrganizationId) -> Option<Organization> {
        let key = SystemKeys::organization_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        let mut org = decode::<Organization>(&entity.value)
            .inspect_err(|e| {
                tracing::warn!(
                    organization = org_id.value(),
                    error = %e,
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
    fn read_org_registry(&self, org_id: DomainOrganizationId) -> Option<OrganizationRegistry> {
        let key = SystemKeys::organization_registry_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<OrganizationRegistry>(&entity.value).ok()
    }

    /// Reads a team record from the REGIONAL state layer.
    fn read_team(&self, org_id: DomainOrganizationId, team_id: TeamId) -> Option<Team> {
        let org_meta = self.ctx.applied_state.get_organization(org_id)?;
        let state = self.ctx.regional_state(org_meta.region).ok()?;
        let key = SystemKeys::team_key(org_id, team_id);
        let entity = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<Team>(&entity.value)
            .inspect_err(|e| {
                tracing::warn!(
                    organization = org_id.value(),
                    team = team_id.value(),
                    error = %e,
                    "corrupt team record, skipping"
                );
            })
            .ok()
    }

    /// Lists all team records for an organization from the REGIONAL state layer.
    fn list_teams(&self, org_id: DomainOrganizationId) -> Vec<Team> {
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

    /// Converts a domain `Team` to its proto representation.
    fn team_to_proto(
        sys_svc: &SystemOrganizationService<FileBackend>,
        team: &Team,
        org_slug: DomainOrganizationSlug,
    ) -> proto::OrganizationTeam {
        let members = team
            .members
            .iter()
            .filter_map(|m| {
                sys_svc.get_user(m.user_id).ok().flatten().map(|user| {
                    proto::OrganizationTeamMember {
                        user: Some(proto::UserSlug { slug: user.slug.value() }),
                        role: crate::proto_compat::team_member_role_to_proto(m.role).into(),
                        joined_at: Some(crate::proto_compat::datetime_to_proto(&m.joined_at)),
                    }
                })
            })
            .collect();

        proto::OrganizationTeam {
            slug: Some(proto::TeamSlug { slug: team.slug.value() }),
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            name: team.name.clone(),
            members,
            created_at: Some(crate::proto_compat::datetime_to_proto(&team.created_at)),
            updated_at: Some(crate::proto_compat::datetime_to_proto(&team.updated_at)),
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
        user_slug: &Option<proto::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(inferadb_ledger_types::UserId, Organization), Status> {
        let user_id = slug_resolver.extract_and_resolve_user(user_slug).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;

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
    fn validate_org_admin(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        initiator_slug: &Option<proto::UserSlug>,
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
    fn validate_org_member(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        caller_slug: &Option<proto::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(inferadb_ledger_types::UserId, Organization), Status> {
        let (caller_id, org) =
            self.resolve_user_and_organization(slug_resolver, organization_id, caller_slug, ctx)?;

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
    fn validate_org_admin_or_team_manager(
        &self,
        slug_resolver: &SlugResolver,
        organization_id: DomainOrganizationId,
        team_id: TeamId,
        initiator_slug: &Option<proto::UserSlug>,
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

    /// Converts a domain `OrganizationMember` to its proto representation.
    ///
    /// Returns `None` if the user cannot be resolved (e.g. deleted user).
    fn member_to_proto(
        sys_svc: &SystemOrganizationService<FileBackend>,
        member: &DomainOrganizationMember,
    ) -> Option<proto::OrganizationMember> {
        sys_svc.get_user(member.user_id).ok().flatten().map(|user| proto::OrganizationMember {
            user: Some(proto::UserSlug { slug: user.slug.value() }),
            role: crate::proto_compat::member_role_to_proto(member.role).into(),
            joined_at: Some(crate::proto_compat::datetime_to_proto(&member.joined_at)),
        })
    }

    /// Builds a `GetOrganizationResponse` from Raft state + StateLayer data.
    ///
    /// Accepts an optional pre-loaded `Organization` to avoid a redundant
    /// StateLayer read when the caller already has it (e.g. from `validate_org_member`).
    fn build_org_response(
        &self,
        org_meta: inferadb_ledger_raft::log_storage::OrganizationMeta,
        resolved_slug: DomainOrganizationSlug,
        cached_org: Option<Organization>,
    ) -> GetOrganizationResponse {
        let org = cached_org.or_else(|| self.read_organization(org_meta.organization));
        let registry = self.read_org_registry(org_meta.organization);
        let status = crate::proto_compat::organization_status_to_proto(org_meta.status);
        let tier = crate::proto_compat::organization_tier_to_proto(org_meta.tier);

        let name = org.as_ref().map_or(String::new(), |o| o.name.clone());
        let member_nodes = registry.as_ref().map_or(vec![], |r| {
            r.member_nodes.iter().map(|id| NodeId { id: id.to_string() }).collect()
        });
        let config_version = registry.as_ref().map_or(0, |r| r.config_version);
        let created_at =
            registry.as_ref().map(|r| crate::proto_compat::datetime_to_proto(&r.created_at));

        let members = org.as_ref().map_or(vec![], |o| {
            let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
            o.members.iter().filter_map(|m| Self::member_to_proto(&sys_svc, m)).collect()
        });
        let updated_at =
            org.as_ref().map(|o| crate::proto_compat::datetime_to_proto(&o.updated_at));

        GetOrganizationResponse {
            slug: Some(OrganizationSlug { slug: resolved_slug.value() }),
            name,
            region: ProtoRegion::from(org_meta.region).into(),
            member_nodes,
            status: status.into(),
            config_version,
            created_at,
            tier: tier.into(),
            members,
            updated_at,
        }
    }
}

#[tonic::async_trait]
impl proto::organization_service_server::OrganizationService for OrganizationService {
    /// Creates a new organization via a fire-and-forget saga.
    ///
    /// Returns immediately with the pre-generated slug and `Provisioning` status.
    /// The saga orchestrator drives the multi-step creation to completion
    /// asynchronously. Poll via `GetOrganization` for the final state.
    async fn create_organization(
        &self,
        request: Request<CreateOrganizationRequest>,
    ) -> Result<Response<CreateOrganizationResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "create_organization",
            &grpc_metadata,
            &trace_ctx,
        );

        // Validate organization name
        validation::validate_organization_name(&req.name, &self.ctx.validation_config)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Validate and convert proto region to domain region
        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        // GLOBAL is the control plane region — organizations must choose a data
        // residency region
        if region == inferadb_ledger_types::Region::GLOBAL {
            let mut context = std::collections::HashMap::new();
            context.insert("region".to_string(), "GLOBAL".to_string());
            let details = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppInvalidRegionAssignment.as_u16(),
                false,
                None,
                context,
                Some(
                    inferadb_ledger_types::DiagnosticCode::AppInvalidRegionAssignment
                        .suggested_action(),
                ),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            return Err(Status::with_details(
                tonic::Code::InvalidArgument,
                "Organizations cannot be assigned to the GLOBAL control plane region",
                encoded.into(),
            ));
        }

        // For protected regions, validate sufficient in-region nodes for quorum
        self.validate_region_nodes(region)?;

        // Resolve admin_slug → UserId (optional — if absent, org has no admin member)
        let admin_user_id = if let Some(admin_slug_proto) = req.admin {
            let admin_user_slug = inferadb_ledger_types::UserSlug::new(admin_slug_proto.slug);
            let sys_svc_admin = SystemOrganizationService::new(self.ctx.state.clone());
            sys_svc_admin
                .get_user_id_by_slug(admin_user_slug)
                .map_err(|e| {
                    tracing::error!(error = %e, "Failed to resolve admin slug");
                    Status::internal("Internal error")
                })?
                .ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "Admin user with slug {} not found",
                        admin_slug_proto.slug
                    ))
                })?
        } else {
            inferadb_ledger_types::UserId::new(0)
        };

        // Generate a Snowflake slug for the organization
        let slug = inferadb_ledger_types::snowflake::generate_organization_slug().map_err(|e| {
            tracing::error!(error = %e, "Failed to generate organization slug");
            Status::internal("Internal error")
        })?;

        let tier = crate::proto_compat::organization_tier_from_proto(req.tier());

        // Submit saga via orchestrator handle (PII stays in-memory, not in GLOBAL Raft log)
        let saga_id = inferadb_ledger_state::system::SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = inferadb_ledger_state::system::CreateOrganizationSaga::new(
            saga_id.clone(),
            inferadb_ledger_state::system::CreateOrganizationInput {
                slug,
                region,
                tier,
                admin: admin_user_id,
            },
        );

        let saga_handle = self.ctx.saga_handle.get().ok_or_else(|| {
            Status::unavailable("Saga orchestrator not ready — try again shortly")
        })?;

        saga_handle
            .submit_saga(inferadb_ledger_raft::SagaSubmission {
                record: inferadb_ledger_state::system::Saga::CreateOrganization(saga),
                pii: None,
                org_pii: Some(inferadb_ledger_raft::OrgPii { name: req.name.clone() }),
                notify: None, // fire-and-forget — client gets slug immediately
            })
            .await
            .map_err(|e| Status::unavailable(format!("Failed to submit saga: {e}")))?;

        // Saga is now persisted. The orchestrator will drive it to completion.
        if let Some(node_id) = self.ctx.node_id {
            self.ctx.record_handler_event(
                inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                    EventAction::OrganizationCreated,
                    inferadb_ledger_types::OrganizationId::new(0),
                    Some(slug),
                    node_id,
                )
                .principal("system")
                .detail("saga_id", saga_id.value())
                .detail("region", region.as_str())
                .trace_id(&trace_ctx.trace_id)
                .outcome(EventOutcomeType::Success)
                .build(self.ctx.default_ttl_days()),
            );
        }

        ctx.set_organization(slug.value());
        ctx.set_region(region);
        ctx.set_success();

        Ok(Response::new(CreateOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: slug.value() }),
            name: req.name,
            region: req.region,
            member_nodes: vec![],
            status: ProtoOrganizationStatus::Provisioning as i32,
            config_version: 0,
            created_at: None,
            tier: req.tier.unwrap_or(0),
            members: vec![],
            updated_at: None,
        }))
    }

    /// Deletes an organization and all its vaults via Raft consensus.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn delete_organization(
        &self,
        request: Request<DeleteOrganizationRequest>,
    ) -> Result<Response<DeleteOrganizationResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "delete_organization",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator
        self.validate_org_admin(&slug_resolver, organization_id, &req.initiator, &mut ctx)?;

        // Best-effort revocation of pending invitations BEFORE deleting the org.
        // Must happen first because ResolveOrganizationInvite's apply handler
        // requires the org to be Active (require_active_org_with_state).
        // Failures are logged — InviteMaintenance will expire them by TTL.
        if let Some(org_meta) = self.ctx.applied_state.get_organization(organization_id)
            && let Ok(regional_state) = self.ctx.regional_state(org_meta.region)
        {
            let sys_svc = SystemOrganizationService::new(regional_state);
            if let Ok(invitations) = sys_svc.list_invitations_by_org(organization_id, None, 1000) {
                for inv in invitations {
                    if inv.status != inferadb_ledger_types::InvitationStatus::Pending {
                        continue;
                    }
                    // GLOBAL resolve
                    let global_req = LedgerRequest::ResolveOrganizationInvite {
                        invite: inv.id,
                        organization: organization_id,
                        status: inferadb_ledger_types::InvitationStatus::Revoked,
                        invitee_email_hmac: inv.invitee_email_hmac.clone(),
                        token_hash: inv.token_hash,
                    };
                    if let Err(e) =
                        self.ctx.propose_request(global_req, &grpc_metadata, &mut ctx).await
                    {
                        tracing::warn!(
                            invite_id = inv.id.value(),
                            error = %e,
                            "Failed to revoke pending invitation during org deletion"
                        );
                    }
                    // REGIONAL status update
                    if let Err(e) = self.ctx.propose_regional_org_encrypted(
                        org_meta.region,
                        inferadb_ledger_raft::types::SystemRequest::UpdateOrganizationInviteStatus {
                            organization: organization_id,
                            invite: inv.id,
                            status: inferadb_ledger_types::InvitationStatus::Revoked,
                        },
                        organization_id,
                        &grpc_metadata,
                        &mut ctx,
                    )
                    .await
                    {
                        tracing::warn!(
                            invite_id = inv.id.value(),
                            error = %e,
                            "REGIONAL invite revocation failed during org deletion"
                        );
                    }
                }
            }
        }

        // Submit delete organization through Raft
        let ledger_request = LedgerRequest::DeleteOrganization { organization: organization_id };

        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::OrganizationDeleted {
                organization_id: deleted_org_id,
                deleted_at,
                retention_days,
            } => {
                ctx.set_success();
                metrics::record_organization_operation(deleted_org_id, "delete");
                metrics::record_organization_latency(deleted_org_id, "delete", ctx.elapsed_secs());

                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                            EventAction::OrganizationDeleted,
                            deleted_org_id,
                            Some(DomainOrganizationSlug::new(organization_slug_val)),
                            node_id,
                        )
                        .principal("system")
                        .trace_id(&trace_ctx.trace_id)
                        .outcome(EventOutcomeType::Success)
                        .build(self.ctx.default_ttl_days()),
                    );
                }

                Ok(Response::new(DeleteOrganizationResponse {
                    deleted_at: Some(crate::proto_compat::datetime_to_proto(&deleted_at)),
                    retention_days,
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    /// Retrieves organization metadata by slug, including region assignment and status.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    /// Caller must be a member of the organization.
    async fn get_organization(
        &self,
        request: Request<GetOrganizationRequest>,
    ) -> Result<Response<GetOrganizationResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "get_organization",
            &grpc_metadata,
            &trace_ctx,
        );

        // Extract organization from request
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        // Validate caller is a member of the organization
        let (_, profile) =
            self.validate_org_member(&slug_resolver, organization_id, &req.caller, &mut ctx)?;

        let org_meta = self.ctx.applied_state.get_organization(organization_id);

        match org_meta {
            Some(org) if org.status == DomainOrganizationStatus::Deleted => {
                ctx.set_error("NotFound", "Organization not found");
                Err(ServiceError::not_found("Organization", organization_slug_val.to_string())
                    .into())
            },
            Some(org) => {
                ctx.set_success();
                let organization = slug_resolver.resolve_slug(org.organization)?;
                Ok(Response::new(self.build_org_response(org, organization, Some(profile))))
            },
            None => {
                ctx.set_error("NotFound", "Organization not found");
                Err(ServiceError::not_found("Organization", organization_slug_val.to_string())
                    .into())
            },
        }
    }

    /// Lists organizations the caller belongs to, with cursor-based pagination.
    ///
    /// Only non-deleted organizations where the caller is a member are returned.
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_organizations(
        &self,
        request: Request<ListOrganizationsRequest>,
    ) -> Result<Response<ListOrganizationsResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "list_organizations",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());

        // Resolve caller — required for authorization filtering
        let caller_id =
            slug_resolver.extract_and_resolve_user(&req.caller).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let page_size = crate::proto_compat::normalize_page_size(req.page_size);
        let start_after = crate::proto_compat::decode_page_token(&req.page_token);

        // Use user→org index for O(1) membership lookup instead of loading
        // every organization profile.
        let caller_org_ids = self.ctx.applied_state.user_organization_ids(caller_id);
        let all_orgs = self.ctx.applied_state.list_organizations();
        let orgs_with_slugs: Vec<_> = all_orgs
            .into_iter()
            .filter(|org| caller_org_ids.contains(&org.organization))
            .filter_map(|org| {
                let slug = slug_resolver.resolve_slug(org.organization).ok()?;
                Some((slug.value(), (slug, org)))
            })
            .collect();

        let (page, next_page_token) =
            crate::proto_compat::paginate_by_slug(orgs_with_slugs, start_after, page_size);

        let organizations: Vec<_> =
            page.into_iter().map(|(slug, org)| self.build_org_response(org, slug, None)).collect();

        ctx.set_keys_count(organizations.len());
        ctx.set_success();

        Ok(Response::new(ListOrganizationsResponse { organizations, next_page_token }))
    }

    /// Migrates an organization to a different region via saga.
    ///
    /// Validates the migration is possible, submits a `StartMigration` through Raft,
    /// and creates a saga for the orchestrator to drive.
    async fn migrate_organization(
        &self,
        request: Request<MigrateOrganizationRequest>,
    ) -> Result<Response<MigrateOrganizationResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "migrate_organization",
            &grpc_metadata,
            &trace_ctx,
        );

        // Resolve organization slug → internal ID
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator
        self.validate_org_admin(&slug_resolver, organization_id, &req.initiator, &mut ctx)?;

        // Validate and convert proto region to domain region
        let target_region = inferadb_ledger_proto::convert::region_from_i32(req.target_region)?;

        // Look up current organization state
        let org_meta =
            self.ctx.applied_state.get_organization(organization_id).ok_or_else(|| {
                ctx.set_error("NotFound", "Organization not found");
                let err: Status =
                    ServiceError::not_found("Organization", organization_slug_val.to_string())
                        .into();
                err
            })?;

        let source_region = org_meta.region;

        // Validate: target must differ from source
        if target_region == source_region {
            return Err(Status::invalid_argument(format!(
                "Organization is already in region {}",
                source_region.as_str()
            )));
        }

        // Validate: organization must be Active
        if org_meta.status != DomainOrganizationStatus::Active {
            let mut context = std::collections::HashMap::new();
            context.insert("status".to_string(), format!("{:?}", org_meta.status));
            let details = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating.as_u16(),
                true,
                None,
                context,
                Some(
                    inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating
                        .suggested_action(),
                ),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            return Err(Status::with_details(
                tonic::Code::FailedPrecondition,
                format!("Organization is not Active (current status: {:?})", org_meta.status),
                encoded.into(),
            ));
        }

        // Validate: GLOBAL is not a valid target
        if target_region == inferadb_ledger_types::Region::GLOBAL {
            return Err(Status::invalid_argument("Cannot migrate to GLOBAL control plane region"));
        }

        // Validate: protected → non-protected requires acknowledgment
        if source_region.requires_residency()
            && !target_region.requires_residency()
            && !req.acknowledge_residency_downgrade
        {
            let mut context = std::collections::HashMap::new();
            context.insert("source_region".to_string(), source_region.as_str().to_string());
            context.insert("target_region".to_string(), target_region.as_str().to_string());
            let details = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppInvalidRegionAssignment.as_u16(),
                false,
                None,
                context,
                Some(
                    "Set acknowledge_residency_downgrade = true to confirm migration from a protected to non-protected region",
                ),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            return Err(Status::with_details(
                tonic::Code::FailedPrecondition,
                "Migration from protected to non-protected region requires explicit acknowledgment",
                encoded.into(),
            ));
        }

        // For protected target regions, validate sufficient in-region nodes
        self.validate_region_nodes(target_region)?;

        // Determine migration type: metadata-only for non-protected → non-protected
        let metadata_only =
            !source_region.requires_residency() && !target_region.requires_residency();

        // Build the migration saga for the orchestrator to drive.
        // The saga starts in MigrationStarted state because StartMigration
        // sets the organization status to Migrating atomically in the same
        // Raft entry (via BatchWrite below).
        let saga_id = inferadb_ledger_state::system::SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = inferadb_ledger_state::system::MigrateOrgSaga::new(
            saga_id,
            inferadb_ledger_state::system::MigrateOrgInput {
                organization_id,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(
                    organization_slug_val,
                ),
                source_region,
                target_region,
                acknowledge_residency_downgrade: req.acknowledge_residency_downgrade,
                metadata_only,
            },
        );

        let saga_key = format!("_meta:saga:{}", saga.id);
        let saga_wrapped = inferadb_ledger_state::system::Saga::MigrateOrg(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped).map_err(|e| {
            tracing::error!(error = %e, "Failed to serialize migration saga");
            Status::internal("Internal error")
        })?;

        let saga_op = inferadb_ledger_types::Operation::SetEntity {
            key: saga_key,
            value: saga_bytes,
            expires_at: None,
            condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
        };

        let saga_txn = inferadb_ledger_types::Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: inferadb_ledger_types::ClientId::new("system:organization"),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![saga_op],
            timestamp: chrono::Utc::now(),
            actor: "system:organization".to_string(),
        };

        let saga_write = LedgerRequest::Write {
            organization: DomainOrganizationId::new(0), // _system
            vault: inferadb_ledger_types::VaultId::new(0),
            transactions: vec![saga_txn],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        // Submit both StartMigration and saga write as a single atomic Raft
        // entry via BatchWrite. This ensures the organization status change
        // and saga persistence are committed together — if one fails, neither
        // takes effect.
        let batch_request = LedgerRequest::BatchWrite {
            requests: vec![
                LedgerRequest::StartMigration {
                    organization: organization_id,
                    target_region_group: target_region,
                },
                saga_write,
            ],
        };

        let response = self.ctx.propose_request(batch_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::BatchWrite { responses } => {
                // BatchWrite processes all inner requests in a single Raft log
                // entry — if any request fails, the entire entry is rejected.
                // We still check both responses defensively.
                if let Some(LedgerResponse::Error { code, message }) = responses.get(1) {
                    ctx.set_error(code.grpc_code_name(), message);
                    return Err(super::helpers::error_code_to_status(
                        *code,
                        format!("Saga persistence failed: {message}"),
                    ));
                }

                match responses.first() {
                    Some(LedgerResponse::MigrationStarted {
                        organization,
                        target_region_group,
                    }) => {
                        ctx.set_success();
                        metrics::record_organization_operation(*organization, "migrate");
                        let proto_source: ProtoRegion = source_region.into();
                        let proto_target: ProtoRegion = (*target_region_group).into();
                        Ok(Response::new(MigrateOrganizationResponse {
                            slug: Some(OrganizationSlug { slug: organization_slug_val }),
                            source_region: proto_source.into(),
                            target_region: proto_target.into(),
                            status: ProtoOrganizationStatus::Migrating.into(),
                        }))
                    },
                    Some(LedgerResponse::Error { code, message }) => {
                        ctx.set_error(code.grpc_code_name(), message);
                        Err(super::helpers::error_code_to_status(*code, message.clone()))
                    },
                    _ => {
                        ctx.set_error("UnexpectedResponse", "Unexpected batch response");
                        Err(Status::internal("Unexpected batch response"))
                    },
                }
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    /// Updates organization metadata (currently: name) via Raft consensus.
    async fn update_organization(
        &self,
        request: Request<UpdateOrganizationRequest>,
    ) -> Result<Response<UpdateOrganizationResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "update_organization",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator
        self.validate_org_admin(&slug_resolver, organization_id, &req.initiator, &mut ctx)?;

        let name = match req.name {
            Some(ref n) => {
                validation::validate_organization_name(n, &self.ctx.validation_config)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                n.clone()
            },
            None => {
                return Err(Status::invalid_argument(
                    "At least one field must be provided for update",
                ));
            },
        };

        // Resolve the organization's data residency region
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        // Route the name update to the REGIONAL Raft group — name is PII
        // and must not appear in the GLOBAL Raft log. Encrypted with OrgShredKey
        // for crypto-shredding on organization purge.
        let system_request =
            inferadb_ledger_raft::types::SystemRequest::UpdateOrganizationProfile {
                organization: organization_id,
                name,
            };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { organization_id: updated_org_id } => {
                ctx.set_success();
                metrics::record_organization_operation(updated_org_id, "update");
                metrics::record_organization_latency(updated_org_id, "update", ctx.elapsed_secs());

                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                            EventAction::OrganizationUpdated,
                            updated_org_id,
                            Some(DomainOrganizationSlug::new(organization_slug_val)),
                            node_id,
                        )
                        .principal("system")
                        .trace_id(&trace_ctx.trace_id)
                        .outcome(EventOutcomeType::Success)
                        .build(self.ctx.default_ttl_days()),
                    );
                }

                // Re-read the org to return full response
                let org_meta = self
                    .ctx
                    .applied_state
                    .get_organization(updated_org_id)
                    .ok_or_else(|| Status::internal("Organization not found after update"))?;
                let resolved_slug = slug_resolver.resolve_slug(org_meta.organization)?;
                let get_resp = self.build_org_response(org_meta, resolved_slug, None);

                Ok(Response::new(UpdateOrganizationResponse {
                    slug: get_resp.slug,
                    name: get_resp.name,
                    region: get_resp.region,
                    member_nodes: get_resp.member_nodes,
                    status: get_resp.status,
                    config_version: get_resp.config_version,
                    created_at: get_resp.created_at,
                    tier: get_resp.tier,
                    members: get_resp.members,
                    updated_at: get_resp.updated_at,
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    /// Lists all members of an organization with cursor-based pagination.
    ///
    /// Caller must be a member of the organization (any role).
    async fn list_organization_members(
        &self,
        request: Request<ListOrganizationMembersRequest>,
    ) -> Result<Response<ListOrganizationMembersResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "list_organization_members",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        // Validate caller is a member (also returns the profile)
        let (_, profile) =
            self.validate_org_member(&slug_resolver, organization_id, &req.caller, &mut ctx)?;

        let page_size = crate::proto_compat::normalize_page_size(req.page_size);
        let start_after = crate::proto_compat::decode_page_token(&req.page_token);

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());

        // Resolve member slugs, then paginate by slug cursor
        let members_with_slugs: Vec<_> = profile
            .members
            .iter()
            .filter_map(|m| {
                let proto_member = Self::member_to_proto(&sys_svc, m)?;
                let user_slug_val = proto_member.user.as_ref().map_or(0, |u| u.slug);
                Some((user_slug_val, proto_member))
            })
            .collect();

        let (members, next_page_token) =
            crate::proto_compat::paginate_by_slug(members_with_slugs, start_after, page_size);

        ctx.set_success();
        Ok(Response::new(ListOrganizationMembersResponse { members, next_page_token }))
    }

    /// Removes a member from an organization.
    ///
    /// Self-removal: any member can leave unless they are the last admin.
    /// Removing others: initiator must be an admin.
    async fn remove_organization_member(
        &self,
        request: Request<RemoveOrganizationMemberRequest>,
    ) -> Result<Response<RemoveOrganizationMemberResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "remove_organization_member",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        let initiator_id =
            slug_resolver.extract_and_resolve_user(&req.initiator).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let target_id =
            slug_resolver.extract_and_resolve_user(&req.target).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        // Read the profile to validate the operation
        let profile = self.read_organization(organization_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Organization profile not found");
            Status::not_found("Organization not found")
        })?;

        if initiator_id == target_id {
            // Self-removal: check target is a member
            let target_member = profile.members.iter().find(|m| m.user_id == target_id);
            match target_member {
                None => {
                    ctx.set_error("NotFound", "User is not a member");
                    return Err(Status::not_found("User is not a member of the organization"));
                },
                Some(m)
                    if m.role == DomainMemberRole::Admin
                        && profile
                            .members
                            .iter()
                            .filter(|m| m.role == DomainMemberRole::Admin)
                            .count()
                            <= 1 =>
                {
                    ctx.set_error("FailedPrecondition", "Cannot remove last admin");
                    return Err(Status::failed_precondition(
                        "Cannot remove the last administrator from the organization",
                    ));
                },
                Some(_) => {},
            }
        } else {
            // Removing others: initiator must be admin
            let is_admin = profile
                .members
                .iter()
                .any(|m| m.user_id == initiator_id && m.role == DomainMemberRole::Admin);
            if !is_admin {
                ctx.set_error("PermissionDenied", "User is not an organization administrator");
                return Err(Status::permission_denied("User is not an organization administrator"));
            }
            // Verify target is actually a member
            if !profile.members.iter().any(|m| m.user_id == target_id) {
                ctx.set_error("NotFound", "Target is not a member");
                return Err(Status::not_found("Target user is not a member of the organization"));
            }
        }

        let ledger_request = LedgerRequest::RemoveOrganizationMember {
            organization: organization_id,
            target: target_id,
        };
        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::OrganizationMemberRemoved { .. } => {
                ctx.set_success();
                Ok(Response::new(RemoveOrganizationMemberResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    /// Promotes or demotes an organization member.
    ///
    /// Initiator must be an admin. Cannot demote the last admin.
    async fn update_organization_member_role(
        &self,
        request: Request<UpdateOrganizationMemberRoleRequest>,
    ) -> Result<Response<UpdateOrganizationMemberRoleResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "update_organization_member_role",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        ctx.set_organization(organization_slug_val);

        // Validate initiator is admin
        self.validate_org_admin(&slug_resolver, organization_id, &req.initiator, &mut ctx)?;

        let target_id =
            slug_resolver.extract_and_resolve_user(&req.target).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let proto_role = proto::OrganizationMemberRole::try_from(req.role).map_err(|_| {
            ctx.set_error("InvalidArgument", "Invalid role value");
            Status::invalid_argument(format!("Invalid organization member role: {}", req.role))
        })?;
        if proto_role == proto::OrganizationMemberRole::Unspecified {
            ctx.set_error("InvalidArgument", "Role must be specified");
            return Err(Status::invalid_argument(
                "Organization member role must be specified (ADMIN or MEMBER)",
            ));
        }
        let new_role = crate::proto_compat::member_role_from_proto(proto_role);

        // Read profile to validate target is a member and check last-admin rule
        let profile = self.read_organization(organization_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Organization profile not found");
            Status::not_found("Organization not found")
        })?;

        let target_member =
            profile.members.iter().find(|m| m.user_id == target_id).ok_or_else(|| {
                ctx.set_error("NotFound", "Target is not a member");
                Status::not_found("Target user is not a member of the organization")
            })?;

        // Prevent demoting the last admin
        if target_member.role == DomainMemberRole::Admin
            && new_role == DomainMemberRole::Member
            && profile.members.iter().filter(|m| m.role == DomainMemberRole::Admin).count() <= 1
        {
            ctx.set_error("FailedPrecondition", "Cannot demote last admin");
            return Err(Status::failed_precondition(
                "Cannot demote the last administrator of the organization",
            ));
        }

        let ledger_request = LedgerRequest::UpdateOrganizationMemberRole {
            organization: organization_id,
            target: target_id,
            role: new_role,
        };
        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::OrganizationMemberRoleUpdated { .. } => {
                ctx.set_success();

                // Re-read profile after Raft apply for fresh data
                let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
                let member = self.read_organization(organization_id).and_then(|p| {
                    p.members
                        .iter()
                        .find(|m| m.user_id == target_id)
                        .and_then(|m| Self::member_to_proto(&sys_svc, m))
                });

                Ok(Response::new(UpdateOrganizationMemberRoleResponse { member }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    // =========================================================================
    // Organization Teams
    // =========================================================================

    /// Lists teams in an organization with cursor-based pagination.
    ///
    /// Admins see all teams; non-admin members see only teams they belong to.
    async fn list_organization_teams(
        &self,
        request: Request<ListOrganizationTeamsRequest>,
    ) -> Result<Response<ListOrganizationTeamsResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "list_organization_teams",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_id =
            slug_resolver.extract_and_resolve(&inner.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Resolve caller, verify membership, and determine role
        let (caller_id, profile) =
            self.validate_org_member(&slug_resolver, organization_id, &inner.caller, &mut ctx)?;

        let is_admin = profile
            .members
            .iter()
            .any(|m| m.user_id == caller_id && m.role == DomainMemberRole::Admin);

        let mut teams = self.list_teams(organization_id);
        if !is_admin {
            // Non-admin members only see teams they belong to
            teams.retain(|t| t.members.iter().any(|m| m.user_id == caller_id));
        }

        let page_size = crate::proto_compat::normalize_page_size(inner.page_size);
        let start_after = crate::proto_compat::decode_page_token(&inner.page_token);

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());

        let teams_with_slugs: Vec<_> = teams
            .iter()
            .map(|t| {
                let proto_team = Self::team_to_proto(&sys_svc, t, org_slug);
                let team_slug_val = proto_team.slug.as_ref().map_or(0, |s| s.slug);
                (team_slug_val, proto_team)
            })
            .collect();

        let (proto_teams, next_page_token) =
            crate::proto_compat::paginate_by_slug(teams_with_slugs, start_after, page_size);

        ctx.set_success();
        Ok(Response::new(ListOrganizationTeamsResponse { teams: proto_teams, next_page_token }))
    }

    /// Retrieves a single team by slug.
    ///
    /// Admins can see any team in the organization; non-admin members can
    /// only see teams they belong to (consistent with list behavior).
    async fn get_organization_team(
        &self,
        request: Request<GetOrganizationTeamRequest>,
    ) -> Result<Response<GetOrganizationTeamResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "get_organization_team",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) =
            slug_resolver.extract_and_resolve_team(&inner.slug).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Verify caller is an org member
        let (caller_id, profile) =
            self.validate_org_member(&slug_resolver, organization_id, &inner.caller, &mut ctx)?;

        let is_admin = profile
            .members
            .iter()
            .any(|m| m.user_id == caller_id && m.role == DomainMemberRole::Admin);

        // Read team profile from REGIONAL state
        let team = self.read_team(organization_id, team_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Team not found");
            Status::not_found("Team not found")
        })?;

        // Non-admin members can only see teams they belong to
        if !is_admin && !team.members.iter().any(|m| m.user_id == caller_id) {
            ctx.set_error("NotFound", "Team not found");
            return Err(Status::not_found("Team not found"));
        }

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let proto_team = Self::team_to_proto(&sys_svc, &team, org_slug);

        ctx.set_success();
        Ok(Response::new(GetOrganizationTeamResponse { team: Some(proto_team) }))
    }

    async fn create_organization_team(
        &self,
        request: Request<CreateOrganizationTeamRequest>,
    ) -> Result<Response<CreateOrganizationTeamResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "create_organization_team",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_id =
            slug_resolver.extract_and_resolve(&inner.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Resolve initiator and check authorization (must be org admin)
        let initiator_id =
            slug_resolver.extract_and_resolve_user(&inner.initiator).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let profile = self.read_organization(organization_id);
        let is_org_admin = profile.as_ref().is_some_and(|p| {
            p.members.iter().any(|m| m.user_id == initiator_id && m.role == DomainMemberRole::Admin)
        });
        if !is_org_admin {
            ctx.set_error("PermissionDenied", "Must be org admin to create a team");
            return Err(Status::permission_denied(
                "Must be an organization administrator to create a team",
            ));
        }

        // Validate team name
        let name = inner.name.trim().to_string();
        if let Err(e) = validation::validate_organization_name(&name, &self.ctx.validation_config) {
            ctx.set_error("InvalidArgument", &e.to_string());
            return Err(Status::invalid_argument(e.to_string()));
        }

        // Generate team slug
        let team_slug = inferadb_ledger_types::snowflake::generate_team_slug().map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            tracing::error!(error = %e, "Failed to generate team slug");
            Status::internal("Internal error")
        })?;

        // Step 1 (GLOBAL): Create team directory entry (ID + slug only, no PII).
        let ledger_request = LedgerRequest::CreateOrganizationTeam {
            organization: organization_id,
            slug: team_slug,
        };
        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        let team_id = match response {
            LedgerResponse::OrganizationTeamCreated { team_id, .. } => team_id,
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                return Err(super::helpers::error_code_to_status(code, message));
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                return Err(Status::internal("Unexpected response type"));
            },
        };

        // Step 2 (REGIONAL): Write team name to the org's regional store.
        // Encrypted with OrgShredKey for crypto-shredding on organization purge.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        let system_request = inferadb_ledger_raft::types::SystemRequest::WriteTeam {
            organization: organization_id,
            team: team_id,
            slug: team_slug,
            name,
        };
        let profile_response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        if let LedgerResponse::Error { code, message } = profile_response {
            ctx.set_error(code.grpc_code_name(), &message);
            return Err(super::helpers::error_code_to_status(code, message));
        }

        ctx.set_success();
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let team = self
            .read_team(organization_id, team_id)
            .map(|t| Self::team_to_proto(&sys_svc, &t, org_slug));
        Ok(Response::new(CreateOrganizationTeamResponse { team }))
    }

    async fn delete_organization_team(
        &self,
        request: Request<DeleteOrganizationTeamRequest>,
    ) -> Result<Response<DeleteOrganizationTeamResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "delete_organization_team",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) =
            slug_resolver.extract_and_resolve_team(&inner.slug).inspect_err(|status| {
                ctx.set_error("NotFound", status.message());
            })?;

        // Resolve initiator and check authorization (must be org admin or team manager)
        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            &inner.initiator,
            &mut ctx,
        )?;

        // Resolve optional move target
        let move_to = slug_resolver
            .extract_and_resolve_team_optional(&inner.move_members_to)
            .inspect_err(|status| {
                ctx.set_error("NotFound", status.message());
            })?;

        // Validate move target belongs to the same organization
        if let Some((target_org_id, _)) = &move_to
            && *target_org_id != organization_id
        {
            ctx.set_error(
                "InvalidArgument",
                "move_members_to team must belong to the same organization",
            );
            return Err(Status::invalid_argument(
                "move_members_to team must belong to the same organization",
            ));
        }
        let move_to = move_to.map(|(_, target_team_id)| target_team_id);

        // Step 1 (REGIONAL): Delete profile, handle member migration, clean up name index.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        let delete_team = inferadb_ledger_raft::types::SystemRequest::DeleteTeam {
            organization: organization_id,
            team: team_id,
            move_members_to: move_to,
        };
        let delete_team_response = self
            .ctx
            .propose_regional(org_meta.region, delete_team, &grpc_metadata, &mut ctx)
            .await?;
        if let LedgerResponse::Error { code, message } = delete_team_response {
            ctx.set_error(code.grpc_code_name(), &message);
            return Err(super::helpers::error_code_to_status(code, message));
        }

        // Step 2 (GLOBAL): Clean up slug index and in-memory maps.
        let ledger_request =
            LedgerRequest::DeleteOrganizationTeam { organization: organization_id, team: team_id };
        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::OrganizationTeamDeleted { .. } => {
                ctx.set_success();
                Ok(Response::new(DeleteOrganizationTeamResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn update_organization_team(
        &self,
        request: Request<UpdateOrganizationTeamRequest>,
    ) -> Result<Response<UpdateOrganizationTeamResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "update_organization_team",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) =
            slug_resolver.extract_and_resolve_team(&inner.slug).inspect_err(|status| {
                ctx.set_error("NotFound", status.message());
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Validate initiator is org admin or team manager
        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            &inner.initiator,
            &mut ctx,
        )?;

        // Validate new name
        let name = match inner.name {
            Some(ref n) => {
                let trimmed = n.trim().to_string();
                if let Err(e) =
                    validation::validate_organization_name(&trimmed, &self.ctx.validation_config)
                {
                    ctx.set_error("InvalidArgument", &e.to_string());
                    return Err(Status::invalid_argument(e.to_string()));
                }
                trimmed
            },
            None => {
                return Err(Status::invalid_argument(
                    "At least one field must be provided for update",
                ));
            },
        };

        // Route name update to the org's regional Raft group (PII).
        // Encrypted with OrgShredKey for crypto-shredding on organization purge.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        let team_slug = self
            .ctx
            .applied_state
            .resolve_team_id_to_slug(team_id)
            .ok_or_else(|| Status::not_found(format!("Team {} slug not found", team_id)))?;
        let system_request = inferadb_ledger_raft::types::SystemRequest::WriteTeam {
            organization: organization_id,
            team: team_id,
            slug: team_slug,
            name,
        };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                ctx.set_success();
                let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
                let team = self
                    .read_team(organization_id, team_id)
                    .map(|t| Self::team_to_proto(&sys_svc, &t, org_slug));
                Ok(Response::new(UpdateOrganizationTeamResponse { team }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(response = %other, "Unexpected Raft response for UpdateOrganizationTeam");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn add_team_member(
        &self,
        request: Request<AddTeamMemberRequest>,
    ) -> Result<Response<AddTeamMemberResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "add_team_member",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(&inner.team)
            .inspect_err(|status| ctx.set_error("InvalidArgument", status.message()))?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            &inner.initiator,
            &mut ctx,
        )?;

        let user_id =
            slug_resolver.extract_and_resolve_user(&inner.user).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let role = crate::proto_compat::proto_to_team_member_role(inner.role());

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        // Verify the target user is a member of the organization
        let org_profile = self.read_organization(organization_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Organization profile not found");
            Status::not_found("Organization profile not found")
        })?;
        if !org_profile.members.iter().any(|m| m.user_id == user_id) {
            ctx.set_error("FailedPrecondition", "User is not a member of the organization");
            return Err(Status::failed_precondition("User is not a member of the organization"));
        }

        let system_request = inferadb_ledger_raft::types::SystemRequest::AddTeamMember {
            organization: organization_id,
            team: team_id,
            user_id,
            role,
        };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                ctx.set_success();
                let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
                let team = self
                    .read_team(organization_id, team_id)
                    .map(|t| Self::team_to_proto(&sys_svc, &t, org_slug));
                Ok(Response::new(AddTeamMemberResponse { team }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(response = %other, "Unexpected Raft response for AddTeamMember");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn remove_team_member(
        &self,
        request: Request<RemoveTeamMemberRequest>,
    ) -> Result<Response<RemoveTeamMemberResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let inner = request.into_inner();
        let mut ctx = self.ctx.make_request_context(
            "OrganizationService",
            "remove_team_member",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(&inner.team)
            .inspect_err(|status| ctx.set_error("InvalidArgument", status.message()))?;

        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            &inner.initiator,
            &mut ctx,
        )?;

        let user_id =
            slug_resolver.extract_and_resolve_user(&inner.user).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        let system_request = inferadb_ledger_raft::types::SystemRequest::RemoveTeamMember {
            organization: organization_id,
            team: team_id,
            user_id,
        };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                ctx.set_success();
                Ok(Response::new(RemoveTeamMemberResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(response = %other, "Unexpected Raft response for RemoveTeamMember");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }
}
