//! Wire-protocol implementation for `OrganizationService` (Phase F.1.f.1.5).
//!
//! Provides the [`inferadb_ledger_wire_services::OrganizationService`] impl
//! on [`super::organization::OrganizationService`]. The wire impl operates
//! on wire types end-to-end — no proto round-trip.

use std::str::FromStr;

use bytes::Bytes;
use inferadb_ledger_raft::{
    metrics,
    types::{LedgerResponse, OrganizationRequest, SystemRequest},
};
use inferadb_ledger_state::system::OrganizationStatus as DomainOrganizationStatus;
use inferadb_ledger_types::{
    OrganizationSlug as DomainOrganizationSlug, UserSlug as DomainUserSlug,
    events::{EventAction, EventOutcome as EventOutcomeType},
    validation,
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{organization as w, shared as ws},
};

use super::{
    error_classify, helpers, organization::OrganizationService, slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error,
};

// ---------------------------------------------------------------------------
// Wire-trait implementation for `OrganizationService`.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::OrganizationService for OrganizationService {
    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `create_organization` handler: validates region + tier, resolves the
    /// admin user slug, submits a fire-and-forget `CreateOrganizationSaga` to
    /// the orchestrator, and returns the pre-generated slug + `Provisioning`
    /// status. The saga drives the multi-step creation asynchronously.
    async fn create_organization(
        &self,
        request: w::CreateOrganizationRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateOrganizationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "create_organization", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Validate organization name.
        validation::validate_organization_name(&req.name, &self.ctx.validation_config).map_err(
            |e| {
                log_ctx.set_error("InvalidArgument", &e.to_string());
                WireError::new(ErrorCode::InvalidArgument, e.to_string())
            },
        )?;

        // Validate and convert region slug to domain region.
        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        // GLOBAL is the control plane region — organizations must choose a
        // data residency region.
        if region == inferadb_ledger_types::Region::GLOBAL {
            log_ctx.set_error(
                "InvalidArgument",
                "Organizations cannot be assigned to the GLOBAL control plane region",
            );
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "Organizations cannot be assigned to the GLOBAL control plane region",
            ));
        }

        // For protected regions, validate sufficient in-region nodes.
        self.validate_region_nodes(region).map_err(tonic_status_to_wire_error)?;

        // Resolve admin_slug → UserId. Every organization requires an admin.
        let admin_slug_wire = req.caller.filter(|s| s.value() != 0).ok_or_else(|| {
            WireError::new(
                ErrorCode::InvalidArgument,
                "caller is required: every organization must have an admin user",
            )
        })?;
        let admin_user_slug = DomainUserSlug::new(admin_slug_wire.value());
        let sys_svc_admin = self.ctx.system_service();
        let admin_user_id = sys_svc_admin
            .get_user_id_by_slug(admin_user_slug)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                WireError::new(
                    ErrorCode::InvalidArgument,
                    format!("Admin user with slug {} not found", admin_slug_wire.value()),
                )
            })?;

        // Client-supplied Snowflake slug — required. Retries across lost
        // responses MUST reuse the same slug so the saga's state-machine
        // idempotency path returns the existing `OrganizationId` instead of
        // creating an orphan body.
        let slug_wire = req.slug.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::InvalidArgument, "CreateOrganizationRequest.slug is required")
        })?;
        if slug_wire.value() == 0 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "CreateOrganizationRequest.slug must be a non-zero Snowflake",
            ));
        }
        let slug = DomainOrganizationSlug::new(slug_wire.value());

        let tier = match req.tier {
            Some(ws::OrganizationTier::Free) | Some(ws::OrganizationTier::Unspecified) | None => {
                inferadb_ledger_state::system::OrganizationTier::Free
            },
            Some(ws::OrganizationTier::Launch) => {
                inferadb_ledger_state::system::OrganizationTier::Launch
            },
            Some(ws::OrganizationTier::Scale) => {
                inferadb_ledger_state::system::OrganizationTier::Scale
            },
        };

        // Submit saga via orchestrator handle (PII stays in-memory, not in
        // GLOBAL Raft log).
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
            WireError::new(
                ErrorCode::StaleRouting,
                "Saga orchestrator not ready \u{2014} try again shortly",
            )
        })?;

        // Capture the originating W3C `traceparent` so the saga can
        // propagate it into downstream regional proposals.
        let traceparent = crate::services::wire_helpers::wire_context_traceparent(&_ctx);

        saga_handle
            .submit_saga(inferadb_ledger_raft::SagaSubmission {
                record: inferadb_ledger_state::system::Saga::CreateOrganization(saga),
                pii: None,
                org_pii: Some(inferadb_ledger_raft::OrgPii { name: req.name.clone() }),
                notify: None,
                traceparent,
            })
            .await
            .map_err(|e| {
                WireError::new(ErrorCode::StaleRouting, format!("Failed to submit saga: {e}"))
            })?;

        // Saga is now persisted. The orchestrator will drive it to
        // completion.
        log_ctx.record_event(
            EventAction::OrganizationCreated,
            EventOutcomeType::Success,
            &[("saga_id", saga_id.value()), ("region", region.as_str())],
        );

        log_ctx.set_organization(slug.value());
        log_ctx.set_region(region);
        log_ctx.set_success();

        let region_str = req.region;
        let tier_wire = req.tier.unwrap_or(ws::OrganizationTier::Unspecified);
        Ok(w::CreateOrganizationResponse {
            slug: Some(ws::OrganizationSlug::new(slug.value())),
            name: req.name,
            region: region_str,
            member_nodes: vec![],
            status: ws::OrganizationStatus::Provisioning,
            config_version: 0,
            created_at: 0,
            tier: tier_wire,
            members: vec![],
            updated_at: 0,
        })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `delete_organization` handler: resolves the org slug, validates the
    /// caller is an admin, best-effort revokes pending invitations, then
    /// proposes a GLOBAL `DeleteOrganization`.
    async fn delete_organization(
        &self,
        request: w::DeleteOrganizationRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteOrganizationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "delete_organization", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        log_ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator. The helper
        // takes a proto `Option<UserSlug>` and returns `tonic::Status`; bridge
        // both at the call site.
        self.validate_org_admin(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Best-effort revocation of pending invitations BEFORE deleting the
        // org. Must happen first because ResolveOrganizationInvite's apply
        // handler requires the org to be Active. Failures are logged —
        // InviteMaintenance will expire them by TTL.
        if let Some(org_meta) = self.ctx.applied_state.get_organization(organization_id)
            && let Ok(regional_state) = self.ctx.regional_state(org_meta.region)
        {
            let sys_svc = self.ctx.regional_system_service(regional_state);
            if let Ok(invitations) = sys_svc.list_invitations_by_org(organization_id, None, 1000) {
                for inv in invitations {
                    if inv.status != inferadb_ledger_types::InvitationStatus::Pending {
                        continue;
                    }
                    if let Err(e) = self
                        .ctx
                        .propose_organization_request(
                            org_meta.region,
                            organization_id,
                            OrganizationRequest::ResolveOrganizationInvite {
                                invite: inv.id,
                                organization: organization_id,
                                status: inferadb_ledger_types::InvitationStatus::Revoked,
                                invitee_email_hmac: inv.invitee_email_hmac.clone(),
                                token_hash: inv.token_hash,
                            },
                            &_ctx,
                            &mut log_ctx,
                        )
                        .await
                    {
                        tracing::warn!(
                            invite_id = inv.id.value(),
                            error = ?e,
                            "Failed to revoke pending invitation during org deletion"
                        );
                    }
                    if let Err(e) = self
                        .ctx
                        .propose_regional_org_encrypted(
                            org_meta.region,
                            SystemRequest::UpdateOrganizationInviteStatus {
                                organization: organization_id,
                                invite: inv.id,
                                status: inferadb_ledger_types::InvitationStatus::Revoked,
                            },
                            organization_id,
                            &_ctx,
                            &mut log_ctx,
                        )
                        .await
                    {
                        tracing::warn!(
                            invite_id = inv.id.value(),
                            error = ?e,
                            "REGIONAL invite revocation failed during org deletion"
                        );
                    }
                }
            }
        }

        // Submit delete organization through Raft.
        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::DeleteOrganization { organization: organization_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationDeleted {
                organization_id: deleted_org_id,
                deleted_at,
                retention_days,
            } => {
                log_ctx.set_success();
                metrics::record_organization_operation(deleted_org_id, "delete");
                metrics::record_organization_latency(
                    deleted_org_id,
                    "delete",
                    log_ctx.elapsed_secs(),
                );

                log_ctx.record_event(
                    EventAction::OrganizationDeleted,
                    EventOutcomeType::Success,
                    &[],
                );

                // `deleted_at` is `chrono::DateTime<Utc>`; the wire type is
                // UNIX nanoseconds (saturating to `u64::MAX` on overflow,
                // `0` for negative timestamps to match the tonic path's
                // proto Timestamp encoding semantics).
                let deleted_at_ns =
                    u64::try_from(deleted_at.timestamp_nanos_opt().unwrap_or(0).max(0))
                        .unwrap_or(0);

                Ok(w::DeleteOrganizationResponse { deleted_at: deleted_at_ns, retention_days })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `get_organization` handler: validates caller membership and returns
    /// the merged GLOBAL skeleton + REGIONAL PII view.
    async fn get_organization(
        &self,
        request: w::GetOrganizationRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetOrganizationResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "get_organization", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        // Validate caller is a member of the organization.
        let (_, profile) = self
            .validate_org_member(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let org_meta = self.ctx.applied_state.get_organization(organization_id);

        match org_meta {
            Some(org) if org.status == DomainOrganizationStatus::Deleted => {
                log_ctx.set_error("NotFound", "Organization not found");
                Err(WireError::new(
                    ErrorCode::NotFound,
                    format!("Organization not found: {organization_slug_val}"),
                ))
            },
            Some(org) => {
                log_ctx.set_success();
                let organization = slug_resolver.resolve_slug(org.organization)?;
                Ok(self.build_org_response(org, organization, Some(profile)))
            },
            None => {
                log_ctx.set_error("NotFound", "Organization not found");
                Err(WireError::new(
                    ErrorCode::NotFound,
                    format!("Organization not found: {organization_slug_val}"),
                ))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `list_organizations` handler: resolves the caller, ensures GLOBAL
    /// freshness, and lists organizations the caller belongs to with cursor
    /// pagination by slug.
    async fn list_organizations(
        &self,
        request: w::ListOrganizationsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListOrganizationsResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "list_organizations", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Ensure GLOBAL state is fresh before listing (recent org creation
        // may not have replicated to this follower yet).
        helpers::ensure_global_consistency(self.ctx.manager.as_deref()).await;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());

        // Resolve caller — required for authorization filtering.
        let caller_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let page_size = super::pagination::normalize_page_size(req.page_size);
        let start_after = super::pagination::decode_page_token(&req.page_token.map(|b| b.to_vec()));

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
            super::pagination::paginate_by_slug(orgs_with_slugs, start_after, page_size);

        let organizations: Vec<_> =
            page.into_iter().map(|(slug, org)| self.build_org_response(org, slug, None)).collect();

        log_ctx.set_keys_count(organizations.len());
        log_ctx.set_success();

        Ok(w::ListOrganizationsResponse {
            organizations,
            next_page_token: next_page_token.map(Bytes::from),
        })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `migrate_organization` handler: validates source/target residency,
    /// transitions the org to `Migrating` via `StartMigration`, then
    /// persists a `MigrateOrgSaga` record so the orchestrator can drive
    /// the cross-region migration to completion.
    async fn migrate_organization(
        &self,
        request: w::MigrateOrganizationRequest,
        _ctx: RequestContext,
    ) -> Result<w::MigrateOrganizationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "migrate_organization",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Resolve organization slug → internal ID.
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator.
        self.validate_org_admin(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Validate and convert proto region slug to domain region.
        let target_region =
            inferadb_ledger_types::Region::from_str(&req.target_region).map_err(|err| {
                WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
            })?;

        // Look up current organization state.
        let org_meta =
            self.ctx.applied_state.get_organization(organization_id).ok_or_else(|| {
                log_ctx.set_error("NotFound", "Organization not found");
                WireError::new(
                    ErrorCode::NotFound,
                    format!("Organization not found: {organization_slug_val}"),
                )
            })?;

        let source_region = org_meta.region;

        // Validate: target must differ from source.
        if target_region == source_region {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!("Organization is already in region {}", source_region.as_str()),
            ));
        }

        // Validate: organization must be Active.
        if org_meta.status != DomainOrganizationStatus::Active {
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                format!("Organization is not Active (current status: {:?})", org_meta.status),
            ));
        }

        // Validate: GLOBAL is not a valid target.
        if target_region == inferadb_ledger_types::Region::GLOBAL {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "Cannot migrate to GLOBAL control plane region",
            ));
        }

        // Resolve residency for both regions from the GLOBAL region
        // directory. An unknown region (no directory entry) is treated as
        // non-protected — the migration validation only fires on a known
        // protected→non-protected jump.
        let source_residency = match inferadb_ledger_state::system::lookup_region_residency(
            &self.ctx.state,
            source_region,
        ) {
            Ok(Some(r)) => r,
            Ok(None) | Err(_) => inferadb_ledger_state::system::RegionResidency {
                requires_residency: false,
                retention_days: 90,
            },
        };
        let target_residency = match inferadb_ledger_state::system::lookup_region_residency(
            &self.ctx.state,
            target_region,
        ) {
            Ok(Some(r)) => r,
            Ok(None) | Err(_) => inferadb_ledger_state::system::RegionResidency {
                requires_residency: false,
                retention_days: 90,
            },
        };

        // Validate: protected → non-protected requires acknowledgment.
        if source_residency.requires_residency
            && !target_residency.requires_residency
            && !req.acknowledge_residency_downgrade
        {
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "Migration from protected to non-protected region requires explicit acknowledgment",
            ));
        }

        // For protected target regions, validate sufficient in-region nodes.
        self.validate_region_nodes(target_region).map_err(tonic_status_to_wire_error)?;

        // Determine migration type: metadata-only for non-protected →
        // non-protected.
        let metadata_only =
            !source_residency.requires_residency && !target_residency.requires_residency;

        // Capture the originating W3C `traceparent` so the saga can
        // propagate it into downstream regional proposals (keeps
        // distributed traces linked across the GLOBAL → regional hop).
        let traceparent = crate::services::wire_helpers::wire_context_traceparent(&_ctx);

        // Build the migration saga for the orchestrator to drive. The saga
        // starts in MigrationStarted state because StartMigration sets the
        // organization status to Migrating atomically in the same Raft entry
        // (via BatchWrite below).
        let saga_id = inferadb_ledger_state::system::SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = inferadb_ledger_state::system::MigrateOrgSaga::new(
            saga_id,
            inferadb_ledger_state::system::MigrateOrgInput {
                organization_id,
                organization_slug: DomainOrganizationSlug::new(organization_slug_val),
                source_region,
                target_region,
                acknowledge_residency_downgrade: req.acknowledge_residency_downgrade,
                metadata_only,
            },
        )
        .with_traceparent(traceparent);

        let saga_key = format!("_meta:saga:{}", saga.id);
        let saga_wrapped = inferadb_ledger_state::system::Saga::MigrateOrg(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped)
            .map_err(|e| error_classify::serialization_error(&e))?;

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
        };

        // Saga records live at `_meta:saga:{id}` in GLOBAL state — the single
        // source the `SagaOrchestrator::load_pending_sagas` poll reads from.
        let saga_write = SystemRequest::Write {
            vault: inferadb_ledger_types::VaultId::new(0),
            transactions: vec![saga_txn],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        // Propose StartMigration to the GLOBAL system tier to transition
        // the organization status to Migrating.
        let start_migration_response = self
            .ctx
            .propose_system_request(
                SystemRequest::StartMigration {
                    organization: organization_id,
                    target_region_group: target_region,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let (migration_organization, migration_target) = match start_migration_response {
            LedgerResponse::MigrationStarted { organization, target_region_group } => {
                (organization, target_region_group)
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            _ => {
                log_ctx.set_error(
                    "UnexpectedResponse",
                    "Unexpected response type from StartMigration",
                );
                return Err(WireError::new(
                    ErrorCode::Internal,
                    "Unexpected response type from StartMigration",
                ));
            },
        };

        // Persist the saga record in GLOBAL so the orchestrator can drive
        // the migration to completion (`load_pending_sagas` polls GLOBAL).
        let saga_response =
            self.ctx.propose_system_request(saga_write, &_ctx, &mut log_ctx).await?;

        match saga_response {
            LedgerResponse::Write { .. } => {
                log_ctx.set_success();
                metrics::record_organization_operation(migration_organization, "migrate");
                Ok(w::MigrateOrganizationResponse {
                    slug: Some(ws::OrganizationSlug::new(organization_slug_val)),
                    source_region: source_region.as_str().to_string(),
                    target_region: migration_target.as_str().to_string(),
                    status: ws::OrganizationStatus::Migrating,
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response from saga write");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from saga write"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `update_organization` handler: validates admin, then proposes a
    /// REGIONAL `UpdateOrganizationProfile` (encrypted with `OrgShredKey`).
    async fn update_organization(
        &self,
        request: w::UpdateOrganizationRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateOrganizationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "update_organization", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        // Validate initiator is an organization administrator.
        self.validate_org_admin(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let name = match req.name {
            Some(ref n) => {
                validation::validate_organization_name(n, &self.ctx.validation_config)
                    .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;
                n.clone()
            },
            None => {
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "At least one field must be provided for update",
                ));
            },
        };

        // Resolve the organization's data residency region.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Route the name update to the REGIONAL Raft group — name is PII
        // and must not appear in the GLOBAL Raft log. Encrypted with
        // OrgShredKey for crypto-shredding on organization purge.
        let system_request =
            SystemRequest::UpdateOrganizationProfile { organization: organization_id, name };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                organization_id,
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { organization_id: updated_org_id } => {
                log_ctx.set_success();
                metrics::record_organization_operation(updated_org_id, "update");
                metrics::record_organization_latency(
                    updated_org_id,
                    "update",
                    log_ctx.elapsed_secs(),
                );

                log_ctx.record_event(
                    EventAction::OrganizationUpdated,
                    EventOutcomeType::Success,
                    &[],
                );

                // Re-read the org to return full response.
                let updated_meta =
                    self.ctx.applied_state.get_organization(updated_org_id).ok_or_else(|| {
                        WireError::new(ErrorCode::Internal, "Organization not found after update")
                    })?;
                let resolved_slug = slug_resolver.resolve_slug(updated_meta.organization)?;
                let get_resp = self.build_org_response(updated_meta, resolved_slug, None);

                Ok(w::UpdateOrganizationResponse {
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
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `list_organization_members` handler: validates caller membership and
    /// paginates the member list by user slug.
    async fn list_organization_members(
        &self,
        request: w::ListOrganizationMembersRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListOrganizationMembersResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "list_organization_members",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        // Validate caller is a member (also returns the profile).
        let (_, profile) = self
            .validate_org_member(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let page_size = super::pagination::normalize_page_size(req.page_size);
        let start_after = super::pagination::decode_page_token(&req.page_token.map(|b| b.to_vec()));

        let sys_svc = self.ctx.system_service();

        // Resolve member slugs, then paginate by slug cursor.
        let members_with_slugs: Vec<_> = profile
            .members
            .iter()
            .filter_map(|m| {
                let wire_member = OrganizationService::member_to_wire(&sys_svc, m)?;
                let user_slug_val = wire_member.user.as_ref().map_or(0, |u| u.value());
                Some((user_slug_val, wire_member))
            })
            .collect();

        let (members, next_page_token) =
            super::pagination::paginate_by_slug(members_with_slugs, start_after, page_size);

        log_ctx.set_success();
        Ok(w::ListOrganizationMembersResponse {
            members,
            next_page_token: next_page_token.map(Bytes::from),
        })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `remove_organization_member` handler: enforces self-removal vs admin
    /// rules (last-admin guard), then proposes
    /// `OrganizationRequest::RemoveOrganizationMember`.
    async fn remove_organization_member(
        &self,
        request: w::RemoveOrganizationMemberRequest,
        _ctx: RequestContext,
    ) -> Result<w::RemoveOrganizationMemberResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "remove_organization_member",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        let initiator_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let target_id = slug_resolver
            .extract_and_resolve_user(req.target.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        // Read the profile to validate the operation.
        let profile = self.read_organization(organization_id).ok_or_else(|| {
            log_ctx.set_error("NotFound", "Organization profile not found");
            WireError::new(ErrorCode::NotFound, "Organization not found")
        })?;

        if initiator_id == target_id {
            // Self-removal: check target is a member.
            let target_member = profile.members.iter().find(|m| m.user_id == target_id);
            match target_member {
                None => {
                    log_ctx.set_error("NotFound", "User is not a member");
                    return Err(WireError::new(
                        ErrorCode::NotFound,
                        "User is not a member of the organization",
                    ));
                },
                Some(m)
                    if m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
                        && profile
                            .members
                            .iter()
                            .filter(|m| {
                                m.role
                                    == inferadb_ledger_state::system::OrganizationMemberRole::Admin
                            })
                            .count()
                            <= 1 =>
                {
                    log_ctx.set_error("FailedPrecondition", "Cannot remove last admin");
                    return Err(WireError::new(
                        ErrorCode::FailedPrecondition,
                        "Cannot remove the last administrator from the organization",
                    ));
                },
                Some(_) => {},
            }
        } else {
            // Removing others: initiator must be admin.
            let is_admin = profile.members.iter().any(|m| {
                m.user_id == initiator_id
                    && m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
            });
            if !is_admin {
                log_ctx.set_error("PermissionDenied", "User is not an organization administrator");
                return Err(WireError::new(
                    ErrorCode::PermissionDenied,
                    "User is not an organization administrator",
                ));
            }
            // Verify target is actually a member.
            if !profile.members.iter().any(|m| m.user_id == target_id) {
                log_ctx.set_error("NotFound", "Target is not a member");
                return Err(WireError::new(
                    ErrorCode::NotFound,
                    "Target user is not a member of the organization",
                ));
            }
        }

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                organization_id,
                OrganizationRequest::RemoveOrganizationMember {
                    organization: organization_id,
                    target: target_id,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationMemberRemoved { .. } => {
                log_ctx.set_success();
                Ok(w::RemoveOrganizationMemberResponse {})
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `update_organization_member_role` handler: admin-only; rejects
    /// demotion of the last admin; proposes
    /// `OrganizationRequest::UpdateOrganizationMemberRole`.
    async fn update_organization_member_role(
        &self,
        request: w::UpdateOrganizationMemberRoleRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateOrganizationMemberRoleResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "update_organization_member_role",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.slug.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(req.slug.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_organization(organization_slug_val);

        // Validate initiator is admin.
        self.validate_org_admin(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let target_id = slug_resolver
            .extract_and_resolve_user(req.target.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        // Wire role is a typed enum — `Unspecified` requires explicit role.
        let new_role = match req.role {
            ws::OrganizationMemberRole::Admin => {
                inferadb_ledger_state::system::OrganizationMemberRole::Admin
            },
            ws::OrganizationMemberRole::Member => {
                inferadb_ledger_state::system::OrganizationMemberRole::Member
            },
            ws::OrganizationMemberRole::Unspecified => {
                log_ctx.set_error("InvalidArgument", "Role must be specified");
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "Organization member role must be specified (ADMIN or MEMBER)",
                ));
            },
        };

        // Read profile to validate target is a member and check last-admin
        // rule.
        let profile = self.read_organization(organization_id).ok_or_else(|| {
            log_ctx.set_error("NotFound", "Organization profile not found");
            WireError::new(ErrorCode::NotFound, "Organization not found")
        })?;

        let target_member =
            profile.members.iter().find(|m| m.user_id == target_id).ok_or_else(|| {
                log_ctx.set_error("NotFound", "Target is not a member");
                WireError::new(
                    ErrorCode::NotFound,
                    "Target user is not a member of the organization",
                )
            })?;

        // Prevent demoting the last admin.
        if target_member.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
            && new_role == inferadb_ledger_state::system::OrganizationMemberRole::Member
            && profile
                .members
                .iter()
                .filter(|m| m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin)
                .count()
                <= 1
        {
            log_ctx.set_error("FailedPrecondition", "Cannot demote last admin");
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "Cannot demote the last administrator of the organization",
            ));
        }

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                organization_id,
                OrganizationRequest::UpdateOrganizationMemberRole {
                    organization: organization_id,
                    target: target_id,
                    role: new_role,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationMemberRoleUpdated { .. } => {
                log_ctx.set_success();

                // Re-read profile after Raft apply for fresh data.
                let sys_svc = self.ctx.system_service();
                let member = self.read_organization(organization_id).and_then(|p| {
                    p.members
                        .iter()
                        .find(|m| m.user_id == target_id)
                        .and_then(|m| OrganizationService::member_to_wire(&sys_svc, m))
                });

                Ok(w::UpdateOrganizationMemberRoleResponse { member })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `list_organization_teams` handler: admins see all teams, non-admin
    /// members see only teams they belong to.
    async fn list_organization_teams(
        &self,
        request: w::ListOrganizationTeamsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListOrganizationTeamsResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "list_organization_teams",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_id = slug_resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Resolve caller, verify membership, and determine role.
        let (caller_id, profile) = self
            .validate_org_member(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let is_admin = profile.members.iter().any(|m| {
            m.user_id == caller_id
                && m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
        });

        let mut teams = self.list_teams(organization_id);
        if !is_admin {
            // Non-admin members only see teams they belong to.
            teams.retain(|t| t.members.iter().any(|m| m.user_id == caller_id));
        }

        let page_size = super::pagination::normalize_page_size(req.page_size);
        let start_after = super::pagination::decode_page_token(&req.page_token.map(|b| b.to_vec()));

        let sys_svc = self.ctx.system_service();

        let teams_with_slugs: Vec<_> = teams
            .iter()
            .map(|t| {
                let wire_team = OrganizationService::team_to_wire(&sys_svc, t, org_slug);
                let team_slug_val = wire_team.slug.as_ref().map_or(0, |s| s.value());
                (team_slug_val, wire_team)
            })
            .collect();

        let (wire_teams, next_page_token) =
            super::pagination::paginate_by_slug(teams_with_slugs, start_after, page_size);

        log_ctx.set_success();
        Ok(w::ListOrganizationTeamsResponse {
            teams: wire_teams,
            next_page_token: next_page_token.map(Bytes::from),
        })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `create_organization_team` handler: admin-only; dual-proposes
    /// (per-org `CreateOrganizationTeam` then REGIONAL `WriteTeam` for
    /// PII-bearing name).
    async fn create_organization_team(
        &self,
        request: w::CreateOrganizationTeamRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateOrganizationTeamResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "create_organization_team",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_id = slug_resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Resolve initiator and check authorization (must be org admin).
        let initiator_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let profile = self.read_organization(organization_id);
        let is_org_admin = profile.as_ref().is_some_and(|p| {
            p.members.iter().any(|m| {
                m.user_id == initiator_id
                    && m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
            })
        });
        if !is_org_admin {
            log_ctx.set_error("PermissionDenied", "Must be org admin to create a team");
            return Err(WireError::new(
                ErrorCode::PermissionDenied,
                "Must be an organization administrator to create a team",
            ));
        }

        // Validate team name.
        let name = req.name.trim().to_string();
        if let Err(e) = validation::validate_organization_name(&name, &self.ctx.validation_config) {
            log_ctx.set_error("InvalidArgument", &e.to_string());
            return Err(WireError::new(ErrorCode::InvalidArgument, e.to_string()));
        }

        // Client-supplied Snowflake slug — required.
        let team_slug_wire = req.slug.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::InvalidArgument,
                "CreateOrganizationTeamRequest.slug is required",
            )
        })?;
        if team_slug_wire.value() == 0 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "CreateOrganizationTeamRequest.slug must be a non-zero Snowflake",
            ));
        }
        let team_slug = inferadb_ledger_types::TeamSlug::new(team_slug_wire.value());

        // Resolve org region before the Raft proposal so Step 2 can use it.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Step 1 (GLOBAL): Create team directory entry (ID + slug only,
        // no PII).
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                organization_id,
                OrganizationRequest::CreateOrganizationTeam {
                    organization: organization_id,
                    slug: team_slug,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let team_id = match response {
            LedgerResponse::OrganizationTeamCreated { team_id, .. } => team_id,
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                return Err(WireError::new(ErrorCode::Internal, "Unexpected response type"));
            },
        };

        // Step 2 (REGIONAL): Write team name to the org's regional store.
        // Encrypted with OrgShredKey for crypto-shredding on organization
        // purge.
        let system_request = SystemRequest::WriteTeam {
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        if let LedgerResponse::Error { code, message } = profile_response {
            log_ctx.set_error(code.grpc_code_name(), &message);
            return Err(helpers::error_code_to_wire_error(code, message));
        }

        log_ctx.set_success();
        let sys_svc = self.ctx.system_service();
        let team = self
            .read_team(organization_id, team_id)
            .map(|t| OrganizationService::team_to_wire(&sys_svc, &t, org_slug));
        Ok(w::CreateOrganizationTeamResponse { team })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `delete_organization_team` handler: admin-or-team-manager;
    /// dual-proposes (REGIONAL `DeleteTeam` then per-org
    /// `DeleteOrganizationTeam` to clean up slug index).
    async fn delete_organization_team(
        &self,
        request: w::DeleteOrganizationTeamRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteOrganizationTeamResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "delete_organization_team",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.slug.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("NotFound", &err.message);
            })?;

        // Resolve initiator and check authorization (admin or team manager).
        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            req.caller,
            &mut log_ctx,
        )
        .map_err(tonic_status_to_wire_error)?;

        // Resolve optional move target.
        let move_to = slug_resolver
            .extract_and_resolve_team_optional(
                req.move_members_to.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("NotFound", &err.message);
            })?;

        // Validate move target belongs to the same organization.
        if let Some((target_org_id, _)) = &move_to
            && *target_org_id != organization_id
        {
            log_ctx.set_error(
                "InvalidArgument",
                "move_members_to team must belong to the same organization",
            );
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "move_members_to team must belong to the same organization",
            ));
        }
        let move_to = move_to.map(|(_, target_team_id)| target_team_id);

        // Step 1 (REGIONAL): Delete profile, handle member migration, clean
        // up name index.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let delete_team = SystemRequest::DeleteTeam {
            organization: organization_id,
            team: team_id,
            move_members_to: move_to,
        };
        let delete_team_response =
            self.ctx.propose_regional(org_meta.region, delete_team, &_ctx, &mut log_ctx).await?;
        if let LedgerResponse::Error { code, message } = delete_team_response {
            log_ctx.set_error(code.grpc_code_name(), &message);
            return Err(helpers::error_code_to_wire_error(code, message));
        }

        // Step 2 (GLOBAL): Clean up slug index and in-memory maps.
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                organization_id,
                OrganizationRequest::DeleteOrganizationTeam {
                    organization: organization_id,
                    team: team_id,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationTeamDeleted { .. } => {
                log_ctx.set_success();
                Ok(w::DeleteOrganizationTeamResponse {})
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `update_organization_team` handler: admin-or-team-manager; routes
    /// the name update to REGIONAL via `WriteTeam` (encrypted).
    async fn update_organization_team(
        &self,
        request: w::UpdateOrganizationTeamRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateOrganizationTeamResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "update_organization_team",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.slug.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("NotFound", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Validate initiator is org admin or team manager.
        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            req.caller,
            &mut log_ctx,
        )
        .map_err(tonic_status_to_wire_error)?;

        // Validate new name.
        let name = match req.name {
            Some(ref n) => {
                let trimmed = n.trim().to_string();
                if let Err(e) =
                    validation::validate_organization_name(&trimmed, &self.ctx.validation_config)
                {
                    log_ctx.set_error("InvalidArgument", &e.to_string());
                    return Err(WireError::new(ErrorCode::InvalidArgument, e.to_string()));
                }
                trimmed
            },
            None => {
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "At least one field must be provided for update",
                ));
            },
        };

        // Route name update to the org's regional Raft group (PII).
        // Encrypted with OrgShredKey for crypto-shredding on organization
        // purge.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let team_slug =
            self.ctx.applied_state.resolve_team_id_to_slug(team_id).ok_or_else(|| {
                WireError::new(ErrorCode::NotFound, format!("Team {} slug not found", team_id))
            })?;
        let system_request = SystemRequest::WriteTeam {
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                log_ctx.set_success();
                let sys_svc = self.ctx.system_service();
                let team = self
                    .read_team(organization_id, team_id)
                    .map(|t| OrganizationService::team_to_wire(&sys_svc, &t, org_slug));
                Ok(w::UpdateOrganizationTeamResponse { team })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(
                    response = %other,
                    "Unexpected Raft response for UpdateOrganizationTeam"
                );
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `get_organization_team` handler: admins see any team; non-admin
    /// members see only teams they belong to.
    async fn get_organization_team(
        &self,
        request: w::GetOrganizationTeamRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetOrganizationTeamResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "get_organization_team",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.slug.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        // Verify caller is an org member.
        let (caller_id, profile) = self
            .validate_org_member(&slug_resolver, organization_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let is_admin = profile.members.iter().any(|m| {
            m.user_id == caller_id
                && m.role == inferadb_ledger_state::system::OrganizationMemberRole::Admin
        });

        // Read team profile from REGIONAL state.
        let team = self.read_team(organization_id, team_id).ok_or_else(|| {
            log_ctx.set_error("NotFound", "Team not found");
            WireError::new(ErrorCode::NotFound, "Team not found")
        })?;

        // Non-admin members can only see teams they belong to.
        if !is_admin && !team.members.iter().any(|m| m.user_id == caller_id) {
            log_ctx.set_error("NotFound", "Team not found");
            return Err(WireError::new(ErrorCode::NotFound, "Team not found"));
        }

        let sys_svc = self.ctx.system_service();
        let wire_team = OrganizationService::team_to_wire(&sys_svc, &team, org_slug);

        log_ctx.set_success();
        Ok(w::GetOrganizationTeamResponse { team: Some(wire_team) })
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `add_team_member` handler: admin-or-team-manager; verifies user is an
    /// org member; proposes REGIONAL `AddTeamMember` (encrypted).
    async fn add_team_member(
        &self,
        request: w::AddTeamMemberRequest,
        _ctx: RequestContext,
    ) -> Result<w::AddTeamMemberResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "add_team_member", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.team.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            req.caller,
            &mut log_ctx,
        )
        .map_err(tonic_status_to_wire_error)?;

        let user_id = slug_resolver
            .extract_and_resolve_user(req.user.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let role = match req.role {
            ws::OrganizationTeamMemberRole::Manager => {
                inferadb_ledger_state::system::TeamMemberRole::Manager
            },
            ws::OrganizationTeamMemberRole::Member
            | ws::OrganizationTeamMemberRole::Unspecified => {
                inferadb_ledger_state::system::TeamMemberRole::Member
            },
        };

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Verify the target user is a member of the organization.
        let org_profile = self.read_organization(organization_id).ok_or_else(|| {
            log_ctx.set_error("NotFound", "Organization profile not found");
            WireError::new(ErrorCode::NotFound, "Organization profile not found")
        })?;
        if !org_profile.members.iter().any(|m| m.user_id == user_id) {
            log_ctx.set_error("FailedPrecondition", "User is not a member of the organization");
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "User is not a member of the organization",
            ));
        }

        let system_request = SystemRequest::AddTeamMember {
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                log_ctx.set_success();
                let sys_svc = self.ctx.system_service();
                let team = self
                    .read_team(organization_id, team_id)
                    .map(|t| OrganizationService::team_to_wire(&sys_svc, &t, org_slug));
                Ok(w::AddTeamMemberResponse { team })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(
                    response = %other,
                    "Unexpected Raft response for AddTeamMember"
                );
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `remove_team_member` handler: admin-or-team-manager; proposes
    /// REGIONAL `RemoveTeamMember` (encrypted).
    async fn remove_team_member(
        &self,
        request: w::RemoveTeamMemberRequest,
        _ctx: RequestContext,
    ) -> Result<w::RemoveTeamMemberResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("OrganizationService", "remove_team_member", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.team.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            req.caller,
            &mut log_ctx,
        )
        .map_err(tonic_status_to_wire_error)?;

        let user_id = slug_resolver
            .extract_and_resolve_user(req.user.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        let system_request = SystemRequest::RemoveTeamMember {
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                log_ctx.set_success();
                Ok(w::RemoveTeamMemberResponse {})
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(
                    response = %other,
                    "Unexpected Raft response for RemoveTeamMember"
                );
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    /// Mirrors the tonic [`organization`](super::organization::OrganizationService)
    /// `update_team_member_role` handler: admin-or-team-manager; proposes
    /// REGIONAL `UpdateTeamMemberRole` (encrypted).
    async fn update_team_member_role(
        &self,
        request: w::UpdateTeamMemberRoleRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateTeamMemberRoleResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "OrganizationService",
            "update_team_member_role",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let (organization_id, team_id) = slug_resolver
            .extract_and_resolve_team(
                req.team.map(|s| inferadb_ledger_types::TeamSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let org_slug = slug_resolver.resolve_slug(organization_id)?;

        self.validate_org_admin_or_team_manager(
            &slug_resolver,
            organization_id,
            team_id,
            req.caller,
            &mut log_ctx,
        )
        .map_err(tonic_status_to_wire_error)?;

        let user_id = slug_resolver
            .extract_and_resolve_user(req.user.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let role = match req.role {
            ws::OrganizationTeamMemberRole::Manager => {
                inferadb_ledger_state::system::TeamMemberRole::Manager
            },
            ws::OrganizationTeamMemberRole::Member
            | ws::OrganizationTeamMemberRole::Unspecified => {
                inferadb_ledger_state::system::TeamMemberRole::Member
            },
        };

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(organization_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        let system_request = SystemRequest::UpdateTeamMemberRole {
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::OrganizationUpdated { .. } => {
                log_ctx.set_success();
                let sys_svc = self.ctx.system_service();
                let team = self
                    .read_team(organization_id, team_id)
                    .map(|t| OrganizationService::team_to_wire(&sys_svc, &t, org_slug));
                Ok(w::UpdateTeamMemberRoleResponse { team })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(
                    response = %other,
                    "Unexpected Raft response for UpdateTeamMemberRole"
                );
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Sanity: serializing a populated wire `GetOrganizationResponse` round-
    /// trips through serde_json (the wire-frame on-wire format).
    #[test]
    fn populated_get_organization_response_round_trips_through_serde_json() {
        let original = w::GetOrganizationResponse {
            slug: Some(ws::OrganizationSlug::new(0xDEAD_BEEF)),
            name: "acme_corp".to_owned(),
            region: "us-east-va".to_owned(),
            member_nodes: vec![ws::NodeId::new("node-1"), ws::NodeId::new("node-2")],
            status: ws::OrganizationStatus::Active,
            config_version: 7,
            created_at: 1_700_000_000_123_456_789,
            tier: ws::OrganizationTier::Launch,
            members: vec![ws::OrganizationMember {
                user: Some(ws::UserSlug::new(42)),
                role: ws::OrganizationMemberRole::Admin,
                joined_at: 1_700_000_000_123_456_789,
            }],
            updated_at: 1_700_000_000_999_999_999,
        };
        let bytes = serde_json::to_vec(&original).unwrap();
        let back: w::GetOrganizationResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, original);
    }
}
