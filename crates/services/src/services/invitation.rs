//! Invitation service implementation.
//!
//! Handles the full organization invitation lifecycle: creation, listing,
//! revocation (admin operations), and acceptance, decline, listing
//! (user operations). Enforces rate limiting, timing equalization,
//! multi-email HMAC matching, and partial failure recovery.

use std::sync::Arc;

use chrono::Utc;
use inferadb_ledger_proto::proto::{
    self, AcceptInvitationRequest, AcceptInvitationResponse, CreateOrganizationInviteRequest,
    CreateOrganizationInviteResponse, DeclineInvitationRequest, DeclineInvitationResponse,
    GetInvitationDetailsRequest, GetInvitationDetailsResponse, GetOrganizationInviteRequest,
    GetOrganizationInviteResponse, ListOrganizationInvitesRequest, ListOrganizationInvitesResponse,
    ListReceivedInvitationsRequest, ListReceivedInvitationsResponse,
    RevokeOrganizationInviteRequest, RevokeOrganizationInviteResponse,
};
use inferadb_ledger_raft::{
    logging::RequestContext,
    metrics,
    types::{LedgerResponse, OrganizationRequest, SystemRequest},
};
use inferadb_ledger_state::system::{
    Organization, OrganizationMemberRole as DomainMemberRole, OrganizationProfile, SYSTEM_VAULT_ID,
    SystemKeys, SystemOrganizationService,
};
use inferadb_ledger_types::{
    EmailBlindingKey, InvitationStatus as DomainInvitationStatus, InviteEmailEntry, InviteId,
    InviteIndexEntry, InviteSlug, OrganizationId, OrganizationInvitation, UserId, decode,
    email_hash::compute_email_hmac, validation,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tonic::{Request, Response, Status};

use super::{error_classify, service_infra::ServiceContext, slug_resolver::SlugResolver};
use crate::proto_compat;

/// Maximum pending invitations per email across all organizations.
const MAX_PENDING_PER_EMAIL: usize = 10;

/// Maximum total invitations targeting a single email within the retention
/// window (90 days). Prevents throughput amplification via short-TTL
/// create/expire cycles. Stricter than a rolling 24-hour window but avoids
/// expensive per-entry REGIONAL reads for `created_at` timestamps.
const MAX_TOTAL_PER_EMAIL: usize = 20;

/// Decline cooldown: 24 hours before re-inviting from the same org.
const DECLINE_COOLDOWN_SECS: i64 = 86_400;

/// Maximum email hash entries to scan before short-circuiting.
const SCAN_CEILING: usize = 500;

/// Maximum invitations returned by a REGIONAL prefix scan.
const MAX_LIST_INVITATIONS: usize = 1_000;

/// gRPC handler for organization invitation lifecycle.
pub struct InvitationService {
    ctx: ServiceContext,
}

impl InvitationService {
    /// Creates a new `InvitationService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Returns the email blinding key, or FAILED_PRECONDITION if unconfigured.
    fn blinding_key(&self) -> Result<&Arc<EmailBlindingKey>, Status> {
        self.ctx
            .email_blinding_key
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Email blinding key not configured"))
    }

    /// Resolves an `InviteSlug` to an `InviteIndexEntry` via the GLOBAL index.
    fn resolve_invite_slug(&self, slug: InviteSlug) -> Result<Option<InviteIndexEntry>, Status> {
        let key = SystemKeys::invite_slug_index_key(slug);
        let entity = self
            .ctx
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| error_classify::storage_error(&e))?;
        match entity {
            Some(e) => {
                let entry: InviteIndexEntry =
                    decode(&e.value).map_err(|e| error_classify::serialization_error(&e))?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Scans the GLOBAL `_idx:invite:email_hash:{hmac}:` prefix and returns
    /// entries up to the scan ceiling.
    fn scan_email_entries(
        &self,
        hmac_hex: &str,
    ) -> Result<Vec<(InviteId, InviteEmailEntry)>, Status> {
        let prefix = SystemKeys::invite_email_hash_prefix(hmac_hex);
        let entities = self
            .ctx
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, SCAN_CEILING + 1)
            .map_err(|e| error_classify::storage_error(&e))?;

        if entities.len() > SCAN_CEILING {
            metrics::record_invitation_scan_ceiling_breached();
            tracing::warn!(
                hmac_prefix = %hmac_hex.get(..8).unwrap_or(hmac_hex),
                scan_count = entities.len(),
                ceiling = SCAN_CEILING,
                "Invitation scan ceiling breached; rejecting request"
            );
            return Err(Status::resource_exhausted(
                "Invitation rate limit exceeded. Try again later.",
            ));
        }

        let mut results = Vec::with_capacity(entities.len());
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            // Parse invite ID from the key suffix
            if let Some(id_str) = key_str.rsplit(':').next()
                && let Ok(id_val) = id_str.parse::<i64>()
                && let Ok(entry) = decode::<InviteEmailEntry>(&entity.value)
            {
                results.push((InviteId::new(id_val), entry));
            }
        }
        Ok(results)
    }

    /// Looks up the organization's current region from the GLOBAL registry.
    fn org_region(&self, org_id: OrganizationId) -> Result<inferadb_ledger_types::Region, Status> {
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let registry = sys_svc
            .get_organization(org_id)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        Ok(registry.region)
    }

    /// Reads the full REGIONAL invitation record.
    fn read_regional_invitation(
        &self,
        org_id: OrganizationId,
        invite_id: InviteId,
    ) -> Result<Option<OrganizationInvitation>, Status> {
        let region = self.org_region(org_id)?;
        let state = self.ctx.regional_state(region)?;
        let key = SystemKeys::invite_key(org_id, invite_id);
        let entity = state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| error_classify::storage_error(&e))?;
        match entity {
            Some(e) => {
                let inv: OrganizationInvitation =
                    decode(&e.value).map_err(|e| error_classify::serialization_error(&e))?;
                Ok(Some(inv))
            },
            None => Ok(None),
        }
    }

    /// Reads the GLOBAL `Organization` skeleton (contains members list).
    fn read_organization(&self, org_id: OrganizationId) -> Option<Organization> {
        let key = SystemKeys::organization_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<Organization>(&entity.value).ok()
    }

    /// Overlays the org name from REGIONAL profile onto an empty proto field.
    fn get_org_name(&self, org_id: OrganizationId) -> String {
        let org_meta = self.ctx.applied_state.get_organization(org_id);
        let Some(meta) = org_meta else { return String::new() };
        let Ok(state) = self.ctx.regional_state(meta.region) else { return String::new() };
        let key = SystemKeys::organization_profile_key(org_id);
        let Ok(Some(entity)) = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) else {
            return String::new();
        };
        decode::<OrganizationProfile>(&entity.value).map(|p| p.name).unwrap_or_default()
    }

    /// Validates that the caller is an admin of the organization.
    fn validate_org_admin(
        &self,
        slug_resolver: &SlugResolver,
        org_id: OrganizationId,
        caller: &Option<proto::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(UserId, inferadb_ledger_types::UserSlug), Status> {
        let user_id = slug_resolver.extract_and_resolve_user(caller).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;
        let user_slug = SlugResolver::extract_user_slug(caller)?;

        let org = self.read_organization(org_id).ok_or_else(|| {
            ctx.set_error("NotFound", "Organization not found");
            Status::not_found("Organization not found")
        })?;

        let is_admin =
            org.members.iter().any(|m| m.user_id == user_id && m.role == DomainMemberRole::Admin);
        if !is_admin {
            ctx.set_error("PermissionDenied", "Caller is not an organization admin");
            return Err(Status::permission_denied("Caller is not an organization admin"));
        }

        Ok((user_id, user_slug))
    }

    /// Gets all verified email HMACs for a user.
    fn get_user_email_hmacs(
        &self,
        user_id: UserId,
        blinding_key: &EmailBlindingKey,
    ) -> Result<Vec<String>, Status> {
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let emails =
            sys_svc.get_user_emails(user_id).map_err(|e| error_classify::storage_error(&e))?;
        Ok(emails
            .iter()
            .filter(|e| e.verified_at.is_some())
            .map(|e| compute_email_hmac(blinding_key, &e.email))
            .collect())
    }

    /// Checks whether any of the user's verified email HMACs match the invitation's.
    /// Uses constant-time comparison without short-circuiting to prevent timing leaks
    /// that would reveal the position of a matching email in the user's list.
    fn email_matches(user_hmacs: &[String], invitation_hmac: &str) -> bool {
        use subtle::Choice;
        let inv_bytes = invitation_hmac.as_bytes();
        let mut result = Choice::from(0);
        for h in user_hmacs {
            result |= h.as_bytes().ct_eq(inv_bytes);
        }
        result.into()
    }

    /// Checks email match and returns the `InviteEmailEntry` status via GLOBAL index.
    ///
    /// Reads `_idx:invite:email_hash:{hmac}:{invite_id}` in GLOBAL for ALL of the
    /// user's verified email HMACs. If any exists with matching `organization`,
    /// returns the `InviteEmailEntry`. Uses GLOBAL state (authoritative after CAS)
    /// rather than REGIONAL for status checks.
    ///
    /// All HMACs are checked even after a match to prevent timing side-channels
    /// that would reveal which email address was targeted.
    fn check_email_match_global(
        &self,
        user_hmacs: &[String],
        invite_id: InviteId,
        org_id: OrganizationId,
    ) -> Result<Option<InviteEmailEntry>, Status> {
        let mut matched: Option<InviteEmailEntry> = None;
        for hmac in user_hmacs {
            let key = SystemKeys::invite_email_hash_index_key(hmac, invite_id);
            let entity = self
                .ctx
                .state
                .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
                .map_err(|e| error_classify::storage_error(&e))?;
            if matched.is_none()
                && let Some(e) = entity
                && let Ok(entry) = decode::<InviteEmailEntry>(&e.value)
                && entry.organization == org_id
            {
                matched = Some(entry);
            }
        }
        Ok(matched)
    }

    /// Extracts and resolves an invite slug from a proto field, with timing
    /// equalization on not-found. Returns `NOT_FOUND` for missing or unknown slugs.
    fn resolve_invite_slug_or_not_found(
        &self,
        proto_slug: &Option<proto::InviteSlug>,
        ctx: &mut RequestContext,
    ) -> Result<InviteIndexEntry, Status> {
        let slug_val = proto_slug.as_ref().map_or(0, |s| s.slug);
        if slug_val == 0 {
            ctx.set_error("InvalidArgument", "invite slug is required");
            return Err(Status::invalid_argument("invite slug is required"));
        }
        let invite_slug = InviteSlug::new(slug_val);
        match self.resolve_invite_slug(invite_slug)? {
            Some(entry) => Ok(entry),
            None => {
                self.timing_equalization_reads(None);
                ctx.set_error("NotFound", "Invitation not found");
                Err(Status::not_found("Invitation not found"))
            },
        }
    }

    /// Performs dummy reads and HMAC computation for timing equalization when
    /// slug is not found. When a `region` is available, the dummy read targets
    /// the REGIONAL state layer to match the real code path; otherwise falls
    /// back to a GLOBAL read.
    fn timing_equalization_reads(&self, region: Option<inferadb_ledger_types::Region>) {
        let dummy_key = SystemKeys::invite_key(OrganizationId::new(0), InviteId::new(0));

        if let Some(r) = region {
            // REGIONAL read — matches the real `read_regional_invitation` path.
            if let Ok(state) = self.ctx.regional_state(r) {
                let _ = state.get_entity(SYSTEM_VAULT_ID, dummy_key.as_bytes());
            } else {
                // Region unavailable — fall back to GLOBAL read.
                let _ = self.ctx.state.get_entity(SYSTEM_VAULT_ID, dummy_key.as_bytes());
            }
        } else {
            // No region available (slug-not-found path) — GLOBAL read fallback.
            let _ = self.ctx.state.get_entity(SYSTEM_VAULT_ID, dummy_key.as_bytes());
        }

        // Dummy HMAC computation to match the real email-comparison path.
        if let Ok(key) = self.blinding_key() {
            let _ = compute_email_hmac(key, "timing-equalization@invalid.example");
        }
    }

    /// Builds a proto `Invitation` with role-based field population.
    ///
    /// Admin view: `invitee_email` populated, `organization_name` empty.
    /// User view: `invitee_email` empty, `organization_name` populated.
    ///
    /// If `status_override` is `Some`, uses that status directly (avoids
    /// recomputing lazy expiration when the caller already checked).
    fn build_invitation(
        &self,
        inv: &OrganizationInvitation,
        slug_resolver: &SlugResolver,
        admin_view: bool,
        status_override: Option<DomainInvitationStatus>,
    ) -> Result<proto::Invitation, Status> {
        let org_slug = slug_resolver.resolve_slug(inv.organization)?;
        let inviter_slug = slug_resolver.resolve_user_slug(inv.inviter)?;

        let status = status_override.unwrap_or_else(|| {
            inferadb_ledger_types::effective_invitation_status(
                inv.status,
                inv.expires_at,
                Utc::now(),
            )
        });

        let team_slug = inv.team.and_then(|team_id| {
            self.ctx
                .applied_state
                .resolve_team_id_to_slug(team_id)
                .map(|s| proto::TeamSlug { slug: s.value() })
        });

        let (invitee_email, organization_name) = if admin_view {
            (inv.invitee_email.clone(), String::new())
        } else {
            (String::new(), self.get_org_name(inv.organization))
        };

        Ok(proto::Invitation {
            slug: Some(proto::InviteSlug { slug: inv.slug.value() }),
            organization: Some(proto::OrganizationSlug { slug: org_slug.value() }),
            inviter: Some(proto::UserSlug { slug: inviter_slug.value() }),
            invitee_email,
            organization_name,
            role: proto_compat::member_role_to_proto(inv.role).into(),
            team: team_slug,
            status: proto::InvitationStatus::from(status).into(),
            created_at: Some(proto_compat::datetime_to_proto(&inv.created_at)),
            expires_at: Some(proto_compat::datetime_to_proto(&inv.expires_at)),
            resolved_at: inv.resolved_at.as_ref().map(proto_compat::datetime_to_proto),
        })
    }

    /// Resolves a GLOBAL `ResolveOrganizationInvite` proposal + REGIONAL
    /// `UpdateOrganizationInviteStatus` proposal for terminal state transitions.
    async fn resolve_invitation(
        &self,
        invite_id: InviteId,
        org_id: OrganizationId,
        status: DomainInvitationStatus,
        invitee_email_hmac: &str,
        token_hash: [u8; 32],
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<(), Status> {
        let region = self.org_region(org_id)?;

        // GLOBAL proposal: CAS on Pending → terminal status
        let global_resp = self
            .ctx
            .propose_organization_request(
                region,
                org_id,
                OrganizationRequest::ResolveOrganizationInvite {
                    invite: invite_id,
                    organization: org_id,
                    status,
                    invitee_email_hmac: invitee_email_hmac.to_owned(),
                    token_hash,
                },
                grpc_metadata,
                ctx,
            )
            .await?;

        // Check for CAS failure
        if let LedgerResponse::Error { code, message } = global_resp {
            return Err(super::helpers::error_code_to_status(code, message));
        }

        // REGIONAL proposal: update status. Failure is logged but not returned —
        // the GLOBAL state is authoritative and the background maintenance job
        // reconciles REGIONAL records.
        if let Err(e) = self
            .ctx
            .propose_regional_org_encrypted(
                region,
                SystemRequest::UpdateOrganizationInviteStatus {
                    organization: org_id,
                    invite: invite_id,
                    status,
                },
                org_id,
                grpc_metadata,
                ctx,
            )
            .await
        {
            tracing::warn!(
                invite_id = invite_id.value(),
                org_id = org_id.value(),
                status = %status,
                error = %e,
                "REGIONAL status update failed after GLOBAL resolve; maintenance job will reconcile"
            );
        }

        Ok(())
    }

    /// Grants organization membership and optional team membership.
    ///
    /// Used in both the normal acceptance path and the partial-failure recovery
    /// path (GLOBAL shows Accepted but membership was not yet granted).
    async fn grant_membership(
        &self,
        org_id: OrganizationId,
        user_id: UserId,
        user_slug: inferadb_ledger_types::UserSlug,
        role: DomainMemberRole,
        team_id: Option<inferadb_ledger_types::TeamId>,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<(), Status> {
        let region = self.org_region(org_id)?;
        let _ = self
            .ctx
            .propose_organization_request(
                region,
                org_id,
                OrganizationRequest::AddOrganizationMember {
                    organization: org_id,
                    user: user_id,
                    user_slug,
                    role,
                },
                grpc_metadata,
                ctx,
            )
            .await?;

        if let Some(team_id) = team_id {
            let _ = self
                .ctx
                .propose_regional_org_encrypted(
                    region,
                    SystemRequest::AddTeamMember {
                        organization: org_id,
                        team: team_id,
                        user_id,
                        role: inferadb_ledger_state::system::TeamMemberRole::Member,
                    },
                    org_id,
                    grpc_metadata,
                    ctx,
                )
                .await;
        }

        Ok(())
    }

    /// Generates a 32-byte CSPRNG token and returns (hex_token, sha256_hash).
    fn generate_token() -> (String, [u8; 32]) {
        use rand::RngExt;
        let mut rng = rand::rng();
        let mut raw = [0u8; 32];
        rng.fill(&mut raw);
        let hex = inferadb_ledger_types::bytes_to_hex(&raw);
        let hash: [u8; 32] = Sha256::digest(raw).into();
        (hex, hash)
    }
}

#[tonic::async_trait]
impl proto::invitation_service_server::InvitationService for InvitationService {
    async fn create_organization_invite(
        &self,
        request: Request<CreateOrganizationInviteRequest>,
    ) -> Result<Response<CreateOrganizationInviteResponse>, Status> {
        // 1. Deadline + drain check
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "create_organization_invite",
            &request,
        );
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        // 4. Input validation
        let email = req.email.trim();
        if email.is_empty() {
            ctx.set_error("InvalidArgument", "email is required");
            return Err(Status::invalid_argument("email is required"));
        }
        validation::validate_email(email).map_err(|e| {
            ctx.set_error("InvalidArgument", &e.to_string());
            Status::invalid_argument(e.to_string())
        })?;

        let ttl_hours = req.ttl_hours;
        if !(1..=720).contains(&ttl_hours) {
            ctx.set_error("InvalidArgument", "ttl_hours must be between 1 and 720");
            return Err(Status::invalid_argument("ttl_hours must be between 1 and 720"));
        }

        let role_proto = proto::OrganizationMemberRole::try_from(req.role)
            .unwrap_or(proto::OrganizationMemberRole::Unspecified);
        let role = proto_compat::member_role_from_proto(role_proto);

        // 5. Resolve slugs
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        // 6. Authorize: verify caller is org admin
        let (inviter_id, _inviter_slug) =
            self.validate_org_admin(&slug_resolver, org_id, &req.caller, &mut ctx)?;

        // Resolve optional team and validate cross-org
        let team_id = if let Some(ref team_slug) = req.team {
            let team_slug_val = inferadb_ledger_types::TeamSlug::new(team_slug.slug);
            let (team_org_id, tid) = slug_resolver
                .resolve_team(team_slug_val)
                .map_err(|_| Status::invalid_argument("Team not found"))?;
            if team_org_id != org_id {
                ctx.set_error("InvalidArgument", "Team belongs to a different organization");
                return Err(Status::invalid_argument("Team belongs to a different organization"));
            }
            Some(tid)
        } else {
            None
        };

        // 7. Compute invitee_email_hmac
        let blinding_key = self.blinding_key()?;
        let invitee_email_hmac = compute_email_hmac(blinding_key, email);

        // 8. Existing member check with timing equalization
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let email_hash_entry = sys_svc
            .get_email_hash(&invitee_email_hmac)
            .map_err(|e| error_classify::storage_error(&e))?;

        // Always read organization members (timing equalization)
        let org = self
            .read_organization(org_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;

        if let Some(inferadb_ledger_state::system::EmailHashEntry::Active(existing_user_id)) =
            email_hash_entry
        {
            let is_member = org.members.iter().any(|m| m.user_id == existing_user_id);
            if is_member {
                return Err(super::helpers::error_code_to_status(
                    inferadb_ledger_types::ErrorCode::InvitationAlreadyMember,
                    "Invitee is already a member of this organization".to_owned(),
                ));
            }
        }

        // 9. Per-email checks
        let email_entries = self.scan_email_entries(&invitee_email_hmac)?;
        let mut pending_count = 0usize;
        let now = Utc::now();

        for (entry_invite_id, entry) in &email_entries {
            if entry.status == DomainInvitationStatus::Pending {
                // Check lazy expiration
                if let Ok(Some(inv)) =
                    self.read_regional_invitation(entry.organization, *entry_invite_id)
                {
                    if inv.expires_at < now {
                        continue; // Expired, don't count
                    }
                    pending_count += 1;

                    // Duplicate pending check: same org+email
                    if entry.organization == org_id {
                        return Err(super::helpers::error_code_to_status(
                            inferadb_ledger_types::ErrorCode::InvitationDuplicatePending,
                            format!(
                                "A pending invitation already exists for this email in this organization (slug: {})",
                                inv.slug.value()
                            ),
                        ));
                    }
                }
            } else if entry.status == DomainInvitationStatus::Declined
                && entry.organization == org_id
            {
                // Decline cooldown: read REGIONAL for resolved_at
                if let Ok(Some(inv)) =
                    self.read_regional_invitation(entry.organization, *entry_invite_id)
                    && let Some(resolved_at) = inv.resolved_at
                {
                    let elapsed = now.signed_duration_since(resolved_at).num_seconds();
                    if elapsed < DECLINE_COOLDOWN_SECS {
                        return Err(Status::resource_exhausted(
                            "Invitation rate limit exceeded. Try again later.",
                        ));
                    }
                }
            }
        }

        // Global pending cap
        if pending_count >= MAX_PENDING_PER_EMAIL {
            return Err(Status::resource_exhausted(
                "Invitation rate limit exceeded. Try again later.",
            ));
        }

        // Per-email total: reject if too many entries exist for this email
        if email_entries.len() >= MAX_TOTAL_PER_EMAIL {
            return Err(Status::resource_exhausted(
                "Invitation rate limit exceeded. Try again later.",
            ));
        }

        // 10. Client-supplied Snowflake slug — required. Retries across
        // lost responses MUST reuse the same slug so the apply-side
        // idempotency path returns the existing invite instead of
        // creating a duplicate record (and sending a duplicate email).
        let invite_slug_proto = req.slug.as_ref().ok_or_else(|| {
            Status::invalid_argument("CreateOrganizationInviteRequest.slug is required")
        })?;
        if invite_slug_proto.slug == 0 {
            return Err(Status::invalid_argument(
                "CreateOrganizationInviteRequest.slug must be a non-zero Snowflake",
            ));
        }
        let invite_slug = InviteSlug::new(invite_slug_proto.slug);
        let (raw_token, token_hash) = Self::generate_token();

        // 11. GLOBAL proposal
        let region = self.org_region(org_id)?;
        let global_resp = self
            .ctx
            .propose_organization_request(
                region,
                org_id,
                OrganizationRequest::CreateOrganizationInvite {
                    organization: org_id,
                    slug: invite_slug,
                    token_hash,
                    invitee_email_hmac: invitee_email_hmac.clone(),
                    ttl_hours,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Extract invite_id and expires_at from response
        let (invite_id, expires_at) = match global_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, invite_slug: _, expires_at } => {
                (invite_id, expires_at)
            },
            LedgerResponse::Error { code, message } => {
                return Err(super::helpers::error_code_to_status(code, message));
            },
            _ => return Err(Status::internal("Unexpected response from CreateOrganizationInvite")),
        };

        // 12. REGIONAL proposal (encrypted with OrgShredKey)
        let regional_result = self
            .ctx
            .propose_regional_org_encrypted(
                region,
                SystemRequest::WriteOrganizationInvite {
                    organization: org_id,
                    invite: invite_id,
                    slug: invite_slug,
                    token_hash,
                    inviter: inviter_id,
                    invitee_email_hmac: invitee_email_hmac.clone(),
                    invitee_email: email.to_owned(),
                    role,
                    team: team_id,
                    expires_at,
                },
                org_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await;

        // If REGIONAL fails, attempt GLOBAL cleanup
        if let Err(regional_err) = regional_result {
            tracing::warn!(
                invite_id = invite_id.value(),
                error = %regional_err,
                "REGIONAL write failed after GLOBAL; orphaned indexes will be reaped"
            );
            // Attempt cleanup by resolving as Revoked
            let _ = self
                .ctx
                .propose_organization_request(
                    region,
                    org_id,
                    OrganizationRequest::ResolveOrganizationInvite {
                        invite: invite_id,
                        organization: org_id,
                        status: DomainInvitationStatus::Revoked,
                        invitee_email_hmac,
                        token_hash,
                    },
                    &grpc_metadata,
                    &mut ctx,
                )
                .await;
            return Err(regional_err);
        }

        ctx.set_success();

        Ok(Response::new(CreateOrganizationInviteResponse {
            slug: Some(proto::InviteSlug { slug: invite_slug.value() }),
            status: proto::InvitationStatus::Pending.into(),
            created_at: Some(proto_compat::datetime_to_proto(&Utc::now())),
            expires_at: Some(proto_compat::datetime_to_proto(&expires_at)),
            token: raw_token,
        }))
    }

    async fn list_organization_invites(
        &self,
        request: Request<ListOrganizationInvitesRequest>,
    ) -> Result<Response<ListOrganizationInvitesResponse>, Status> {
        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "list_organization_invites",
            &request,
        );
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let _ = self.validate_org_admin(&slug_resolver, org_id, &req.caller, &mut ctx)?;

        let page_size = proto_compat::normalize_page_size(req.page_size);
        let status_filter: Option<DomainInvitationStatus> = req
            .status_filter
            .and_then(|v| inferadb_ledger_proto::convert::invitation_status_from_i32(v).ok());

        // REGIONAL prefix scan
        let region = self.org_region(org_id)?;
        let state = self.ctx.regional_state(region)?;
        let prefix = SystemKeys::invite_prefix(org_id);
        let entities = state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, MAX_LIST_INVITATIONS)
            .map_err(|e| error_classify::storage_error(&e))?;

        let now = Utc::now();
        let mut invitations: Vec<(u64, proto::Invitation)> = Vec::new();

        for entity in &entities {
            let Ok(inv) = decode::<OrganizationInvitation>(&entity.value) else {
                continue;
            };

            let effective_status =
                inferadb_ledger_types::effective_invitation_status(inv.status, inv.expires_at, now);

            // Apply status filter
            if let Some(filter) = status_filter
                && effective_status != filter
            {
                continue;
            }

            if let Ok(proto_inv) =
                self.build_invitation(&inv, &slug_resolver, true, Some(effective_status))
            {
                invitations.push((inv.slug.value(), proto_inv));
            }
        }

        let (page, next_page_token) = proto_compat::paginate_by_slug(
            invitations,
            proto_compat::decode_page_token(&req.page_token),
            page_size,
        );

        ctx.set_success();

        Ok(Response::new(ListOrganizationInvitesResponse { invitations: page, next_page_token }))
    }

    async fn get_organization_invite(
        &self,
        request: Request<GetOrganizationInviteRequest>,
    ) -> Result<Response<GetOrganizationInviteResponse>, Status> {
        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "get_organization_invite",
            &request,
        );
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let index_entry = self.resolve_invite_slug_or_not_found(&req.slug, &mut ctx)?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let _ = self.validate_org_admin(
            &slug_resolver,
            index_entry.organization,
            &req.caller,
            &mut ctx,
        )?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .ok_or_else(|| {
                ctx.set_error("NotFound", "Invitation record not found");
                Status::not_found("Invitation not found")
            })?;

        let proto_inv = self.build_invitation(&inv, &slug_resolver, true, None)?;
        ctx.set_success();

        Ok(Response::new(GetOrganizationInviteResponse { invitation: Some(proto_inv) }))
    }

    async fn revoke_organization_invite(
        &self,
        request: Request<RevokeOrganizationInviteRequest>,
    ) -> Result<Response<RevokeOrganizationInviteResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "revoke_organization_invite",
            &request,
        );
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let index_entry = self.resolve_invite_slug_or_not_found(&req.slug, &mut ctx)?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let _ = self.validate_org_admin(
            &slug_resolver,
            index_entry.organization,
            &req.caller,
            &mut ctx,
        )?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        if inv.status != DomainInvitationStatus::Pending {
            return Err(super::helpers::error_code_to_status(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                format!("Invitation is already {}", inv.status),
            ));
        }

        self.resolve_invitation(
            index_entry.invite,
            index_entry.organization,
            DomainInvitationStatus::Revoked,
            &inv.invitee_email_hmac,
            inv.token_hash,
            &grpc_metadata,
            &mut ctx,
        )
        .await?;

        // Re-read for updated state
        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .unwrap_or(inv);
        let proto_inv = self.build_invitation(&updated_inv, &slug_resolver, true, None)?;

        ctx.set_success();

        Ok(Response::new(RevokeOrganizationInviteResponse { invitation: Some(proto_inv) }))
    }

    async fn list_received_invitations(
        &self,
        request: Request<ListReceivedInvitationsRequest>,
    ) -> Result<Response<ListReceivedInvitationsResponse>, Status> {
        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "list_received_invitations",
            &request,
        );
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id =
            slug_resolver.extract_and_resolve_user(&req.caller).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let blinding_key = self.blinding_key()?;
        let user_hmacs = self.get_user_email_hmacs(user_id, blinding_key)?;

        let page_size = proto_compat::normalize_page_size(req.page_size);
        let status_filter: Option<DomainInvitationStatus> = req
            .status_filter
            .and_then(|v| inferadb_ledger_proto::convert::invitation_status_from_i32(v).ok());

        let now = Utc::now();
        let mut invitations: Vec<(u64, proto::Invitation)> = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for hmac in &user_hmacs {
            let entries = self.scan_email_entries(hmac)?;
            for (invite_id, entry) in entries {
                if !seen_ids.insert((entry.organization, invite_id)) {
                    continue;
                }

                let Ok(Some(inv)) = self.read_regional_invitation(entry.organization, invite_id)
                else {
                    continue;
                };

                let effective_status = inferadb_ledger_types::effective_invitation_status(
                    entry.status,
                    inv.expires_at,
                    now,
                );

                if let Some(filter) = status_filter
                    && effective_status != filter
                {
                    continue;
                }

                if let Ok(proto_inv) =
                    self.build_invitation(&inv, &slug_resolver, false, Some(effective_status))
                {
                    invitations.push((inv.slug.value(), proto_inv));
                }
            }
        }

        let (page, next_page_token) = proto_compat::paginate_by_slug(
            invitations,
            proto_compat::decode_page_token(&req.page_token),
            page_size,
        );

        ctx.set_success();

        Ok(Response::new(ListReceivedInvitationsResponse { invitations: page, next_page_token }))
    }

    async fn get_invitation_details(
        &self,
        request: Request<GetInvitationDetailsRequest>,
    ) -> Result<Response<GetInvitationDetailsResponse>, Status> {
        let mut ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "get_invitation_details",
            &request,
        );
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let index_entry = self.resolve_invite_slug_or_not_found(&req.slug, &mut ctx)?;

        // Multi-email HMAC match
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id =
            slug_resolver.extract_and_resolve_user(&req.caller).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let blinding_key = self.blinding_key()?;
        let user_hmacs = self.get_user_email_hmacs(user_id, blinding_key)?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        if !Self::email_matches(&user_hmacs, &inv.invitee_email_hmac) {
            ctx.set_error("NotFound", "Invitation not found");
            return Err(Status::not_found("Invitation not found"));
        }

        let proto_inv = self.build_invitation(&inv, &slug_resolver, false, None)?;
        ctx.set_success();

        Ok(Response::new(GetInvitationDetailsResponse { invitation: Some(proto_inv) }))
    }

    async fn accept_invitation(
        &self,
        request: Request<AcceptInvitationRequest>,
    ) -> Result<Response<AcceptInvitationResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("InvitationService", "accept_invitation", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        // 2. Resolve slug with timing equalization
        let index_entry = self.resolve_invite_slug_or_not_found(&req.slug, &mut ctx)?;

        // 3. Multi-email HMAC match
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id =
            slug_resolver.extract_and_resolve_user(&req.caller).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let user_slug = SlugResolver::extract_user_slug(&req.caller)?;

        let blinding_key = self.blinding_key()?;
        let user_hmacs = self.get_user_email_hmacs(user_id, blinding_key)?;

        // 4. Email match via GLOBAL index (authoritative after CAS)
        let email_entry = self
            .check_email_match_global(&user_hmacs, index_entry.invite, index_entry.organization)?
            .ok_or_else(|| {
                ctx.set_error("NotFound", "Invitation not found");
                Status::not_found("Invitation not found")
            })?;

        // 5. Check current status from GLOBAL InviteEmailEntry (authoritative after CAS).
        // Terminal non-Pending statuses are rejected without a REGIONAL read.
        if email_entry.status == DomainInvitationStatus::Accepted {
            // Partial failure recovery: GLOBAL shows Accepted but membership may not
            // have been granted. Read REGIONAL for role/team, then complete membership.
            let org = self
                .read_organization(index_entry.organization)
                .ok_or_else(|| Status::not_found("Organization not found"))?;
            let is_member = org.members.iter().any(|m| m.user_id == user_id);

            if is_member {
                // Idempotent: previous acceptance fully completed
                if let Ok(Some(inv)) =
                    self.read_regional_invitation(index_entry.organization, index_entry.invite)
                {
                    let proto_inv = self.build_invitation(&inv, &slug_resolver, false, None)?;
                    ctx.set_success();
                    return Ok(Response::new(AcceptInvitationResponse {
                        invitation: Some(proto_inv),
                    }));
                }
                ctx.set_success();
                return Ok(Response::new(AcceptInvitationResponse { invitation: None }));
            }

            // Not a member yet: complete membership grant (partial failure recovery)
            let inv = self
                .read_regional_invitation(index_entry.organization, index_entry.invite)?
                .ok_or_else(|| {
                    Status::internal("REGIONAL invitation missing after GLOBAL accept")
                })?;

            self.grant_membership(
                index_entry.organization,
                user_id,
                user_slug,
                inv.role,
                inv.team,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

            let proto_inv = self.build_invitation(&inv, &slug_resolver, false, None)?;
            ctx.set_success();
            return Ok(Response::new(AcceptInvitationResponse { invitation: Some(proto_inv) }));
        }

        if email_entry.status.is_terminal() {
            return Err(super::helpers::error_code_to_status(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                format!("Invitation is already {}", email_entry.status),
            ));
        }

        // 6. Read REGIONAL for role, team, and proposal fields
        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        // Check expiration
        if inv.expires_at < Utc::now() {
            return Err(super::helpers::error_code_to_status(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                "Invitation has expired".to_owned(),
            ));
        }

        // 7. GLOBAL proposal: Resolve as Accepted
        self.resolve_invitation(
            index_entry.invite,
            index_entry.organization,
            DomainInvitationStatus::Accepted,
            &inv.invitee_email_hmac,
            inv.token_hash,
            &grpc_metadata,
            &mut ctx,
        )
        .await?;

        // 9. Grant membership (org + optional team)
        self.grant_membership(
            index_entry.organization,
            user_id,
            user_slug,
            inv.role,
            inv.team,
            &grpc_metadata,
            &mut ctx,
        )
        .await?;

        // Re-read for updated state
        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .unwrap_or(inv);
        let proto_inv = self.build_invitation(&updated_inv, &slug_resolver, false, None)?;

        ctx.set_success();

        Ok(Response::new(AcceptInvitationResponse { invitation: Some(proto_inv) }))
    }

    async fn decline_invitation(
        &self,
        request: Request<DeclineInvitationRequest>,
    ) -> Result<Response<DeclineInvitationResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("InvitationService", "decline_invitation", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let index_entry = self.resolve_invite_slug_or_not_found(&req.slug, &mut ctx)?;

        // Multi-email HMAC match via GLOBAL index (matches accept_invitation pattern)
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id =
            slug_resolver.extract_and_resolve_user(&req.caller).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let blinding_key = self.blinding_key()?;
        let user_hmacs = self.get_user_email_hmacs(user_id, blinding_key)?;

        // Email match via GLOBAL index (authoritative after CAS)
        let email_entry = self
            .check_email_match_global(&user_hmacs, index_entry.invite, index_entry.organization)?
            .ok_or_else(|| {
                ctx.set_error("NotFound", "Invitation not found");
                Status::not_found("Invitation not found")
            })?;

        // Check current status from GLOBAL InviteEmailEntry (authoritative after CAS).
        // Terminal non-Pending statuses are rejected without a REGIONAL read.
        if email_entry.status.is_terminal() {
            return Err(super::helpers::error_code_to_status(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                format!("Invitation is already {}", email_entry.status),
            ));
        }

        // Read REGIONAL for proposal fields (email HMAC, token hash)
        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .ok_or_else(|| Status::not_found("Invitation not found"))?;

        // Check expiration from REGIONAL record
        if inv.expires_at < Utc::now() {
            return Err(super::helpers::error_code_to_status(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                "Invitation has expired".to_owned(),
            ));
        }

        self.resolve_invitation(
            index_entry.invite,
            index_entry.organization,
            DomainInvitationStatus::Declined,
            &inv.invitee_email_hmac,
            inv.token_hash,
            &grpc_metadata,
            &mut ctx,
        )
        .await?;

        // Re-read for updated state
        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)?
            .unwrap_or(inv);
        let proto_inv = self.build_invitation(&updated_inv, &slug_resolver, false, None)?;

        ctx.set_success();

        Ok(Response::new(DeclineInvitationResponse { invitation: Some(proto_inv) }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use inferadb_ledger_proto::proto::{
        self, invitation_service_server::InvitationService as InvitationServiceTrait,
    };
    use inferadb_ledger_raft::log_storage::AppliedState;
    use inferadb_ledger_test_utils::TestDir;
    use tonic::Request;

    use super::*;
    use crate::proposal::mock::MockProposalService;

    /// Creates an `InvitationService` backed by a [`MockProposalService`].
    fn make_invitation_service()
    -> (super::InvitationService, Arc<MockProposalService>, Arc<ArcSwap<AppliedState>>, TestDir)
    {
        let temp = TestDir::new();
        let mock = Arc::new(MockProposalService::new());
        let (ctx, applied_state) =
            super::super::service_infra::tests::test_service_context_with_mock(mock.clone(), &temp);
        let service = super::InvitationService::new(ctx);
        (service, mock, applied_state, temp)
    }

    // =========================================================================
    // create_organization_invite — validation paths
    // =========================================================================

    #[tokio::test]
    async fn create_invite_empty_email_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: 1000 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            email: String::new(),
            role: 0,
            ttl_hours: 24,
            team: None,
            slug: Some(proto::InviteSlug {
                slug: inferadb_ledger_types::snowflake::generate().expect("snowflake"),
            }),
        });

        let err = InvitationServiceTrait::create_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_invite_invalid_email_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: 1000 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            email: "not-an-email".to_string(),
            role: 0,
            ttl_hours: 24,
            team: None,
            slug: Some(proto::InviteSlug {
                slug: inferadb_ledger_types::snowflake::generate().expect("snowflake"),
            }),
        });

        let err = InvitationServiceTrait::create_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_invite_ttl_too_low_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: 1000 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            email: "test@example.com".to_string(),
            role: 0,
            ttl_hours: 0,
            team: None,
            slug: Some(proto::InviteSlug {
                slug: inferadb_ledger_types::snowflake::generate().expect("snowflake"),
            }),
        });

        let err = InvitationServiceTrait::create_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("ttl_hours"));
    }

    #[tokio::test]
    async fn create_invite_ttl_too_high_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: 1000 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            email: "test@example.com".to_string(),
            role: 0,
            ttl_hours: 721,
            team: None,
            slug: Some(proto::InviteSlug {
                slug: inferadb_ledger_types::snowflake::generate().expect("snowflake"),
            }),
        });

        let err = InvitationServiceTrait::create_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("ttl_hours"));
    }

    #[tokio::test]
    async fn create_invite_unknown_org_returns_not_found() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::CreateOrganizationInviteRequest {
            organization: Some(proto::OrganizationSlug { slug: 9999 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            email: "test@example.com".to_string(),
            role: 1,
            ttl_hours: 24,
            team: None,
            slug: Some(proto::InviteSlug {
                slug: inferadb_ledger_types::snowflake::generate().expect("snowflake"),
            }),
        });

        let err = InvitationServiceTrait::create_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // =========================================================================
    // list_organization_invites — validation paths
    // =========================================================================

    #[tokio::test]
    async fn list_invites_missing_org_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::ListOrganizationInvitesRequest {
            organization: None,
            caller: None,
            page_size: 10,
            page_token: None,
            status_filter: None,
        });

        let err =
            InvitationServiceTrait::list_organization_invites(&service, request).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_invites_unknown_org_returns_not_found() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::ListOrganizationInvitesRequest {
            organization: Some(proto::OrganizationSlug { slug: 9999 }),
            caller: Some(proto::UserSlug { slug: 1 }),
            page_size: 10,
            page_token: None,
            status_filter: None,
        });

        let err =
            InvitationServiceTrait::list_organization_invites(&service, request).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // =========================================================================
    // revoke_organization_invite — validation paths
    // =========================================================================

    #[tokio::test]
    async fn revoke_invite_missing_slug_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request =
            Request::new(proto::RevokeOrganizationInviteRequest { slug: None, caller: None });

        let err = InvitationServiceTrait::revoke_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn revoke_invite_unknown_slug_returns_not_found() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::RevokeOrganizationInviteRequest {
            slug: Some(proto::InviteSlug { slug: 9999 }),
            caller: Some(proto::UserSlug { slug: 1 }),
        });

        let err = InvitationServiceTrait::revoke_organization_invite(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // =========================================================================
    // get_organization_invite — validation paths
    // =========================================================================

    #[tokio::test]
    async fn get_invite_missing_slug_returns_invalid_argument() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request =
            Request::new(proto::GetOrganizationInviteRequest { slug: None, caller: None });

        let err =
            InvitationServiceTrait::get_organization_invite(&service, request).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_invite_unknown_slug_returns_not_found() {
        let (service, _mock, _applied_state, _temp) = make_invitation_service();

        let request = Request::new(proto::GetOrganizationInviteRequest {
            slug: Some(proto::InviteSlug { slug: 9999 }),
            caller: Some(proto::UserSlug { slug: 1 }),
        });

        let err =
            InvitationServiceTrait::get_organization_invite(&service, request).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // =========================================================================
    // Constants
    // =========================================================================

    #[test]
    fn constants_are_reasonable() {
        const {
            assert!(MAX_PENDING_PER_EMAIL > 0);
            assert!(MAX_TOTAL_PER_EMAIL >= MAX_PENDING_PER_EMAIL);
            assert!(DECLINE_COOLDOWN_SECS > 0);
            assert!(SCAN_CEILING > 0);
            assert!(MAX_LIST_INVITATIONS > 0);
        }
    }

    #[test]
    fn decline_cooldown_is_24_hours() {
        const { assert!(DECLINE_COOLDOWN_SECS == 86_400) }
    }

    // =========================================================================
    // email_matches (constant-time email HMAC comparison)
    // =========================================================================

    #[test]
    fn email_matches_single_match() {
        let user_hmacs = vec!["abc123".to_string()];
        assert!(InvitationService::email_matches(&user_hmacs, "abc123"));
    }

    #[test]
    fn email_matches_no_match() {
        let user_hmacs = vec!["abc123".to_string()];
        assert!(!InvitationService::email_matches(&user_hmacs, "xyz789"));
    }

    #[test]
    fn email_matches_multiple_hmacs_second_matches() {
        let user_hmacs = vec!["aaa".to_string(), "bbb".to_string(), "ccc".to_string()];
        assert!(InvitationService::email_matches(&user_hmacs, "bbb"));
    }

    #[test]
    fn email_matches_empty_user_hmacs() {
        let user_hmacs: Vec<String> = vec![];
        assert!(!InvitationService::email_matches(&user_hmacs, "abc123"));
    }

    #[test]
    fn email_matches_empty_invitation_hmac() {
        let user_hmacs = vec!["abc".to_string()];
        assert!(!InvitationService::email_matches(&user_hmacs, ""));
    }

    #[test]
    fn email_matches_both_empty() {
        let user_hmacs: Vec<String> = vec!["".to_string()];
        assert!(InvitationService::email_matches(&user_hmacs, ""));
    }

    #[test]
    fn email_matches_case_sensitive() {
        let user_hmacs = vec!["AbC".to_string()];
        assert!(!InvitationService::email_matches(&user_hmacs, "abc"));
    }

    #[test]
    fn email_matches_last_element_matches() {
        let user_hmacs: Vec<String> = (0..10).map(|i| format!("hmac_{i}")).collect();
        assert!(InvitationService::email_matches(&user_hmacs, "hmac_9"));
    }

    // =========================================================================
    // generate_token
    // =========================================================================

    #[test]
    fn generate_token_returns_hex_and_hash() {
        let (hex, hash) = InvitationService::generate_token();
        // Hex string should be 64 chars (32 bytes * 2 hex chars)
        assert_eq!(hex.len(), 64, "hex token should be 64 characters");
        // Hash should be 32 bytes (SHA-256)
        assert_eq!(hash.len(), 32, "hash should be 32 bytes");
    }

    #[test]
    fn generate_token_unique() {
        let (hex1, hash1) = InvitationService::generate_token();
        let (hex2, hash2) = InvitationService::generate_token();
        assert_ne!(hex1, hex2, "two generated tokens should differ");
        assert_ne!(hash1, hash2, "two generated hashes should differ");
    }

    #[test]
    fn generate_token_hash_is_sha256_of_raw_bytes() {
        let (hex, hash) = InvitationService::generate_token();
        // Decode the hex back to raw bytes and verify the hash
        let raw_bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let expected_hash: [u8; 32] = Sha256::digest(&raw_bytes).into();
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn generate_token_hex_is_valid_lowercase() {
        let (hex, _) = InvitationService::generate_token();
        assert!(
            hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "token should be lowercase hex"
        );
    }
}
