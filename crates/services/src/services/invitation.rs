//! Invitation service implementation.
//!
//! Handles the full organization invitation lifecycle: creation, listing,
//! revocation (admin operations), and acceptance, decline, listing
//! (user operations). Enforces rate limiting, timing equalization,
//! multi-email HMAC matching, and partial failure recovery.

use std::sync::Arc;

use chrono::Utc;
use inferadb_ledger_raft::{
    logging::RequestContext,
    metrics,
    types::{LedgerResponse, OrganizationRequest, SystemRequest},
};
use inferadb_ledger_state::system::{
    Organization, OrganizationMemberRole as DomainMemberRole, OrganizationProfile, SYSTEM_VAULT_ID,
    SystemKeys,
};
use inferadb_ledger_types::{
    EmailBlindingKey, InvitationStatus as DomainInvitationStatus, InviteEmailEntry, InviteId,
    InviteIndexEntry, InviteSlug, OrganizationId, OrganizationInvitation, OrganizationMemberRole,
    UserId, decode, email_hash::compute_email_hmac,
};
use inferadb_ledger_wire::services::{invitation as w, shared as ws};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tonic::Status;

use super::{
    error_classify, service_infra::ServiceContext, slug_resolver::SlugResolver,
    wire_helpers::wire_error_to_tonic_status,
};

/// Maximum pending invitations per email across all organizations.
pub(super) const MAX_PENDING_PER_EMAIL: usize = 10;

/// Maximum total invitations targeting a single email within the retention
/// window (90 days). Prevents throughput amplification via short-TTL
/// create/expire cycles. Stricter than a rolling 24-hour window but avoids
/// expensive per-entry REGIONAL reads for `created_at` timestamps.
pub(super) const MAX_TOTAL_PER_EMAIL: usize = 20;

/// Decline cooldown: 24 hours before re-inviting from the same org.
pub(super) const DECLINE_COOLDOWN_SECS: i64 = 86_400;

/// Maximum email hash entries to scan before short-circuiting.
const SCAN_CEILING: usize = 500;

/// Maximum invitations returned by a REGIONAL prefix scan.
pub(super) const MAX_LIST_INVITATIONS: usize = 1_000;

/// gRPC handler for organization invitation lifecycle.
pub struct InvitationService {
    pub(super) ctx: ServiceContext,
}

impl InvitationService {
    /// Creates a new `InvitationService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Returns the email blinding key, or FAILED_PRECONDITION if unconfigured.
    pub(super) fn blinding_key(&self) -> Result<&Arc<EmailBlindingKey>, Status> {
        self.ctx
            .email_blinding_key
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Email blinding key not configured"))
    }

    /// Resolves an `InviteSlug` to an `InviteIndexEntry` via the GLOBAL index.
    pub(super) fn resolve_invite_slug(
        &self,
        slug: InviteSlug,
    ) -> Result<Option<InviteIndexEntry>, Status> {
        let key = SystemKeys::invite_slug_index_key(slug);
        let entity = self
            .ctx
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;
        match entity {
            Some(e) => {
                let entry: InviteIndexEntry = decode(&e.value).map_err(|e| {
                    wire_error_to_tonic_status(error_classify::serialization_error(&e))
                })?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Scans the GLOBAL `_idx:invite:email_hash:{hmac}:` prefix and returns
    /// entries up to the scan ceiling.
    pub(super) fn scan_email_entries(
        &self,
        hmac_hex: &str,
    ) -> Result<Vec<(InviteId, InviteEmailEntry)>, Status> {
        let prefix = SystemKeys::invite_email_hash_prefix(hmac_hex);
        let entities = self
            .ctx
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, SCAN_CEILING + 1)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;

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
    pub(super) fn org_region(
        &self,
        org_id: OrganizationId,
    ) -> Result<inferadb_ledger_types::Region, Status> {
        let sys_svc = self.ctx.system_service();
        let registry = sys_svc
            .get_organization(org_id)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        Ok(registry.region)
    }

    /// Reads the full REGIONAL invitation record.
    pub(super) fn read_regional_invitation(
        &self,
        org_id: OrganizationId,
        invite_id: InviteId,
    ) -> Result<Option<OrganizationInvitation>, Status> {
        let region = self.org_region(org_id)?;
        let state = self.ctx.regional_state(region)?;
        let key = SystemKeys::invite_key(org_id, invite_id);
        let entity = state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;
        match entity {
            Some(e) => {
                let inv: OrganizationInvitation = decode(&e.value).map_err(|e| {
                    wire_error_to_tonic_status(error_classify::serialization_error(&e))
                })?;
                Ok(Some(inv))
            },
            None => Ok(None),
        }
    }

    /// Reads the GLOBAL `Organization` skeleton (contains members list).
    pub(super) fn read_organization(&self, org_id: OrganizationId) -> Option<Organization> {
        let key = SystemKeys::organization_key(org_id);
        let entity = self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).ok()??;
        decode::<Organization>(&entity.value).ok()
    }

    /// Overlays the org name from REGIONAL profile onto an empty proto field.
    pub(super) fn get_org_name(&self, org_id: OrganizationId) -> String {
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
    pub(super) fn validate_org_admin(
        &self,
        slug_resolver: &SlugResolver,
        org_id: OrganizationId,
        caller: Option<inferadb_ledger_types::UserSlug>,
        ctx: &mut RequestContext,
    ) -> Result<(UserId, inferadb_ledger_types::UserSlug), Status> {
        let user_id = slug_resolver
            .extract_and_resolve_user(caller)
            .inspect_err(|err| {
                ctx.set_error("InvalidArgument", &err.message);
            })
            .map_err(wire_error_to_tonic_status)?;
        let user_slug =
            SlugResolver::extract_user_slug(caller).map_err(wire_error_to_tonic_status)?;

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
    pub(super) fn get_user_email_hmacs(
        &self,
        user_id: UserId,
        blinding_key: &EmailBlindingKey,
    ) -> Result<Vec<String>, Status> {
        let sys_svc = self.ctx.system_service();
        let emails = sys_svc
            .get_user_emails(user_id)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;
        Ok(emails
            .iter()
            .filter(|e| e.verified_at.is_some())
            .map(|e| compute_email_hmac(blinding_key, &e.email))
            .collect())
    }

    /// Checks whether any of the user's verified email HMACs match the invitation's.
    /// Uses constant-time comparison without short-circuiting to prevent timing leaks
    /// that would reveal the position of a matching email in the user's list.
    pub(super) fn email_matches(user_hmacs: &[String], invitation_hmac: &str) -> bool {
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
    pub(super) fn check_email_match_global(
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
                .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;
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

    /// Extracts and resolves an invite slug, with timing equalization on
    /// not-found. Returns `NOT_FOUND` for missing or unknown slugs.
    pub(super) fn resolve_invite_slug_or_not_found(
        &self,
        slug: Option<InviteSlug>,
        ctx: &mut RequestContext,
    ) -> Result<InviteIndexEntry, Status> {
        let slug_val = slug.map_or(0, |s| s.value());
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
    pub(super) fn timing_equalization_reads(&self, region: Option<inferadb_ledger_types::Region>) {
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

    /// Builds a wire `Invitation` with role-based field population.
    ///
    /// Admin view: `invitee_email` populated, `organization_name` empty.
    /// User view: `invitee_email` empty, `organization_name` populated.
    ///
    /// If `status_override` is `Some`, uses that status directly (avoids
    /// recomputing lazy expiration when the caller already checked).
    pub(super) fn build_invitation(
        &self,
        inv: &OrganizationInvitation,
        slug_resolver: &SlugResolver,
        admin_view: bool,
        status_override: Option<DomainInvitationStatus>,
    ) -> Result<w::Invitation, Status> {
        let org_slug =
            slug_resolver.resolve_slug(inv.organization).map_err(wire_error_to_tonic_status)?;
        let inviter_slug =
            slug_resolver.resolve_user_slug(inv.inviter).map_err(wire_error_to_tonic_status)?;

        let status = status_override.unwrap_or_else(|| {
            inferadb_ledger_types::effective_invitation_status(
                inv.status,
                inv.expires_at,
                Utc::now(),
            )
        });

        let team_slug =
            inv.team.and_then(|team_id| self.ctx.applied_state.resolve_team_id_to_slug(team_id));

        let (invitee_email, organization_name) = if admin_view {
            (inv.invitee_email.clone(), String::new())
        } else {
            (String::new(), self.get_org_name(inv.organization))
        };

        // Numeric tags match between domain and wire enums; map via match for
        // clarity (legacy path went `domain → proto::* → i32 → wire`).
        let role = match inv.role {
            OrganizationMemberRole::Admin => ws::OrganizationMemberRole::Admin,
            OrganizationMemberRole::Member => ws::OrganizationMemberRole::Member,
        };
        let wire_status = match status {
            DomainInvitationStatus::Pending => w::InvitationStatus::Pending,
            DomainInvitationStatus::Accepted => w::InvitationStatus::Accepted,
            DomainInvitationStatus::Declined => w::InvitationStatus::Declined,
            DomainInvitationStatus::Expired => w::InvitationStatus::Expired,
            DomainInvitationStatus::Revoked => w::InvitationStatus::Revoked,
        };

        // Wire timestamp fields are UNIX nanoseconds (`u64`).
        let created_at_ns = inv.created_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
        let expires_at_ns = inv.expires_at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
        let resolved_at_ns =
            inv.resolved_at.as_ref().map(|dt| dt.timestamp_nanos_opt().unwrap_or(0).max(0) as u64);

        Ok(w::Invitation {
            slug: Some(inv.slug),
            organization: Some(org_slug),
            inviter: Some(inviter_slug),
            invitee_email,
            organization_name,
            role,
            team: team_slug,
            status: wire_status,
            created_at: created_at_ns,
            expires_at: expires_at_ns,
            resolved_at: resolved_at_ns,
        })
    }

    /// Resolves a GLOBAL `ResolveOrganizationInvite` proposal + REGIONAL
    /// `UpdateOrganizationInviteStatus` proposal for terminal state transitions.
    pub(super) async fn resolve_invitation(
        &self,
        invite_id: InviteId,
        org_id: OrganizationId,
        status: DomainInvitationStatus,
        invitee_email_hmac: &str,
        token_hash: [u8; 32],
        wire_ctx: &inferadb_ledger_wire::RequestContext,
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
                wire_ctx,
                ctx,
            )
            .await
            .map_err(wire_error_to_tonic_status)?;

        // Check for CAS failure
        if let LedgerResponse::Error { code, message } = global_resp {
            return Err(wire_error_to_tonic_status(super::helpers::error_code_to_wire_error(
                code, message,
            )));
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
                wire_ctx,
                ctx,
            )
            .await
        {
            tracing::warn!(
                invite_id = invite_id.value(),
                org_id = org_id.value(),
                status = %status,
                error = ?e,
                "REGIONAL status update failed after GLOBAL resolve; maintenance job will reconcile"
            );
        }

        Ok(())
    }

    /// Grants organization membership and optional team membership.
    ///
    /// Used in both the normal acceptance path and the partial-failure recovery
    /// path (GLOBAL shows Accepted but membership was not yet granted).
    pub(super) async fn grant_membership(
        &self,
        org_id: OrganizationId,
        user_id: UserId,
        user_slug: inferadb_ledger_types::UserSlug,
        role: DomainMemberRole,
        team_id: Option<inferadb_ledger_types::TeamId>,
        wire_ctx: &inferadb_ledger_wire::RequestContext,
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
                wire_ctx,
                ctx,
            )
            .await
            .map_err(wire_error_to_tonic_status)?;

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
                    wire_ctx,
                    ctx,
                )
                .await;
        }

        Ok(())
    }

    /// Generates a 32-byte CSPRNG token and returns (hex_token, sha256_hash).
    pub(super) fn generate_token() -> (String, [u8; 32]) {
        use rand::RngExt;
        let mut rng = rand::rng();
        let mut raw = [0u8; 32];
        rng.fill(&mut raw);
        let hex = inferadb_ledger_types::bytes_to_hex(&raw);
        let hash: [u8; 32] = Sha256::digest(raw).into();
        (hex, hash)
    }
}
