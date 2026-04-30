//! Wire-trait implementation for `InvitationService` (Phase F.1.f.1.8).
//!
//! Inlines the wire-protocol [`inferadb_ledger_wire_services::InvitationService`]
//! impl on [`super::invitation::InvitationService`]. The wire impl mirrors the
//! tonic handler bodies line-by-line on wire types — no UFCS delegation
//! through the tonic impl. Both impls share the same private helpers on
//! [`InvitationService`] (promoted to `pub(super)` for this purpose) and are
//! decoupled from each other except for the shared field/method surface.
//!
//! The proto↔wire conversion helpers below are retained for symmetry with
//! the surviving tonic impl and for round-trip tests, even though the wire
//! impl itself only calls `proto_to_wire_invitation` (the helpers return
//! `proto::Invitation` and the wire response carries `w::Invitation`).
//! They are dead-code allowed because the wire impl no longer routes
//! through the proto-shaped per-RPC conversions. F.1.f.2 (delete tonic
//! impls) revisits whether these stay.
//!
//! Three classes of non-trivial mapping are handled here:
//!
//! - `created_at` / `expires_at` / `resolved_at` are `Option<prost_types::Timestamp>` in proto and
//!   either `u64` or `Option<u64>` UNIX nanoseconds on the wire — bridged by the shared helpers in
//!   [`super::wire_shared`].
//! - `page_token` / `next_page_token` are `Option<Vec<u8>>` in proto and `Option<Bytes>` on the
//!   wire.
//! - `InvitationStatus` and `OrganizationMemberRole` carry raw `i32` values in proto; the wire
//!   enums are `#[repr(i32)]` with matching numeric tags. Unknown tags collapse to `Unspecified` to
//!   match proto's open-enum semantics.

use bytes::Bytes;
use chrono::Utc;
use inferadb_ledger_raft::types::{LedgerResponse, OrganizationRequest, SystemRequest};
use inferadb_ledger_state::system::EmailHashEntry;
use inferadb_ledger_types::{
    InvitationStatus as DomainInvitationStatus, InviteSlug, OrganizationInvitation,
    OrganizationSlug as DomainOrganizationSlug, TeamSlug as DomainTeamSlug,
    UserSlug as DomainUserSlug, decode, email_hash::compute_email_hmac, validation,
};
use inferadb_ledger_wire::{
    RequestContext, WireError,
    services::{invitation as w, shared as ws},
};

use super::{
    error_classify, helpers,
    invitation::{
        DECLINE_COOLDOWN_SECS, InvitationService, MAX_LIST_INVITATIONS, MAX_PENDING_PER_EMAIL,
        MAX_TOTAL_PER_EMAIL,
    },
    pagination,
    slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error,
};

/// Convert a chrono `DateTime<Utc>` to UNIX nanoseconds, clamping pre-1970
/// values and saturating on overflow.
fn datetime_to_ns(dt: &chrono::DateTime<chrono::Utc>) -> u64 {
    let secs = u64::try_from(dt.timestamp().max(0)).unwrap_or(0);
    let nanos = u64::from(dt.timestamp_subsec_nanos());
    secs.saturating_mul(1_000_000_000).saturating_add(nanos)
}

/// Convert a wire `InvitationStatus` filter into the domain enum.
///
/// Operates directly on the wire enum (no `i32` round-trip). Returns `None`
/// for `Unspecified` so callers fall back to "no filter".
fn wire_invitation_status_filter_to_domain(
    status: w::InvitationStatus,
) -> Option<DomainInvitationStatus> {
    match status {
        w::InvitationStatus::Pending => Some(DomainInvitationStatus::Pending),
        w::InvitationStatus::Accepted => Some(DomainInvitationStatus::Accepted),
        w::InvitationStatus::Declined => Some(DomainInvitationStatus::Declined),
        w::InvitationStatus::Expired => Some(DomainInvitationStatus::Expired),
        w::InvitationStatus::Revoked => Some(DomainInvitationStatus::Revoked),
        w::InvitationStatus::Unspecified => None,
    }
}

// ---------------------------------------------------------------------------
// Enum tag mappings.
// ---------------------------------------------------------------------------

/// Numeric-tag mapping from proto `InvitationStatus` (raw i32) to the wire enum.
///
/// Both enums declare the same `Unspecified = 0`, `Pending = 1`,
/// `Accepted = 2`, `Declined = 3`, `Expired = 4`, `Revoked = 5` values.
/// Unknown tags collapse to `Unspecified` to match proto's open-enum
/// semantics — protects against future proto additions.
#[allow(dead_code)]
fn proto_invitation_status_to_wire(value: i32) -> w::InvitationStatus {
    match value {
        x if x == w::InvitationStatus::Pending as i32 => w::InvitationStatus::Pending,
        x if x == w::InvitationStatus::Accepted as i32 => w::InvitationStatus::Accepted,
        x if x == w::InvitationStatus::Declined as i32 => w::InvitationStatus::Declined,
        x if x == w::InvitationStatus::Expired as i32 => w::InvitationStatus::Expired,
        x if x == w::InvitationStatus::Revoked as i32 => w::InvitationStatus::Revoked,
        _ => w::InvitationStatus::Unspecified,
    }
}

// `build_invitation` now returns `wire::Invitation` directly; no
// proto-bridging conversions remain in this file.

// ---------------------------------------------------------------------------
// Wire-trait implementation for `InvitationService`.
//
// Mirrors the tonic [`super::invitation::InvitationService`] handler bodies
// line-by-line on wire types. Both impls share the same private helpers on
// [`InvitationService`] (promoted to `pub(super)` for this purpose) — the
// helper return types remain `tonic::Status` and proto types because the
// surviving tonic impl still uses them; the wire-trait body bridges via
// [`tonic_status_to_wire_error`] at each call site, and converts proto
// `build_invitation` outputs to wire via [`proto_to_wire_invitation`].
//
// The deadline pre-check that the tonic impl performs via
// `super::tonic_compat::check_near_deadline(&request)` has no
// direct equivalent on the wire path: the wire dispatcher derives
// `RequestContext::deadline` from the frame header and is the natural
// home for any pre-handler near-deadline rejection. F.1.f.2 (delete tonic
// impls) revisits whether a wire-side helper is needed.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::InvitationService for InvitationService {
    async fn create_organization_invite(
        &self,
        request: w::CreateOrganizationInviteRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateOrganizationInviteResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "create_organization_invite",
            &_ctx,
        );
        let req = request;

        // Mirror `extract_caller_from_proto_slug`: stash the caller's slug on
        // the canonical-log-line context. Wire `UserSlug` carries the same
        // `u64` Snowflake value the proto path forwarded.
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Input validation (parity with tonic).
        let email = req.email.trim();
        if email.is_empty() {
            log_ctx.set_error("InvalidArgument", "email is required");
            return Err(tonic_status_to_wire_error(tonic::Status::invalid_argument(
                "email is required",
            )));
        }
        validation::validate_email(email).map_err(|e| {
            log_ctx.set_error("InvalidArgument", &e.to_string());
            tonic_status_to_wire_error(tonic::Status::invalid_argument(e.to_string()))
        })?;

        let ttl_hours = req.ttl_hours;
        if !(1..=720).contains(&ttl_hours) {
            log_ctx.set_error("InvalidArgument", "ttl_hours must be between 1 and 720");
            return Err(tonic_status_to_wire_error(tonic::Status::invalid_argument(
                "ttl_hours must be between 1 and 720",
            )));
        }

        // Map wire role enum directly to the domain enum (wire `Unspecified`
        // is normalized to `Member`, mirroring the prior proto path).
        let role = match req.role {
            ws::OrganizationMemberRole::Admin => {
                inferadb_ledger_types::OrganizationMemberRole::Admin
            },
            ws::OrganizationMemberRole::Member | ws::OrganizationMemberRole::Unspecified => {
                inferadb_ledger_types::OrganizationMemberRole::Member
            },
        };

        // Resolve slugs.
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_id = slug_resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let (inviter_id, _inviter_slug) = self
            .validate_org_admin(&slug_resolver, org_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Resolve optional team and validate cross-org.
        let team_id = if let Some(team_slug) = req.team {
            let team_slug_val = DomainTeamSlug::new(team_slug.value());
            let (team_org_id, tid) = slug_resolver.resolve_team(team_slug_val).map_err(|_| {
                tonic_status_to_wire_error(tonic::Status::invalid_argument("Team not found"))
            })?;
            if team_org_id != org_id {
                log_ctx.set_error("InvalidArgument", "Team belongs to a different organization");
                return Err(tonic_status_to_wire_error(tonic::Status::invalid_argument(
                    "Team belongs to a different organization",
                )));
            }
            Some(tid)
        } else {
            None
        };

        // Compute invitee_email_hmac.
        let blinding_key = self.blinding_key().map_err(tonic_status_to_wire_error)?;
        let invitee_email_hmac = compute_email_hmac(blinding_key, email);

        // Existing-member check with timing equalization.
        let sys_svc = self.ctx.system_service();
        let email_hash_entry = sys_svc
            .get_email_hash(&invitee_email_hmac)
            .map_err(|e| error_classify::storage_error(&e))?;

        // Always read organization members (timing equalization).
        let org = self.read_organization(org_id).ok_or_else(|| {
            tonic_status_to_wire_error(tonic::Status::not_found("Organization not found"))
        })?;

        if let Some(EmailHashEntry::Active(existing_user_id)) = email_hash_entry {
            let is_member = org.members.iter().any(|m| m.user_id == existing_user_id);
            if is_member {
                return Err(helpers::error_code_to_wire_error(
                    inferadb_ledger_types::ErrorCode::InvitationAlreadyMember,
                    "Invitee is already a member of this organization".to_owned(),
                ));
            }
        }

        // Per-email checks.
        let email_entries =
            self.scan_email_entries(&invitee_email_hmac).map_err(tonic_status_to_wire_error)?;
        let mut pending_count = 0usize;
        let now = Utc::now();

        for (entry_invite_id, entry) in &email_entries {
            if entry.status == DomainInvitationStatus::Pending {
                if let Ok(Some(inv)) =
                    self.read_regional_invitation(entry.organization, *entry_invite_id)
                {
                    if inv.expires_at < now {
                        continue;
                    }
                    pending_count += 1;

                    if entry.organization == org_id {
                        return Err(helpers::error_code_to_wire_error(
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
                && let Ok(Some(inv)) =
                    self.read_regional_invitation(entry.organization, *entry_invite_id)
                && let Some(resolved_at) = inv.resolved_at
            {
                let elapsed = now.signed_duration_since(resolved_at).num_seconds();
                if elapsed < DECLINE_COOLDOWN_SECS {
                    return Err(tonic_status_to_wire_error(tonic::Status::resource_exhausted(
                        "Invitation rate limit exceeded. Try again later.",
                    )));
                }
            }
        }

        if pending_count >= MAX_PENDING_PER_EMAIL {
            return Err(tonic_status_to_wire_error(tonic::Status::resource_exhausted(
                "Invitation rate limit exceeded. Try again later.",
            )));
        }

        if email_entries.len() >= MAX_TOTAL_PER_EMAIL {
            return Err(tonic_status_to_wire_error(tonic::Status::resource_exhausted(
                "Invitation rate limit exceeded. Try again later.",
            )));
        }

        // Client-supplied Snowflake slug — required.
        let invite_slug_wire = req.slug.ok_or_else(|| {
            tonic_status_to_wire_error(tonic::Status::invalid_argument(
                "CreateOrganizationInviteRequest.slug is required",
            ))
        })?;
        if invite_slug_wire.value() == 0 {
            return Err(tonic_status_to_wire_error(tonic::Status::invalid_argument(
                "CreateOrganizationInviteRequest.slug must be a non-zero Snowflake",
            )));
        }
        let invite_slug = InviteSlug::new(invite_slug_wire.value());
        let (raw_token, token_hash) = InvitationService::generate_token();

        // GLOBAL proposal.
        let region = self.org_region(org_id).map_err(tonic_status_to_wire_error)?;
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let (invite_id, expires_at) = match global_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, invite_slug: _, expires_at } => {
                (invite_id, expires_at)
            },
            LedgerResponse::Error { code, message } => {
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            _ => {
                return Err(tonic_status_to_wire_error(tonic::Status::internal(
                    "Unexpected response from CreateOrganizationInvite",
                )));
            },
        };

        // REGIONAL proposal (encrypted with OrgShredKey).
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
                &_ctx,
                &mut log_ctx,
            )
            .await;

        // If REGIONAL fails, attempt GLOBAL cleanup.
        if let Err(regional_err) = regional_result {
            tracing::warn!(
                invite_id = invite_id.value(),
                error = ?regional_err,
                "REGIONAL write failed after GLOBAL; orphaned indexes will be reaped"
            );
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
                    &_ctx,
                    &mut log_ctx,
                )
                .await;
            return Err(regional_err);
        }

        log_ctx.set_success();

        Ok(w::CreateOrganizationInviteResponse {
            slug: Some(ws::InviteSlug::new(invite_slug.value())),
            status: w::InvitationStatus::Pending,
            created_at: datetime_to_ns(&Utc::now()),
            expires_at: datetime_to_ns(&expires_at),
            token: raw_token,
        })
    }

    async fn list_organization_invites(
        &self,
        request: w::ListOrganizationInvitesRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListOrganizationInvitesResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "list_organization_invites",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_id = slug_resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let _ = self
            .validate_org_admin(&slug_resolver, org_id, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let page_size = pagination::normalize_page_size(req.page_size);
        let status_filter: Option<DomainInvitationStatus> =
            req.status_filter.and_then(wire_invitation_status_filter_to_domain);

        // REGIONAL prefix scan.
        let region = self.org_region(org_id).map_err(tonic_status_to_wire_error)?;
        let state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let prefix = inferadb_ledger_state::system::SystemKeys::invite_prefix(org_id);
        let entities = state
            .list_entities(
                inferadb_ledger_state::system::SYSTEM_VAULT_ID,
                Some(&prefix),
                None,
                MAX_LIST_INVITATIONS,
            )
            .map_err(|e| error_classify::storage_error(&e))?;

        let now = Utc::now();
        let mut invitations: Vec<(u64, w::Invitation)> = Vec::new();

        for entity in &entities {
            let Ok(inv) = decode::<OrganizationInvitation>(&entity.value) else {
                continue;
            };

            let effective_status =
                inferadb_ledger_types::effective_invitation_status(inv.status, inv.expires_at, now);

            if let Some(filter) = status_filter
                && effective_status != filter
            {
                continue;
            }

            if let Ok(wire_inv) =
                self.build_invitation(&inv, &slug_resolver, true, Some(effective_status))
            {
                invitations.push((inv.slug.value(), wire_inv));
            }
        }

        let page_token_vec = req.page_token.as_ref().map(|b| b.to_vec());
        let (page, next_page_token) = pagination::paginate_by_slug(
            invitations,
            pagination::decode_page_token(&page_token_vec),
            page_size,
        );

        log_ctx.set_success();

        Ok(w::ListOrganizationInvitesResponse {
            invitations: page,
            next_page_token: next_page_token.map(Bytes::from),
        })
    }

    async fn get_organization_invite(
        &self,
        request: w::GetOrganizationInviteRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetOrganizationInviteResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "get_organization_invite",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let index_entry = self
            .resolve_invite_slug_or_not_found(req.slug, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let _ = self
            .validate_org_admin(&slug_resolver, index_entry.organization, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                log_ctx.set_error("NotFound", "Invitation record not found");
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        let wire_inv = self
            .build_invitation(&inv, &slug_resolver, true, None)
            .map_err(tonic_status_to_wire_error)?;
        log_ctx.set_success();

        Ok(w::GetOrganizationInviteResponse { invitation: Some(wire_inv) })
    }

    async fn revoke_organization_invite(
        &self,
        request: w::RevokeOrganizationInviteRequest,
        _ctx: RequestContext,
    ) -> Result<w::RevokeOrganizationInviteResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "revoke_organization_invite",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let index_entry = self
            .resolve_invite_slug_or_not_found(req.slug, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let _ = self
            .validate_org_admin(&slug_resolver, index_entry.organization, req.caller, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        if inv.status != DomainInvitationStatus::Pending {
            return Err(helpers::error_code_to_wire_error(
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
            &_ctx,
            &mut log_ctx,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;

        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .unwrap_or(inv);
        let wire_inv = self
            .build_invitation(&updated_inv, &slug_resolver, true, None)
            .map_err(tonic_status_to_wire_error)?;

        log_ctx.set_success();

        Ok(w::RevokeOrganizationInviteResponse { invitation: Some(wire_inv) })
    }

    async fn list_received_invitations(
        &self,
        request: w::ListReceivedInvitationsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListReceivedInvitationsResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "list_received_invitations",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let blinding_key = self.blinding_key().map_err(tonic_status_to_wire_error)?;
        let user_hmacs =
            self.get_user_email_hmacs(user_id, blinding_key).map_err(tonic_status_to_wire_error)?;

        let page_size = pagination::normalize_page_size(req.page_size);
        let status_filter: Option<DomainInvitationStatus> =
            req.status_filter.and_then(wire_invitation_status_filter_to_domain);

        let now = Utc::now();
        let mut invitations: Vec<(u64, w::Invitation)> = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for hmac in &user_hmacs {
            let entries = self.scan_email_entries(hmac).map_err(tonic_status_to_wire_error)?;
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

                if let Ok(wire_inv) =
                    self.build_invitation(&inv, &slug_resolver, false, Some(effective_status))
                {
                    invitations.push((inv.slug.value(), wire_inv));
                }
            }
        }

        let page_token_vec = req.page_token.as_ref().map(|b| b.to_vec());
        let (page, next_page_token) = pagination::paginate_by_slug(
            invitations,
            pagination::decode_page_token(&page_token_vec),
            page_size,
        );

        log_ctx.set_success();

        Ok(w::ListReceivedInvitationsResponse {
            invitations: page,
            next_page_token: next_page_token.map(Bytes::from),
        })
    }

    async fn get_invitation_details(
        &self,
        request: w::GetInvitationDetailsRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetInvitationDetailsResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from(
            "InvitationService",
            "get_invitation_details",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let index_entry = self
            .resolve_invite_slug_or_not_found(req.slug, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Multi-email HMAC match.
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let blinding_key = self.blinding_key().map_err(tonic_status_to_wire_error)?;
        let user_hmacs =
            self.get_user_email_hmacs(user_id, blinding_key).map_err(tonic_status_to_wire_error)?;

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        if !InvitationService::email_matches(&user_hmacs, &inv.invitee_email_hmac) {
            log_ctx.set_error("NotFound", "Invitation not found");
            return Err(tonic_status_to_wire_error(tonic::Status::not_found(
                "Invitation not found",
            )));
        }

        let wire_inv = self
            .build_invitation(&inv, &slug_resolver, false, None)
            .map_err(tonic_status_to_wire_error)?;
        log_ctx.set_success();

        Ok(w::GetInvitationDetailsResponse { invitation: Some(wire_inv) })
    }

    async fn accept_invitation(
        &self,
        request: w::AcceptInvitationRequest,
        _ctx: RequestContext,
    ) -> Result<w::AcceptInvitationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("InvitationService", "accept_invitation", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let index_entry = self
            .resolve_invite_slug_or_not_found(req.slug, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Multi-email HMAC match.
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let user_slug =
            SlugResolver::extract_user_slug(req.caller.map(|s| DomainUserSlug::new(s.value())))?;

        let blinding_key = self.blinding_key().map_err(tonic_status_to_wire_error)?;
        let user_hmacs =
            self.get_user_email_hmacs(user_id, blinding_key).map_err(tonic_status_to_wire_error)?;

        // Email match via GLOBAL index (authoritative after CAS).
        let email_entry = self
            .check_email_match_global(&user_hmacs, index_entry.invite, index_entry.organization)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                log_ctx.set_error("NotFound", "Invitation not found");
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        // GLOBAL InviteEmailEntry is authoritative after CAS.
        if email_entry.status == DomainInvitationStatus::Accepted {
            // Partial-failure recovery path.
            let org = self.read_organization(index_entry.organization).ok_or_else(|| {
                tonic_status_to_wire_error(tonic::Status::not_found("Organization not found"))
            })?;
            let is_member = org.members.iter().any(|m| m.user_id == user_id);

            if is_member {
                if let Ok(Some(inv)) =
                    self.read_regional_invitation(index_entry.organization, index_entry.invite)
                {
                    let wire_inv = self
                        .build_invitation(&inv, &slug_resolver, false, None)
                        .map_err(tonic_status_to_wire_error)?;
                    log_ctx.set_success();
                    return Ok(w::AcceptInvitationResponse { invitation: Some(wire_inv) });
                }
                log_ctx.set_success();
                return Ok(w::AcceptInvitationResponse { invitation: None });
            }

            // Not a member yet: complete membership grant (partial-failure recovery).
            let inv = self
                .read_regional_invitation(index_entry.organization, index_entry.invite)
                .map_err(tonic_status_to_wire_error)?
                .ok_or_else(|| {
                    tonic_status_to_wire_error(tonic::Status::internal(
                        "REGIONAL invitation missing after GLOBAL accept",
                    ))
                })?;

            self.grant_membership(
                index_entry.organization,
                user_id,
                user_slug,
                inv.role,
                inv.team,
                &_ctx,
                &mut log_ctx,
            )
            .await
            .map_err(tonic_status_to_wire_error)?;

            let wire_inv = self
                .build_invitation(&inv, &slug_resolver, false, None)
                .map_err(tonic_status_to_wire_error)?;
            log_ctx.set_success();
            return Ok(w::AcceptInvitationResponse { invitation: Some(wire_inv) });
        }

        if email_entry.status.is_terminal() {
            return Err(helpers::error_code_to_wire_error(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                format!("Invitation is already {}", email_entry.status),
            ));
        }

        // Read REGIONAL for role, team, and proposal fields.
        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        if inv.expires_at < Utc::now() {
            return Err(helpers::error_code_to_wire_error(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                "Invitation has expired".to_owned(),
            ));
        }

        // GLOBAL proposal: Resolve as Accepted.
        self.resolve_invitation(
            index_entry.invite,
            index_entry.organization,
            DomainInvitationStatus::Accepted,
            &inv.invitee_email_hmac,
            inv.token_hash,
            &_ctx,
            &mut log_ctx,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;

        // Grant membership (org + optional team).
        self.grant_membership(
            index_entry.organization,
            user_id,
            user_slug,
            inv.role,
            inv.team,
            &_ctx,
            &mut log_ctx,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;

        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .unwrap_or(inv);
        let wire_inv = self
            .build_invitation(&updated_inv, &slug_resolver, false, None)
            .map_err(tonic_status_to_wire_error)?;

        log_ctx.set_success();

        Ok(w::AcceptInvitationResponse { invitation: Some(wire_inv) })
    }

    async fn decline_invitation(
        &self,
        request: w::DeclineInvitationRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeclineInvitationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("InvitationService", "decline_invitation", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let index_entry = self
            .resolve_invite_slug_or_not_found(req.slug, &mut log_ctx)
            .map_err(tonic_status_to_wire_error)?;

        // Multi-email HMAC match via GLOBAL index.
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(req.caller.map(|s| DomainUserSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let blinding_key = self.blinding_key().map_err(tonic_status_to_wire_error)?;
        let user_hmacs =
            self.get_user_email_hmacs(user_id, blinding_key).map_err(tonic_status_to_wire_error)?;

        let email_entry = self
            .check_email_match_global(&user_hmacs, index_entry.invite, index_entry.organization)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                log_ctx.set_error("NotFound", "Invitation not found");
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        if email_entry.status.is_terminal() {
            return Err(helpers::error_code_to_wire_error(
                inferadb_ledger_types::ErrorCode::InvitationAlreadyResolved,
                format!("Invitation is already {}", email_entry.status),
            ));
        }

        let inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .ok_or_else(|| {
                tonic_status_to_wire_error(tonic::Status::not_found("Invitation not found"))
            })?;

        if inv.expires_at < Utc::now() {
            return Err(helpers::error_code_to_wire_error(
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
            &_ctx,
            &mut log_ctx,
        )
        .await
        .map_err(tonic_status_to_wire_error)?;

        let updated_inv = self
            .read_regional_invitation(index_entry.organization, index_entry.invite)
            .map_err(tonic_status_to_wire_error)?
            .unwrap_or(inv);
        let wire_inv = self
            .build_invitation(&updated_inv, &slug_resolver, false, None)
            .map_err(tonic_status_to_wire_error)?;

        log_ctx.set_success();

        Ok(w::DeclineInvitationResponse { invitation: Some(wire_inv) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Enum tag mappings.
    // -----------------------------------------------------------------------

    #[test]
    fn proto_invitation_status_each_known_tag_maps_correctly() {
        assert_eq!(proto_invitation_status_to_wire(0), w::InvitationStatus::Unspecified);
        assert_eq!(proto_invitation_status_to_wire(1), w::InvitationStatus::Pending);
        assert_eq!(proto_invitation_status_to_wire(2), w::InvitationStatus::Accepted);
        assert_eq!(proto_invitation_status_to_wire(3), w::InvitationStatus::Declined);
        assert_eq!(proto_invitation_status_to_wire(4), w::InvitationStatus::Expired);
        assert_eq!(proto_invitation_status_to_wire(5), w::InvitationStatus::Revoked);
    }

    #[test]
    fn proto_invitation_status_unknown_tag_collapses_to_unspecified() {
        // 99 is not a registered InvitationStatus value — protect against
        // future proto additions silently mapping to a wire variant that
        // doesn't exist yet.
        assert_eq!(proto_invitation_status_to_wire(99), w::InvitationStatus::Unspecified);
    }

    #[test]
    fn wire_invitation_status_round_trips_through_proto_i32() {
        for status in [
            w::InvitationStatus::Unspecified,
            w::InvitationStatus::Pending,
            w::InvitationStatus::Accepted,
            w::InvitationStatus::Declined,
            w::InvitationStatus::Expired,
            w::InvitationStatus::Revoked,
        ] {
            let tag = status as i32;
            assert_eq!(proto_invitation_status_to_wire(tag), status);
        }
    }

    // `proto_organization_member_role_to_wire` was deleted along with the
    // proto-bridge layer. The domain → wire `OrganizationMemberRole` mapping
    // happens directly inside `OrganizationService::team_to_wire` /
    // `member_to_wire` and is covered by the end-to-end wire handler tests.

    // End-to-end shim test (InvitationService instantiation against a live
    // `ServiceContext` / `MockProposalService`) is deferred to E.7
    // (TestCluster migration). The conversion helpers above are unit-tested
    // in isolation; tonic-status → WireError mapping is covered by
    // `wire_helpers::tests`.
}
