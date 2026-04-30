//! Wire-protocol implementation for `TokenService` (Phase F.1.f.1.9).
//!
//! Provides the [`inferadb_ledger_wire_services::TokenService`] impl on
//! [`super::token::TokenServiceImpl`]. The wire impl operates on wire
//! types end-to-end — it does not delegate through the tonic impl. Each
//! handler body mirrors the corresponding tonic [`TokenServiceImpl`]
//! method one-for-one; both impls access the same `TokenServiceImpl`
//! private fields (made `pub(super)` for this purpose) and call into the
//! same `SlugResolver` / `JwtEngine` / `ProposalService` primitives.
//!
//! Helpers in this module that bridge wire ↔ proto representations are
//! preserved (and marked `#[allow(dead_code)]` where the wire impl no
//! longer routes through them) so the round-trip tests at the bottom of
//! the file continue to exercise the conversion shape — the tonic impl
//! still depends on those proto types in the meantime, and F.1.f.2
//! deletes both the tonic impl and the now-redundant helpers in the same
//! pass.
//!
//! `SlugResolver::*`, `helpers::*`, `error_classify::*`, and
//! `ServiceContext::propose_*` return [`WireError`] directly (F.1.f.0) —
//! no adapter required on the wire path. Helpers that still return
//! `tonic::Status` (F.1.f.0 gaps: `auth_errors::unified_auth_error`,
//! `JwtEngine` errors via `Self::jwt_error_to_status`, the proto-side
//! `signing_key_scope_from_i32`, and the assertion-parsing helpers on
//! `TokenServiceImpl`) are bridged inline via
//! [`tonic_status_to_wire_error`].
//!
//! Conversions are mechanical field-by-field copies because the wire
//! types in [`inferadb_ledger_wire::services::token`] mirror the proto
//! layout exactly (E.2). The non-trivial mappings:
//!
//! - `TokenPair.{access,refresh}_expires_at`, `PublicKeyInfo.{valid_from,valid_until,created_at}`,
//!   and `ValidateTokenResponse.expires_at` are `Option<prost_types::Timestamp>` in proto and `u64`
//!   (UNIX nanoseconds) on the wire — bridged by the shared helpers in [`super::wire_shared`].
//! - `PublicKeyInfo.public_key` is `Vec<u8>` in proto (32-byte Ed25519 public key) and `Bytes` on
//!   the wire.
//! - `SigningKeyScope` carries a raw `i32` in proto; the wire enum is `#[repr(i32)]` with matching
//!   numeric values, so we map by numeric tag and fall back to `Unspecified` for unknown tags.
//! - `ValidateTokenResponse.claims` is a proto oneof (`UserSession` / `VaultAccess`); the wire type
//!   is a Rust enum with the same two variants (`ValidateTokenClaims`).

use bytes::Bytes;
use chrono::Utc;
use inferadb_ledger_raft::types::{LedgerResponse, SystemRequest};
use inferadb_ledger_state::system::{SigningKeyScope as DomainSigningKeyScope, SigningKeyStatus};
use inferadb_ledger_types::{
    OrganizationSlug as DomainOrganizationSlug, UserRole, UserSlug as DomainUserSlug,
    VaultSlug as DomainVaultSlug,
    events::{EventAction, EventOutcome as EventOutcomeType},
    token::{TokenSubject, TokenType, ValidatedToken},
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{shared as ws, token as w},
};

use super::{
    auth_errors::{AuthFailureReason, unified_auth_error},
    error_classify, helpers,
    slug_resolver::SlugResolver,
    token::TokenServiceImpl,
    wire_helpers::tonic_status_to_wire_error,
};
use crate::jwt::{JwtEngine, generate_family_id, generate_refresh_token};

// ---------------------------------------------------------------------------
// SigningKeyScope conversion (numeric-tag mapping).
// ---------------------------------------------------------------------------

/// Numeric-tag mapping from proto `SigningKeyScope` (raw i32) to the wire enum.
///
/// Both enums declare `Unspecified = 0`, `Global = 1`, `Organization = 2`.
/// Unknown tags collapse to `Unspecified` to match proto's open-enum
/// semantics — protects against future proto additions.
#[cfg(test)]
fn proto_signing_key_scope_to_wire(value: i32) -> w::SigningKeyScope {
    match value {
        x if x == w::SigningKeyScope::Global as i32 => w::SigningKeyScope::Global,
        x if x == w::SigningKeyScope::Organization as i32 => w::SigningKeyScope::Organization,
        _ => w::SigningKeyScope::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// Helpers for converting domain `SigningKey` and `ValidatedToken` directly
// to wire types — used inline by the `TokenServiceImpl` wire impl below.
// ---------------------------------------------------------------------------

/// Converts a domain `SigningKey` to the wire `PublicKeyInfo`.
///
/// Mirrors `TokenServiceImpl::signing_key_to_public_info` (proto) but
/// produces the wire shape directly: timestamps in UNIX nanoseconds and
/// the public key as `Bytes` (rather than `Option<Timestamp>` and
/// `Vec<u8>`).
fn domain_signing_key_to_wire(key: &inferadb_ledger_state::system::SigningKey) -> w::PublicKeyInfo {
    let status = match key.status {
        SigningKeyStatus::Active => "active",
        SigningKeyStatus::Rotated => "rotated",
        SigningKeyStatus::Revoked => "revoked",
    };
    let valid_from_ns =
        u64::try_from(key.valid_from.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0);
    let valid_until_ns = key
        .valid_until
        .as_ref()
        .map(|dt| u64::try_from(dt.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0))
        .unwrap_or(0);
    let created_at_ns =
        u64::try_from(key.created_at.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0);
    w::PublicKeyInfo {
        kid: key.kid.clone(),
        public_key: Bytes::copy_from_slice(&key.public_key_bytes),
        status: status.to_owned(),
        valid_from: valid_from_ns,
        valid_until: valid_until_ns,
        created_at: created_at_ns,
    }
}

/// Converts a `ValidatedToken` directly to the wire `ValidateTokenResponse`.
///
/// Emits the wire shape with `expires_at` as UNIX nanoseconds and the
/// oneof flattened to a Rust enum.
fn validated_token_to_wire(validated: ValidatedToken) -> w::ValidateTokenResponse {
    match validated {
        ValidatedToken::UserSession(claims) => w::ValidateTokenResponse {
            subject: claims.sub.clone(),
            token_type: TokenType::UserSession.to_string(),
            // `claims.exp` is UNIX seconds; promote to ns.
            expires_at: u64::try_from(claims.exp.max(0)).unwrap_or(0).saturating_mul(1_000_000_000),
            claims: Some(w::ValidateTokenClaims::UserSession(w::UserSessionClaims {
                user_slug: claims.user.value(),
                role: claims.role.clone(),
            })),
        },
        ValidatedToken::VaultAccess(claims) => w::ValidateTokenResponse {
            subject: claims.sub.clone(),
            token_type: TokenType::VaultAccess.to_string(),
            expires_at: u64::try_from(claims.exp.max(0)).unwrap_or(0).saturating_mul(1_000_000_000),
            claims: Some(w::ValidateTokenClaims::VaultAccess(w::VaultAccessClaims {
                org_slug: claims.org.value(),
                app_slug: claims.app.value(),
                vault_slug: claims.vault.value(),
                scopes: claims.scopes.clone(),
            })),
        },
    }
}

/// Build a wire `TokenPair` from raw access/refresh strings + their expiries.
///
/// Wire timestamps are UNIX nanoseconds (the wire shape is non-optional
/// `u64`). The proto path materialises both via `datetime_to_proto`; the
/// wire path materialises via direct ns extraction here.
fn build_wire_token_pair(
    access_token: String,
    refresh_token: String,
    access_expires_at: chrono::DateTime<Utc>,
    refresh_expires_at: chrono::DateTime<Utc>,
) -> ws::TokenPair {
    let access_ns =
        u64::try_from(access_expires_at.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0);
    let refresh_ns =
        u64::try_from(refresh_expires_at.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0);
    ws::TokenPair {
        access_token,
        refresh_token,
        access_expires_at: access_ns,
        refresh_expires_at: refresh_ns,
    }
}

/// Maps the wire `SigningKeyScope` enum onto the domain
/// [`SigningKeyScope`](inferadb_ledger_state::system::SigningKeyScope).
///
/// Mirrors the proto-side `signing_key_scope_from_i32` discipline:
/// `Unspecified` is rejected; for `Organization` the caller must supply an
/// org slug, which is resolved to an `OrganizationId` here.
fn wire_signing_key_scope_to_domain(
    scope: w::SigningKeyScope,
    organization: Option<ws::OrganizationSlug>,
    resolver: &SlugResolver,
) -> Result<DomainSigningKeyScope, WireError> {
    match scope {
        w::SigningKeyScope::Global => Ok(DomainSigningKeyScope::Global),
        w::SigningKeyScope::Organization => {
            let org_slug = SlugResolver::extract_slug(
                organization.map(|s| DomainOrganizationSlug::new(s.value())),
            )?;
            let org_id = resolver.resolve(org_slug)?;
            Ok(DomainSigningKeyScope::Organization(org_id))
        },
        w::SigningKeyScope::Unspecified => {
            Err(WireError::new(ErrorCode::InvalidArgument, "Signing key scope is required"))
        },
    }
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `TokenServiceImpl`.
//
// Inlined directly on the wire types — no proto round-trip, no UFCS
// dispatch through the tonic impl. Each handler body mirrors the
// corresponding tonic [`TokenServiceImpl`] method in [`super::token`]
// one-for-one; both impls access the same `TokenServiceImpl` private
// fields (made `pub(super)` for this purpose) and call into the same
// `SlugResolver` / `JwtEngine` / `ProposalService` primitives.
//
// `SlugResolver::*`, `helpers::*`, `error_classify::*`, and
// `ServiceContext::propose_*` return `WireError` directly (F.1.f.0) — no
// adapter required on the wire path. Helpers that still return
// `tonic::Status` (`auth_errors::unified_auth_error`, `JwtEngine` errors
// via `Self::jwt_error_to_status`, the assertion-parsing helpers) are
// bridged with `tonic_status_to_wire_error`.
//
// Audit-emission preservation: every mutating RPC keeps its
// `log_ctx.record_event(...)` call site identical to the proto path so
// the AuditLogger pipeline is unchanged. Non-mutating reads
// (`validate_token`, `get_public_keys`) intentionally do not emit audit
// events on either path.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::TokenService for TokenServiceImpl {
    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `create_user_session` handler: resolves the user slug, reads the
    /// user from state on the leader, signs a session access token with
    /// the active GLOBAL signing key, and proposes
    /// `SystemRequest::CreateRefreshToken` through Raft.
    async fn create_user_session(
        &self,
        request: w::CreateUserSessionRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateUserSessionResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "create_user_session", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Resolve user slug → (UserId, UserSlug).
        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_slug = SlugResolver::extract_user_slug(
            req.user.as_ref().map(|s| DomainUserSlug::new(s.value())),
        )?;
        let user_id = resolver.resolve_user(user_slug)?;

        // Read user from state (on leader for consistency).
        let sys = self.system_service();
        let user = sys
            .get_user(user_id)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "User not found"))?;

        // Verify user is active (suspended/pending users cannot get session tokens).
        if user.status != inferadb_ledger_types::UserStatus::Active {
            return Err(WireError::new(ErrorCode::FailedPrecondition, "User is not active"));
        }

        let role = match user.role {
            UserRole::Admin => "admin",
            UserRole::User => "user",
        };

        // Get active global signing key.
        let signing_key = self
            .active_key_for_scope(&DomainSigningKeyScope::Global)
            .map_err(tonic_status_to_wire_error)?;

        // Sign access token.
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_user_session(user_slug, role, user.version, &signing_key.kid)
            .map_err(|e| tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e)))?;

        // Generate refresh token.
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft.
        self.ctx
            .propose_system_request(
                SystemRequest::CreateRefreshToken {
                    token_hash: refresh_token_hash,
                    family,
                    token_type: TokenType::UserSession,
                    subject: TokenSubject::User(user_slug),
                    organization: None,
                    vault: None,
                    kid: signing_key.kid.clone(),
                    ttl_secs: self.jwt_config.session_refresh_ttl_secs,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.session_refresh_ttl_secs as i64);

        log_ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::CreateUserSessionResponse {
            tokens: Some(build_wire_token_pair(
                access_token,
                refresh_token_str,
                access_expires_at,
                refresh_expires_at,
            )),
        })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `validate_token` handler: extracts the kid header, ensures the
    /// signing key is cached (loading from state on miss, evicting
    /// revoked / past-grace keys), validates the JWT, and for user
    /// session tokens cross-checks the `TokenVersion` against the user's
    /// current version. Reads local applied state — not forwarded to
    /// leader.
    async fn validate_token(
        &self,
        request: w::ValidateTokenRequest,
        _ctx: RequestContext,
    ) -> Result<w::ValidateTokenResponse, WireError> {
        let req = request;

        if req.token.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "token is required"));
        }
        if req.expected_audience.is_empty() {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "expected_audience is required",
            ));
        }

        // Extract kid from header and ensure key is cached (load from
        // state on miss). This handles followers and nodes after restart
        // where the cache is cold. IMPORTANT: never cache revoked keys —
        // tokens signed with revoked keys must fail validation even if
        // the key is still in state.
        if let Ok(kid) = JwtEngine::extract_kid(&req.token) {
            if self.jwt_engine.has_cached_key(&kid) {
                // Key is cached — verify it hasn't been revoked since caching.
                let sys = self.system_service();
                let key_result = sys.get_signing_key_by_kid(&kid);
                if let Ok(Some(key)) = key_result {
                    match key.status {
                        SigningKeyStatus::Revoked => {
                            self.jwt_engine.evict_key(&kid);
                            return Err(tonic_status_to_wire_error(unified_auth_error(
                                AuthFailureReason::SigningKeyRevoked,
                            )));
                        },
                        SigningKeyStatus::Rotated => {
                            // Check if the grace period has expired.
                            if let Some(valid_until) = key.valid_until
                                && chrono::Utc::now() > valid_until
                            {
                                self.jwt_engine.evict_key(&kid);
                                return Err(tonic_status_to_wire_error(unified_auth_error(
                                    AuthFailureReason::SigningKeyExpired,
                                )));
                            }
                        },
                        _ => {},
                    }
                }
            } else {
                // Key not cached — load from state, but only cache if Active or Rotated.
                let sys = self.system_service();
                if let Ok(Some(key)) = sys.get_signing_key_by_kid(&kid) {
                    if key.status == SigningKeyStatus::Revoked {
                        return Err(tonic_status_to_wire_error(unified_auth_error(
                            AuthFailureReason::SigningKeyRevoked,
                        )));
                    }
                    if let Err(e) = self.ensure_key_cached(&key) {
                        tracing::warn!(
                            kid = %kid,
                            error = ?e,
                            "Failed to cache signing key during validation"
                        );
                    }
                }
            }
        }

        // Validate the JWT (signature, exp, nbf, iss, aud, claims).
        let validated = self
            .jwt_engine
            .validate(&req.token, &req.expected_audience)
            .map_err(|e| tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e)))?;

        // For user sessions: check TokenVersion against current user state.
        if let ValidatedToken::UserSession(ref claims) = validated {
            let resolver = SlugResolver::new(self.ctx.applied_state.clone());
            let user_id = resolver.resolve_user(claims.user)?;
            let sys = self.system_service();
            let user = sys
                .get_user(user_id)
                .map_err(|e| error_classify::storage_error(&e))?
                .ok_or_else(|| {
                    tonic_status_to_wire_error(unified_auth_error(AuthFailureReason::UserNotFound))
                })?;

            // Defense-in-depth: reject suspended/deactivated users even
            // if TokenVersion hasn't been bumped yet.
            if user.status != inferadb_ledger_types::UserStatus::Active {
                return Err(tonic_status_to_wire_error(unified_auth_error(
                    AuthFailureReason::UserInactive,
                )));
            }

            if user.version != claims.version {
                return Err(tonic_status_to_wire_error(unified_auth_error(
                    AuthFailureReason::SessionInvalidated,
                )));
            }
        }

        Ok(validated_token_to_wire(validated))
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `create_vault_token` handler: resolves org/app/vault slugs, rate
    /// limits per app within the organization, verifies the app is
    /// enabled and the requested scopes are a subset of the connection's
    /// allowed scopes, signs a vault access token with the active org
    /// signing key, and proposes `SystemRequest::CreateRefreshToken`
    /// through Raft.
    async fn create_vault_token(
        &self,
        request: w::CreateVaultTokenRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateVaultTokenResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "create_vault_token", &_ctx);
        let req = request;

        // Resolve slugs.
        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_slug = SlugResolver::extract_slug(
            req.organization.map(|s| DomainOrganizationSlug::new(s.value())),
        )?;
        let org_id = resolver.resolve(org_slug)?;
        let app_slug = SlugResolver::extract_app_slug(
            req.app.map(|s| inferadb_ledger_types::AppSlug::new(s.value())),
        )?;
        let (_, app_id) = resolver.resolve_app(app_slug)?;
        let vault_slug =
            SlugResolver::extract_vault_slug(req.vault.map(|s| DomainVaultSlug::new(s.value())))?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        // Rate limit per app within the organization. Token requests
        // don't carry a client_id field (the upstream Engine handles
        // caller identity), so we use the resolved app_id as the per-
        // client bucket key.
        let rate_limit_key = format!("app:{}", app_id.value());
        helpers::check_rate_limit(self.rate_limiter.as_ref(), &rate_limit_key, org_id)?;

        // Verify app exists and is enabled.
        let app = self.load_app(org_id, app_id).map_err(tonic_status_to_wire_error)?;
        if !app.enabled {
            return Err(WireError::new(ErrorCode::FailedPrecondition, "App is disabled"));
        }

        // Verify vault connection exists.
        let connection = self
            .read_vault_connection(org_id, app_id, vault_id)
            .map_err(tonic_status_to_wire_error)?;

        // Verify requested scopes are a subset of allowed.
        for scope in &req.scopes {
            if !connection.allowed_scopes.contains(scope) {
                return Err(WireError::new(
                    ErrorCode::PermissionDenied,
                    format!("Scope '{scope}' not allowed for this app-vault connection"),
                ));
            }
        }

        // Get active org signing key.
        let signing_key = self
            .active_key_for_scope(&DomainSigningKeyScope::Organization(org_id))
            .map_err(tonic_status_to_wire_error)?;

        // Sign vault access token.
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_vault_token(org_slug, app_slug, vault_slug, &req.scopes, &signing_key.kid)
            .map_err(|e| tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e)))?;

        // Generate refresh token.
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft.
        self.ctx
            .propose_system_request(
                SystemRequest::CreateRefreshToken {
                    token_hash: refresh_token_hash,
                    family,
                    token_type: TokenType::VaultAccess,
                    subject: TokenSubject::App(app_slug),
                    organization: Some(org_id),
                    vault: Some(vault_id),
                    kid: signing_key.kid.clone(),
                    ttl_secs: self.jwt_config.vault_refresh_ttl_secs,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.vault_refresh_ttl_secs as i64);

        log_ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::CreateVaultTokenResponse {
            tokens: Some(build_wire_token_pair(
                access_token,
                refresh_token_str,
                access_expires_at,
                refresh_expires_at,
            )),
        })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `refresh_token` handler: looks up the old refresh token to
    /// determine its type/scope, proposes
    /// `SystemRequest::UseRefreshToken` through Raft (atomic
    /// validate-and-rotate), then signs a new access token using the
    /// authoritative state from the Raft response.
    async fn refresh_token(
        &self,
        request: w::RefreshTokenRequest,
        _ctx: RequestContext,
    ) -> Result<w::RefreshTokenResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "refresh_token", &_ctx);
        let req = request;

        if req.refresh_token.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "refresh_token is required"));
        }

        // Hash the provided refresh token.
        let old_hash = TokenServiceImpl::hash_refresh_token(&req.refresh_token);

        // Look up the old refresh token to determine type/scope.
        let sys = self.system_service();
        let old_token = sys
            .get_refresh_token_by_hash(&old_hash)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                tonic_status_to_wire_error(unified_auth_error(
                    AuthFailureReason::InvalidRefreshToken,
                ))
            })?;

        // Determine the signing key scope and expected_version.
        let (scope, expected_version) = match old_token.token_type {
            TokenType::UserSession => {
                // For user session: read current TokenVersion.
                let user_slug = match old_token.subject {
                    TokenSubject::User(s) => s,
                    TokenSubject::App(_) => {
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "User session token has App subject",
                        ));
                    },
                };
                let resolver = SlugResolver::new(self.ctx.applied_state.clone());
                let user_id = resolver.resolve_user(user_slug)?;
                let user = sys
                    .get_user(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        tonic_status_to_wire_error(unified_auth_error(
                            AuthFailureReason::UserNotFound,
                        ))
                    })?;
                (DomainSigningKeyScope::Global, Some(user.version))
            },
            TokenType::VaultAccess => {
                let org_id = old_token.organization.ok_or_else(|| {
                    WireError::new(ErrorCode::Internal, "Vault token missing organization")
                })?;
                (DomainSigningKeyScope::Organization(org_id), None)
            },
        };

        // Get active signing key for the scope.
        let signing_key = self.active_key_for_scope(&scope).map_err(tonic_status_to_wire_error)?;

        // Generate new refresh token.
        let (new_refresh_str, new_hash) = generate_refresh_token();

        // Determine TTL.
        let ttl_secs = match old_token.token_type {
            TokenType::UserSession => self.jwt_config.session_refresh_ttl_secs,
            TokenType::VaultAccess => self.jwt_config.vault_refresh_ttl_secs,
        };

        // Propose UseRefreshToken through Raft.
        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::UseRefreshToken {
                    old_token_hash: old_hash,
                    new_token_hash: new_hash,
                    new_kid: signing_key.kid.clone(),
                    ttl_secs,
                    expected_version,
                    max_family_lifetime_secs: self.jwt_config.max_family_lifetime_secs,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        // Extract authoritative data from the Raft response.
        let (raft_token_version, raft_allowed_scopes) = match response {
            LedgerResponse::RefreshTokenRotated { token_version, allowed_scopes, .. } => {
                (token_version, allowed_scopes)
            },
            LedgerResponse::Error { code, message } => {
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            other => {
                return Err(error_classify::raft_error(&other));
            },
        };

        // Sign new access token using authoritative state from Raft response.
        let (access_token, access_expires_at) = match old_token.token_type {
            TokenType::UserSession => {
                let user_slug = match old_token.subject {
                    TokenSubject::User(s) => s,
                    TokenSubject::App(_) => {
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "User session token has App subject",
                        ));
                    },
                };
                let version = raft_token_version.ok_or_else(|| {
                    WireError::new(
                        ErrorCode::Internal,
                        "User session refresh missing token_version from Raft",
                    )
                })?;
                // Re-read user for role.
                let resolver = SlugResolver::new(self.ctx.applied_state.clone());
                let user_id = resolver.resolve_user(user_slug)?;
                let user = self
                    .system_service()
                    .get_user(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        tonic_status_to_wire_error(unified_auth_error(
                            AuthFailureReason::UserNotFound,
                        ))
                    })?;
                let role = match user.role {
                    UserRole::Admin => "admin",
                    UserRole::User => "user",
                };
                self.jwt_engine
                    .sign_user_session(user_slug, role, version, &signing_key.kid)
                    .map_err(|e| {
                        tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e))
                    })?
            },
            TokenType::VaultAccess => {
                let scopes = raft_allowed_scopes.ok_or_else(|| {
                    WireError::new(
                        ErrorCode::Internal,
                        "Vault token refresh missing allowed_scopes in Raft response",
                    )
                })?;
                if scopes.is_empty() {
                    return Err(WireError::new(
                        ErrorCode::FailedPrecondition,
                        "No allowed scopes on vault connection",
                    ));
                }
                let app_slug = match old_token.subject {
                    TokenSubject::App(s) => s,
                    TokenSubject::User(_) => {
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "Vault token has User subject",
                        ));
                    },
                };
                let org_id = old_token.organization.ok_or_else(|| {
                    WireError::new(ErrorCode::Internal, "Vault token missing organization")
                })?;
                let vault_id = old_token.vault.ok_or_else(|| {
                    WireError::new(ErrorCode::Internal, "Vault token missing vault")
                })?;
                // Resolve org slug.
                let resolver = SlugResolver::new(self.ctx.applied_state.clone());
                let org_slug = resolver.resolve_slug(org_id).map_err(|_| {
                    WireError::new(ErrorCode::Internal, "Failed to resolve org slug")
                })?;
                // Resolve vault slug.
                let vault_slug = resolver.resolve_vault_slug(org_id, vault_id)?;
                self.jwt_engine
                    .sign_vault_token(org_slug, app_slug, vault_slug, &scopes, &signing_key.kid)
                    .map_err(|e| {
                        tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e))
                    })?
            },
        };

        let refresh_expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs as i64);

        log_ctx.record_event(EventAction::TokenRefreshed, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RefreshTokenResponse {
            tokens: Some(build_wire_token_pair(
                access_token,
                new_refresh_str,
                access_expires_at,
                refresh_expires_at,
            )),
        })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `revoke_token` handler: looks up the refresh token to discover
    /// its family, then proposes `SystemRequest::RevokeTokenFamily`
    /// through Raft to invalidate every token in the family.
    async fn revoke_token(
        &self,
        request: w::RevokeTokenRequest,
        _ctx: RequestContext,
    ) -> Result<w::RevokeTokenResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("TokenService", "revoke_token", &_ctx);
        let req = request;

        if req.refresh_token.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "refresh_token is required"));
        }

        let hash = TokenServiceImpl::hash_refresh_token(&req.refresh_token);

        // Look up the token to get the family.
        let sys = self.system_service();
        let token = sys
            .get_refresh_token_by_hash(&hash)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                tonic_status_to_wire_error(unified_auth_error(
                    AuthFailureReason::InvalidRefreshToken,
                ))
            })?;

        // Propose RevokeTokenFamily through Raft.
        self.ctx
            .propose_system_request(
                SystemRequest::RevokeTokenFamily { family: token.family },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        log_ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RevokeTokenResponse {})
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `revoke_all_user_sessions` handler: resolves the user slug and
    /// proposes `SystemRequest::RevokeAllUserSessions` through Raft
    /// (which atomically increments the user's `TokenVersion`).
    async fn revoke_all_user_sessions(
        &self,
        request: w::RevokeAllUserSessionsRequest,
        _ctx: RequestContext,
    ) -> Result<w::RevokeAllUserSessionsResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "revoke_all_user_sessions", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_slug = SlugResolver::extract_user_slug(
            req.user.as_ref().map(|s| DomainUserSlug::new(s.value())),
        )?;
        let user_id = resolver.resolve_user(user_slug)?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::RevokeAllUserSessions { user: user_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let revoked_count = match response {
            LedgerResponse::AllUserSessionsRevoked { count, .. } => count,
            LedgerResponse::Error { code, message } => {
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            other => {
                return Err(error_classify::raft_error(&other));
            },
        };

        log_ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RevokeAllUserSessionsResponse { revoked_count })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `revoke_all_app_sessions` handler: resolves the app slug to its
    /// `(org, app)` pair and proposes
    /// `SystemRequest::RevokeAllAppSessions` through Raft.
    async fn revoke_all_app_sessions(
        &self,
        request: w::RevokeAllAppSessionsRequest,
        _ctx: RequestContext,
    ) -> Result<w::RevokeAllAppSessionsResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "revoke_all_app_sessions", &_ctx);
        let req = request;

        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let app_slug = SlugResolver::extract_app_slug(
            req.app.as_ref().map(|s| inferadb_ledger_types::AppSlug::new(s.value())),
        )?;
        let (org_id, app_id) = resolver.resolve_app(app_slug)?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::RevokeAllAppSessions { organization: org_id, app: app_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let revoked_count = match response {
            LedgerResponse::AllAppSessionsRevoked { count, .. } => count,
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(
                    response = %other,
                    "Unexpected Raft response for RevokeAllAppSessions"
                );
                return Err(WireError::new(ErrorCode::Internal, "Unexpected response type"));
            },
        };

        log_ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RevokeAllAppSessionsResponse { revoked_count })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `create_signing_key` handler: parses the wire scope (rejecting
    /// `Unspecified`), generates an Ed25519 keypair encrypted with the
    /// scope's RMK, proposes `SystemRequest::CreateSigningKey` through
    /// Raft, then loads the new key from state for the response.
    async fn create_signing_key(
        &self,
        request: w::CreateSigningKeyRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateSigningKeyResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "create_signing_key", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Parse scope.
        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let scope = wire_signing_key_scope_to_domain(req.scope, req.organization, &resolver)?;

        let (kid, public_key_bytes, encrypted_private_key, rmk_version) =
            self.generate_encrypted_keypair(&scope).map_err(tonic_status_to_wire_error)?;

        // Propose CreateSigningKey through Raft.
        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::CreateSigningKey {
                    scope,
                    kid: kid.clone(),
                    public_key_bytes,
                    encrypted_private_key,
                    rmk_version,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        // Check for apply-level errors (e.g., active key already exists for scope).
        if let LedgerResponse::Error { code, message } = response {
            return Err(helpers::error_code_to_wire_error(code, message));
        }

        // Load key from state for cache and response (authoritative timestamps).
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                WireError::new(ErrorCode::Internal, "Signing key not found after creation")
            })?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(
                kid = %kid,
                error = ?e,
                "Failed to cache signing key after creation"
            );
        }

        log_ctx.record_event(EventAction::SigningKeyCreated, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::CreateSigningKeyResponse { key: Some(domain_signing_key_to_wire(&stored_key)) })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `rotate_signing_key` handler: looks up the old key, generates a
    /// replacement keypair, and proposes
    /// `SystemRequest::RotateSigningKey` (which atomically marks the old
    /// key as `Rotated` with the chosen grace period and inserts the
    /// new active key).
    async fn rotate_signing_key(
        &self,
        request: w::RotateSigningKeyRequest,
        _ctx: RequestContext,
    ) -> Result<w::RotateSigningKeyResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "rotate_signing_key", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        if req.kid.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "kid is required"));
        }

        // Look up old key.
        let sys = self.system_service();
        let old_key = sys
            .get_signing_key_by_kid(&req.kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Signing key not found"))?;

        if old_key.status != SigningKeyStatus::Active {
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "Can only rotate an active key",
            ));
        }

        let (new_kid, new_public_key_bytes, new_encrypted_private_key, rmk_version) =
            self.generate_encrypted_keypair(&old_key.scope).map_err(tonic_status_to_wire_error)?;

        // force_revoke=true skips the grace period entirely (immediate
        // revocation). Otherwise, 0 means "use default from JwtConfig"
        // (proto convention for unset).
        let grace_period_secs = if req.force_revoke {
            0 // Apply handler treats 0 as immediate Revoked status.
        } else if req.grace_period_secs == 0 {
            self.jwt_config.key_rotation_grace_secs
        } else {
            req.grace_period_secs
        };

        self.ctx
            .propose_system_request(
                SystemRequest::RotateSigningKey {
                    old_kid: req.kid.clone(),
                    new_kid: new_kid.clone(),
                    new_public_key_bytes,
                    new_encrypted_private_key,
                    rmk_version,
                    grace_period_secs,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        // Load new key from state for cache and response (authoritative timestamps).
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&new_kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                WireError::new(ErrorCode::Internal, "Signing key not found after rotation")
            })?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(
                kid = %new_kid,
                error = ?e,
                "Failed to cache signing key after rotation"
            );
        }

        // If effective grace period is 0 (immediate revocation), evict
        // old key from cache.
        if grace_period_secs == 0 {
            self.jwt_engine.evict_key(&req.kid);
        }

        log_ctx.record_event(EventAction::SigningKeyRotated, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RotateSigningKeyResponse { new_key: Some(domain_signing_key_to_wire(&stored_key)) })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `revoke_signing_key` handler: proposes
    /// `SystemRequest::RevokeSigningKey` through Raft (immediate, no
    /// grace period) and evicts the kid from the in-memory JWT cache.
    async fn revoke_signing_key(
        &self,
        request: w::RevokeSigningKeyRequest,
        _ctx: RequestContext,
    ) -> Result<w::RevokeSigningKeyResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("TokenService", "revoke_signing_key", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        if req.kid.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "kid is required"));
        }

        self.ctx
            .propose_system_request(
                SystemRequest::RevokeSigningKey { kid: req.kid.clone() },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        // Evict from cache.
        self.jwt_engine.evict_key(&req.kid);

        log_ctx.record_event(EventAction::SigningKeyRevoked, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::RevokeSigningKeyResponse {})
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `get_public_keys` handler: resolves the scope (Global if no org
    /// supplied, Organization otherwise), reads `SigningKey` records
    /// from local applied state, filters out revoked keys, and
    /// converts each surviving key directly to the wire shape.
    async fn get_public_keys(
        &self,
        request: w::GetPublicKeysRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetPublicKeysResponse, WireError> {
        let req = request;

        let scope = if req.organization.is_some() {
            let resolver = SlugResolver::new(self.ctx.applied_state.clone());
            let org_slug = SlugResolver::extract_slug(
                req.organization.map(|s| DomainOrganizationSlug::new(s.value())),
            )?;
            let org_id = resolver.resolve(org_slug)?;
            DomainSigningKeyScope::Organization(org_id)
        } else {
            DomainSigningKeyScope::Global
        };

        let sys = self.system_service();
        let keys = sys.list_signing_keys(&scope).map_err(|e| error_classify::storage_error(&e))?;

        // Filter to Active + Rotated (exclude Revoked).
        let keys: Vec<w::PublicKeyInfo> = keys
            .iter()
            .filter(|k| k.status != SigningKeyStatus::Revoked)
            .map(domain_signing_key_to_wire)
            .collect();

        Ok(w::GetPublicKeysResponse { keys })
    }

    /// Mirrors the tonic [`token`](super::token::TokenServiceImpl)
    /// `authenticate_client_assertion` handler: parses the assertion
    /// JWT header to extract `kid` (assertion id), looks up the issuing
    /// app and the specific assertion entry, verifies the JWT signature
    /// against the entry's public key, and on success signs a vault
    /// access token + proposes `SystemRequest::CreateRefreshToken`
    /// through Raft. Auth failures collapse to a single
    /// `unified_auth_error` to prevent enumeration of org / app /
    /// assertion state.
    async fn authenticate_client_assertion(
        &self,
        request: w::AuthenticateClientAssertionRequest,
        _ctx: RequestContext,
    ) -> Result<w::AuthenticateClientAssertionResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "TokenService",
            "authenticate_client_assertion",
            &_ctx,
        );
        let req = request;

        if req.assertion_jwt.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "assertion_jwt is required"));
        }

        // Resolve organization and vault slugs.
        let resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_slug = SlugResolver::extract_slug(
            req.organization.map(|s| DomainOrganizationSlug::new(s.value())),
        )?;
        let org_id = resolver.resolve(org_slug)?;
        let vault_slug =
            SlugResolver::extract_vault_slug(req.vault.map(|s| DomainVaultSlug::new(s.value())))?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        // Rate limit per organization for assertion auth.
        let rate_limit_key = format!("assertion_auth:{}", org_id.value());
        helpers::check_rate_limit(self.rate_limiter.as_ref(), &rate_limit_key, org_id)?;

        // Parse assertion JWT header to extract kid (assertion ID) and verify alg.
        let (kid_str, assertion_id) = TokenServiceImpl::parse_assertion_header(&req.assertion_jwt)
            .map_err(tonic_status_to_wire_error)?;

        // Parse unverified payload to extract iss (app slug) for app lookup.
        let app_slug = TokenServiceImpl::extract_issuer_from_jwt(&req.assertion_jwt)
            .map_err(tonic_status_to_wire_error)?;

        // Resolve the app from the issuer claim. All auth failures
        // below return unified_auth_error to prevent enumeration of
        // org/app/assertion state.
        let (resolved_org, app_id) = resolver.resolve_app(app_slug)?;
        if org_id != resolved_org {
            return Err(tonic_status_to_wire_error(unified_auth_error(
                AuthFailureReason::AppNotFound,
            )));
        }

        // Load the app and verify it is enabled.
        let app = self.load_app(org_id, app_id).map_err(tonic_status_to_wire_error)?;
        if !app.enabled {
            return Err(tonic_status_to_wire_error(unified_auth_error(
                AuthFailureReason::AppNotFound,
            )));
        }

        // Verify client assertion authentication is enabled for this app.
        if !app.credentials.client_assertion.enabled {
            return Err(tonic_status_to_wire_error(unified_auth_error(
                AuthFailureReason::ClientAssertionDisabled,
            )));
        }

        // Look up the specific assertion entry by kid.
        let entry = self
            .load_assertion_entry(org_id, app_id, assertion_id)
            .map_err(tonic_status_to_wire_error)?;

        if !entry.enabled {
            return Err(tonic_status_to_wire_error(unified_auth_error(
                AuthFailureReason::ClientAssertionDisabled,
            )));
        }

        // Check assertion entry expiry.
        if entry.expires_at < Utc::now() {
            return Err(tonic_status_to_wire_error(unified_auth_error(
                AuthFailureReason::ClientAssertionExpired,
            )));
        }

        // Verify JWT signature and validate claims against the assertion's public key.
        TokenServiceImpl::verify_assertion_jwt(
            &req.assertion_jwt,
            &entry.public_key_bytes,
            app_slug,
            &self.jwt_config.issuer,
            &kid_str,
        )
        .map_err(tonic_status_to_wire_error)?;

        // Verify vault connection exists and scopes are allowed.
        let connection = self
            .read_vault_connection(org_id, app_id, vault_id)
            .map_err(tonic_status_to_wire_error)?;
        for scope in &req.scopes {
            if !connection.allowed_scopes.contains(scope) {
                return Err(WireError::new(
                    ErrorCode::PermissionDenied,
                    format!("Scope '{scope}' not allowed for this app-vault connection"),
                ));
            }
        }

        // Get active org signing key.
        let signing_key = self
            .active_key_for_scope(&DomainSigningKeyScope::Organization(org_id))
            .map_err(tonic_status_to_wire_error)?;

        // Sign vault access token.
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_vault_token(org_slug, app_slug, vault_slug, &req.scopes, &signing_key.kid)
            .map_err(|e| tonic_status_to_wire_error(TokenServiceImpl::jwt_error_to_status(e)))?;

        // Generate refresh token.
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft.
        self.ctx
            .propose_system_request(
                SystemRequest::CreateRefreshToken {
                    token_hash: refresh_token_hash,
                    family,
                    token_type: TokenType::VaultAccess,
                    subject: TokenSubject::App(app_slug),
                    organization: Some(org_id),
                    vault: Some(vault_id),
                    kid: signing_key.kid.clone(),
                    ttl_secs: self.jwt_config.vault_refresh_ttl_secs,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.vault_refresh_ttl_secs as i64);

        log_ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        log_ctx.set_success();

        Ok(w::AuthenticateClientAssertionResponse {
            tokens: Some(build_wire_token_pair(
                access_token,
                refresh_token_str,
                access_expires_at,
                refresh_expires_at,
            )),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // SigningKeyScope enum tag mapping.
    // -----------------------------------------------------------------------

    #[test]
    fn proto_signing_key_scope_each_known_tag_maps_correctly() {
        assert_eq!(proto_signing_key_scope_to_wire(0), w::SigningKeyScope::Unspecified);
        assert_eq!(proto_signing_key_scope_to_wire(1), w::SigningKeyScope::Global);
        assert_eq!(proto_signing_key_scope_to_wire(2), w::SigningKeyScope::Organization);
    }

    #[test]
    fn proto_signing_key_scope_unknown_tag_collapses_to_unspecified() {
        assert_eq!(proto_signing_key_scope_to_wire(99), w::SigningKeyScope::Unspecified);
    }

    #[test]
    fn wire_signing_key_scope_round_trips_through_proto_i32() {
        for scope in [
            w::SigningKeyScope::Unspecified,
            w::SigningKeyScope::Global,
            w::SigningKeyScope::Organization,
        ] {
            let tag = scope as i32;
            assert_eq!(proto_signing_key_scope_to_wire(tag), scope);
        }
    }
}
