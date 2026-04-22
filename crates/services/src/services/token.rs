//! Token service implementation.
//!
//! Handles JWT lifecycle: user session creation, vault token creation,
//! token validation, refresh (rotate-on-use), revocation, and signing key
//! management. All write mutations flow through Raft for consistency;
//! validation and public key reads hit local applied state.

use std::sync::Arc;

use base64::Engine as _;
use chrono::Utc;
use inferadb_ledger_proto::proto::{
    self, AuthenticateClientAssertionRequest, AuthenticateClientAssertionResponse,
    CreateSigningKeyRequest, CreateSigningKeyResponse, CreateUserSessionRequest,
    CreateUserSessionResponse, CreateVaultTokenRequest, CreateVaultTokenResponse,
    GetPublicKeysRequest, GetPublicKeysResponse, PublicKeyInfo, RefreshTokenRequest,
    RefreshTokenResponse, RevokeAllAppSessionsRequest, RevokeAllAppSessionsResponse,
    RevokeAllUserSessionsRequest, RevokeAllUserSessionsResponse, RevokeSigningKeyRequest,
    RevokeSigningKeyResponse, RevokeTokenRequest, RevokeTokenResponse, RotateSigningKeyRequest,
    RotateSigningKeyResponse, ValidateTokenRequest, ValidateTokenResponse,
};
use inferadb_ledger_raft::{
    rate_limit::RateLimiter,
    types::{LedgerResponse, SystemRequest},
};
use inferadb_ledger_state::system::{
    App, AppVaultConnection, ClientAssertionEntry, SYSTEM_VAULT_ID, SigningKey, SigningKeyScope,
    SigningKeyStatus, SystemKeys,
};
use inferadb_ledger_store::crypto::RegionKeyManager;
use inferadb_ledger_types::{
    AppId, ClientAssertionId, OrganizationId as DomainOrganizationId, UserRole,
    VaultId as DomainVaultId,
    config::JwtConfig,
    decode,
    events::{EventAction, EventOutcome as EventOutcomeType},
    token::{TokenSubject, TokenType, ValidatedToken},
    types::AppSlug,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

use super::{error_classify, service_infra::ServiceContext, slug_resolver::SlugResolver};
use crate::{
    jwt::{JwtEngine, encrypt_private_key, generate_family_id, generate_refresh_token},
    proto_compat::datetime_to_proto,
};

/// Token lifecycle service.
///
/// Handles user sessions, vault tokens, refresh/revocation, and signing key
/// management. Embeds `ServiceContext` for shared Raft/state infrastructure.
pub struct TokenServiceImpl {
    ctx: ServiceContext,
    jwt_engine: Arc<JwtEngine>,
    jwt_config: JwtConfig,
    key_manager: Arc<dyn RegionKeyManager>,
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl TokenServiceImpl {
    /// Creates a new `TokenServiceImpl`.
    pub(crate) fn new(
        ctx: ServiceContext,
        jwt_engine: Arc<JwtEngine>,
        jwt_config: JwtConfig,
        key_manager: Arc<dyn RegionKeyManager>,
    ) -> Self {
        Self { ctx, jwt_engine, jwt_config, key_manager, rate_limiter: None }
    }

    /// Adds per-organization rate limiting.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Builds a [`SlugResolver`] from the current applied state.
    fn resolver(&self) -> SlugResolver {
        SlugResolver::new(self.ctx.applied_state.clone())
    }

    /// Creates a `SystemOrganizationService` for direct state reads.
    fn system_service(
        &self,
    ) -> inferadb_ledger_state::system::SystemOrganizationService<inferadb_ledger_store::FileBackend>
    {
        inferadb_ledger_state::system::SystemOrganizationService::new(self.ctx.state.clone())
    }

    /// Ensures the signing key identified by `kid` is loaded in the JwtEngine cache.
    /// If not cached, reads from state and decrypts.
    fn ensure_key_cached(&self, key: &SigningKey) -> Result<(), Status> {
        if self.jwt_engine.has_cached_key(&key.kid) {
            return Ok(());
        }
        let scope = &key.scope;
        let region = crate::jwt::scope_to_region(scope, &self.system_service())
            .map_err(|e| error_classify::crypto_error(&e))?;
        let rmk = self
            .key_manager
            .rmk_by_version(region, key.rmk_version)
            .map_err(|e| error_classify::crypto_error(&e))?;
        self.jwt_engine.load_key(key, &rmk).map_err(|e| error_classify::crypto_error(&e))?;
        Ok(())
    }

    /// Generates a new Ed25519 keypair, encrypts the private key with the scope's RMK,
    /// and zeroizes the secret material. Returns `(kid, public_key_bytes, encrypted_private_key,
    /// rmk_version)`.
    fn generate_encrypted_keypair(
        &self,
        scope: &SigningKeyScope,
    ) -> Result<(String, Vec<u8>, Vec<u8>, u32), Status> {
        // Use Zeroizing wrapper to ensure secret material is wiped on all exit
        // paths (including early returns via `?`).
        let mut secret_bytes = zeroize::Zeroizing::new([0u8; 32]);
        rand::RngExt::fill(&mut rand::rng(), &mut *secret_bytes);
        let signing_key_dalek = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key_dalek.verifying_key().to_bytes().to_vec();
        drop(signing_key_dalek); // Triggers Zeroize on Drop (ed25519-dalek "zeroize" feature)
        let kid = uuid::Uuid::new_v4().to_string();

        let region = crate::jwt::scope_to_region(scope, &self.system_service())
            .map_err(|e| error_classify::crypto_error(&e))?;
        let rmk =
            self.key_manager.current_rmk(region).map_err(|e| error_classify::crypto_error(&e))?;
        let (envelope, rmk_version) = encrypt_private_key(secret_bytes.as_ref(), &kid, &rmk)
            .map_err(Self::jwt_error_to_status)?;

        let encrypted_private_key = envelope.to_bytes().to_vec();
        Ok((kid, public_key_bytes, encrypted_private_key, rmk_version))
    }

    /// Returns the active signing key for a scope, ensuring it's cached.
    fn active_key_for_scope(&self, scope: &SigningKeyScope) -> Result<SigningKey, Status> {
        let sys = self.system_service();
        let key = sys
            .get_active_signing_key(scope)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::failed_precondition("No active signing key for scope"))?;
        self.ensure_key_cached(&key)?;
        Ok(key)
    }

    /// Loads an app from state by organization and app ID.
    fn load_app(
        &self,
        org_id: DomainOrganizationId,
        app_id: inferadb_ledger_types::AppId,
    ) -> Result<App, Status> {
        super::helpers::load_app(&self.ctx.state, org_id, app_id)
    }

    /// Reads a vault connection from state.
    fn read_vault_connection(
        &self,
        org_id: DomainOrganizationId,
        app_id: inferadb_ledger_types::AppId,
        vault_id: DomainVaultId,
    ) -> Result<AppVaultConnection, Status> {
        super::helpers::read_vault_connection(
            &self.ctx.state,
            org_id,
            app_id,
            vault_id,
            Status::not_found("Vault connection not found"),
        )
    }

    /// Converts a domain `SigningKey` to a proto `PublicKeyInfo`.
    fn signing_key_to_public_info(key: &SigningKey) -> PublicKeyInfo {
        let status = match key.status {
            SigningKeyStatus::Active => "active",
            SigningKeyStatus::Rotated => "rotated",
            SigningKeyStatus::Revoked => "revoked",
        };
        PublicKeyInfo {
            kid: key.kid.clone(),
            public_key: key.public_key_bytes.clone(),
            status: status.to_string(),
            valid_from: Some(datetime_to_proto(&key.valid_from)),
            valid_until: key.valid_until.as_ref().map(datetime_to_proto),
            created_at: Some(datetime_to_proto(&key.created_at)),
        }
    }

    /// Maps a `JwtError` to a gRPC `Status` following the error mapping table.
    fn jwt_error_to_status(err: crate::jwt::JwtError) -> Status {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        // All token-validation failures return a unified unauthenticated
        // response. The specific reason is logged but not exposed, preventing
        // attackers from probing token/key/claim state.
        match &err {
            JwtError::Token { source, .. } => {
                use super::auth_errors::{AuthFailureReason, unified_auth_error};
                let reason = match source {
                    TokenError::Expired => AuthFailureReason::SigningKeyExpired,
                    TokenError::InvalidSignature => AuthFailureReason::ClientAssertionInvalid,
                    TokenError::InvalidAudience { .. } => AuthFailureReason::ClientAssertionInvalid,
                    TokenError::MissingClaim { .. } => AuthFailureReason::ClientAssertionInvalid,
                    TokenError::InvalidTokenType { .. } => {
                        AuthFailureReason::ClientAssertionInvalid
                    },
                    TokenError::SigningKeyNotFound { .. } => AuthFailureReason::SigningKeyRevoked,
                    TokenError::SigningKeyExpired { .. } => AuthFailureReason::SigningKeyExpired,
                };
                tracing::debug!(token_error = ?source, "JWT validation failed");
                unified_auth_error(reason)
            },
            JwtError::Signing { .. } | JwtError::KeyEncryption | JwtError::KeyDecryption => {
                tracing::error!(error = %err, "Internal JWT error");
                Status::internal("Internal token processing error")
            },
            JwtError::Decoding { .. } => super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::ClientAssertionInvalid,
            ),
            JwtError::StateLookup { .. } => {
                tracing::error!(error = %err, "State lookup failure during JWT validation");
                Status::internal("Internal token processing error")
            },
        }
    }

    /// Hashes a refresh token string with SHA-256.
    fn hash_refresh_token(token: &str) -> [u8; 32] {
        Sha256::digest(token.as_bytes()).into()
    }
}

// =============================================================================
// TokenService gRPC trait implementation
// =============================================================================

#[tonic::async_trait]
impl proto::token_service_server::TokenService for TokenServiceImpl {
    /// Creates a user session token pair.
    ///
    /// Forwarded to leader for consistent `TokenVersion` and signing key reads.
    async fn create_user_session(
        &self,
        request: Request<CreateUserSessionRequest>,
    ) -> Result<Response<CreateUserSessionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "create_user_session", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Resolve user slug → (UserId, UserSlug)
        let resolver = self.resolver();
        let user_slug = SlugResolver::extract_user_slug(&req.user)?;
        let user_id = resolver.resolve_user(user_slug)?;

        // Read user from state (on leader for consistency)
        let sys = self.system_service();
        let user = sys
            .get_user(user_id)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::not_found("User not found"))?;

        // Verify user is active (suspended/pending users cannot get session tokens)
        if user.status != inferadb_ledger_types::UserStatus::Active {
            return Err(Status::failed_precondition("User is not active"));
        }

        let role = match user.role {
            UserRole::Admin => "admin",
            UserRole::User => "user",
        };

        // Get active global signing key
        let signing_key = self.active_key_for_scope(&SigningKeyScope::Global)?;

        // Sign access token
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_user_session(user_slug, role, user.version, &signing_key.kid)
            .map_err(Self::jwt_error_to_status)?;

        // Generate refresh token
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Compute refresh expiry
        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.session_refresh_ttl_secs as i64);

        ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let token_pair = proto::TokenPair {
            access_token,
            refresh_token: refresh_token_str,
            access_expires_at: Some(datetime_to_proto(&access_expires_at)),
            refresh_expires_at: Some(datetime_to_proto(&refresh_expires_at)),
        };

        Ok(Response::new(CreateUserSessionResponse { tokens: Some(token_pair) }))
    }

    /// Validates an access token and returns parsed claims.
    ///
    /// Reads local applied state — not forwarded to leader.
    async fn validate_token(
        &self,
        request: Request<ValidateTokenRequest>,
    ) -> Result<Response<ValidateTokenResponse>, Status> {
        let req = request.into_inner();

        if req.token.is_empty() {
            return Err(Status::invalid_argument("token is required"));
        }
        if req.expected_audience.is_empty() {
            return Err(Status::invalid_argument("expected_audience is required"));
        }

        // Extract kid from header and ensure key is cached (load from state on miss).
        // This handles followers and nodes after restart where the cache is cold.
        // IMPORTANT: never cache revoked keys — tokens signed with revoked keys must
        // fail validation even if the key is still in state.
        if let Ok(kid) = JwtEngine::extract_kid(&req.token) {
            if self.jwt_engine.has_cached_key(&kid) {
                // Key is cached — verify it hasn't been revoked since caching.
                let sys = self.system_service();
                let key_result = sys.get_signing_key_by_kid(&kid);
                if let Ok(Some(key)) = key_result {
                    use inferadb_ledger_state::system::SigningKeyStatus;
                    match key.status {
                        SigningKeyStatus::Revoked => {
                            self.jwt_engine.evict_key(&kid);
                            return Err(super::auth_errors::unified_auth_error(
                                super::auth_errors::AuthFailureReason::SigningKeyRevoked,
                            ));
                        },
                        SigningKeyStatus::Rotated => {
                            // Check if the grace period has expired
                            if let Some(valid_until) = key.valid_until
                                && chrono::Utc::now() > valid_until
                            {
                                self.jwt_engine.evict_key(&kid);
                                return Err(super::auth_errors::unified_auth_error(
                                    super::auth_errors::AuthFailureReason::SigningKeyExpired,
                                ));
                            }
                        },
                        _ => {},
                    }
                }
            } else {
                // Key not cached — load from state, but only cache if Active or Rotated
                let sys = self.system_service();
                if let Ok(Some(key)) = sys.get_signing_key_by_kid(&kid) {
                    use inferadb_ledger_state::system::SigningKeyStatus;
                    if key.status == SigningKeyStatus::Revoked {
                        return Err(super::auth_errors::unified_auth_error(
                            super::auth_errors::AuthFailureReason::SigningKeyRevoked,
                        ));
                    }
                    if let Err(e) = self.ensure_key_cached(&key) {
                        tracing::warn!(kid = %kid, error = %e, "Failed to cache signing key during validation");
                    }
                }
            }
        }

        // Validate the JWT (signature, exp, nbf, iss, aud, claims)
        let validated = self
            .jwt_engine
            .validate(&req.token, &req.expected_audience)
            .map_err(Self::jwt_error_to_status)?;

        // For user sessions: check TokenVersion against current user state
        if let ValidatedToken::UserSession(ref claims) = validated {
            let resolver = self.resolver();
            let user_id = resolver.resolve_user(claims.user)?;
            let sys = self.system_service();
            let user = sys
                .get_user(user_id)
                .map_err(|e| error_classify::storage_error(&e))?
                .ok_or_else(|| {
                    super::auth_errors::unified_auth_error(
                        super::auth_errors::AuthFailureReason::UserNotFound,
                    )
                })?;

            // Defense-in-depth: reject suspended/deactivated users even if
            // TokenVersion hasn't been bumped yet.
            if user.status != inferadb_ledger_types::UserStatus::Active {
                return Err(super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::UserInactive,
                ));
            }

            if user.version != claims.version {
                return Err(super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::SessionInvalidated,
                ));
            }
        }

        let response: ValidateTokenResponse = validated.into();
        Ok(Response::new(response))
    }

    /// Creates a vault access token pair for an app.
    ///
    /// Forwarded to leader for consistent reads.
    async fn create_vault_token(
        &self,
        request: Request<CreateVaultTokenRequest>,
    ) -> Result<Response<CreateVaultTokenResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "create_vault_token", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Resolve slugs
        let resolver = self.resolver();
        let org_slug = SlugResolver::extract_slug(&req.organization)?;
        let org_id = resolver.resolve(org_slug)?;
        let app_slug = SlugResolver::extract_app_slug(&req.app)?;
        let (_, app_id) = resolver.resolve_app(app_slug)?;
        let vault_slug = SlugResolver::extract_vault_slug(&req.vault)?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        // Rate limit per app within the organization. Token requests don't carry
        // a client_id field (the upstream Engine handles caller identity), so we
        // use the resolved app_id as the per-client bucket key.
        let rate_limit_key = format!("app:{}", app_id.value());
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), &rate_limit_key, org_id)?;

        // Verify app exists and is enabled
        let app = self.load_app(org_id, app_id)?;
        if !app.enabled {
            return Err(Status::failed_precondition("App is disabled"));
        }

        // Verify vault connection exists
        let connection = self.read_vault_connection(org_id, app_id, vault_id)?;

        // Verify requested scopes are a subset of allowed
        for scope in &req.scopes {
            if !connection.allowed_scopes.contains(scope) {
                return Err(Status::permission_denied(format!(
                    "Scope '{scope}' not allowed for this app-vault connection"
                )));
            }
        }

        // Get active org signing key
        let signing_key = self.active_key_for_scope(&SigningKeyScope::Organization(org_id))?;

        // Sign vault access token
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_vault_token(org_slug, app_slug, vault_slug, &req.scopes, &signing_key.kid)
            .map_err(Self::jwt_error_to_status)?;

        // Generate refresh token
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.vault_refresh_ttl_secs as i64);

        ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let token_pair = proto::TokenPair {
            access_token,
            refresh_token: refresh_token_str,
            access_expires_at: Some(datetime_to_proto(&access_expires_at)),
            refresh_expires_at: Some(datetime_to_proto(&refresh_expires_at)),
        };

        Ok(Response::new(CreateVaultTokenResponse { tokens: Some(token_pair) }))
    }

    /// Refreshes an access token using a refresh token.
    ///
    /// Forwarded to leader. The state machine atomically validates and rotates.
    async fn refresh_token(
        &self,
        request: Request<RefreshTokenRequest>,
    ) -> Result<Response<RefreshTokenResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from("TokenService", "refresh_token", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        if req.refresh_token.is_empty() {
            return Err(Status::invalid_argument("refresh_token is required"));
        }

        // Hash the provided refresh token
        let old_hash = Self::hash_refresh_token(&req.refresh_token);

        // Look up the old refresh token to determine type/scope
        let sys = self.system_service();
        let old_token = sys
            .get_refresh_token_by_hash(&old_hash)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::InvalidRefreshToken,
                )
            })?;

        // Determine the signing key scope and expected_version
        let (scope, expected_version) = match old_token.token_type {
            TokenType::UserSession => {
                // For user session: read current TokenVersion
                let user_slug = match old_token.subject {
                    TokenSubject::User(s) => s,
                    TokenSubject::App(_) => {
                        return Err(Status::internal("User session token has App subject"));
                    },
                };
                let resolver = self.resolver();
                let user_id = resolver.resolve_user(user_slug)?;
                let user = sys
                    .get_user(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        super::auth_errors::unified_auth_error(
                            super::auth_errors::AuthFailureReason::UserNotFound,
                        )
                    })?;
                (SigningKeyScope::Global, Some(user.version))
            },
            TokenType::VaultAccess => {
                let org_id = old_token
                    .organization
                    .ok_or_else(|| Status::internal("Vault token missing organization"))?;
                (SigningKeyScope::Organization(org_id), None)
            },
        };

        // Get active signing key for the scope
        let signing_key = self.active_key_for_scope(&scope)?;

        // Generate new refresh token
        let (new_refresh_str, new_hash) = generate_refresh_token();

        // Determine TTL
        let ttl_secs = match old_token.token_type {
            TokenType::UserSession => self.jwt_config.session_refresh_ttl_secs,
            TokenType::VaultAccess => self.jwt_config.vault_refresh_ttl_secs,
        };

        // Propose UseRefreshToken through Raft
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Extract authoritative data from the Raft response
        let (raft_token_version, raft_allowed_scopes) = match response {
            LedgerResponse::RefreshTokenRotated { token_version, allowed_scopes, .. } => {
                (token_version, allowed_scopes)
            },
            LedgerResponse::Error { code, message } => {
                return Err(super::helpers::error_code_to_status(code, message));
            },
            other => {
                return Err(error_classify::raft_error(&other));
            },
        };

        // Sign new access token using authoritative state from Raft response
        let (access_token, access_expires_at) = match old_token.token_type {
            TokenType::UserSession => {
                let user_slug = match old_token.subject {
                    TokenSubject::User(s) => s,
                    TokenSubject::App(_) => {
                        return Err(Status::internal("User session token has App subject"));
                    },
                };
                let version = raft_token_version.ok_or_else(|| {
                    Status::internal("User session refresh missing token_version from Raft")
                })?;
                // Re-read user for role
                let resolver = self.resolver();
                let user_id = resolver.resolve_user(user_slug)?;
                let user = self
                    .system_service()
                    .get_user(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        super::auth_errors::unified_auth_error(
                            super::auth_errors::AuthFailureReason::UserNotFound,
                        )
                    })?;
                let role = match user.role {
                    UserRole::Admin => "admin",
                    UserRole::User => "user",
                };
                self.jwt_engine
                    .sign_user_session(user_slug, role, version, &signing_key.kid)
                    .map_err(Self::jwt_error_to_status)?
            },
            TokenType::VaultAccess => {
                let scopes = raft_allowed_scopes.ok_or_else(|| {
                    Status::internal("Vault token refresh missing allowed_scopes in Raft response")
                })?;
                if scopes.is_empty() {
                    return Err(Status::failed_precondition(
                        "No allowed scopes on vault connection",
                    ));
                }
                let app_slug = match old_token.subject {
                    TokenSubject::App(s) => s,
                    TokenSubject::User(_) => {
                        return Err(Status::internal("Vault token has User subject"));
                    },
                };
                let org_id = old_token
                    .organization
                    .ok_or_else(|| Status::internal("Vault token missing organization"))?;
                let vault_id =
                    old_token.vault.ok_or_else(|| Status::internal("Vault token missing vault"))?;
                // Resolve org slug
                let resolver = self.resolver();
                let org_slug = resolver
                    .resolve_slug(org_id)
                    .map_err(|_| Status::internal("Failed to resolve org slug"))?;
                // Resolve vault slug
                let vault_slug = resolver.resolve_vault_slug(vault_id)?;
                self.jwt_engine
                    .sign_vault_token(org_slug, app_slug, vault_slug, &scopes, &signing_key.kid)
                    .map_err(Self::jwt_error_to_status)?
            },
        };

        let refresh_expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs as i64);

        ctx.record_event(EventAction::TokenRefreshed, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let token_pair = proto::TokenPair {
            access_token,
            refresh_token: new_refresh_str,
            access_expires_at: Some(datetime_to_proto(&access_expires_at)),
            refresh_expires_at: Some(datetime_to_proto(&refresh_expires_at)),
        };

        Ok(Response::new(RefreshTokenResponse { tokens: Some(token_pair) }))
    }

    /// Revokes a refresh token and its entire family.
    ///
    /// Forwarded to leader.
    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from("TokenService", "revoke_token", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        if req.refresh_token.is_empty() {
            return Err(Status::invalid_argument("refresh_token is required"));
        }

        let hash = Self::hash_refresh_token(&req.refresh_token);

        // Look up the token to get the family
        let sys = self.system_service();
        let token = sys
            .get_refresh_token_by_hash(&hash)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::InvalidRefreshToken,
                )
            })?;

        // Propose RevokeTokenFamily through Raft
        self.ctx
            .propose_system_request(
                SystemRequest::RevokeTokenFamily { family: token.family },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        ctx.set_success();

        Ok(Response::new(RevokeTokenResponse {}))
    }

    /// Revokes all sessions for a user (increments token version).
    ///
    /// Forwarded to leader.
    async fn revoke_all_user_sessions(
        &self,
        request: Request<RevokeAllUserSessionsRequest>,
    ) -> Result<Response<RevokeAllUserSessionsResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from(
            "TokenService",
            "revoke_all_user_sessions",
            &request,
        );
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        let resolver = self.resolver();
        let user_slug = SlugResolver::extract_user_slug(&req.user)?;
        let user_id = resolver.resolve_user(user_slug)?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::RevokeAllUserSessions { user: user_id },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        let revoked_count = match response {
            LedgerResponse::AllUserSessionsRevoked { count, .. } => count,
            LedgerResponse::Error { code, message } => {
                return Err(super::helpers::error_code_to_status(code, message));
            },
            other => {
                return Err(error_classify::raft_error(&other));
            },
        };

        ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        ctx.set_success();

        Ok(Response::new(RevokeAllUserSessionsResponse { revoked_count }))
    }

    /// Revokes all sessions for an app (increments app token version).
    ///
    /// Forwarded to leader.
    async fn revoke_all_app_sessions(
        &self,
        request: Request<RevokeAllAppSessionsRequest>,
    ) -> Result<Response<RevokeAllAppSessionsResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "revoke_all_app_sessions", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let resolver = self.resolver();
        let app_slug = SlugResolver::extract_app_slug(&req.app)?;
        let (org_id, app_id) = resolver.resolve_app(app_slug)?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::RevokeAllAppSessions { organization: org_id, app: app_id },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        let revoked_count = match response {
            LedgerResponse::AllAppSessionsRevoked { count, .. } => count,
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                return Err(super::helpers::error_code_to_status(code, message));
            },
            other => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                tracing::error!(response = %other, "Unexpected Raft response for RevokeAllAppSessions");
                return Err(Status::internal("Unexpected response type"));
            },
        };

        ctx.record_event(EventAction::TokenRevoked, EventOutcomeType::Success, &[]);
        ctx.set_success();

        Ok(Response::new(RevokeAllAppSessionsResponse { revoked_count }))
    }

    /// Creates a new signing key for the given scope.
    async fn create_signing_key(
        &self,
        request: Request<CreateSigningKeyRequest>,
    ) -> Result<Response<CreateSigningKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "create_signing_key", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Parse scope
        let proto_scope = inferadb_ledger_proto::convert::signing_key_scope_from_i32(req.scope)?;
        let scope = match proto_scope {
            proto::SigningKeyScope::Global => SigningKeyScope::Global,
            proto::SigningKeyScope::Organization => {
                let resolver = self.resolver();
                let org_id = resolver.extract_and_resolve(&req.organization)?;
                SigningKeyScope::Organization(org_id)
            },
            proto::SigningKeyScope::Unspecified => {
                return Err(Status::invalid_argument("Signing key scope is required"));
            },
        };

        let (kid, public_key_bytes, encrypted_private_key, rmk_version) =
            self.generate_encrypted_keypair(&scope)?;

        // Propose CreateSigningKey through Raft
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Check for apply-level errors (e.g., active key already exists for scope)
        if let LedgerResponse::Error { code, message } = response {
            return Err(super::helpers::error_code_to_status(code, message));
        }

        // Load key from state for cache and response (authoritative timestamps)
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::internal("Signing key not found after creation"))?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(kid = %kid, error = %e, "Failed to cache signing key after creation");
        }

        ctx.record_event(EventAction::SigningKeyCreated, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let info = Self::signing_key_to_public_info(&stored_key);
        Ok(Response::new(CreateSigningKeyResponse { key: Some(info) }))
    }

    /// Rotates a signing key: creates a replacement and marks the old key as rotated.
    async fn rotate_signing_key(
        &self,
        request: Request<RotateSigningKeyRequest>,
    ) -> Result<Response<RotateSigningKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "rotate_signing_key", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        if req.kid.is_empty() {
            return Err(Status::invalid_argument("kid is required"));
        }

        // Look up old key
        let sys = self.system_service();
        let old_key = sys
            .get_signing_key_by_kid(&req.kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::not_found("Signing key not found"))?;

        if old_key.status != SigningKeyStatus::Active {
            return Err(Status::failed_precondition("Can only rotate an active key"));
        }

        let (new_kid, new_public_key_bytes, new_encrypted_private_key, rmk_version) =
            self.generate_encrypted_keypair(&old_key.scope)?;

        // force_revoke=true skips the grace period entirely (immediate revocation).
        // Otherwise, 0 means "use default from JwtConfig" (proto convention for unset).
        let grace_period_secs = if req.force_revoke {
            0 // Apply handler treats 0 as immediate Revoked status
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Load new key from state for cache and response (authoritative timestamps)
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&new_kid)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| Status::internal("Signing key not found after rotation"))?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(kid = %new_kid, error = %e, "Failed to cache signing key after rotation");
        }

        // If effective grace period is 0 (immediate revocation), evict old key from cache
        if grace_period_secs == 0 {
            self.jwt_engine.evict_key(&req.kid);
        }

        ctx.record_event(EventAction::SigningKeyRotated, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let info = Self::signing_key_to_public_info(&stored_key);
        Ok(Response::new(RotateSigningKeyResponse { new_key: Some(info) }))
    }

    /// Revokes a signing key immediately (no grace period).
    async fn revoke_signing_key(
        &self,
        request: Request<RevokeSigningKeyRequest>,
    ) -> Result<Response<RevokeSigningKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx =
            self.ctx.make_request_context_from("TokenService", "revoke_signing_key", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        super::helpers::extract_caller(&mut ctx, &req.caller);

        if req.kid.is_empty() {
            return Err(Status::invalid_argument("kid is required"));
        }

        self.ctx
            .propose_system_request(
                SystemRequest::RevokeSigningKey { kid: req.kid.clone() },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        // Evict from cache
        self.jwt_engine.evict_key(&req.kid);

        ctx.record_event(EventAction::SigningKeyRevoked, EventOutcomeType::Success, &[]);
        ctx.set_success();

        Ok(Response::new(RevokeSigningKeyResponse {}))
    }

    /// Gets active public keys for token verification (JWKS-style).
    ///
    /// Reads local applied state — not forwarded to leader.
    async fn get_public_keys(
        &self,
        request: Request<GetPublicKeysRequest>,
    ) -> Result<Response<GetPublicKeysResponse>, Status> {
        let req = request.into_inner();

        let scope = if req.organization.is_some() {
            let resolver = self.resolver();
            let org_slug = SlugResolver::extract_slug(&req.organization)?;
            let org_id = resolver.resolve(org_slug)?;
            SigningKeyScope::Organization(org_id)
        } else {
            SigningKeyScope::Global
        };

        let sys = self.system_service();
        let keys = sys.list_signing_keys(&scope).map_err(|e| error_classify::storage_error(&e))?;

        // Filter to Active + Rotated (exclude Revoked)
        let keys: Vec<PublicKeyInfo> = keys
            .iter()
            .filter(|k| k.status != SigningKeyStatus::Revoked)
            .map(Self::signing_key_to_public_info)
            .collect();

        Ok(Response::new(GetPublicKeysResponse { keys }))
    }

    /// Authenticates a client assertion JWT and returns a vault access token pair.
    ///
    /// The assertion JWT is signed by the client app using its registered Ed25519
    /// private key. Ledger verifies the signature against the app's stored public
    /// key, validates claims, and issues a scoped vault token if authorized.
    async fn authenticate_client_assertion(
        &self,
        request: Request<AuthenticateClientAssertionRequest>,
    ) -> Result<Response<AuthenticateClientAssertionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut ctx = self.ctx.make_request_context_from(
            "TokenService",
            "authenticate_client_assertion",
            &request,
        );
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        if req.assertion_jwt.is_empty() {
            return Err(Status::invalid_argument("assertion_jwt is required"));
        }

        // Resolve organization and vault slugs
        let resolver = self.resolver();
        let org_slug = SlugResolver::extract_slug(&req.organization)?;
        let org_id = resolver.resolve(org_slug)?;
        let vault_slug = SlugResolver::extract_vault_slug(&req.vault)?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        // Rate limit per organization for assertion auth
        let rate_limit_key = format!("assertion_auth:{}", org_id.value());
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), &rate_limit_key, org_id)?;

        // Parse assertion JWT header to extract kid (assertion ID) and verify alg
        let (kid_str, assertion_id) = Self::parse_assertion_header(&req.assertion_jwt)?;

        // Parse unverified payload to extract iss (app slug) for app lookup
        let app_slug = Self::extract_issuer_from_jwt(&req.assertion_jwt)?;

        // Resolve the app from the issuer claim.
        // All auth failures below return unified_auth_error to prevent
        // enumeration of org/app/assertion state.
        let (resolved_org, app_id) = resolver.resolve_app(app_slug)?;
        if org_id != resolved_org {
            return Err(super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::AppNotFound,
            ));
        }

        // Load the app and verify it is enabled.
        let app = self.load_app(org_id, app_id)?;
        if !app.enabled {
            return Err(super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::AppNotFound,
            ));
        }

        // Verify client assertion authentication is enabled for this app.
        if !app.credentials.client_assertion.enabled {
            return Err(super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::ClientAssertionDisabled,
            ));
        }

        // Look up the specific assertion entry by kid.
        let entry = self.load_assertion_entry(org_id, app_id, assertion_id)?;

        if !entry.enabled {
            return Err(super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::ClientAssertionDisabled,
            ));
        }

        // Check assertion entry expiry.
        if entry.expires_at < Utc::now() {
            return Err(super::auth_errors::unified_auth_error(
                super::auth_errors::AuthFailureReason::ClientAssertionExpired,
            ));
        }

        // Verify JWT signature and validate claims against the assertion's public key
        Self::verify_assertion_jwt(
            &req.assertion_jwt,
            &entry.public_key_bytes,
            app_slug,
            &self.jwt_config.issuer,
            &kid_str,
        )?;

        // Verify vault connection exists and scopes are allowed
        let connection = self.read_vault_connection(org_id, app_id, vault_id)?;
        for scope in &req.scopes {
            if !connection.allowed_scopes.contains(scope) {
                return Err(Status::permission_denied(format!(
                    "Scope '{scope}' not allowed for this app-vault connection"
                )));
            }
        }

        // Get active org signing key
        let signing_key = self.active_key_for_scope(&SigningKeyScope::Organization(org_id))?;

        // Sign vault access token
        let (access_token, access_expires_at) = self
            .jwt_engine
            .sign_vault_token(org_slug, app_slug, vault_slug, &req.scopes, &signing_key.kid)
            .map_err(Self::jwt_error_to_status)?;

        // Generate refresh token
        let (refresh_token_str, refresh_token_hash) = generate_refresh_token();
        let family = generate_family_id();

        // Propose CreateRefreshToken through Raft
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
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.vault_refresh_ttl_secs as i64);

        ctx.record_event(EventAction::TokenCreated, EventOutcomeType::Success, &[]);
        ctx.set_success();

        let token_pair = proto::TokenPair {
            access_token,
            refresh_token: refresh_token_str,
            access_expires_at: Some(datetime_to_proto(&access_expires_at)),
            refresh_expires_at: Some(datetime_to_proto(&refresh_expires_at)),
        };

        Ok(Response::new(AuthenticateClientAssertionResponse { tokens: Some(token_pair) }))
    }
}

impl TokenServiceImpl {
    /// Parses the assertion JWT header to extract the `kid` (assertion ID) and
    /// validates the algorithm is `EdDSA`.
    ///
    /// Returns `(kid_string, ClientAssertionId)` on success.
    ///
    /// All parse errors collapse to a single `unified_auth_error` to prevent
    /// probing JWT structure. The specific reason is logged via the
    /// `unified_auth_error` helper.
    fn parse_assertion_header(token: &str) -> Result<(String, ClientAssertionId), Status> {
        use super::auth_errors::{AuthFailureReason, unified_auth_error};

        let invalid = || unified_auth_error(AuthFailureReason::ClientAssertionInvalid);

        let header_part = token.split('.').next().ok_or_else(invalid)?;

        let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(header_part)
            .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(header_part))
            .map_err(|_| invalid())?;

        let header: serde_json::Value =
            serde_json::from_slice(&header_bytes).map_err(|_| invalid())?;

        // Reject any algorithm other than EdDSA.
        let alg = header.get("alg").and_then(|v| v.as_str()).ok_or_else(invalid)?;
        if alg != "EdDSA" {
            tracing::debug!(alg, "Assertion JWT rejected: unsupported algorithm");
            return Err(invalid());
        }

        let kid_str = header.get("kid").and_then(|v| v.as_str()).ok_or_else(invalid)?;

        let kid_i64: i64 = kid_str.parse().map_err(|_| invalid())?;

        Ok((kid_str.to_string(), ClientAssertionId::new(kid_i64)))
    }

    /// Extracts the `iss` (issuer) claim from an unverified JWT payload and
    /// parses it as an `AppSlug`. All parse errors collapse to a single
    /// `unified_auth_error` response.
    fn extract_issuer_from_jwt(token: &str) -> Result<AppSlug, Status> {
        use super::auth_errors::{AuthFailureReason, unified_auth_error};

        let invalid = || unified_auth_error(AuthFailureReason::ClientAssertionInvalid);

        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(invalid());
        }

        let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(parts[1]))
            .map_err(|_| invalid())?;

        let payload: serde_json::Value =
            serde_json::from_slice(&payload_bytes).map_err(|_| invalid())?;

        let iss = payload.get("iss").and_then(|v| v.as_str()).ok_or_else(invalid)?;

        let slug_u64: u64 = iss.parse().map_err(|_| invalid())?;

        Ok(AppSlug::new(slug_u64))
    }

    /// Loads a `ClientAssertionEntry` from state by organization, app, and assertion ID.
    fn load_assertion_entry(
        &self,
        org_id: DomainOrganizationId,
        app_id: AppId,
        assertion_id: ClientAssertionId,
    ) -> Result<ClientAssertionEntry, Status> {
        let key = SystemKeys::app_assertion_key(org_id, app_id, assertion_id);
        let entity = self
            .ctx
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::ClientAssertionUnknown,
                )
            })?;

        decode::<ClientAssertionEntry>(&entity.value)
            .map_err(|e| error_classify::serialization_error(&e))
    }

    /// Verifies the assertion JWT signature and validates standard claims.
    ///
    /// Checks:
    /// - Signature using the assertion entry's Ed25519 public key
    /// - `iss` matches the app slug
    /// - `aud` matches the ledger issuer (ledger is the intended audience)
    /// - `exp` is not in the past
    fn verify_assertion_jwt(
        token: &str,
        public_key_bytes: &[u8],
        expected_app_slug: AppSlug,
        ledger_issuer: &str,
        expected_kid: &str,
    ) -> Result<(), Status> {
        let decoding_key = DecodingKey::from_ed_der(public_key_bytes);

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&expected_app_slug.value().to_string()]);
        validation.set_audience(&[ledger_issuer]);
        // 5 seconds clock skew tolerance. Server-to-server clocks should be
        // NTP-synchronized; a larger window allows replay near expiry.
        validation.leeway = 5;
        // Enforce "not before" — reject tokens whose nbf is in the future.
        validation.validate_nbf = true;
        // Require nbf claim presence. Without this, absent nbf bypasses the
        // validate_nbf check entirely (jsonwebtoken treats missing nbf as valid).
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud"]);

        let token_data = jsonwebtoken::decode::<serde_json::Value>(
            token,
            &decoding_key,
            &validation,
        )
        .map_err(|e| {
            tracing::debug!(error = %e, kid = %expected_kid, "Assertion JWT verification failed");
            Status::unauthenticated("Invalid client assertion")
        })?;

        // Defense-in-depth: verify sub is present (optional but logged)
        if token_data.claims.get("sub").is_none() {
            tracing::debug!(kid = %expected_kid, "Assertion JWT missing sub claim");
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn hash_refresh_token_deterministic() {
        let hash1 = TokenServiceImpl::hash_refresh_token("ilrt_test_token_123");
        let hash2 = TokenServiceImpl::hash_refresh_token("ilrt_test_token_123");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn hash_refresh_token_different_inputs() {
        let hash1 = TokenServiceImpl::hash_refresh_token("ilrt_token_a");
        let hash2 = TokenServiceImpl::hash_refresh_token("ilrt_token_b");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn hash_refresh_token_is_32_bytes() {
        let hash = TokenServiceImpl::hash_refresh_token("ilrt_some_token");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn datetime_to_proto_roundtrip() {
        let dt = Utc::now();
        let proto = datetime_to_proto(&dt);
        assert_eq!(proto.seconds, dt.timestamp());
        assert_eq!(proto.nanos, dt.timestamp_subsec_nanos() as i32);
    }

    #[test]
    fn signing_key_to_public_info_active() {
        let key = SigningKey {
            id: inferadb_ledger_types::SigningKeyId::new(1),
            kid: "test-kid".to_string(),
            public_key_bytes: vec![1, 2, 3],
            encrypted_private_key: vec![],
            rmk_version: 1,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Active,
            valid_from: Utc::now(),
            valid_until: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: None,
        };
        let info = TokenServiceImpl::signing_key_to_public_info(&key);
        assert_eq!(info.kid, "test-kid");
        assert_eq!(info.public_key, vec![1, 2, 3]);
        assert_eq!(info.status, "active");
        assert!(info.valid_until.is_none());
    }

    #[test]
    fn signing_key_to_public_info_rotated() {
        let valid_until = Utc::now() + chrono::Duration::hours(4);
        let key = SigningKey {
            id: inferadb_ledger_types::SigningKeyId::new(2),
            kid: "rotated-kid".to_string(),
            public_key_bytes: vec![4, 5, 6],
            encrypted_private_key: vec![],
            rmk_version: 1,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Rotated,
            valid_from: Utc::now(),
            valid_until: Some(valid_until),
            created_at: Utc::now(),
            rotated_at: Some(Utc::now()),
            revoked_at: None,
        };
        let info = TokenServiceImpl::signing_key_to_public_info(&key);
        assert_eq!(info.status, "rotated");
        assert!(info.valid_until.is_some());
    }

    #[test]
    fn signing_key_to_public_info_revoked() {
        let key = SigningKey {
            id: inferadb_ledger_types::SigningKeyId::new(3),
            kid: "revoked-kid".to_string(),
            public_key_bytes: vec![7, 8, 9],
            encrypted_private_key: vec![],
            rmk_version: 1,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Revoked,
            valid_from: Utc::now(),
            valid_until: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: Some(Utc::now()),
        };
        let info = TokenServiceImpl::signing_key_to_public_info(&key);
        assert_eq!(info.status, "revoked");
    }

    #[test]
    fn jwt_error_to_status_expired() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token { source: TokenError::Expired, location: snafu::location!() };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn jwt_error_to_status_invalid_audience() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::InvalidAudience { expected: "test".to_string() },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        // Unified auth error: invalid audience cannot be distinguished from
        // other JWT validation failures.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn jwt_error_to_status_missing_claim() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::MissingClaim { claim: "sub".to_string() },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        // Unified auth error: missing claim cannot be distinguished.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn jwt_error_to_status_key_encryption() {
        let err = crate::jwt::JwtError::KeyEncryption;
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn jwt_error_to_status_key_decryption() {
        let err = crate::jwt::JwtError::KeyDecryption;
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn jwt_error_to_status_signing() {
        let err = crate::jwt::JwtError::Signing {
            source: jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidKeyFormat,
            ),
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn jwt_error_to_status_decoding() {
        let err = crate::jwt::JwtError::Decoding {
            source: jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidToken,
            ),
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn jwt_error_to_status_invalid_token_type() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::InvalidTokenType { expected: "access".to_string() },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        // Unified auth error — expected token type is not leaked.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn jwt_error_to_status_signing_key_not_found() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::SigningKeyNotFound { kid: "missing-kid".to_string() },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        // Unified auth error — "signing key not found" is indistinguishable
        // from other auth failures to prevent kid enumeration.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn jwt_error_to_status_signing_key_expired() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::SigningKeyExpired { kid: "old-kid".to_string() },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        // Unified auth error.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn jwt_error_to_status_invalid_signature() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err =
            JwtError::Token { source: TokenError::InvalidSignature, location: snafu::location!() };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    // =========================================================================
    // parse_assertion_header tests
    // =========================================================================

    /// Encodes a JSON header as base64url (no padding).
    fn encode_jwt_part(json: &serde_json::Value) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(json).unwrap())
    }

    #[test]
    fn parse_assertion_header_valid_eddsa() {
        let header = serde_json::json!({ "alg": "EdDSA", "kid": "123" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let (kid, assertion_id) = TokenServiceImpl::parse_assertion_header(&token).unwrap();
        assert_eq!(kid, "123");
        assert_eq!(assertion_id.value(), 123);
    }

    #[test]
    fn parse_assertion_header_rejects_non_eddsa_algorithm() {
        let header = serde_json::json!({ "alg": "RS256", "kid": "123" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let status = TokenServiceImpl::parse_assertion_header(&token).unwrap_err();
        // Unified auth error: the rejection reason is not leaked.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn parse_assertion_header_rejects_missing_alg() {
        let header = serde_json::json!({ "kid": "123" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let status = TokenServiceImpl::parse_assertion_header(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn parse_assertion_header_rejects_missing_kid() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let status = TokenServiceImpl::parse_assertion_header(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn parse_assertion_header_rejects_non_numeric_kid() {
        let header = serde_json::json!({ "alg": "EdDSA", "kid": "not-a-number" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let status = TokenServiceImpl::parse_assertion_header(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn parse_assertion_header_rejects_invalid_base64() {
        let status = TokenServiceImpl::parse_assertion_header("!!!.payload.sig").unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn parse_assertion_header_rejects_no_dots() {
        // Token with no dot separators still has a "first part" that isn't valid base64 JSON
        let status = TokenServiceImpl::parse_assertion_header("nodotshere").unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    // =========================================================================
    // extract_issuer_from_jwt tests
    // =========================================================================

    #[test]
    fn extract_issuer_valid_jwt() {
        let header = serde_json::json!({ "alg": "EdDSA", "kid": "1" });
        let payload = serde_json::json!({ "iss": "12345", "sub": "test" });
        let token =
            format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload),);
        let slug = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap();
        assert_eq!(slug.value(), 12345);
    }

    #[test]
    fn extract_issuer_missing_iss_claim() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let payload = serde_json::json!({ "sub": "test" });
        let token =
            format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload),);
        let status = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap_err();
        // Unified auth error: missing iss is indistinguishable from other
        // parse failures, preventing JWT structure probing.
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn extract_issuer_non_numeric_iss() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let payload = serde_json::json!({ "iss": "not-a-number" });
        let token =
            format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload),);
        let status = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn extract_issuer_wrong_part_count() {
        let status = TokenServiceImpl::extract_issuer_from_jwt("only.two").unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        // Unified auth error: message is generic, not structural.
        assert_eq!(status.message(), "Authentication failed");
    }

    #[test]
    fn extract_issuer_invalid_payload_base64() {
        let header = encode_jwt_part(&serde_json::json!({ "alg": "EdDSA" }));
        let token = format!("{header}.!!!invalid!!!.signature");
        let status = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn extract_issuer_invalid_payload_json() {
        let header = encode_jwt_part(&serde_json::json!({ "alg": "EdDSA" }));
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"not json");
        let token = format!("{header}.{payload}.signature");
        let status = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    // =========================================================================
    // hash_refresh_token edge cases
    // =========================================================================

    #[test]
    fn hash_refresh_token_empty_string() {
        let hash = TokenServiceImpl::hash_refresh_token("");
        assert_eq!(hash.len(), 32);
        // SHA-256 of empty string is a well-known value
        let expected: [u8; 32] = Sha256::digest(b"").into();
        assert_eq!(hash, expected);
    }

    // =========================================================================
    // jwt_error_to_status — StateLookup variant
    // =========================================================================

    #[test]
    fn jwt_error_to_status_state_lookup() {
        let err = crate::jwt::JwtError::StateLookup {
            source: inferadb_ledger_state::system::SystemError::NotFound {
                entity: "signing key scope region".to_string(),
            },
            location: snafu::location!(),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    // =========================================================================
    // signing_key_to_public_info — edge cases
    // =========================================================================

    #[test]
    fn signing_key_to_public_info_has_valid_from_timestamp() {
        let now = Utc::now();
        let key = SigningKey {
            id: inferadb_ledger_types::SigningKeyId::new(1),
            kid: "time-kid".to_string(),
            public_key_bytes: vec![],
            encrypted_private_key: vec![],
            rmk_version: 1,
            scope: SigningKeyScope::Global,
            status: SigningKeyStatus::Active,
            valid_from: now,
            valid_until: None,
            created_at: now,
            rotated_at: None,
            revoked_at: None,
        };
        let info = TokenServiceImpl::signing_key_to_public_info(&key);
        assert!(info.valid_from.is_some());
        assert_eq!(info.valid_from.unwrap().seconds, now.timestamp());
        assert!(info.created_at.is_some());
        assert_eq!(info.created_at.unwrap().seconds, now.timestamp());
    }

    // =========================================================================
    // parse_assertion_header — additional edge cases
    // =========================================================================

    #[test]
    fn parse_assertion_header_rejects_empty_string() {
        let status = TokenServiceImpl::parse_assertion_header("").unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn parse_assertion_header_valid_large_kid() {
        let header = serde_json::json!({ "alg": "EdDSA", "kid": "9999999999" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let (kid, assertion_id) = TokenServiceImpl::parse_assertion_header(&token).unwrap();
        assert_eq!(kid, "9999999999");
        assert_eq!(assertion_id.value(), 9_999_999_999);
    }

    #[test]
    fn parse_assertion_header_negative_kid_parses_as_i64() {
        // Negative kid values parse as negative i64 — the downstream lookup will
        // simply fail to find a matching assertion entry.
        let header = serde_json::json!({ "alg": "EdDSA", "kid": "-1" });
        let token = format!("{}.payload.signature", encode_jwt_part(&header));
        let (kid, assertion_id) = TokenServiceImpl::parse_assertion_header(&token).unwrap();
        assert_eq!(kid, "-1");
        assert_eq!(assertion_id.value(), -1);
    }

    // =========================================================================
    // extract_issuer_from_jwt — additional edge cases
    // =========================================================================

    #[test]
    fn extract_issuer_zero_iss() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let payload = serde_json::json!({ "iss": "0" });
        let token = format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload));
        let slug = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap();
        assert_eq!(slug.value(), 0);
    }

    #[test]
    fn extract_issuer_large_numeric_iss() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let payload = serde_json::json!({ "iss": "18446744073709551615" });
        let token = format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload));
        let slug = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap();
        assert_eq!(slug.value(), u64::MAX);
    }

    #[test]
    fn extract_issuer_empty_iss_string() {
        let header = serde_json::json!({ "alg": "EdDSA" });
        let payload = serde_json::json!({ "iss": "" });
        let token = format!("{}.{}.signature", encode_jwt_part(&header), encode_jwt_part(&payload));
        let status = TokenServiceImpl::extract_issuer_from_jwt(&token).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    // =========================================================================
    // hash_refresh_token — additional edge cases
    // =========================================================================

    #[test]
    fn hash_refresh_token_long_input() {
        let long_token = "ilrt_".to_string() + &"a".repeat(1000);
        let hash = TokenServiceImpl::hash_refresh_token(&long_token);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn hash_refresh_token_special_chars() {
        let hash1 = TokenServiceImpl::hash_refresh_token("ilrt_abc+def/ghi=");
        let hash2 = TokenServiceImpl::hash_refresh_token("ilrt_abc+def/ghi=");
        assert_eq!(hash1, hash2);
    }
}
