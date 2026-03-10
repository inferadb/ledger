//! Token service implementation.
//!
//! Handles JWT lifecycle: user session creation, vault token creation,
//! token validation, refresh (rotate-on-use), revocation, and signing key
//! management. All write mutations flow through Raft for consistency;
//! validation and public key reads hit local applied state.

use std::sync::Arc;

use chrono::Utc;
use inferadb_ledger_proto::proto::{
    self, CreateSigningKeyRequest, CreateSigningKeyResponse, CreateUserSessionRequest,
    CreateUserSessionResponse, CreateVaultTokenRequest, CreateVaultTokenResponse,
    GetPublicKeysRequest, GetPublicKeysResponse, PublicKeyInfo, RefreshTokenRequest,
    RefreshTokenResponse, RevokeAllUserSessionsRequest, RevokeAllUserSessionsResponse,
    RevokeSigningKeyRequest, RevokeSigningKeyResponse, RevokeTokenRequest, RevokeTokenResponse,
    RotateSigningKeyRequest, RotateSigningKeyResponse, ValidateTokenRequest, ValidateTokenResponse,
};
use inferadb_ledger_raft::{
    rate_limit::RateLimiter,
    trace_context,
    types::{LedgerRequest, LedgerResponse},
};
use inferadb_ledger_state::system::{
    App, AppVaultConnection, SigningKey, SigningKeyScope, SigningKeyStatus,
};
use inferadb_ledger_store::crypto::RegionKeyManager;
use inferadb_ledger_types::{
    OrganizationId as DomainOrganizationId, UserRole, VaultId as DomainVaultId,
    config::JwtConfig,
    events::{EventAction, EventOutcome as EventOutcomeType},
    token::{TokenSubject, TokenType, ValidatedToken},
};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

use super::{service_infra::ServiceContext, slug_resolver::SlugResolver};
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
            .map_err(|e| Status::internal(format!("Failed to resolve region for key: {e}")))?;
        let rmk = self
            .key_manager
            .rmk_by_version(region, key.rmk_version)
            .map_err(|e| Status::internal(format!("Failed to load RMK: {e}")))?;
        self.jwt_engine
            .load_key(key, &rmk)
            .map_err(|e| Status::internal(format!("Failed to load signing key: {e}")))?;
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
            .map_err(|e| Status::internal(format!("Failed to resolve region: {e}")))?;
        let rmk = self
            .key_manager
            .current_rmk(region)
            .map_err(|e| Status::internal(format!("Failed to load RMK: {e}")))?;
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
            .map_err(|e| Status::internal(format!("Failed to read signing key: {e}")))?
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

        match &err {
            JwtError::Token { source, .. } => match source {
                TokenError::Expired => Status::unauthenticated("Token expired"),
                TokenError::InvalidSignature => Status::unauthenticated("Invalid token"),
                TokenError::InvalidAudience { expected } => {
                    Status::permission_denied(format!("Invalid audience: expected {expected}"))
                },
                TokenError::MissingClaim { claim } => {
                    Status::invalid_argument(format!("Missing required claim: {claim}"))
                },
                TokenError::InvalidTokenType { expected } => {
                    Status::invalid_argument(format!("Invalid token type: expected {expected}"))
                },
                TokenError::SigningKeyNotFound { .. } => Status::not_found("Signing key not found"),
                TokenError::SigningKeyExpired { .. } => {
                    Status::failed_precondition("Signing key expired")
                },
            },
            JwtError::Signing { .. } | JwtError::KeyEncryption | JwtError::KeyDecryption => {
                Status::internal(err.to_string())
            },
            JwtError::Decoding { .. } => Status::unauthenticated("Invalid token"),
            JwtError::StateLookup { .. } => Status::internal(err.to_string()),
        }
    }

    /// Hashes a refresh token string with SHA-256.
    fn hash_refresh_token(token: &str) -> [u8; 32] {
        Sha256::digest(token.as_bytes()).into()
    }

    /// Emits an audit event if event recording is configured.
    fn emit_event(
        &self,
        action: EventAction,
        trace_ctx: &inferadb_ledger_raft::trace_context::TraceContext,
    ) {
        use inferadb_ledger_raft::event_writer::HandlerPhaseEmitter;

        if let Some(node_id) = self.ctx.node_id {
            self.ctx.record_handler_event(
                HandlerPhaseEmitter::for_system(action, node_id)
                    .principal("system")
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.ctx.default_ttl_days()),
            );
        }
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "create_user_session",
            &grpc_metadata,
            &trace_ctx,
        );

        // Resolve user slug → (UserId, UserSlug)
        let resolver = self.resolver();
        let user_slug = SlugResolver::extract_user_slug(&req.user)?;
        let user_id = resolver.resolve_user(user_slug)?;

        // Read user from state (on leader for consistency)
        let sys = self.system_service();
        let user = sys
            .get_user(user_id)
            .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
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
        let ledger_request = LedgerRequest::CreateRefreshToken {
            token_hash: refresh_token_hash,
            family,
            token_type: TokenType::UserSession,
            subject: TokenSubject::User(user_slug),
            organization: None,
            vault: None,
            kid: signing_key.kid.clone(),
            ttl_secs: self.jwt_config.session_refresh_ttl_secs,
        };

        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        // Compute refresh expiry
        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.session_refresh_ttl_secs as i64);

        self.emit_event(EventAction::TokenCreated, &trace_ctx);
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
        if let Ok(kid) = JwtEngine::extract_kid(&req.token)
            && !self.jwt_engine.has_cached_key(&kid)
        {
            let sys = self.system_service();
            if let Ok(Some(key)) = sys.get_signing_key_by_kid(&kid)
                && let Err(e) = self.ensure_key_cached(&key)
            {
                tracing::warn!(kid = %kid, error = %e, "Failed to cache signing key during validation");
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
                .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
                .ok_or_else(|| Status::unauthenticated("User not found"))?;

            // Defense-in-depth: reject suspended/deactivated users even if
            // TokenVersion hasn't been bumped yet.
            if user.status != inferadb_ledger_types::UserStatus::Active {
                return Err(Status::unauthenticated("User is not active"));
            }

            if user.version != claims.version {
                return Err(Status::unauthenticated("Session invalidated"));
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "create_vault_token",
            &grpc_metadata,
            &trace_ctx,
        );

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
        let ledger_request = LedgerRequest::CreateRefreshToken {
            token_hash: refresh_token_hash,
            family,
            token_type: TokenType::VaultAccess,
            subject: TokenSubject::App(app_slug),
            organization: Some(org_id),
            vault: Some(vault_id),
            kid: signing_key.kid.clone(),
            ttl_secs: self.jwt_config.vault_refresh_ttl_secs,
        };

        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(self.jwt_config.vault_refresh_ttl_secs as i64);

        self.emit_event(EventAction::TokenCreated, &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "refresh_token",
            &grpc_metadata,
            &trace_ctx,
        );

        if req.refresh_token.is_empty() {
            return Err(Status::invalid_argument("refresh_token is required"));
        }

        // Hash the provided refresh token
        let old_hash = Self::hash_refresh_token(&req.refresh_token);

        // Look up the old refresh token to determine type/scope
        let sys = self.system_service();
        let old_token = sys
            .get_refresh_token_by_hash(&old_hash)
            .map_err(|e| Status::internal(format!("Failed to look up refresh token: {e}")))?
            .ok_or_else(|| Status::unauthenticated("Invalid refresh token"))?;

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
                    .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
                    .ok_or_else(|| Status::unauthenticated("User not found"))?;
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
        let ledger_request = LedgerRequest::UseRefreshToken {
            old_token_hash: old_hash,
            new_token_hash: new_hash,
            new_kid: signing_key.kid.clone(),
            ttl_secs,
            expected_version,
        };

        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        // Extract authoritative data from the Raft response
        let (raft_token_version, raft_allowed_scopes) = match response {
            LedgerResponse::RefreshTokenRotated { token_version, allowed_scopes, .. } => {
                (token_version, allowed_scopes)
            },
            other => {
                return Err(Status::internal(format!(
                    "Unexpected Raft response for UseRefreshToken: {other}"
                )));
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
                    .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
                    .ok_or_else(|| Status::unauthenticated("User not found"))?;
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

        self.emit_event(EventAction::TokenRefreshed, &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "revoke_token",
            &grpc_metadata,
            &trace_ctx,
        );

        if req.refresh_token.is_empty() {
            return Err(Status::invalid_argument("refresh_token is required"));
        }

        let hash = Self::hash_refresh_token(&req.refresh_token);

        // Look up the token to get the family
        let sys = self.system_service();
        let token = sys
            .get_refresh_token_by_hash(&hash)
            .map_err(|e| Status::internal(format!("Failed to look up refresh token: {e}")))?
            .ok_or_else(|| Status::unauthenticated("Invalid refresh token"))?;

        // Propose RevokeTokenFamily through Raft
        let ledger_request = LedgerRequest::RevokeTokenFamily { family: token.family };
        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        self.emit_event(EventAction::TokenRevoked, &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "revoke_all_user_sessions",
            &grpc_metadata,
            &trace_ctx,
        );

        let resolver = self.resolver();
        let user_slug = SlugResolver::extract_user_slug(&req.user)?;
        let user_id = resolver.resolve_user(user_slug)?;

        let ledger_request = LedgerRequest::RevokeAllUserSessions { user: user_id };
        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        let revoked_count = match response {
            LedgerResponse::AllUserSessionsRevoked { count, .. } => count,
            other => {
                return Err(Status::internal(format!(
                    "Unexpected Raft response for RevokeAllUserSessions: {other}"
                )));
            },
        };

        self.emit_event(EventAction::TokenRevoked, &trace_ctx);
        ctx.set_success();

        Ok(Response::new(RevokeAllUserSessionsResponse { revoked_count }))
    }

    /// Creates a new signing key for the given scope.
    async fn create_signing_key(
        &self,
        request: Request<CreateSigningKeyRequest>,
    ) -> Result<Response<CreateSigningKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "create_signing_key",
            &grpc_metadata,
            &trace_ctx,
        );

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
        let ledger_request = LedgerRequest::CreateSigningKey {
            scope,
            kid: kid.clone(),
            public_key_bytes,
            encrypted_private_key,
            rmk_version,
        };

        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        // Load key from state for cache and response (authoritative timestamps)
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&kid)
            .map_err(|e| Status::internal(format!("Failed to read signing key: {e}")))?
            .ok_or_else(|| Status::internal("Signing key not found after creation"))?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(kid = %kid, error = %e, "Failed to cache signing key after creation");
        }

        self.emit_event(EventAction::SigningKeyCreated, &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "rotate_signing_key",
            &grpc_metadata,
            &trace_ctx,
        );

        if req.kid.is_empty() {
            return Err(Status::invalid_argument("kid is required"));
        }

        // Look up old key
        let sys = self.system_service();
        let old_key = sys
            .get_signing_key_by_kid(&req.kid)
            .map_err(|e| Status::internal(format!("Failed to read signing key: {e}")))?
            .ok_or_else(|| Status::not_found("Signing key not found"))?;

        if old_key.status != SigningKeyStatus::Active {
            return Err(Status::failed_precondition("Can only rotate an active key"));
        }

        let (new_kid, new_public_key_bytes, new_encrypted_private_key, rmk_version) =
            self.generate_encrypted_keypair(&old_key.scope)?;

        // 0 means "use default from JwtConfig" (proto convention for unset).
        let grace_period_secs = if req.grace_period_secs == 0 {
            self.jwt_config.key_rotation_grace_secs
        } else {
            req.grace_period_secs
        };

        let ledger_request = LedgerRequest::RotateSigningKey {
            old_kid: req.kid.clone(),
            new_kid: new_kid.clone(),
            new_public_key_bytes,
            new_encrypted_private_key,
            rmk_version,
            grace_period_secs,
        };

        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        // Load new key from state for cache and response (authoritative timestamps)
        let sys = self.system_service();
        let stored_key = sys
            .get_signing_key_by_kid(&new_kid)
            .map_err(|e| Status::internal(format!("Failed to read signing key: {e}")))?
            .ok_or_else(|| Status::internal("Signing key not found after rotation"))?;

        if let Err(e) = self.ensure_key_cached(&stored_key) {
            tracing::warn!(kid = %new_kid, error = %e, "Failed to cache signing key after rotation");
        }

        // If effective grace period is 0 (immediate revocation), evict old key from cache
        if grace_period_secs == 0 {
            self.jwt_engine.evict_key(&req.kid);
        }

        self.emit_event(EventAction::SigningKeyRotated, &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "TokenService",
            "revoke_signing_key",
            &grpc_metadata,
            &trace_ctx,
        );

        if req.kid.is_empty() {
            return Err(Status::invalid_argument("kid is required"));
        }

        let ledger_request = LedgerRequest::RevokeSigningKey { kid: req.kid.clone() };
        self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        // Evict from cache
        self.jwt_engine.evict_key(&req.kid);

        self.emit_event(EventAction::SigningKeyRevoked, &trace_ctx);
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
        let keys = sys
            .list_signing_keys(&scope)
            .map_err(|e| Status::internal(format!("Failed to list signing keys: {e}")))?;

        // Filter to Active + Rotated (exclude Revoked)
        let keys: Vec<PublicKeyInfo> = keys
            .iter()
            .filter(|k| k.status != SigningKeyStatus::Revoked)
            .map(Self::signing_key_to_public_info)
            .collect();

        Ok(Response::new(GetPublicKeysResponse { keys }))
    }
}

#[cfg(test)]
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

        let err = JwtError::Token {
            source: TokenError::Expired,
            location: snafu::Location::new("", 0, 0),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn jwt_error_to_status_invalid_audience() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::InvalidAudience { expected: "test".to_string() },
            location: snafu::Location::new("", 0, 0),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn jwt_error_to_status_missing_claim() {
        use inferadb_ledger_types::token::TokenError;

        use crate::jwt::JwtError;

        let err = JwtError::Token {
            source: TokenError::MissingClaim { claim: "sub".to_string() },
            location: snafu::Location::new("", 0, 0),
        };
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn jwt_error_to_status_key_encryption() {
        let err = crate::jwt::JwtError::KeyEncryption;
        let status = TokenServiceImpl::jwt_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
