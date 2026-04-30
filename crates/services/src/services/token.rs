//! Token service implementation.
//!
//! Handles JWT lifecycle: user session creation, vault token creation,
//! token validation, refresh (rotate-on-use), revocation, and signing key
//! management. All write mutations flow through Raft for consistency;
//! validation and public key reads hit local applied state.

use std::sync::Arc;

use base64::Engine as _;
use inferadb_ledger_raft::rate_limit::RateLimiter;
use inferadb_ledger_state::system::{
    App, AppVaultConnection, ClientAssertionEntry, SYSTEM_VAULT_ID, SigningKey, SigningKeyScope,
    SystemKeys,
};
use inferadb_ledger_store::crypto::RegionKeyManager;
use inferadb_ledger_types::{
    AppId, ClientAssertionId, OrganizationId as DomainOrganizationId, VaultId as DomainVaultId,
    config::JwtConfig, decode, types::AppSlug,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use sha2::{Digest, Sha256};
use tonic::Status;

use super::{
    error_classify, service_infra::ServiceContext, wire_helpers::wire_error_to_tonic_status,
};
use crate::jwt::{JwtEngine, encrypt_private_key};

/// Token lifecycle service.
///
/// Handles user sessions, vault tokens, refresh/revocation, and signing key
/// management. Embeds `ServiceContext` for shared Raft/state infrastructure.
pub struct TokenServiceImpl {
    pub(super) ctx: ServiceContext,
    pub(super) jwt_engine: Arc<JwtEngine>,
    pub(super) jwt_config: JwtConfig,
    pub(super) key_manager: Arc<dyn RegionKeyManager>,
    pub(super) rate_limiter: Option<Arc<RateLimiter>>,
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

    /// Creates a `SystemOrganizationService` for direct state reads.
    ///
    /// Reuses the process-wide signing-key cache from the
    /// [`ServiceContext`](super::service_infra::ServiceContext) so the
    /// per-RPC token verification path does not allocate a fresh
    /// `moka::Cache` (P11).
    pub(super) fn system_service(
        &self,
    ) -> inferadb_ledger_state::system::SystemOrganizationService<inferadb_ledger_store::FileBackend>
    {
        self.ctx.system_service()
    }

    /// Ensures the signing key identified by `kid` is loaded in the JwtEngine cache.
    /// If not cached, reads from state and decrypts.
    pub(super) fn ensure_key_cached(&self, key: &SigningKey) -> Result<(), Status> {
        if self.jwt_engine.has_cached_key(&key.kid) {
            return Ok(());
        }
        let scope = &key.scope;
        let region = crate::jwt::scope_to_region(scope, &self.system_service())
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        let rmk = self
            .key_manager
            .rmk_by_version(region, key.rmk_version)
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        self.jwt_engine
            .load_key(key, &rmk)
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        Ok(())
    }

    /// Generates a new Ed25519 keypair, encrypts the private key with the scope's RMK,
    /// and zeroizes the secret material. Returns `(kid, public_key_bytes, encrypted_private_key,
    /// rmk_version)`.
    pub(super) fn generate_encrypted_keypair(
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
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        let rmk = self
            .key_manager
            .current_rmk(region)
            .map_err(|e| wire_error_to_tonic_status(error_classify::crypto_error(&e)))?;
        let (envelope, rmk_version) = encrypt_private_key(secret_bytes.as_ref(), &kid, &rmk)
            .map_err(Self::jwt_error_to_status)?;

        let encrypted_private_key = envelope.to_bytes().to_vec();
        Ok((kid, public_key_bytes, encrypted_private_key, rmk_version))
    }

    /// Returns the active signing key for a scope, ensuring it's cached.
    pub(super) fn active_key_for_scope(
        &self,
        scope: &SigningKeyScope,
    ) -> Result<SigningKey, Status> {
        let sys = self.system_service();
        let key = sys
            .get_active_signing_key(scope)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?
            .ok_or_else(|| Status::failed_precondition("No active signing key for scope"))?;
        self.ensure_key_cached(&key)?;
        Ok(key)
    }

    /// Loads an app from state by organization and app ID.
    pub(super) fn load_app(
        &self,
        org_id: DomainOrganizationId,
        app_id: inferadb_ledger_types::AppId,
    ) -> Result<App, Status> {
        super::helpers::load_app(&self.ctx.state, org_id, app_id)
            .map_err(wire_error_to_tonic_status)
    }

    /// Reads a vault connection from state.
    pub(super) fn read_vault_connection(
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
            super::wire_helpers::build_wire_error(
                inferadb_ledger_wire::ErrorCode::NotFound,
                "Vault connection not found",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            ),
        )
        .map_err(wire_error_to_tonic_status)
    }

    /// Maps a `JwtError` to a gRPC `Status` following the error mapping table.
    pub(super) fn jwt_error_to_status(err: crate::jwt::JwtError) -> Status {
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
    pub(super) fn hash_refresh_token(token: &str) -> [u8; 32] {
        Sha256::digest(token.as_bytes()).into()
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
    pub(super) fn parse_assertion_header(
        token: &str,
    ) -> Result<(String, ClientAssertionId), Status> {
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
    pub(super) fn extract_issuer_from_jwt(token: &str) -> Result<AppSlug, Status> {
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
    pub(super) fn load_assertion_entry(
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
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?
            .ok_or_else(|| {
                super::auth_errors::unified_auth_error(
                    super::auth_errors::AuthFailureReason::ClientAssertionUnknown,
                )
            })?;

        decode::<ClientAssertionEntry>(&entity.value)
            .map_err(|e| wire_error_to_tonic_status(error_classify::serialization_error(&e)))
    }

    /// Verifies the assertion JWT signature and validates standard claims.
    ///
    /// Checks:
    /// - Signature using the assertion entry's Ed25519 public key
    /// - `iss` matches the app slug
    /// - `aud` matches the ledger issuer (ledger is the intended audience)
    /// - `exp` is not in the past
    pub(super) fn verify_assertion_jwt(
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
            tracing::debug!(error = ?e, kid = %expected_kid, "Assertion JWT verification failed");
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
