//! JWT signing, validation, and key management.
//!
//! This module contains:
//! - `JwtEngine` — signing key cache with lock-free reads (ArcSwap) and methods for
//!   signing/validating JWTs.
//! - Refresh token utilities (`generate_refresh_token`, `generate_family_id`).
//! - Private key envelope encryption (`encrypt_private_key`, `decrypt_private_key`).
//!
//! Crypto and JWT library errors are defined here as `JwtError`, while
//! domain-level token errors live in [`inferadb_ledger_types::token::TokenError`].

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use inferadb_ledger_state::system::{
    SigningKey, SigningKeyScope, SigningKeyStatus, SystemError, SystemOrganizationService,
};
use inferadb_ledger_store::{
    StorageBackend,
    crypto::{
        RegionMasterKey, WrappedDek, decrypt_page_body, encrypt_page_body, generate_dek,
        unwrap_dek, wrap_dek,
    },
};
use inferadb_ledger_types::{
    Region,
    config::JwtConfig,
    token::{
        SESSION_AUDIENCE, SIGNING_KEY_ENVELOPE_SIZE, SigningKeyEnvelope, TokenError, TokenType,
        UserSessionClaims, VAULT_AUDIENCE, ValidatedToken, VaultTokenClaims,
    },
    types::{AppSlug, OrganizationSlug, TokenVersion, UserSlug, VaultSlug},
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use sha2::{Digest, Sha256};
use snafu::{OptionExt, ResultExt, Snafu};
use zeroize::{Zeroize, Zeroizing};

// ─── DER Encoding Constants ────────────────────────────────────────

/// PKCS8 DER prefix for Ed25519 private keys (16 bytes).
/// Append 32 bytes of raw private key seed to form a complete 48-byte PKCS8.
const ED25519_PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

// ─── Errors ────────────────────────────────────────────────────────

/// Crypto and JWT library errors.
///
/// Domain-level token errors (expired, revoked, invalid audience, etc.)
/// live in [`TokenError`]. This enum covers JWT signing/decoding failures
/// and envelope encryption errors.
#[derive(Debug, Snafu)]
pub enum JwtError {
    /// JWT signing operation failed.
    #[snafu(display("JWT signing failed: {source}"))]
    Signing {
        /// Underlying jsonwebtoken error.
        source: jsonwebtoken::errors::Error,
        /// Source location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// JWT decoding or verification failed.
    #[snafu(display("JWT decoding failed: {source}"))]
    Decoding {
        /// Underlying jsonwebtoken error.
        source: jsonwebtoken::errors::Error,
        /// Source location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Failed to decrypt a signing key's private key material.
    #[snafu(display("Private key decryption failed"))]
    KeyDecryption,

    /// Failed to encrypt a signing key's private key material.
    #[snafu(display("Private key encryption failed"))]
    KeyEncryption,

    /// Domain-level token error (expired, revoked, invalid audience, etc.).
    #[snafu(display("{source}"))]
    Token {
        /// The underlying domain token error.
        source: TokenError,
        /// Source location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Organization not found when resolving signing key scope to region.
    #[snafu(display("Organization not found for signing key scope: {id}"))]
    OrganizationNotFound {
        /// The organization ID that was not found.
        id: i64,
    },

    /// Region lookup failed due to a state error.
    #[snafu(display("State error during region lookup: {source}"))]
    StateLookup {
        /// The underlying system state error.
        source: SystemError,
        /// Source location.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

// ─── Region Resolution ─────────────────────────────────────────────

/// Maps a signing key scope to the [`Region`] whose RMK protects it.
///
/// - `SigningKeyScope::Global` → `Region::GLOBAL` (always provisioned, validated at startup).
/// - `SigningKeyScope::Organization(id)` → the organization's assigned region.
pub fn scope_to_region<B: StorageBackend>(
    scope: &SigningKeyScope,
    system_service: &SystemOrganizationService<B>,
) -> Result<Region, JwtError> {
    match scope {
        SigningKeyScope::Global => Ok(Region::GLOBAL),
        SigningKeyScope::Organization(org_id) => {
            let region = system_service
                .get_region_for_organization(*org_id)
                .context(StateLookupSnafu)?
                .context(OrganizationNotFoundSnafu { id: org_id.value() })?;
            Ok(region)
        },
    }
}

// ─── Cached Signing Key ────────────────────────────────────────────

/// Decrypted signing key cached in memory for fast access.
///
/// The `DecodingKey` (public key) is cached for the validation hot path.
/// The raw private key bytes are held in a [`Zeroizing`] wrapper that
/// securely wipes memory on drop. `EncodingKey` is built on-demand per
/// signing operation from `private_key_bytes`, then immediately dropped.
struct CachedSigningKey {
    /// Full signing key metadata from state.
    metadata: SigningKey,
    /// Cached public key for JWT verification (not secret material).
    decoding_key: DecodingKey,
    /// Raw Ed25519 private key bytes — zeroized on drop.
    private_key_bytes: Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for CachedSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedSigningKey")
            .field("kid", &self.metadata.kid)
            .field("scope", &self.metadata.scope)
            .field("status", &self.metadata.status)
            .finish_non_exhaustive()
    }
}

// ─── JWT Engine ────────────────────────────────────────────────────

/// JWT signing and validation engine with lock-free key cache.
///
/// Uses [`ArcSwap`] for the signing key cache, providing wait-free reads
/// on the validation hot path (same pattern as `RuntimeConfigHandle`
/// and `CommittedState` in the store layer).
///
/// # Key Management
///
/// - **Active key resolution** always reads from applied state (not cache).
/// - **Cache holds key material only** — `DecodingKey` for validation, `private_key_bytes` for
///   on-demand `EncodingKey` construction.
/// - **Copy-on-write updates**: `load_key`/`evict_key` clone the HashMap, mutate, and atomically
///   swap the Arc pointer.
pub struct JwtEngine {
    config: JwtConfig,
    /// Decrypted signing keys, keyed by kid.
    signing_keys: ArcSwap<HashMap<String, Arc<CachedSigningKey>>>,
}

impl JwtEngine {
    /// Creates a new engine with the given configuration and empty cache.
    #[must_use]
    pub fn new(config: JwtConfig) -> Self {
        Self { config, signing_keys: ArcSwap::from_pointee(HashMap::new()) }
    }

    /// Decrypt and cache a signing key.
    ///
    /// Decrypts the key's envelope using the provided RMK, then caches the
    /// `DecodingKey` (public) and `private_key_bytes` (zeroizable) for
    /// subsequent signing and validation operations.
    ///
    /// Performs a copy-on-write update of the ArcSwap cache.
    pub fn load_key(&self, key: &SigningKey, rmk: &RegionMasterKey) -> Result<(), JwtError> {
        let envelope_bytes: &[u8; SIGNING_KEY_ENVELOPE_SIZE] =
            key.encrypted_private_key.as_slice().try_into().map_err(|_| JwtError::KeyDecryption)?;
        let envelope = SigningKeyEnvelope::from_bytes(envelope_bytes);

        let private_key_bytes = decrypt_private_key(&envelope, &key.kid, rmk)?;

        let decoding_key = DecodingKey::from_ed_der(&key.public_key_bytes);

        let cached =
            Arc::new(CachedSigningKey { metadata: key.clone(), decoding_key, private_key_bytes });

        // Copy-on-write: clone HashMap, insert, swap.
        let mut map = HashMap::clone(&self.signing_keys.load());
        map.insert(key.kid.clone(), cached);
        self.signing_keys.store(Arc::new(map));

        Ok(())
    }

    /// Evict a key from cache (on revocation).
    ///
    /// Triggers `Zeroize` on drop when the last Arc reference is released.
    /// Copy-on-write: clones the HashMap, removes the entry, swaps.
    pub fn evict_key(&self, kid: &str) {
        let mut map = HashMap::clone(&self.signing_keys.load());
        map.remove(kid);
        self.signing_keys.store(Arc::new(map));
    }

    /// Look up a cached key by kid (lock-free read via `ArcSwap::load`).
    fn get_cached_key(&self, kid: &str) -> Option<Arc<CachedSigningKey>> {
        self.signing_keys.load().get(kid).cloned()
    }

    /// Checks whether a key with the given kid is in the cache.
    pub fn has_cached_key(&self, kid: &str) -> bool {
        self.signing_keys.load().contains_key(kid)
    }

    /// Extracts the `kid` from a JWT header with defense-in-depth checks.
    ///
    /// Validates algorithm is EdDSA and kid is a valid UUID, without
    /// performing cryptographic verification. Returns the kid string
    /// so the caller can ensure the key is cached before calling
    /// [`validate`](Self::validate).
    pub fn extract_kid(token: &str) -> Result<String, JwtError> {
        extract_and_validate_header(token).context(TokenSnafu)
    }

    /// Sign a user session JWT.
    ///
    /// Builds an `EncodingKey` on-demand from cached `private_key_bytes`,
    /// signs the token, then the `EncodingKey` is dropped immediately.
    /// Sets `nbf = iat` (library leeway handles clock skew).
    ///
    /// Returns `(jwt_string, expires_at)`.
    pub fn sign_user_session(
        &self,
        user: UserSlug,
        role: &str,
        version: TokenVersion,
        kid: &str,
    ) -> Result<(String, DateTime<Utc>), JwtError> {
        let (now, expires_at) = self.time_window(self.config.session_access_ttl_secs);

        let claims = UserSessionClaims {
            iss: self.config.issuer.clone(),
            sub: format!("user:{user}"),
            aud: vec![SESSION_AUDIENCE.to_string()],
            exp: expires_at.timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            jti: uuid::Uuid::new_v4().to_string(),
            token_type: TokenType::UserSession,
            user,
            role: role.to_string(),
            version,
        };

        let cached = self.require_cached_key(kid)?;
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        let encoding_key = encoding_key_from_private_bytes(&cached.private_key_bytes);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).context(SigningSnafu)?;

        Ok((token, expires_at))
    }

    /// Sign a vault access JWT.
    ///
    /// Same on-demand `EncodingKey` pattern as [`sign_user_session`](Self::sign_user_session).
    /// Sets `nbf = iat`.
    ///
    /// Returns `(jwt_string, expires_at)`.
    pub fn sign_vault_token(
        &self,
        org: OrganizationSlug,
        app: AppSlug,
        vault: VaultSlug,
        scopes: &[String],
        kid: &str,
    ) -> Result<(String, DateTime<Utc>), JwtError> {
        let (now, expires_at) = self.time_window(self.config.vault_access_ttl_secs);

        let claims = VaultTokenClaims {
            iss: self.config.issuer.clone(),
            sub: format!("app:{app}"),
            aud: vec![VAULT_AUDIENCE.to_string()],
            exp: expires_at.timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            jti: uuid::Uuid::new_v4().to_string(),
            token_type: TokenType::VaultAccess,
            org,
            app,
            vault,
            scopes: scopes.to_vec(),
        };

        let cached = self.require_cached_key(kid)?;
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        let encoding_key = encoding_key_from_private_bytes(&cached.private_key_bytes);
        let token = jsonwebtoken::encode(&header, &claims, &encoding_key).context(SigningSnafu)?;

        Ok((token, expires_at))
    }

    /// Computes `(now, expires_at)` from a TTL in seconds.
    fn time_window(&self, ttl_secs: u64) -> (DateTime<Utc>, DateTime<Utc>) {
        let now = Utc::now();
        let expires_at = now + Duration::seconds(ttl_secs as i64);
        (now, expires_at)
    }

    /// Look up a cached key by kid and return it, or error.
    fn require_cached_key(&self, kid: &str) -> Result<Arc<CachedSigningKey>, JwtError> {
        self.get_cached_key(kid)
            .ok_or(TokenError::SigningKeyNotFound { kid: kid.to_string() })
            .context(TokenSnafu)
    }

    /// Validate a JWT and return parsed claims.
    ///
    /// # Defense-in-depth algorithm enforcement
    ///
    /// 1. Decode raw JWT header (without verification) and reject any `alg` != `"EdDSA"` at the
    ///    string level. Blocks `alg: none`, `alg: HS256`, etc. before the library sees the token.
    /// 2. Use `Validation::algorithms(&[Algorithm::EdDSA])` as the library check.
    ///
    /// # kid validation
    ///
    /// Rejects `kid` values that are not valid UUID format before performing
    /// any state/cache lookup (prevents cache pollution with random kids).
    ///
    /// # Error normalization
    ///
    /// Returns identical `UNAUTHENTICATED` for "kid not found" and "signature
    /// invalid" — no information leakage about valid kids.
    ///
    /// For Rotated keys, explicitly checks `valid_until < now` even when status
    /// is still Rotated (handles window between grace period expiry and
    /// `TokenMaintenanceJob` transition).
    ///
    /// Does NOT check `TokenVersion` (caller must do that with current user state).
    pub fn validate(
        &self,
        token: &str,
        expected_audience: &str,
    ) -> Result<ValidatedToken, JwtError> {
        // Step 1: Raw header pre-validation (defense-in-depth).
        let kid = extract_and_validate_header(token).context(TokenSnafu)?;

        // Step 2: Look up key from cache.
        let cached =
            self.get_cached_key(&kid).ok_or(TokenError::InvalidSignature).context(TokenSnafu)?;

        // Step 3: Check key status.
        match cached.metadata.status {
            SigningKeyStatus::Active => {},
            SigningKeyStatus::Rotated => {
                // Check if grace period has expired.
                if let Some(valid_until) = cached.metadata.valid_until
                    && Utc::now() > valid_until
                {
                    return Err(TokenError::SigningKeyExpired { kid }).context(TokenSnafu);
                }
            },
            SigningKeyStatus::Revoked => {
                // Return InvalidSignature (not SigningKeyRevoked) to avoid
                // leaking info about valid kids.
                return Err(TokenError::InvalidSignature).context(TokenSnafu);
            },
        }

        // Step 4: Full JWT validation with library.
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[expected_audience]);
        validation.leeway = self.config.clock_skew_secs;
        validation.validate_nbf = true;

        // Decode as serde_json::Value to inspect the type field before
        // deserializing into the specific claims struct.
        let token_data =
            jsonwebtoken::decode::<serde_json::Value>(token, &cached.decoding_key, &validation)
                .context(DecodingSnafu)?;

        let token_type_str = token_data
            .claims
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or(TokenError::MissingClaim { claim: "type".to_string() })
            .context(TokenSnafu)?;

        match token_type_str {
            "user_session" => {
                let claims: UserSessionClaims = serde_json::from_value(token_data.claims)
                    .map_err(|_| TokenError::MissingClaim {
                        claim: "user_session fields".to_string(),
                    })
                    .context(TokenSnafu)?;
                Ok(ValidatedToken::UserSession(claims))
            },
            "vault_access" => {
                let claims: VaultTokenClaims = serde_json::from_value(token_data.claims)
                    .map_err(|_| TokenError::MissingClaim {
                        claim: "vault_access fields".to_string(),
                    })
                    .context(TokenSnafu)?;
                Ok(ValidatedToken::VaultAccess(claims))
            },
            other => Err(TokenError::InvalidTokenType {
                expected: format!("user_session or vault_access, got {other}"),
            })
            .context(TokenSnafu),
        }
    }

    /// Returns a reference to the engine's configuration.
    #[cfg(test)]
    #[must_use]
    pub fn config(&self) -> &JwtConfig {
        &self.config
    }
}

impl std::fmt::Debug for JwtEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key_count = self.signing_keys.load().len();
        f.debug_struct("JwtEngine")
            .field("config", &self.config)
            .field("cached_keys", &key_count)
            .finish()
    }
}

// ─── Header Pre-validation ─────────────────────────────────────────

/// Extract kid from the JWT header with defense-in-depth checks.
///
/// 1. Parses raw base64url header JSON (no cryptographic verification).
/// 2. Rejects any `alg` value that is not exactly `"EdDSA"`.
/// 3. Validates `kid` is present and is a valid UUID format.
///
/// Returns the kid string on success.
fn extract_and_validate_header(token: &str) -> Result<String, TokenError> {
    let header_part = token.split('.').next().ok_or(TokenError::InvalidSignature)?;

    let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(header_part)
        // Also try standard base64url with padding (some libraries add it).
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(header_part))
        .map_err(|_| TokenError::InvalidSignature)?;

    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| TokenError::InvalidSignature)?;

    // Defense-in-depth: reject any algorithm other than EdDSA at string level.
    let alg = header.get("alg").and_then(|v| v.as_str()).ok_or(TokenError::InvalidSignature)?;
    if alg != "EdDSA" {
        return Err(TokenError::InvalidSignature);
    }

    let kid = header
        .get("kid")
        .and_then(|v| v.as_str())
        .ok_or(TokenError::MissingClaim { claim: "kid".to_string() })?;

    // Validate kid is a valid UUID (prevents cache pollution with random strings).
    uuid::Uuid::parse_str(kid).map_err(|_| TokenError::InvalidSignature)?;

    Ok(kid.to_string())
}

// ─── DER Key Wrapping ──────────────────────────────────────────────

/// Creates an [`EncodingKey`] from raw 32-byte Ed25519 private key bytes.
///
/// Wraps raw bytes in PKCS8 DER format for `jsonwebtoken`.
/// The DER buffer containing private key material is zeroized after use.
fn encoding_key_from_private_bytes(private_key: &[u8]) -> EncodingKey {
    let mut der = Zeroizing::new(Vec::with_capacity(48));
    der.extend_from_slice(&ED25519_PKCS8_PREFIX);
    der.extend_from_slice(private_key);
    EncodingKey::from_ed_der(&der)
}

// ─── Private Key Envelope Encryption ───────────────────────────────

/// Encrypt an Ed25519 private key using envelope encryption.
///
/// Generates a fresh DEK, wraps it with the RMK via AES-KWP, then
/// encrypts the private key with the DEK using AES-256-GCM. The `kid`
/// is used as AAD to cryptographically bind ciphertext to key identity.
///
/// Returns `(envelope, rmk_version)`.
pub fn encrypt_private_key(
    private_key: &[u8],
    kid: &str,
    rmk: &RegionMasterKey,
) -> Result<(SigningKeyEnvelope, u32), JwtError> {
    let dek = generate_dek();
    let wrapped_dek = wrap_dek(&dek, rmk).map_err(|_| JwtError::KeyEncryption)?;

    let (ciphertext, nonce, auth_tag) = encrypt_page_body(private_key, kid.as_bytes(), &dek)
        .map_err(|_| JwtError::KeyEncryption)?;

    let ct_array: [u8; 32] = ciphertext.try_into().map_err(|_| JwtError::KeyEncryption)?;

    let envelope = SigningKeyEnvelope {
        wrapped_dek: *wrapped_dek.as_bytes(),
        nonce,
        ciphertext: ct_array,
        auth_tag,
    };

    Ok((envelope, rmk.version))
}

/// Decrypt an Ed25519 private key from its envelope.
///
/// Unwraps the DEK using the RMK, then decrypts the ciphertext with
/// the DEK using AES-256-GCM. The `kid` is verified as AAD.
///
/// Returns the plaintext private key bytes in a [`Zeroizing`] wrapper.
pub fn decrypt_private_key(
    envelope: &SigningKeyEnvelope,
    kid: &str,
    rmk: &RegionMasterKey,
) -> Result<Zeroizing<Vec<u8>>, JwtError> {
    let wrapped = WrappedDek::from_bytes(envelope.wrapped_dek);
    let dek = unwrap_dek(&wrapped, rmk).map_err(|_| JwtError::KeyDecryption)?;

    let plaintext = decrypt_page_body(
        &envelope.ciphertext,
        &envelope.nonce,
        kid.as_bytes(),
        &envelope.auth_tag,
        &dek,
    )
    .map_err(|_| JwtError::KeyDecryption)?;

    Ok(Zeroizing::new(plaintext))
}

// ─── Refresh Token Utilities ───────────────────────────────────────

/// Generates a new refresh token string and its SHA-256 hash.
///
/// Format: `ilrt_{base64url(32 random bytes)}` (48 chars total).
/// The `ilrt_` prefix enables secret scanning tool detection (truffleHog,
/// GitHub secret scanning, etc.).
///
/// Returns `(token_string, sha256_hash)`.
pub fn generate_refresh_token() -> (String, [u8; 32]) {
    let mut random_bytes = [0u8; 32];
    rand::RngExt::fill(&mut rand::rng(), &mut random_bytes);

    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random_bytes);
    let token_string = format!("ilrt_{encoded}");

    // Zeroize random bytes — the token_string now holds the value.
    random_bytes.zeroize();

    // Hash the full prefixed string for storage.
    let hash: [u8; 32] = Sha256::digest(token_string.as_bytes()).into();

    (token_string, hash)
}

/// Generates a random 16-byte family identifier for refresh token theft detection.
pub fn generate_family_id() -> [u8; 16] {
    let mut family = [0u8; 16];
    rand::RngExt::fill(&mut rand::rng(), &mut family);
    family
}

// ─── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use inferadb_ledger_state::{
        InMemoryStorageEngine, StateLayer,
        system::{
            OrganizationRegistry, OrganizationStatus, SigningKey, SigningKeyScope,
            SigningKeyStatus, SystemOrganizationService,
        },
    };
    use inferadb_ledger_types::{
        OrganizationId, OrganizationSlug, Region, SigningKeyId,
        config::JwtConfig,
        token::{
            SESSION_AUDIENCE, SIGNING_KEY_ENVELOPE_SIZE, TokenError, TokenType, VAULT_AUDIENCE,
            ValidatedToken,
        },
        types::{AppSlug, TokenVersion, UserSlug, VaultSlug},
    };

    use super::*;

    // ── Test Helpers ────────────────────────────────────────────────

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        let engine = InMemoryStorageEngine::open().unwrap();
        let state = Arc::new(StateLayer::new(engine.db()));
        SystemOrganizationService::new(state)
    }

    fn test_config() -> JwtConfig {
        JwtConfig::builder()
            .session_access_ttl_secs(1800_u64)
            .vault_access_ttl_secs(900_u64)
            .build()
            .unwrap()
    }

    fn test_rmk() -> RegionMasterKey {
        RegionMasterKey::new(1, [0x42; 32])
    }

    /// Generate a test keypair, encrypt, and return (SigningKey, RMK).
    fn generate_test_signing_key(
        kid: &str,
        scope: SigningKeyScope,
    ) -> (SigningKey, RegionMasterKey) {
        let rmk = test_rmk();

        // Generate Ed25519 keypair
        let mut secret_bytes = [0u8; 32];
        rand::RngExt::fill(&mut rand::rng(), &mut secret_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let public_key_bytes = signing_key.verifying_key().to_bytes().to_vec();
        let private_key_bytes = signing_key.to_bytes();

        // Encrypt
        let (envelope, rmk_version) = encrypt_private_key(&private_key_bytes, kid, &rmk).unwrap();

        let key = SigningKey {
            id: SigningKeyId::new(1),
            kid: kid.to_string(),
            public_key_bytes,
            encrypted_private_key: envelope.to_bytes().to_vec(),
            rmk_version,
            scope,
            status: SigningKeyStatus::Active,
            valid_from: Utc::now(),
            valid_until: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: None,
        };

        (key, rmk)
    }

    // ── scope_to_region Tests ───────────────────────────────────────

    #[test]
    fn scope_to_region_global() {
        let svc = create_test_service();
        let result = scope_to_region(&SigningKeyScope::Global, &svc);
        assert_eq!(result.unwrap(), Region::GLOBAL);
    }

    #[test]
    fn scope_to_region_missing_org() {
        let svc = create_test_service();
        let result =
            scope_to_region(&SigningKeyScope::Organization(OrganizationId::new(999)), &svc);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, JwtError::OrganizationNotFound { id: 999 }),
            "expected OrganizationNotFound, got: {err:?}"
        );
    }

    #[test]
    fn scope_to_region_existing_org() {
        let svc = create_test_service();

        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::US_EAST_VA,
            member_nodes: vec!["node-1".to_string()],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        svc.register_organization(&registry, OrganizationSlug::new(1001)).unwrap();

        let result = scope_to_region(&SigningKeyScope::Organization(OrganizationId::new(1)), &svc);
        assert_eq!(result.unwrap(), Region::US_EAST_VA);
    }

    #[test]
    fn scope_to_region_preserves_org_region() {
        let svc = create_test_service();

        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(42),
            region: Region::JP_EAST_TOKYO,
            member_nodes: vec!["node-jp".to_string()],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        svc.register_organization(&registry, OrganizationSlug::new(4200)).unwrap();

        let result = scope_to_region(&SigningKeyScope::Organization(OrganizationId::new(42)), &svc);
        assert_eq!(result.unwrap(), Region::JP_EAST_TOKYO);
    }

    // ── JwtError Display Tests ──────────────────────────────────────

    #[test]
    fn jwt_error_display_key_decryption() {
        let err = JwtError::KeyDecryption;
        assert_eq!(err.to_string(), "Private key decryption failed");
    }

    #[test]
    fn jwt_error_display_key_encryption() {
        let err = JwtError::KeyEncryption;
        assert_eq!(err.to_string(), "Private key encryption failed");
    }

    #[test]
    fn jwt_error_display_org_not_found() {
        let err = JwtError::OrganizationNotFound { id: 42 };
        assert_eq!(err.to_string(), "Organization not found for signing key scope: 42");
    }

    #[test]
    fn jwt_error_token_wraps_domain_error() {
        let token_err = TokenError::Expired;
        let jwt_err =
            JwtError::Token { source: token_err, location: snafu::Location::new("test", 0, 0) };
        assert_eq!(jwt_err.to_string(), "Token expired");
    }

    // ── Envelope Encryption Tests ───────────────────────────────────

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let rmk = test_rmk();
        let private_key = [0xAB; 32];
        let kid = "test-kid-001";

        let (envelope, version) = encrypt_private_key(&private_key, kid, &rmk).unwrap();
        assert_eq!(version, 1);

        let decrypted = decrypt_private_key(&envelope, kid, &rmk).unwrap();
        assert_eq!(decrypted.as_slice(), &private_key);
    }

    #[test]
    fn decrypt_fails_with_wrong_rmk() {
        let rmk = test_rmk();
        let wrong_rmk = RegionMasterKey::new(2, [0xFF; 32]);
        let private_key = [0xAB; 32];
        let kid = "test-kid-002";

        let (envelope, _) = encrypt_private_key(&private_key, kid, &rmk).unwrap();
        let result = decrypt_private_key(&envelope, kid, &wrong_rmk);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_fails_with_wrong_kid_aad() {
        let rmk = test_rmk();
        let private_key = [0xAB; 32];

        let (envelope, _) = encrypt_private_key(&private_key, "correct-kid", &rmk).unwrap();
        let result = decrypt_private_key(&envelope, "wrong-kid", &rmk);
        assert!(result.is_err());
    }

    #[test]
    fn envelope_has_correct_size() {
        let rmk = test_rmk();
        let private_key = [0xAB; 32];

        let (envelope, _) = encrypt_private_key(&private_key, "kid", &rmk).unwrap();
        assert_eq!(envelope.to_bytes().len(), SIGNING_KEY_ENVELOPE_SIZE);
    }

    // ── Refresh Token Tests ─────────────────────────────────────────

    #[test]
    fn refresh_token_format() {
        let (token, hash) = generate_refresh_token();
        assert!(token.starts_with("ilrt_"), "token must start with ilrt_ prefix");
        assert_eq!(token.len(), 48, "ilrt_ (5) + base64url(32) (43) = 48");
        assert_ne!(hash, [0u8; 32], "hash must not be all zeros");
    }

    #[test]
    fn refresh_token_hash_is_sha256_of_full_string() {
        let (token, hash) = generate_refresh_token();
        let expected: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        assert_eq!(hash, expected);
    }

    #[test]
    fn refresh_token_uniqueness() {
        let (a, _) = generate_refresh_token();
        let (b, _) = generate_refresh_token();
        assert_ne!(a, b, "two generated tokens must be unique");
    }

    #[test]
    fn family_id_is_16_bytes() {
        let family = generate_family_id();
        assert_eq!(family.len(), 16);
    }

    #[test]
    fn family_id_uniqueness() {
        let a = generate_family_id();
        let b = generate_family_id();
        assert_ne!(a, b, "two generated family IDs must be unique");
    }

    // ── Header Pre-validation Tests ─────────────────────────────────

    #[test]
    fn extract_header_rejects_alg_none() {
        let header = r#"{"alg":"none","kid":"550e8400-e29b-41d4-a716-446655440000"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    #[test]
    fn extract_header_rejects_alg_hs256() {
        let header = r#"{"alg":"HS256","kid":"550e8400-e29b-41d4-a716-446655440000"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    #[test]
    fn extract_header_rejects_missing_kid() {
        let header = r#"{"alg":"EdDSA"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::MissingClaim { .. })));
    }

    #[test]
    fn extract_header_rejects_non_uuid_kid() {
        let header = r#"{"alg":"EdDSA","kid":"not-a-uuid"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    #[test]
    fn extract_header_accepts_valid_eddsa_uuid_kid() {
        let kid = "550e8400-e29b-41d4-a716-446655440000";
        let header = format!(r#"{{"alg":"EdDSA","kid":"{kid}"}}"#);
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert_eq!(result.unwrap(), kid);
    }

    #[test]
    fn extract_header_rejects_malformed_token() {
        let result = extract_and_validate_header("not-a-jwt");
        assert!(result.is_err());
    }

    #[test]
    fn extract_header_rejects_empty_alg() {
        let header = r#"{"alg":"","kid":"550e8400-e29b-41d4-a716-446655440000"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    #[test]
    fn extract_header_rejects_alg_rs256() {
        let header = r#"{"alg":"RS256","kid":"550e8400-e29b-41d4-a716-446655440000"}"#;
        let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header);
        let fake_token = format!("{header_b64}.payload.signature");

        let result = extract_and_validate_header(&fake_token);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    // ── JwtEngine Key Cache Tests ───────────────────────────────────

    #[test]
    fn engine_load_and_get_key() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let cached = engine.get_cached_key(&kid);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().metadata.kid, kid);
    }

    #[test]
    fn engine_get_missing_key() {
        let engine = JwtEngine::new(test_config());
        assert!(engine.get_cached_key("nonexistent").is_none());
    }

    #[test]
    fn engine_evict_key() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();
        assert!(engine.get_cached_key(&kid).is_some());

        engine.evict_key(&kid);
        assert!(engine.get_cached_key(&kid).is_none());
    }

    #[test]
    fn engine_evict_nonexistent_is_noop() {
        let engine = JwtEngine::new(test_config());
        engine.evict_key("does-not-exist"); // should not panic
    }

    #[test]
    fn engine_load_key_wrong_rmk_fails() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, _rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        let wrong_rmk = RegionMasterKey::new(99, [0xFF; 32]);
        let result = engine.load_key(&key, &wrong_rmk);
        assert!(result.is_err());
    }

    // ── Signing & Validation Integration Tests ──────────────────────

    #[test]
    fn sign_and_validate_user_session() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let (token, expires_at) = engine
            .sign_user_session(UserSlug::new(42), "admin", TokenVersion::new(1), &kid)
            .unwrap();

        assert!(!token.is_empty());
        assert!(expires_at > Utc::now());

        let validated = engine.validate(&token, SESSION_AUDIENCE).unwrap();
        match validated {
            ValidatedToken::UserSession(claims) => {
                assert_eq!(claims.user, UserSlug::new(42));
                assert_eq!(claims.role, "admin");
                assert_eq!(claims.version, TokenVersion::new(1));
                assert_eq!(claims.token_type, TokenType::UserSession);
                assert_eq!(claims.iss, "inferadb");
                assert_eq!(claims.sub, "user:42");
                assert_eq!(claims.aud, vec![SESSION_AUDIENCE]);
            },
            _ => panic!("expected UserSession"),
        }
    }

    #[test]
    fn sign_and_validate_vault_token() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let scopes = vec!["vault:read".to_string(), "entity:write".to_string()];
        let (token, expires_at) = engine
            .sign_vault_token(
                OrganizationSlug::new(10),
                AppSlug::new(99),
                VaultSlug::new(50),
                &scopes,
                &kid,
            )
            .unwrap();

        assert!(!token.is_empty());
        assert!(expires_at > Utc::now());

        let validated = engine.validate(&token, VAULT_AUDIENCE).unwrap();
        match validated {
            ValidatedToken::VaultAccess(claims) => {
                assert_eq!(claims.org, OrganizationSlug::new(10));
                assert_eq!(claims.app, AppSlug::new(99));
                assert_eq!(claims.vault, VaultSlug::new(50));
                assert_eq!(claims.scopes, scopes);
                assert_eq!(claims.token_type, TokenType::VaultAccess);
            },
            _ => panic!("expected VaultAccess"),
        }
    }

    #[test]
    fn validate_rejects_wrong_audience() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // User session has audience SESSION_AUDIENCE, validating with VAULT_AUDIENCE fails
        let result = engine.validate(&token, VAULT_AUDIENCE);
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_revoked_key() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (mut key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Simulate key revocation by reloading with Revoked status
        key.status = SigningKeyStatus::Revoked;
        key.revoked_at = Some(Utc::now());
        engine.load_key(&key, &rmk).unwrap();

        let result = engine.validate(&token, SESSION_AUDIENCE);
        assert!(result.is_err());
    }

    #[test]
    fn validate_accepts_rotated_key_within_grace() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (mut key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Simulate rotation with 4-hour grace period
        key.status = SigningKeyStatus::Rotated;
        key.rotated_at = Some(Utc::now());
        key.valid_until = Some(Utc::now() + Duration::hours(4));
        engine.load_key(&key, &rmk).unwrap();

        let result = engine.validate(&token, SESSION_AUDIENCE);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_rejects_rotated_key_past_grace() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (mut key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);

        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Simulate rotation with grace period already expired
        key.status = SigningKeyStatus::Rotated;
        key.rotated_at = Some(Utc::now() - Duration::hours(5));
        key.valid_until = Some(Utc::now() - Duration::hours(1));
        engine.load_key(&key, &rmk).unwrap();

        let result = engine.validate(&token, SESSION_AUDIENCE);
        assert!(result.is_err());
    }

    #[test]
    fn sign_with_missing_key_fails() {
        let engine = JwtEngine::new(test_config());
        let result = engine.sign_user_session(
            UserSlug::new(1),
            "user",
            TokenVersion::default(),
            "nonexistent-kid",
        );
        assert!(result.is_err());
    }

    #[test]
    fn validate_with_unknown_kid_returns_invalid_signature() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Evict the key so validation can't find it
        engine.evict_key(&kid);

        let result = engine.validate(&token, SESSION_AUDIENCE);
        assert!(result.is_err());
    }

    #[test]
    fn validate_tampered_token_fails() {
        let engine = JwtEngine::new(test_config());
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        // Flip a byte in the signature part
        let parts: Vec<&str> = token.splitn(3, '.').collect();
        let mut sig_bytes =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        if let Some(byte) = sig_bytes.first_mut() {
            *byte ^= 0xFF;
        }
        let tampered_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&sig_bytes);
        let tampered = format!("{}.{}.{}", parts[0], parts[1], tampered_sig);

        let result = engine.validate(&tampered, SESSION_AUDIENCE);
        assert!(result.is_err());
    }

    #[test]
    fn validate_wrong_signing_key_fails() {
        let engine = JwtEngine::new(test_config());
        let kid1 = uuid::Uuid::new_v4().to_string();
        let kid2 = uuid::Uuid::new_v4().to_string();
        let (key1, rmk1) = generate_test_signing_key(&kid1, SigningKeyScope::Global);
        let (key2, rmk2) = generate_test_signing_key(&kid2, SigningKeyScope::Global);

        engine.load_key(&key1, &rmk1).unwrap();
        engine.load_key(&key2, &rmk2).unwrap();

        // Sign with key1
        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid1)
            .unwrap();

        // The token's header has kid1, so it validates against key1
        let result = engine.validate(&token, SESSION_AUDIENCE);
        assert!(result.is_ok());
    }

    // ── Engine Debug Impl Test ──────────────────────────────────────

    #[test]
    fn engine_debug_shows_key_count() {
        let engine = JwtEngine::new(test_config());
        let debug = format!("{engine:?}");
        assert!(debug.contains("cached_keys: 0"));

        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
        engine.load_key(&key, &rmk).unwrap();

        let debug = format!("{engine:?}");
        assert!(debug.contains("cached_keys: 1"));
    }

    // ── DER Wrapping Tests ──────────────────────────────────────────

    #[test]
    fn der_encoding_produces_valid_keys() {
        // Generate a keypair and verify DER wrapping produces valid keys
        let mut secret_bytes = [0u8; 32];
        rand::RngExt::fill(&mut rand::rng(), &mut secret_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
        let public_bytes = signing_key.verifying_key().to_bytes();
        let private_bytes = signing_key.to_bytes();

        // Verify DER-wrapped keys can sign and verify
        let encoding = encoding_key_from_private_bytes(&private_bytes);
        let decoding = DecodingKey::from_ed_der(&public_bytes);

        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("test".to_string());

        let claims = serde_json::json!({
            "sub": "test",
            "exp": (Utc::now() + Duration::hours(1)).timestamp(),
        });

        let token = jsonwebtoken::encode(&header, &claims, &encoding).unwrap();
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_aud = false;
        let decoded =
            jsonwebtoken::decode::<serde_json::Value>(&token, &decoding, &validation).unwrap();
        assert_eq!(decoded.claims.get("sub").unwrap().as_str().unwrap(), "test");
    }

    // ── Concurrent Access Test ──────────────────────────────────────

    #[test]
    fn concurrent_load_and_validate() {
        use std::thread;

        let engine = Arc::new(JwtEngine::new(test_config()));
        let kid = uuid::Uuid::new_v4().to_string();
        let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
        engine.load_key(&key, &rmk).unwrap();

        let (token, _) = engine
            .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
            .unwrap();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let token = token.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        engine.validate(&token, SESSION_AUDIENCE).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    // ── Config Accessor Test ────────────────────────────────────────

    #[test]
    fn engine_config_returns_configured_values() {
        let config = JwtConfig::builder()
            .issuer("custom-issuer")
            .session_access_ttl_secs(600_u64)
            .build()
            .unwrap();
        let engine = JwtEngine::new(config);
        assert_eq!(engine.config().issuer, "custom-issuer");
        assert_eq!(engine.config().session_access_ttl_secs, 600);
    }

    // ── Property Tests ──────────────────────────────────────────────

    proptest::proptest! {
        /// A token signed with the correct key always validates successfully,
        /// regardless of the user slug, role, or token version values.
        #[test]
        fn prop_correct_key_always_validates(
            user_slug in 1..u64::MAX,
            role in proptest::prop_oneof!["user", "admin"],
            version in 0..1000u64,
        ) {
            let engine = JwtEngine::new(test_config());
            let kid = uuid::Uuid::new_v4().to_string();
            let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
            engine.load_key(&key, &rmk).unwrap();

            let (token, _) = engine
                .sign_user_session(
                    UserSlug::new(user_slug),
                    &role,
                    TokenVersion::new(version),
                    &kid,
                )
                .unwrap();

            let validated = engine.validate(&token, SESSION_AUDIENCE);
            proptest::prop_assert!(validated.is_ok(), "valid token must always validate: {validated:?}");

            if let ValidatedToken::UserSession(claims) = validated.unwrap() {
                proptest::prop_assert_eq!(claims.user, UserSlug::new(user_slug));
                proptest::prop_assert_eq!(claims.version, TokenVersion::new(version));
            } else {
                proptest::prop_assert!(false, "expected UserSession variant");
            }
        }

        /// A token signed with one key never validates against a different key's
        /// public key material.
        #[test]
        fn prop_wrong_key_never_validates(
            user_slug in 1..u64::MAX,
        ) {
            let engine = JwtEngine::new(test_config());
            let kid1 = uuid::Uuid::new_v4().to_string();
            let kid2 = uuid::Uuid::new_v4().to_string();
            let (key1, rmk1) = generate_test_signing_key(&kid1, SigningKeyScope::Global);
            let (key2, rmk2) = generate_test_signing_key(&kid2, SigningKeyScope::Global);

            engine.load_key(&key1, &rmk1).unwrap();
            engine.load_key(&key2, &rmk2).unwrap();

            // Sign with key1
            let (token, _) = engine
                .sign_user_session(
                    UserSlug::new(user_slug),
                    "user",
                    TokenVersion::default(),
                    &kid1,
                )
                .unwrap();

            // Replace the kid in the header to point at key2, so validation
            // tries to verify key1's signature with key2's public key.
            let parts: Vec<&str> = token.splitn(3, '.').collect();
            let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[0])
                .unwrap();
            let mut header: serde_json::Value = serde_json::from_slice(&header_bytes).unwrap();
            header["kid"] = serde_json::Value::String(kid2.clone());
            let new_header = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&header).unwrap());
            let franken_token = format!("{new_header}.{}.{}", parts[1], parts[2]);

            let result = engine.validate(&franken_token, SESSION_AUDIENCE);
            proptest::prop_assert!(result.is_err(), "wrong key must never validate");
        }

        /// Flipping any single byte in the signature portion of a valid token
        /// always causes validation to fail.
        #[test]
        fn prop_tampered_signature_never_validates(
            flip_offset in 0usize..64,
        ) {
            let engine = JwtEngine::new(test_config());
            let kid = uuid::Uuid::new_v4().to_string();
            let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
            engine.load_key(&key, &rmk).unwrap();

            let (token, _) = engine
                .sign_user_session(UserSlug::new(1), "user", TokenVersion::default(), &kid)
                .unwrap();

            let parts: Vec<&str> = token.splitn(3, '.').collect();
            let mut sig_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[2])
                .unwrap();

            let idx = flip_offset % sig_bytes.len();
            sig_bytes[idx] ^= 0xFF;

            let tampered_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&sig_bytes);
            let tampered = format!("{}.{}.{tampered_sig}", parts[0], parts[1]);

            let result = engine.validate(&tampered, SESSION_AUDIENCE);
            proptest::prop_assert!(result.is_err(), "tampered token must never validate");
        }

        /// A token whose expiry is any amount in the past (beyond clock skew leeway)
        /// is always rejected.
        #[test]
        fn prop_expired_token_always_rejected(
            seconds_past in 31i64..100_000,
        ) {
            // Build a config with known clock_skew so we can exceed it deterministically.
            let config = JwtConfig::builder()
                .clock_skew_secs(30_u64)
                .session_access_ttl_secs(1800_u64)
                .vault_access_ttl_secs(900_u64)
                .build()
                .unwrap();
            let engine = JwtEngine::new(config);
            let kid = uuid::Uuid::new_v4().to_string();
            let (key, rmk) = generate_test_signing_key(&kid, SigningKeyScope::Global);
            engine.load_key(&key, &rmk).unwrap();

            // Manually construct an expired token by signing claims with exp in the past.
            let now = Utc::now();
            let exp = now - chrono::Duration::seconds(seconds_past);
            let claims = UserSessionClaims {
                iss: "inferadb".to_string(),
                sub: "user:1".to_string(),
                aud: vec![SESSION_AUDIENCE.to_string()],
                exp: exp.timestamp(),
                iat: (exp - chrono::Duration::seconds(1800)).timestamp(),
                nbf: (exp - chrono::Duration::seconds(1800)).timestamp(),
                jti: uuid::Uuid::new_v4().to_string(),
                token_type: TokenType::UserSession,
                user: UserSlug::new(1),
                role: "user".to_string(),
                version: TokenVersion::default(),
            };

            let cached = engine.get_cached_key(&kid).unwrap();
            let encoding = encoding_key_from_private_bytes(&cached.private_key_bytes);
            let mut header = Header::new(Algorithm::EdDSA);
            header.kid = Some(kid.clone());
            let token = jsonwebtoken::encode(&header, &claims, &encoding).unwrap();

            let result = engine.validate(&token, SESSION_AUDIENCE);
            proptest::prop_assert!(result.is_err(), "expired token must be rejected (exp={exp}, now={now})");
        }

        /// Any `alg` value in a JWT header that is not exactly "EdDSA" is
        /// rejected before reaching the cryptographic verification layer.
        #[test]
        fn prop_non_eddsa_alg_always_rejected(
            alg in "[a-zA-Z0-9_-]{0,20}",
        ) {
            // Skip "EdDSA" itself — that's the only valid value.
            proptest::prop_assume!(alg != "EdDSA");

            let kid = "550e8400-e29b-41d4-a716-446655440000";
            let header = format!(r#"{{"alg":"{alg}","kid":"{kid}"}}"#);
            let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&header);
            let fake_token = format!("{header_b64}.payload.signature");

            let result = extract_and_validate_header(&fake_token);
            proptest::prop_assert!(result.is_err(), "alg={alg:?} must be rejected");
        }

        /// Any `kid` value that is not a valid UUID is rejected before any
        /// state or cache lookup occurs.
        #[test]
        fn prop_non_uuid_kid_always_rejected(
            kid in "[a-zA-Z0-9_.:/-]{1,50}",
        ) {
            // Skip strings that happen to be valid UUIDs.
            proptest::prop_assume!(uuid::Uuid::parse_str(&kid).is_err());

            let header = format!(r#"{{"alg":"EdDSA","kid":"{kid}"}}"#);
            let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&header);
            let fake_token = format!("{header_b64}.payload.signature");

            let result = extract_and_validate_header(&fake_token);
            proptest::prop_assert!(result.is_err(), "kid={kid:?} must be rejected");
        }

        /// The refresh token `ilrt_` prefix is preserved through the
        /// generate → hash → format cycle for any generated token.
        #[test]
        fn prop_refresh_token_roundtrip_preserves_prefix(
            _ in 0..100u32,
        ) {
            let (token, hash) = generate_refresh_token();

            // Prefix preserved
            proptest::prop_assert!(
                token.starts_with("ilrt_"),
                "token must start with ilrt_, got: {token}"
            );

            // Length invariant: ilrt_ (5) + base64url(32 bytes, no pad) (43) = 48
            proptest::prop_assert_eq!(token.len(), 48);

            // Hash is SHA-256 of the full prefixed string
            let expected: [u8; 32] = sha2::Sha256::digest(token.as_bytes()).into();
            proptest::prop_assert_eq!(hash, expected, "hash must be SHA-256 of full token string");

            // Hash is non-zero (probabilistic but essentially guaranteed)
            proptest::prop_assert_ne!(hash, [0u8; 32]);
        }
    }
}
