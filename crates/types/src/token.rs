//! JWT token types for user sessions and vault access.
//!
//! Domain-level types only — no dependency on `jsonwebtoken` or other
//! crypto crates. JWT library errors live in `services/src/jwt.rs`.

use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::types::{AppSlug, OrganizationSlug, TokenVersion, UserSlug, VaultSlug};

// ─── Audience Constants ──────────────────────────────────────────────

/// User session tokens are consumed by the control plane.
pub const SESSION_AUDIENCE: &str = "inferadb-control";

/// Vault access tokens are consumed by the engine service.
pub const VAULT_AUDIENCE: &str = "inferadb-engine";

// ─── Envelope Constants ──────────────────────────────────────────────

/// Fixed size of a [`SigningKeyEnvelope`] in bytes.
///
/// Layout: `wrapped_dek(40) + nonce(12) + ciphertext(32) + auth_tag(16)`.
pub const SIGNING_KEY_ENVELOPE_SIZE: usize = 40 + 12 + 32 + 16;

// ─── Token Type ──────────────────────────────────────────────────────

/// Discriminator for the two token types issued by Ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenType {
    /// Short-lived user session token consumed by Control for RBAC.
    UserSession,
    /// Short-lived vault access token consumed by Engine for data ops.
    VaultAccess,
}

impl std::fmt::Display for TokenType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserSession => write!(f, "user_session"),
            Self::VaultAccess => write!(f, "vault_access"),
        }
    }
}

// ─── Token Subject ───────────────────────────────────────────────────

/// Refresh token subject disambiguation.
///
/// Identifies whether a refresh token belongs to a user session or an
/// app's vault access grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenSubject {
    /// User session refresh token.
    User(UserSlug),
    /// App vault access refresh token.
    App(AppSlug),
}

impl std::fmt::Display for TokenSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User(slug) => write!(f, "user:{slug}"),
            Self::App(slug) => write!(f, "app:{slug}"),
        }
    }
}

// ─── JWT Claims ──────────────────────────────────────────────────────

/// Claims embedded in a user session JWT.
///
/// Identity-only: user + role. No org membership — Control resolves
/// that separately per request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSessionClaims {
    /// Issuer (always `"inferadb"`).
    pub iss: String,
    /// Subject in `"user:{user_slug}"` format.
    pub sub: String,
    /// Audience (always `[SESSION_AUDIENCE]`).
    pub aud: Vec<String>,
    /// Expiration (seconds since epoch).
    pub exp: i64,
    /// Issued-at (seconds since epoch).
    pub iat: i64,
    /// Not-before (seconds since epoch, set to `iat`).
    pub nbf: i64,
    /// Unique token ID (UUID).
    pub jti: String,
    /// Token type discriminator.
    #[serde(rename = "type")]
    pub token_type: TokenType,
    /// User slug.
    pub user: UserSlug,
    /// User role (`"user"` or `"admin"`).
    pub role: String,
    /// Forced invalidation counter.
    pub version: TokenVersion,
}

/// Claims embedded in a vault access JWT.
///
/// Scoped to a single app+vault combination with explicit permission grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultTokenClaims {
    /// Issuer (always `"inferadb"`).
    pub iss: String,
    /// Subject in `"app:{app_slug}"` format.
    pub sub: String,
    /// Audience (always `[VAULT_AUDIENCE]`).
    pub aud: Vec<String>,
    /// Expiration (seconds since epoch).
    pub exp: i64,
    /// Issued-at (seconds since epoch).
    pub iat: i64,
    /// Not-before (seconds since epoch, set to `iat`).
    pub nbf: i64,
    /// Unique token ID (UUID).
    pub jti: String,
    /// Token type discriminator.
    #[serde(rename = "type")]
    pub token_type: TokenType,
    /// Organization slug.
    pub org: OrganizationSlug,
    /// Application slug.
    pub app: AppSlug,
    /// Vault slug.
    pub vault: VaultSlug,
    /// Granted scopes (e.g. `["vault:read", "entity:write"]`).
    pub scopes: Vec<String>,
}

/// Validated token with parsed claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedToken {
    /// Validated user session with claims.
    UserSession(UserSessionClaims),
    /// Validated vault access with claims.
    VaultAccess(VaultTokenClaims),
}

// ─── Token Errors ────────────────────────────────────────────────────

/// Domain-level token errors.
///
/// Covers validation failures and signing key lookup errors.
/// Crypto/JWT library errors live in `services::jwt::JwtError`.
#[derive(Debug, Snafu)]
pub enum TokenError {
    /// Token has expired.
    #[snafu(display("Token expired"))]
    Expired,

    /// Token signature verification failed.
    #[snafu(display("Invalid token signature"))]
    InvalidSignature,

    /// Token audience does not match the expected service.
    #[snafu(display("Invalid audience: expected {expected}"))]
    InvalidAudience {
        /// Expected audience value.
        expected: String,
    },

    /// A required JWT claim is missing.
    #[snafu(display("Missing required claim: {claim}"))]
    MissingClaim {
        /// Name of the missing claim.
        claim: String,
    },

    /// Token type does not match the expected type.
    #[snafu(display("Invalid token type: expected {expected}"))]
    InvalidTokenType {
        /// Expected token type.
        expected: String,
    },

    /// Signing key not found by kid.
    #[snafu(display("Signing key not found: {kid}"))]
    SigningKeyNotFound {
        /// Key identifier.
        kid: String,
    },

    /// Signing key has expired past its grace period.
    #[snafu(display("Signing key expired: {kid}"))]
    SigningKeyExpired {
        /// Key identifier.
        kid: String,
    },
}

// ─── Signing Key Envelope ────────────────────────────────────────────

/// Fixed-size envelope for a signing key's encrypted private key.
///
/// Uses envelope encryption: a per-key DEK (wrapped by the Region Master
/// Key via AES-KWP) encrypts the Ed25519 private key with AES-256-GCM.
/// The `kid` is used as AAD during encryption to bind ciphertext to key
/// identity, preventing key-blob swapping attacks.
///
/// Layout: `wrapped_dek(40) + nonce(12) + ciphertext(32) + auth_tag(16) = 100 bytes`.
#[derive(Debug, Clone)]
pub struct SigningKeyEnvelope {
    /// AES-KWP wrapped data encryption key (40 bytes).
    pub wrapped_dek: [u8; 40],
    /// AES-256-GCM nonce (12 bytes).
    pub nonce: [u8; 12],
    /// Encrypted Ed25519 private key (32 bytes).
    pub ciphertext: [u8; 32],
    /// GCM authentication tag (16 bytes).
    pub auth_tag: [u8; 16],
}

impl SigningKeyEnvelope {
    /// Serializes the envelope to a fixed-size byte array.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; SIGNING_KEY_ENVELOPE_SIZE] {
        let mut buf = [0u8; SIGNING_KEY_ENVELOPE_SIZE];
        buf[..40].copy_from_slice(&self.wrapped_dek);
        buf[40..52].copy_from_slice(&self.nonce);
        buf[52..84].copy_from_slice(&self.ciphertext);
        buf[84..100].copy_from_slice(&self.auth_tag);
        buf
    }

    /// Deserializes an envelope from a fixed-size byte array.
    #[must_use]
    pub fn from_bytes(buf: &[u8; SIGNING_KEY_ENVELOPE_SIZE]) -> Self {
        let mut wrapped_dek = [0u8; 40];
        let mut nonce = [0u8; 12];
        let mut ciphertext = [0u8; 32];
        let mut auth_tag = [0u8; 16];

        wrapped_dek.copy_from_slice(&buf[..40]);
        nonce.copy_from_slice(&buf[40..52]);
        ciphertext.copy_from_slice(&buf[52..84]);
        auth_tag.copy_from_slice(&buf[84..100]);

        Self { wrapped_dek, nonce, ciphertext, auth_tag }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ── TokenType ────────────────────────────────────────────────────

    #[test]
    fn token_type_serde_roundtrip() {
        let json = serde_json::to_string(&TokenType::UserSession).unwrap();
        assert_eq!(json, "\"user_session\"");
        let parsed: TokenType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TokenType::UserSession);

        let json = serde_json::to_string(&TokenType::VaultAccess).unwrap();
        assert_eq!(json, "\"vault_access\"");
        let parsed: TokenType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TokenType::VaultAccess);
    }

    #[test]
    fn token_type_display() {
        assert_eq!(TokenType::UserSession.to_string(), "user_session");
        assert_eq!(TokenType::VaultAccess.to_string(), "vault_access");
    }

    // ── TokenSubject ─────────────────────────────────────────────────

    #[test]
    fn token_subject_display() {
        let user = TokenSubject::User(UserSlug::new(42));
        assert_eq!(user.to_string(), "user:42");

        let app = TokenSubject::App(AppSlug::new(99));
        assert_eq!(app.to_string(), "app:99");
    }

    #[test]
    fn token_subject_serde_roundtrip() {
        let subject = TokenSubject::User(UserSlug::new(123));
        let json = serde_json::to_string(&subject).unwrap();
        let parsed: TokenSubject = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, subject);

        let subject = TokenSubject::App(AppSlug::new(456));
        let json = serde_json::to_string(&subject).unwrap();
        let parsed: TokenSubject = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, subject);
    }

    // ── UserSessionClaims ────────────────────────────────────────────

    #[test]
    fn user_session_claims_serde_roundtrip() {
        let claims = UserSessionClaims {
            iss: "inferadb".to_string(),
            sub: "user:42".to_string(),
            aud: vec![SESSION_AUDIENCE.to_string()],
            exp: 1700000000,
            iat: 1699998200,
            nbf: 1699998200,
            jti: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            token_type: TokenType::UserSession,
            user: UserSlug::new(42),
            role: "admin".to_string(),
            version: TokenVersion::new(3),
        };

        let json = serde_json::to_string(&claims).unwrap();
        // Verify the serde rename: field is serialized as "type"
        assert!(json.contains("\"type\":\"user_session\""));
        assert!(!json.contains("\"token_type\""));

        let parsed: UserSessionClaims = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.user, claims.user);
        assert_eq!(parsed.role, claims.role);
        assert_eq!(parsed.version, claims.version);
        assert_eq!(parsed.token_type, TokenType::UserSession);
    }

    // ── VaultTokenClaims ─────────────────────────────────────────────

    #[test]
    fn vault_token_claims_serde_roundtrip() {
        let claims = VaultTokenClaims {
            iss: "inferadb".to_string(),
            sub: "app:99".to_string(),
            aud: vec![VAULT_AUDIENCE.to_string()],
            exp: 1700000000,
            iat: 1699999100,
            nbf: 1699999100,
            jti: "660e8400-e29b-41d4-a716-446655440001".to_string(),
            token_type: TokenType::VaultAccess,
            org: OrganizationSlug::new(10),
            app: AppSlug::new(99),
            vault: VaultSlug::new(50),
            scopes: vec!["vault:read".to_string(), "entity:write".to_string()],
        };

        let json = serde_json::to_string(&claims).unwrap();
        assert!(json.contains("\"type\":\"vault_access\""));

        let parsed: VaultTokenClaims = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.org, claims.org);
        assert_eq!(parsed.app, claims.app);
        assert_eq!(parsed.vault, claims.vault);
        assert_eq!(parsed.scopes, claims.scopes);
    }

    // ── TokenError ───────────────────────────────────────────────────

    #[test]
    fn token_error_display() {
        let err = TokenError::Expired;
        assert_eq!(err.to_string(), "Token expired");

        let err = TokenError::InvalidAudience { expected: "inferadb-control".to_string() };
        assert_eq!(err.to_string(), "Invalid audience: expected inferadb-control");

        let err = TokenError::SigningKeyNotFound { kid: "abc-123".to_string() };
        assert_eq!(err.to_string(), "Signing key not found: abc-123");

        let err = TokenError::SigningKeyExpired { kid: "k-456".to_string() };
        assert_eq!(err.to_string(), "Signing key expired: k-456");
    }

    #[test]
    fn all_token_error_variants_display() {
        let variants: Vec<TokenError> = vec![
            TokenError::Expired,
            TokenError::InvalidSignature,
            TokenError::InvalidAudience { expected: "x".into() },
            TokenError::MissingClaim { claim: "sub".into() },
            TokenError::InvalidTokenType { expected: "user_session".into() },
            TokenError::SigningKeyNotFound { kid: "k".into() },
            TokenError::SigningKeyExpired { kid: "k".into() },
        ];
        for err in &variants {
            assert!(!err.to_string().is_empty());
        }
    }

    // ── SigningKeyEnvelope ────────────────────────────────────────────

    #[test]
    fn envelope_size_constant() {
        assert_eq!(SIGNING_KEY_ENVELOPE_SIZE, 100);
    }

    #[test]
    fn envelope_roundtrip() {
        let envelope = SigningKeyEnvelope {
            wrapped_dek: [0xAA; 40],
            nonce: [0xBB; 12],
            ciphertext: [0xCC; 32],
            auth_tag: [0xDD; 16],
        };

        let bytes = envelope.to_bytes();
        assert_eq!(bytes.len(), SIGNING_KEY_ENVELOPE_SIZE);

        let restored = SigningKeyEnvelope::from_bytes(&bytes);
        assert_eq!(restored.wrapped_dek, [0xAA; 40]);
        assert_eq!(restored.nonce, [0xBB; 12]);
        assert_eq!(restored.ciphertext, [0xCC; 32]);
        assert_eq!(restored.auth_tag, [0xDD; 16]);
    }

    #[test]
    fn envelope_byte_layout() {
        let envelope = SigningKeyEnvelope {
            wrapped_dek: {
                let mut arr = [0u8; 40];
                arr[0] = 1;
                arr[39] = 2;
                arr
            },
            nonce: {
                let mut arr = [0u8; 12];
                arr[0] = 3;
                arr[11] = 4;
                arr
            },
            ciphertext: {
                let mut arr = [0u8; 32];
                arr[0] = 5;
                arr[31] = 6;
                arr
            },
            auth_tag: {
                let mut arr = [0u8; 16];
                arr[0] = 7;
                arr[15] = 8;
                arr
            },
        };

        let bytes = envelope.to_bytes();
        // wrapped_dek boundaries
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[39], 2);
        // nonce boundaries
        assert_eq!(bytes[40], 3);
        assert_eq!(bytes[51], 4);
        // ciphertext boundaries
        assert_eq!(bytes[52], 5);
        assert_eq!(bytes[83], 6);
        // auth_tag boundaries
        assert_eq!(bytes[84], 7);
        assert_eq!(bytes[99], 8);
    }

    // ── ValidatedToken ───────────────────────────────────────────────

    #[test]
    fn validated_token_variants() {
        let user_claims = UserSessionClaims {
            iss: "inferadb".to_string(),
            sub: "user:1".to_string(),
            aud: vec![SESSION_AUDIENCE.to_string()],
            exp: 0,
            iat: 0,
            nbf: 0,
            jti: "id".to_string(),
            token_type: TokenType::UserSession,
            user: UserSlug::new(1),
            role: "user".to_string(),
            version: TokenVersion::default(),
        };

        let validated = ValidatedToken::UserSession(user_claims);
        assert!(matches!(validated, ValidatedToken::UserSession(_)));
    }

    // ── Constants ────────────────────────────────────────────────────

    #[test]
    fn audience_constants() {
        assert_eq!(SESSION_AUDIENCE, "inferadb-control");
        assert_eq!(VAULT_AUDIENCE, "inferadb-engine");
    }
}
