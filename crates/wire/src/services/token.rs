//! Wire types for `TokenService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    AppSlug, CredentialInfo, OrganizationSlug, TokenPair, UserSlug, VaultSlug,
};

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SigningKeyScope {
    Unspecified = 0,
    Global = 1,
    Organization = 2,
}

// ---------------------------------------------------------------------------
// User session
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserSessionRequest {
    pub user: Option<UserSlug>,
    pub credential_used: Option<CredentialInfo>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserSessionResponse {
    pub tokens: Option<TokenPair>,
}

// ---------------------------------------------------------------------------
// Token validation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValidateTokenRequest {
    pub token: String,
    pub expected_audience: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValidateTokenResponse {
    pub subject: String,
    pub token_type: String,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    pub claims: Option<ValidateTokenClaims>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValidateTokenClaims {
    UserSession(UserSessionClaims),
    VaultAccess(VaultAccessClaims),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserSessionClaims {
    pub user_slug: u64,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VaultAccessClaims {
    pub org_slug: u64,
    pub app_slug: u64,
    pub vault_slug: u64,
    pub scopes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Vault token
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateVaultTokenRequest {
    pub organization: Option<OrganizationSlug>,
    pub app: Option<AppSlug>,
    pub vault: Option<VaultSlug>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateVaultTokenResponse {
    pub tokens: Option<TokenPair>,
}

// ---------------------------------------------------------------------------
// Client assertion authentication
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthenticateClientAssertionRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub assertion_jwt: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthenticateClientAssertionResponse {
    pub tokens: Option<TokenPair>,
}

// ---------------------------------------------------------------------------
// Refresh / revoke
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefreshTokenResponse {
    pub tokens: Option<TokenPair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeTokenResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeAllUserSessionsRequest {
    pub user: Option<UserSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeAllUserSessionsResponse {
    pub revoked_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeAllAppSessionsRequest {
    pub organization: Option<OrganizationSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeAllAppSessionsResponse {
    pub revoked_count: u64,
}

// ---------------------------------------------------------------------------
// Signing key management
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateSigningKeyRequest {
    pub scope: SigningKeyScope,
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateSigningKeyResponse {
    pub key: Option<PublicKeyInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateSigningKeyRequest {
    pub kid: String,
    pub grace_period_secs: u64,
    pub force_revoke: bool,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateSigningKeyResponse {
    pub new_key: Option<PublicKeyInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeSigningKeyRequest {
    pub kid: String,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokeSigningKeyResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetPublicKeysRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetPublicKeysResponse {
    pub keys: Vec<PublicKeyInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublicKeyInfo {
    pub kid: String,
    /// 32-byte Ed25519 public key.
    pub public_key: Bytes,
    pub status: String,
    /// UNIX nanoseconds.
    pub valid_from: u64,
    /// UNIX nanoseconds.
    pub valid_until: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn validate_token_response_with_user_session_claims_roundtrips() {
        let resp = ValidateTokenResponse {
            subject: "user:42".to_owned(),
            token_type: "user_session".to_owned(),
            expires_at: 1_700_000_000_000_000_000,
            claims: Some(ValidateTokenClaims::UserSession(UserSessionClaims {
                user_slug: 42,
                role: "admin".to_owned(),
            })),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: ValidateTokenResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }
}
