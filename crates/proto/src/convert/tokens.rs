//! Token conversions: `ValidatedToken` <-> proto `ValidateTokenResponse`.

use inferadb_ledger_types::{
    AppSlug as DomainAppSlug, OrganizationSlug, UserSlug, VaultSlug,
    token::{
        TokenType, UserSessionClaims as DomainUserSessionClaims, ValidatedToken,
        VaultTokenClaims as DomainVaultTokenClaims,
    },
};
use tonic::Status;

use super::domain::datetime_from_epoch_secs;
use crate::proto;

// =============================================================================
// ValidatedToken conversions (domain::ValidatedToken -> proto::ValidateTokenResponse)
// =============================================================================

/// Converts a domain [`ValidatedToken`] to its protobuf
/// [`ValidateTokenResponse`](proto::ValidateTokenResponse).
///
/// Dispatches on the `ValidatedToken` variant to populate the `oneof claims` field
/// and set the `token_type` / `subject` / `expires_at` from the underlying claims.
impl From<ValidatedToken> for proto::ValidateTokenResponse {
    fn from(validated: ValidatedToken) -> Self {
        match validated {
            ValidatedToken::UserSession(claims) => proto::ValidateTokenResponse {
                subject: claims.sub.clone(),
                token_type: TokenType::UserSession.to_string(),
                expires_at: Some(datetime_from_epoch_secs(claims.exp)),
                claims: Some(proto::validate_token_response::Claims::UserSession(
                    proto::UserSessionClaims {
                        user_slug: claims.user.value(),
                        role: claims.role.clone(),
                    },
                )),
            },
            ValidatedToken::VaultAccess(claims) => proto::ValidateTokenResponse {
                subject: claims.sub.clone(),
                token_type: TokenType::VaultAccess.to_string(),
                expires_at: Some(datetime_from_epoch_secs(claims.exp)),
                claims: Some(proto::validate_token_response::Claims::VaultAccess(
                    proto::VaultAccessClaims {
                        org_slug: claims.org.value(),
                        app_slug: claims.app.value(),
                        vault_slug: claims.vault.value(),
                        scopes: claims.scopes.clone(),
                    },
                )),
            },
        }
    }
}

/// Converts a [`ValidateTokenResponse`](proto::ValidateTokenResponse) to its domain
/// [`ValidatedToken`].
///
/// Returns `INVALID_ARGUMENT` when:
/// - The `claims` oneof is absent
/// - The `token_type` string is unrecognized
/// - Required timestamp fields are missing
impl TryFrom<proto::ValidateTokenResponse> for ValidatedToken {
    type Error = Status;

    fn try_from(resp: proto::ValidateTokenResponse) -> Result<Self, Status> {
        let claims = resp
            .claims
            .ok_or_else(|| Status::invalid_argument("validate token response missing claims"))?;

        let expires_at = resp.expires_at.ok_or_else(|| {
            Status::invalid_argument("validate token response missing expires_at")
        })?;
        let exp = expires_at.seconds;

        match claims {
            proto::validate_token_response::Claims::UserSession(c) => {
                Ok(ValidatedToken::UserSession(DomainUserSessionClaims {
                    iss: String::new(),
                    sub: resp.subject,
                    aud: Vec::new(),
                    exp,
                    iat: 0,
                    nbf: 0,
                    jti: String::new(),
                    token_type: TokenType::UserSession,
                    user: UserSlug::new(c.user_slug),
                    role: c.role,
                    version: Default::default(),
                }))
            },
            proto::validate_token_response::Claims::VaultAccess(c) => {
                Ok(ValidatedToken::VaultAccess(DomainVaultTokenClaims {
                    iss: String::new(),
                    sub: resp.subject,
                    aud: Vec::new(),
                    exp,
                    iat: 0,
                    nbf: 0,
                    jti: String::new(),
                    token_type: TokenType::VaultAccess,
                    org: OrganizationSlug::new(c.org_slug),
                    app: DomainAppSlug::new(c.app_slug),
                    vault: VaultSlug::new(c.vault_slug),
                    scopes: c.scopes,
                }))
            },
        }
    }
}
