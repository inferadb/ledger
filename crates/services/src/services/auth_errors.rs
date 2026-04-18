//! Unified authentication error responses.
//!
//! Auth failures must return **identical** client-facing errors regardless of
//! the underlying reason, to prevent enumeration attacks. Different messages
//! for "user not found", "wrong password", "account disabled", "key revoked",
//! etc. let attackers distinguish account states.
//!
//! This module centralizes auth failure responses. The detailed reason is
//! logged via structured `tracing` fields — never exposed to the client.
//!
//! # Usage
//!
//! ```no_run
//! use inferadb_ledger_services::services::auth_errors::{unified_auth_error, AuthFailureReason};
//!
//! // Instead of: Status::unauthenticated("User not found")
//! let err = unified_auth_error(AuthFailureReason::UserNotFound);
//! ```

use tonic::Status;

/// Categorizes why an authentication failed. The reason is logged server-side
/// but **never** included in the client-facing message.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AuthFailureReason {
    /// Email address does not correspond to a registered user.
    UserNotFound,
    /// User exists but account is not active (suspended, disabled, deleted).
    UserInactive,
    /// Session was invalidated (token version bumped after revocation).
    SessionInvalidated,
    /// Refresh token is unknown, malformed, or has been poisoned.
    InvalidRefreshToken,
    /// Signing key referenced by the token has been revoked.
    SigningKeyRevoked,
    /// Signing key referenced by the token is rotated past its grace period.
    SigningKeyExpired,
    /// App + organization pair does not exist.
    AppNotFound,
    /// Client assertion credential entry does not exist for the given kid.
    ClientAssertionUnknown,
    /// Client assertion credential entry is disabled.
    ClientAssertionDisabled,
    /// Client assertion credential entry has expired.
    ClientAssertionExpired,
    /// Client assertion JWT is malformed or fails signature verification.
    ClientAssertionInvalid,
    /// Onboarding token is missing or invalid.
    OnboardingTokenInvalid,
    /// Onboarding record not found (email was never verified).
    OnboardingRecordMissing,
}

impl std::fmt::Display for AuthFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::UserNotFound => "user_not_found",
            Self::UserInactive => "user_inactive",
            Self::SessionInvalidated => "session_invalidated",
            Self::InvalidRefreshToken => "invalid_refresh_token",
            Self::SigningKeyRevoked => "signing_key_revoked",
            Self::SigningKeyExpired => "signing_key_expired",
            Self::AppNotFound => "app_not_found",
            Self::ClientAssertionUnknown => "client_assertion_unknown",
            Self::ClientAssertionDisabled => "client_assertion_disabled",
            Self::ClientAssertionExpired => "client_assertion_expired",
            Self::ClientAssertionInvalid => "client_assertion_invalid",
            Self::OnboardingTokenInvalid => "onboarding_token_invalid",
            Self::OnboardingRecordMissing => "onboarding_record_missing",
        };
        f.write_str(s)
    }
}

/// Returns a unified `Status::unauthenticated` response with a generic message.
///
/// The detailed `reason` is logged via `tracing` at `info` level (not `debug`
/// — auth failures are operationally relevant) but is **not** returned to the
/// client. This prevents user/app/credential enumeration.
pub(crate) fn unified_auth_error(reason: AuthFailureReason) -> Status {
    tracing::info!(auth_failure_reason = %reason, "Authentication failed");
    Status::unauthenticated("Authentication failed")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn unified_auth_error_returns_identical_message_regardless_of_reason() {
        let msg_user_not_found =
            unified_auth_error(AuthFailureReason::UserNotFound).message().to_string();
        let msg_user_inactive =
            unified_auth_error(AuthFailureReason::UserInactive).message().to_string();
        let msg_session_invalid =
            unified_auth_error(AuthFailureReason::SessionInvalidated).message().to_string();
        let msg_key_revoked =
            unified_auth_error(AuthFailureReason::SigningKeyRevoked).message().to_string();
        let msg_assertion_unknown =
            unified_auth_error(AuthFailureReason::ClientAssertionUnknown).message().to_string();
        let msg_assertion_disabled =
            unified_auth_error(AuthFailureReason::ClientAssertionDisabled).message().to_string();

        assert_eq!(msg_user_not_found, msg_user_inactive);
        assert_eq!(msg_user_inactive, msg_session_invalid);
        assert_eq!(msg_session_invalid, msg_key_revoked);
        assert_eq!(msg_key_revoked, msg_assertion_unknown);
        assert_eq!(msg_assertion_unknown, msg_assertion_disabled);
    }

    #[test]
    fn unified_auth_error_returns_unauthenticated_code() {
        let err = unified_auth_error(AuthFailureReason::UserNotFound);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn auth_failure_reason_display_is_stable_snake_case() {
        assert_eq!(AuthFailureReason::UserNotFound.to_string(), "user_not_found");
        assert_eq!(
            AuthFailureReason::ClientAssertionDisabled.to_string(),
            "client_assertion_disabled"
        );
    }
}
