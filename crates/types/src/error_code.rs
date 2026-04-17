//! Structured error codes for Raft state machine responses.
//!
//! Replaces string-based error matching (`message.contains("already exists")`)
//! with typed enum variants that service and SDK layers can match on.

use serde::{Deserialize, Serialize};

/// Error code returned by the Raft state machine in `LedgerResponse::Error`.
///
/// Service layers match on this code instead of parsing error message strings.
/// Each variant maps to a specific gRPC status code at the service boundary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Entity not found (organization, vault, user, team, etc.).
    /// Maps to `NOT_FOUND`.
    NotFound,

    /// Entity already exists (duplicate name, slug collision, etc.).
    /// Maps to `ALREADY_EXISTS`.
    AlreadyExists,

    /// Operation violates a precondition (wrong state, dependency exists, etc.).
    /// Maps to `FAILED_PRECONDITION`.
    FailedPrecondition,

    /// Caller lacks permission for this operation.
    /// Maps to `PERMISSION_DENIED`.
    PermissionDenied,

    /// Invalid input (bad name, missing field, etc.).
    /// Maps to `INVALID_ARGUMENT`.
    InvalidArgument,

    /// Internal error (storage failure, serialization error, etc.).
    /// Maps to `INTERNAL`.
    #[default]
    Internal,

    /// Authentication failure (invalid token, expired, revoked, bad signature).
    /// Maps to `UNAUTHENTICATED`.
    Unauthenticated,

    /// Rate limit exceeded (too many requests in the time window).
    /// Maps to `RESOURCE_EXHAUSTED`.
    RateLimited,

    /// Resource has expired (verification code, onboarding token, etc.).
    /// Maps to `FAILED_PRECONDITION`.
    Expired,

    /// Too many failed attempts (code verification, authentication, etc.).
    /// Maps to `FAILED_PRECONDITION`.
    TooManyAttempts,

    /// Invitation rate limit exceeded (per-user, per-org, per-email, or cooldown).
    /// Maps to `RESOURCE_EXHAUSTED`.
    InvitationRateLimited,

    /// Invitation is no longer Pending (already accepted, declined, expired, or revoked).
    /// Maps to `FAILED_PRECONDITION`.
    InvitationAlreadyResolved,

    /// User's email does not match invitee.
    /// Maps to `NOT_FOUND` (privacy: avoids confirming invitation existence).
    InvitationEmailMismatch,

    /// Invitee email belongs to an existing member of the inviting organization.
    /// Maps to `ALREADY_EXISTS`.
    InvitationAlreadyMember,

    /// A Pending invitation already exists for this org+email combination.
    /// Maps to `ALREADY_EXISTS`.
    InvitationDuplicatePending,

    /// Vault routing changed between slug resolution and proposal submission.
    /// Maps to `FAILED_PRECONDITION`.
    StaleRouting,
}

impl ErrorCode {
    /// Returns the canonical gRPC status code name for this error code.
    pub const fn grpc_code_name(self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::Internal => "INTERNAL",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::RateLimited | Self::InvitationRateLimited => "RESOURCE_EXHAUSTED",
            Self::Expired | Self::TooManyAttempts | Self::InvitationAlreadyResolved => {
                "FAILED_PRECONDITION"
            },
            Self::InvitationEmailMismatch => "NOT_FOUND",
            Self::InvitationAlreadyMember | Self::InvitationDuplicatePending => "ALREADY_EXISTS",
            Self::StaleRouting => "FAILED_PRECONDITION",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.grpc_code_name())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn grpc_code_name_maps_every_variant() {
        let cases = [
            (ErrorCode::NotFound, "NOT_FOUND"),
            (ErrorCode::AlreadyExists, "ALREADY_EXISTS"),
            (ErrorCode::FailedPrecondition, "FAILED_PRECONDITION"),
            (ErrorCode::PermissionDenied, "PERMISSION_DENIED"),
            (ErrorCode::InvalidArgument, "INVALID_ARGUMENT"),
            (ErrorCode::Internal, "INTERNAL"),
            (ErrorCode::Unauthenticated, "UNAUTHENTICATED"),
            (ErrorCode::RateLimited, "RESOURCE_EXHAUSTED"),
            (ErrorCode::Expired, "FAILED_PRECONDITION"),
            (ErrorCode::TooManyAttempts, "FAILED_PRECONDITION"),
            (ErrorCode::InvitationRateLimited, "RESOURCE_EXHAUSTED"),
            (ErrorCode::InvitationAlreadyResolved, "FAILED_PRECONDITION"),
            (ErrorCode::InvitationEmailMismatch, "NOT_FOUND"),
            (ErrorCode::InvitationAlreadyMember, "ALREADY_EXISTS"),
            (ErrorCode::InvitationDuplicatePending, "ALREADY_EXISTS"),
            (ErrorCode::StaleRouting, "FAILED_PRECONDITION"),
        ];
        for (code, expected) in cases {
            assert_eq!(code.grpc_code_name(), expected, "mismatch for {code:?}");
        }
    }

    #[test]
    fn display_delegates_to_grpc_code_name() {
        assert_eq!(ErrorCode::NotFound.to_string(), "NOT_FOUND");
        assert_eq!(ErrorCode::Internal.to_string(), "INTERNAL");
        assert_eq!(ErrorCode::RateLimited.to_string(), "RESOURCE_EXHAUSTED");
    }

    #[test]
    fn default_is_internal() {
        assert_eq!(ErrorCode::default(), ErrorCode::Internal);
    }

    #[test]
    fn serde_roundtrip() {
        let codes = [
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::Internal,
            ErrorCode::RateLimited,
            ErrorCode::InvitationDuplicatePending,
            ErrorCode::StaleRouting,
        ];
        for code in codes {
            let json = serde_json::to_string(&code).expect("serialize");
            let back: ErrorCode = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, code);
        }
    }

    #[test]
    fn clone_copy_eq_hash() {
        let a = ErrorCode::NotFound;
        let b = a;
        #[allow(clippy::clone_on_copy)]
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&ErrorCode::NotFound));
    }
}
