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
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.grpc_code_name())
    }
}
