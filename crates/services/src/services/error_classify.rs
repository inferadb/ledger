//! Centralized error-to-gRPC-Status classification.
//!
//! Replaces ad-hoc `Status::internal("Internal error")` with structured
//! error responses that carry category metadata for server-side diagnosis.

use tonic::Status;

/// Classifies an internal error into a gRPC Status with category metadata.
///
/// Logs the full error at ERROR level for server-side diagnosis, then returns
/// a Status with the error category (but not implementation details) for clients.
pub(crate) fn internal_error(category: &str, error: &dyn std::fmt::Display) -> Status {
    tracing::error!(category, error = %error, "Internal service error");
    Status::internal(format!("{category} error"))
}

/// Classifies a storage-layer error.
pub(crate) fn storage_error(error: &dyn std::fmt::Display) -> Status {
    internal_error("storage", error)
}

/// Classifies a consensus/raft-layer error.
pub(crate) fn raft_error(error: &dyn std::fmt::Display) -> Status {
    internal_error("raft", error)
}

/// Classifies a cryptographic or key-management error.
pub(crate) fn crypto_error(error: &dyn std::fmt::Display) -> Status {
    internal_error("crypto", error)
}

/// Classifies a serialization/deserialization error.
pub(crate) fn serialization_error(error: &dyn std::fmt::Display) -> Status {
    internal_error("serialization", error)
}
