//! Centralized error-to-`WireError` classification for service handlers.
//!
//! Provides typed wrappers that log the full error at `ERROR` level
//! (server-side) while returning only a generic category string to the client.
//! This prevents leaking internal implementation details over the wire while
//! keeping the server logs actionable.
//!
//! Each function maps an error category — storage, Raft/consensus, crypto,
//! serialization — to a consistent `WireError { code: ErrorCode::Internal,
//! message: "{category} error" }` response. Services call these helpers
//! instead of constructing `WireError`s directly so that log fields are
//! uniformly structured.
//!
//! Tonic call sites bridge the returned `WireError` back to `tonic::Status`
//! via [`super::wire_helpers::wire_error_to_tonic_status`]; once F.1.f.1
//! inlines service bodies onto the wire trait directly, the bridge
//! disappears and these helpers become the canonical primitives.

use std::collections::BTreeMap;

use inferadb_ledger_wire::{ErrorCode, WireError};

use super::wire_helpers;

/// Classifies an internal error into a `WireError` with category metadata.
///
/// Logs the full error at ERROR level for server-side diagnosis, then returns
/// a `WireError` with the error category (but not implementation details) for
/// clients.
pub(crate) fn internal_error(category: &str, error: &dyn std::fmt::Display) -> WireError {
    tracing::error!(category, error = %error, "Internal service error");
    wire_helpers::build_wire_error(
        ErrorCode::Internal,
        format!("{category} error"),
        "",
        false,
        0,
        BTreeMap::new(),
        "",
    )
}

/// Classifies a storage-layer error.
pub(crate) fn storage_error(error: &dyn std::fmt::Display) -> WireError {
    internal_error("storage", error)
}

/// Classifies a consensus/raft-layer error.
pub(crate) fn raft_error(error: &dyn std::fmt::Display) -> WireError {
    internal_error("raft", error)
}

/// Classifies a cryptographic or key-management error.
pub(crate) fn crypto_error(error: &dyn std::fmt::Display) -> WireError {
    internal_error("crypto", error)
}

/// Classifies a serialization/deserialization error.
pub(crate) fn serialization_error(error: &dyn std::fmt::Display) -> WireError {
    internal_error("serialization", error)
}

/// Wire-shaped classifier for Raft error messages — mirrors
/// [`inferadb_ledger_raft::error::classify_raft_error`] but produces a
/// [`WireError`] directly. Used by `proposal.rs` after F.1.f.0.3 so the
/// proposal pipeline avoids round-tripping through `tonic::Status`.
///
/// Leadership-class errors (NotLeader / no leader / leadership transferred /
/// election in progress) map to [`ErrorCode::StaleRouting`]; everything else
/// is `Internal`. Reuses [`inferadb_ledger_raft::error::is_leadership_error`]
/// for the predicate so both classifiers agree on the categorization.
#[must_use]
pub(crate) fn classify_raft_error_wire(message: &str) -> WireError {
    if inferadb_ledger_raft::error::is_leadership_error(message) {
        wire_helpers::build_wire_error(
            ErrorCode::StaleRouting,
            format!("Raft error: {message}"),
            "",
            false,
            0,
            BTreeMap::new(),
            "",
        )
    } else {
        wire_helpers::build_wire_error(
            ErrorCode::Internal,
            format!("Raft error: {message}"),
            "",
            false,
            0,
            BTreeMap::new(),
            "",
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_returns_internal_wire_error() {
        let err = storage_error(&"disk full");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "storage error");
        assert!(!err.retryable);
    }

    #[test]
    fn raft_error_returns_internal_wire_error() {
        let err = raft_error(&"applied state mismatch");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "raft error");
    }

    #[test]
    fn crypto_error_returns_internal_wire_error() {
        let err = crypto_error(&"keyring unavailable");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "crypto error");
    }

    #[test]
    fn serialization_error_returns_internal_wire_error() {
        let err = serialization_error(&"truncated input");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "serialization error");
    }

    #[test]
    fn internal_error_uses_supplied_category() {
        let err = internal_error("id-generation", &"snowflake exhausted");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "id-generation error");
    }

    #[test]
    fn classify_raft_error_wire_leadership_maps_to_stale_routing() {
        let err = classify_raft_error_wire("Not leader, forward to node 2");
        assert_eq!(err.code, ErrorCode::StaleRouting);
        assert_eq!(err.message, "Raft error: Not leader, forward to node 2");
    }

    #[test]
    fn classify_raft_error_wire_non_leadership_maps_to_internal() {
        let err = classify_raft_error_wire("internal state machine error");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "Raft error: internal state machine error");
    }

    #[test]
    fn classify_raft_error_wire_case_insensitive_leadership() {
        let err = classify_raft_error_wire("NOT LEADER");
        assert_eq!(err.code, ErrorCode::StaleRouting);
    }
}
