//! Error types for the inferadb-ledger-raft crate using snafu.
//!
//! Structured error types that preserve error chains,
//! source locations, and enable proper error handling patterns.
//!
//! ## OpenRaft Compatibility
//!
//! OpenRaft's error types have complex generic bounds incompatible with
//! Snafu's derive macro. Instead of wrapping them directly, we capture the error
//! message and preserve the semantic information in our error variants.

use inferadb_ledger_state::{BlockArchiveError, StateError};
use inferadb_ledger_types::{OrganizationId, VaultId};
use snafu::{Backtrace, GenerateImplicitData, Snafu};

// ============================================================================
// Recovery Errors
// ============================================================================

/// Vault recovery failure.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum RecoveryError {
    /// Block archive is not configured for recovery.
    #[snafu(display("Block archive not configured for recovery"))]
    BlockArchiveNotConfigured { backtrace: Backtrace },

    /// Failed to look up block index for vault height.
    #[snafu(display(
        "Index lookup failed for organization {organization}, vault {vault}, height {height}: {source}"
    ))]
    IndexLookup {
        organization: OrganizationId,
        vault: VaultId,
        height: u64,
        source: BlockArchiveError,
    },

    /// Failed to read block from archive.
    #[snafu(display("Block read failed at region height {region_height}: {source}"))]
    BlockRead { region_height: u64, source: BlockArchiveError },

    /// Failed to apply operations during replay.
    #[snafu(display("Apply operations failed at height {height}: {source}"))]
    ApplyOperations { height: u64, source: StateError },

    /// Failed to compute state root.
    #[snafu(display("State root computation failed for vault {vault}: {source}"))]
    StateRootComputation { vault: VaultId, source: StateError },

    /// Raft consensus write failed.
    #[snafu(display("Raft consensus write failed: {message}"))]
    RaftConsensus { message: String, backtrace: Backtrace },

    /// Health update was rejected by the state machine.
    #[snafu(display("Health update rejected: {reason}"))]
    HealthUpdateRejected { reason: String },

    /// Unexpected response from Raft.
    #[snafu(display("Unexpected Raft response: {description}"))]
    UnexpectedRaftResponse { description: String },
}

// ============================================================================
// Saga Errors
// ============================================================================

/// Saga orchestration failure.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SagaError {
    /// Failed to serialize saga state.
    #[snafu(display("Saga serialization failed: {source}"))]
    Serialization { source: serde_json::Error },

    /// Failed to deserialize saga or entity.
    #[snafu(display("Deserialization failed for {entity_type}: {source}"))]
    Deserialization { entity_type: String, source: serde_json::Error },

    /// Raft consensus write failed.
    #[snafu(display("Raft write failed for saga operation: {message}"))]
    SagaRaftWrite { message: String, backtrace: Backtrace },

    /// Entity read from state failed.
    #[snafu(display("Failed to read {entity_type} from state: {source}"))]
    StateRead { entity_type: String, source: StateError },

    /// Entity not found.
    #[snafu(display("{entity_type} not found: {identifier}"))]
    EntityNotFound { entity_type: String, identifier: String },

    /// Sequence allocation failed.
    #[snafu(display("Sequence allocation failed: {message}"))]
    SequenceAllocation { message: String, backtrace: Backtrace },

    /// Unexpected response from state machine.
    #[snafu(display("Unexpected saga response ({code}): {description}"))]
    UnexpectedSagaResponse { code: inferadb_ledger_types::ErrorCode, description: String },

    /// Signing key generation failed.
    #[snafu(display("Key generation failed: {message}"))]
    KeyGeneration { message: String },

    /// Signing key envelope encryption failed.
    #[snafu(display("Key encryption failed: {message}"))]
    KeyEncryption { message: String },

    /// Orchestrator has shut down (submission channel closed).
    #[snafu(display("Orchestrator shut down: {message}"))]
    OrchestratorShutdown { message: String },

    /// PII lost due to crash or leader transfer — saga must compensate.
    #[snafu(display("PII lost for saga — compensating"))]
    PiiLost,
}

// ============================================================================
// Orphan Cleanup Errors
// ============================================================================

/// Orphan cleanup failure.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum OrphanCleanupError {
    /// Raft consensus write failed.
    #[snafu(display("Raft write failed during orphan cleanup: {message}"))]
    OrphanRaftWrite { message: String, backtrace: Backtrace },

    /// Failed to delete orphaned entity.
    #[snafu(display("Failed to delete orphan entity {key}: {reason}"))]
    DeleteFailed { key: String, reason: String },
}

// ============================================================================
// Service Errors (for gRPC services)
// ============================================================================

/// Errors from gRPC service operations.
///
/// Service handlers consume these via the wire-shaped classifier in
/// `crates/services/src/services/error_classify.rs::classify_raft_error_wire`;
/// the `tonic::Status` conversion that previously lived here was deleted in
/// stage F.1.f.2.5c when the wire path became canonical.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ServiceError {
    /// Raft consensus operation failed.
    #[snafu(display("Raft operation failed: {message}"))]
    Raft { message: String, backtrace: Backtrace },

    /// Storage operation failed.
    #[snafu(display("Storage operation failed: {source}"))]
    Storage { source: StateError },

    /// Block archive operation failed.
    #[snafu(display("Block archive operation failed: {source}"))]
    BlockArchive { source: BlockArchiveError },

    /// Snapshot operation failed.
    #[snafu(display("Snapshot operation failed: {message}"))]
    Snapshot { message: String },

    /// Invalid request argument.
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    /// Resource not found.
    #[snafu(display("{resource_type} not found: {identifier}"))]
    ResourceNotFound { resource_type: String, identifier: String },

    /// Precondition failed.
    #[snafu(display("Precondition failed: {message}"))]
    PreconditionFailed { message: String },

    /// Rate limit exceeded.
    #[snafu(display("Rate limited: {message}"))]
    RateLimited { message: String, retry_after_ms: u64 },

    /// Operation timed out.
    #[snafu(display("Operation timed out: {message}"))]
    Timeout { message: String },

    /// Service unavailable.
    #[snafu(display("Service unavailable: {message}"))]
    Unavailable { message: String },
}

// ============================================================================
// ServiceError Helper Methods
// ============================================================================

/// Returns true if the error message indicates a leadership-related Raft error.
///
/// Leadership errors are transient — they occur when a client sends a request to
/// a non-leader node, or during leader election. These map to `UNAVAILABLE`
/// rather than `INTERNAL` because the client should retry on a different node
/// or after election completes.
///
/// Recognized patterns (from openraft 0.9 error messages):
/// - "not leader" / "NotAMembershipLog" / "forward to leader"
/// - "leader" combined with "unknown" / "lost" / "changing"
///
/// Public so the wire-shaped classifier in
/// `crates/services/src/services/error_classify.rs::classify_raft_error_wire`
/// can share the same predicate.
pub fn is_leadership_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("not leader")
        || lower.contains("forward to leader")
        || lower.contains("not a leader")
        || lower.contains("notamembershiplog")
        || (lower.contains("leader") && lower.contains("unknown"))
        || (lower.contains("leader") && lower.contains("lost"))
        || (lower.contains("leader") && lower.contains("changing"))
        || lower.contains("no leader")
}

impl ServiceError {
    /// Creates a Raft error from any error type.
    pub fn raft<E: std::fmt::Debug>(err: E) -> Self {
        ServiceError::Raft { message: format!("{:?}", err), backtrace: Backtrace::generate() }
    }

    /// Creates an invalid argument error.
    pub fn invalid_arg(message: impl Into<String>) -> Self {
        ServiceError::InvalidArgument { message: message.into() }
    }

    /// Creates a resource not found error.
    pub fn not_found(resource_type: impl Into<String>, identifier: impl Into<String>) -> Self {
        ServiceError::ResourceNotFound {
            resource_type: resource_type.into(),
            identifier: identifier.into(),
        }
    }

    /// Creates a precondition failed error.
    pub fn precondition(message: impl Into<String>) -> Self {
        ServiceError::PreconditionFailed { message: message.into() }
    }

    /// Creates a snapshot error.
    pub fn snapshot(message: impl Into<String>) -> Self {
        ServiceError::Snapshot { message: message.into() }
    }

    /// Creates a rate limited error.
    pub fn rate_limited(message: impl Into<String>, retry_after_ms: u64) -> Self {
        ServiceError::RateLimited { message: message.into(), retry_after_ms }
    }

    /// Creates a timeout error.
    pub fn timeout(message: impl Into<String>) -> Self {
        ServiceError::Timeout { message: message.into() }
    }

    /// Creates an unavailable error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        ServiceError::Unavailable { message: message.into() }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use snafu::GenerateImplicitData;

    use super::*;

    #[test]
    fn test_recovery_error_display() {
        let err = RecoveryError::BlockArchiveNotConfigured { backtrace: Backtrace::generate() };
        assert!(err.to_string().contains("Block archive not configured"));
    }

    #[test]
    fn test_saga_error_display() {
        let err = SagaError::EntityNotFound {
            entity_type: "User".to_string(),
            identifier: "user-123".to_string(),
        };
        assert_eq!(err.to_string(), "User not found: user-123");
    }

    // ========================================================================
    // is_leadership_error tests
    // ========================================================================

    #[test]
    fn test_leadership_error_detection_comprehensive() {
        // Positive cases — should be detected as leadership errors
        let leadership_messages = [
            "not leader",
            "Not Leader",
            "forward to leader",
            "Forward to Leader node 3",
            "not a leader",
            "NotAMembershipLog",
            "Leader is unknown",
            "leader lost",
            "Leader changing",
            "no leader available",
            "NO LEADER",
        ];
        for msg in &leadership_messages {
            assert!(
                is_leadership_error(msg),
                "Expected '{}' to be detected as leadership error",
                msg
            );
        }

        // Negative cases — should NOT be detected as leadership errors
        let non_leadership_messages = [
            "log compaction failed",
            "quorum lost: insufficient replicas",
            "state machine error",
            "serialization failed",
            "storage backend error",
            "network timeout",
            "",
        ];
        for msg in &non_leadership_messages {
            assert!(
                !is_leadership_error(msg),
                "Expected '{}' to NOT be detected as leadership error",
                msg
            );
        }
    }

    // ========================================================================
    // ServiceError helper method tests
    // ========================================================================

    #[test]
    fn test_service_error_raft_helper() {
        let err = ServiceError::raft("some openraft error");
        assert!(
            matches!(err, ServiceError::Raft { ref message, .. } if message.contains("some openraft error"))
        );
    }

    #[test]
    fn test_service_error_rate_limited_helper() {
        let err = ServiceError::rate_limited("too many requests", 2000);
        assert!(
            matches!(err, ServiceError::RateLimited { ref message, retry_after_ms } if message == "too many requests" && retry_after_ms == 2000)
        );
    }

    #[test]
    fn test_service_error_timeout_helper() {
        let err = ServiceError::timeout("proposal timed out");
        assert!(
            matches!(err, ServiceError::Timeout { ref message } if message == "proposal timed out")
        );
    }

    #[test]
    fn test_service_error_unavailable_helper() {
        let err = ServiceError::unavailable("shutting down");
        assert!(
            matches!(err, ServiceError::Unavailable { ref message } if message == "shutting down")
        );
    }
}
