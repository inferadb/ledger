//! Error types for the inferadb-ledger-raft crate using snafu.
//!
//! This module provides structured error types that preserve error chains,
//! source locations, and enable proper error handling patterns.
//!
//! ## Design Decisions
//!
//! OpenRaft's error types have complex generic bounds that don't work well with
//! Snafu's derive macro. Instead of wrapping them directly, we capture the error
//! message and preserve the semantic information in our error variants.

use inferadb_ledger_state::{BlockArchiveError, StateError};
use snafu::{Backtrace, GenerateImplicitData, Snafu};

// ============================================================================
// Recovery Errors
// ============================================================================

/// Errors that can occur during vault recovery operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum RecoveryError {
    /// Block archive is not configured for recovery.
    #[snafu(display("Block archive not configured for recovery"))]
    BlockArchiveNotConfigured { backtrace: Backtrace },

    /// Failed to look up block index for vault height.
    #[snafu(display(
        "Index lookup failed for namespace {namespace_id}, vault {vault_id}, height {height}: {source}"
    ))]
    IndexLookup { namespace_id: i64, vault_id: i64, height: u64, source: BlockArchiveError },

    /// Failed to read block from archive.
    #[snafu(display("Block read failed at shard height {shard_height}: {source}"))]
    BlockRead { shard_height: u64, source: BlockArchiveError },

    /// Failed to apply operations during replay.
    #[snafu(display("Apply operations failed at height {height}: {source}"))]
    ApplyOperations { height: u64, source: StateError },

    /// Failed to compute state root.
    #[snafu(display("State root computation failed for vault {vault_id}: {source}"))]
    StateRootComputation { vault_id: i64, source: StateError },

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

/// Errors that can occur during saga orchestration.
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
    #[snafu(display("Unexpected saga response: {description}"))]
    UnexpectedSagaResponse { description: String },
}

// ============================================================================
// Orphan Cleanup Errors
// ============================================================================

/// Errors that can occur during orphan cleanup operations.
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
/// These errors are converted to `tonic::Status` at the service boundary.
///
/// ## gRPC Status Code Mapping
///
/// | Variant             | gRPC Code             | Retryable | Notes                                    |
/// |---------------------|-----------------------|-----------|------------------------------------------|
/// | `Raft` (leadership) | `UNAVAILABLE`         | Yes       | "not leader", "forward to leader"        |
/// | `Raft` (other)      | `INTERNAL`            | No        | Genuine internal Raft failures            |
/// | `Storage`           | `INTERNAL`            | No        | Storage engine failures                   |
/// | `BlockArchive`      | `INTERNAL`            | No        | Block archive failures                    |
/// | `Snapshot`          | `FAILED_PRECONDITION` | No        | Snapshot in progress or unavailable       |
/// | `InvalidArgument`   | `INVALID_ARGUMENT`    | No        | Malformed request or validation failure   |
/// | `ResourceNotFound`  | `NOT_FOUND`           | No        | Namespace/vault/entity not found          |
/// | `PreconditionFailed`| `FAILED_PRECONDITION` | No        | State precondition violated               |
/// | `RateLimited`       | `RESOURCE_EXHAUSTED`  | Yes       | Includes `retry-after-ms` metadata        |
/// | `Timeout`           | `DEADLINE_EXCEEDED`   | Yes       | Raft proposal or operation timeout        |
/// | `Unavailable`       | `UNAVAILABLE`         | Yes       | Node not ready, shutting down, no leader  |
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

impl From<ServiceError> for tonic::Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::Raft { message, .. } => {
                if is_leadership_error(&message) {
                    tonic::Status::unavailable(message)
                } else {
                    tonic::Status::internal(message)
                }
            },
            ServiceError::Storage { source } => {
                tonic::Status::internal(format!("Storage error: {}", source))
            },
            ServiceError::BlockArchive { source } => {
                tonic::Status::internal(format!("Block archive error: {}", source))
            },
            ServiceError::Snapshot { message } => {
                tonic::Status::failed_precondition(format!("Snapshot error: {}", message))
            },
            ServiceError::InvalidArgument { message } => tonic::Status::invalid_argument(message),
            ServiceError::ResourceNotFound { resource_type, identifier } => {
                tonic::Status::not_found(format!("{} not found: {}", resource_type, identifier))
            },
            ServiceError::PreconditionFailed { message } => {
                tonic::Status::failed_precondition(message)
            },
            ServiceError::RateLimited { message, retry_after_ms } => {
                let mut status = tonic::Status::resource_exhausted(message);
                if let Ok(val) =
                    tonic::metadata::MetadataValue::try_from(retry_after_ms.to_string())
                {
                    status.metadata_mut().insert("retry-after-ms", val);
                }
                status
            },
            ServiceError::Timeout { message } => tonic::Status::deadline_exceeded(message),
            ServiceError::Unavailable { message } => tonic::Status::unavailable(message),
        }
    }
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
fn is_leadership_error(message: &str) -> bool {
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

/// Classifies a Raft error message into the appropriate `tonic::Status` code.
///
/// Leadership-related errors → `UNAVAILABLE` (retryable)
/// All other Raft errors → `INTERNAL` (not retryable)
pub fn classify_raft_error(message: &str) -> tonic::Status {
    if is_leadership_error(message) {
        tonic::Status::unavailable(format!("Raft error: {}", message))
    } else {
        tonic::Status::internal(format!("Raft error: {}", message))
    }
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
    // ServiceError → tonic::Status mapping tests
    // ========================================================================

    #[test]
    fn test_raft_leadership_error_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "not leader".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("not leader"));
    }

    #[test]
    fn test_raft_forward_to_leader_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "Forward to leader node 3".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_no_leader_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "No leader available".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_leader_unknown_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "Leader is unknown during election".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_leader_lost_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "Leader lost: network partition".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_leader_changing_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "Leader changing, retry later".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_not_a_leader_maps_to_unavailable() {
        let err = ServiceError::Raft {
            message: "Not a leader, current leader is node 2".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_raft_generic_error_maps_to_internal() {
        let err = ServiceError::Raft {
            message: "log compaction failed".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_raft_quorum_error_maps_to_internal() {
        let err = ServiceError::Raft {
            message: "quorum lost: insufficient replicas".to_string(),
            backtrace: Backtrace::generate(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_storage_error_maps_to_internal() {
        // Storage errors are always internal — they indicate a node-level failure
        let status: tonic::Status = ServiceError::snapshot("storage test").into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_snapshot_maps_to_failed_precondition() {
        let err = ServiceError::Snapshot { message: "snapshot in progress".to_string() };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("snapshot in progress"));
    }

    #[test]
    fn test_invalid_argument_maps_correctly() {
        let err = ServiceError::InvalidArgument { message: "missing field".to_string() };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("missing field"));
    }

    #[test]
    fn test_resource_not_found_maps_correctly() {
        let err = ServiceError::ResourceNotFound {
            resource_type: "Namespace".to_string(),
            identifier: "123".to_string(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("Namespace"));
        assert!(status.message().contains("123"));
    }

    #[test]
    fn test_precondition_failed_maps_correctly() {
        let err = ServiceError::PreconditionFailed { message: "vault locked".to_string() };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("vault locked"));
    }

    #[test]
    fn test_rate_limited_maps_to_resource_exhausted() {
        let err = ServiceError::RateLimited {
            message: "namespace rate limit exceeded".to_string(),
            retry_after_ms: 500,
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("namespace rate limit exceeded"));
    }

    #[test]
    fn test_rate_limited_includes_retry_after_metadata() {
        let err =
            ServiceError::RateLimited { message: "rate limited".to_string(), retry_after_ms: 1500 };
        let status: tonic::Status = err.into();
        let retry_after = status.metadata().get("retry-after-ms").unwrap().to_str().unwrap();
        assert_eq!(retry_after, "1500");
    }

    #[test]
    fn test_timeout_maps_to_deadline_exceeded() {
        let err = ServiceError::Timeout { message: "Raft proposal timed out".to_string() };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert!(status.message().contains("Raft proposal timed out"));
    }

    #[test]
    fn test_unavailable_maps_correctly() {
        let err = ServiceError::Unavailable { message: "node shutting down".to_string() };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("node shutting down"));
    }

    // ========================================================================
    // classify_raft_error tests
    // ========================================================================

    #[test]
    fn test_classify_raft_error_leadership() {
        let status = classify_raft_error("Not leader, forward to node 2");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_classify_raft_error_generic() {
        let status = classify_raft_error("internal state machine error");
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_classify_raft_error_case_insensitive() {
        let status = classify_raft_error("NOT LEADER");
        assert_eq!(status.code(), tonic::Code::Unavailable);
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
