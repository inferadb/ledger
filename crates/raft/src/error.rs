//! Error types for the ledger-raft crate using snafu.
//!
//! This module provides structured error types that preserve error chains,
//! source locations, and enable proper error handling patterns.
//!
//! ## Design Decisions
//!
//! OpenRaft's error types have complex generic bounds that don't work well with
//! Snafu's derive macro. Instead of wrapping them directly, we capture the error
//! message and preserve the semantic information in our error variants.

// Snafu generates struct fields for context selectors that don't need documentation
#![allow(missing_docs)]

use snafu::{Backtrace, GenerateImplicitData, Snafu};

use ledger_storage::{BlockArchiveError, StateError};

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
    IndexLookup {
        namespace_id: i64,
        vault_id: i64,
        height: u64,
        source: BlockArchiveError,
    },

    /// Failed to read block from archive.
    #[snafu(display("Block read failed at shard height {shard_height}: {source}"))]
    BlockRead {
        shard_height: u64,
        source: BlockArchiveError,
    },

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
    Deserialization {
        entity_type: String,
        source: serde_json::Error,
    },

    /// Raft consensus write failed.
    #[snafu(display("Raft write failed for saga operation: {message}"))]
    SagaRaftWrite { message: String, backtrace: Backtrace },

    /// Entity read from state failed.
    #[snafu(display("Failed to read {entity_type} from state: {source}"))]
    StateRead {
        entity_type: String,
        source: StateError,
    },

    /// Entity not found.
    #[snafu(display("{entity_type} not found: {identifier}"))]
    EntityNotFound {
        entity_type: String,
        identifier: String,
    },

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
    ResourceNotFound {
        resource_type: String,
        identifier: String,
    },

    /// Precondition failed.
    #[snafu(display("Precondition failed: {message}"))]
    PreconditionFailed { message: String },
}

impl From<ServiceError> for tonic::Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::Raft { message, .. } => tonic::Status::internal(message),
            ServiceError::Storage { source } => {
                tonic::Status::internal(format!("Storage error: {}", source))
            }
            ServiceError::BlockArchive { source } => {
                tonic::Status::internal(format!("Block archive error: {}", source))
            }
            ServiceError::Snapshot { message } => {
                tonic::Status::internal(format!("Snapshot error: {}", message))
            }
            ServiceError::InvalidArgument { message } => tonic::Status::invalid_argument(message),
            ServiceError::ResourceNotFound {
                resource_type,
                identifier,
            } => tonic::Status::not_found(format!("{} not found: {}", resource_type, identifier)),
            ServiceError::PreconditionFailed { message } => {
                tonic::Status::failed_precondition(message)
            }
        }
    }
}

// ============================================================================
// ServiceError Helper Methods
// ============================================================================

impl ServiceError {
    /// Create a Raft error from any error type.
    pub fn raft<E: std::fmt::Debug>(err: E) -> Self {
        ServiceError::Raft {
            message: format!("{:?}", err),
            backtrace: Backtrace::generate(),
        }
    }

    /// Create an invalid argument error.
    pub fn invalid_arg(message: impl Into<String>) -> Self {
        ServiceError::InvalidArgument {
            message: message.into(),
        }
    }

    /// Create a resource not found error.
    pub fn not_found(resource_type: impl Into<String>, identifier: impl Into<String>) -> Self {
        ServiceError::ResourceNotFound {
            resource_type: resource_type.into(),
            identifier: identifier.into(),
        }
    }

    /// Create a precondition failed error.
    pub fn precondition(message: impl Into<String>) -> Self {
        ServiceError::PreconditionFailed {
            message: message.into(),
        }
    }

    /// Create a snapshot error.
    pub fn snapshot(message: impl Into<String>) -> Self {
        ServiceError::Snapshot {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snafu::GenerateImplicitData;

    #[test]
    fn test_recovery_error_display() {
        let err = RecoveryError::BlockArchiveNotConfigured {
            backtrace: Backtrace::generate(),
        };
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

    #[test]
    fn test_service_error_to_status() {
        let err = ServiceError::InvalidArgument {
            message: "missing field".to_string(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("missing field"));
    }

    #[test]
    fn test_service_error_not_found() {
        let err = ServiceError::ResourceNotFound {
            resource_type: "Namespace".to_string(),
            identifier: "123".to_string(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
}
