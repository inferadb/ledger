//! Error types for InferaDB Ledger using snafu.
//!
//! This module provides a unified error type hierarchy that captures:
//! - Storage errors (redb, I/O)
//! - Consensus errors (Raft)
//! - Validation errors (hash mismatches, divergence)
//! - Protocol errors (gRPC, serialization)

use snafu::{Location, Snafu};

use crate::hash::Hash;
use crate::types::{NamespaceId, VaultId};

/// Unified result type for ledger operations.
pub type Result<T, E = LedgerError> = std::result::Result<T, E>;

/// Top-level error type for ledger operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LedgerError {
    /// Storage layer error.
    #[snafu(display("Storage error at {location}: {message}"))]
    Storage {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Raft consensus error.
    #[snafu(display("Consensus error at {location}: {message}"))]
    Consensus {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Hash verification failed.
    #[snafu(display("Hash mismatch: expected {expected:02x?}, got {actual:02x?}"))]
    HashMismatch {
        /// Expected hash value.
        expected: Hash,
        /// Actual hash value.
        actual: Hash,
    },

    /// Vault state has diverged from expected.
    #[snafu(display("Vault {vault_id} diverged at height {height}"))]
    VaultDiverged {
        /// Vault identifier.
        vault_id: VaultId,
        /// Block height where divergence detected.
        height: u64,
    },

    /// Vault is currently unavailable (e.g., diverged, recovering).
    #[snafu(display("Vault {vault_id} is unavailable: {reason}"))]
    VaultUnavailable {
        /// Vault identifier.
        vault_id: VaultId,
        /// Reason for unavailability.
        reason: String,
    },

    /// Namespace not found.
    #[snafu(display("Namespace {namespace_id} not found"))]
    NamespaceNotFound {
        /// Namespace identifier.
        namespace_id: NamespaceId,
    },

    /// Vault not found.
    #[snafu(display("Vault {vault_id} not found in namespace {namespace_id}"))]
    VaultNotFound {
        /// Namespace identifier.
        namespace_id: NamespaceId,
        /// Vault identifier.
        vault_id: VaultId,
    },

    /// Entity not found.
    #[snafu(display("Entity not found: {key}"))]
    EntityNotFound {
        /// Entity key.
        key: String,
    },

    /// Precondition failed for conditional write.
    #[snafu(display("Precondition failed for key {key}: {reason}"))]
    PreconditionFailed {
        /// Entity key.
        key: String,
        /// Failure reason.
        reason: String,
    },

    /// Duplicate transaction (idempotency).
    #[snafu(display("Transaction already committed: client={client_id}, sequence={sequence}"))]
    AlreadyCommitted {
        /// Client identifier.
        client_id: String,
        /// Sequence number.
        sequence: u64,
    },

    /// Sequence number violation.
    #[snafu(display("Sequence violation for client {client_id}: expected {expected}, got {got}"))]
    SequenceViolation {
        /// Client identifier.
        client_id: String,
        /// Expected sequence number.
        expected: u64,
        /// Actual sequence number received.
        got: u64,
    },

    /// Serialization error.
    #[snafu(display("Serialization error at {location}: {message}"))]
    Serialization {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Configuration error.
    #[snafu(display("Configuration error: {message}"))]
    Config {
        /// Error description.
        message: String,
    },

    /// I/O error.
    #[snafu(display("I/O error at {location}: {source}"))]
    Io {
        /// Underlying I/O error.
        source: std::io::Error,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Invalid argument.
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument {
        /// Error description.
        message: String,
    },

    /// Internal error (unexpected state).
    #[snafu(display("Internal error at {location}: {message}"))]
    Internal {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Storage-specific errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StorageError {
    /// Database open failed.
    #[snafu(display("Failed to open database at {path}: {message}"))]
    DatabaseOpen {
        /// Database path.
        path: String,
        /// Error description.
        message: String,
    },

    /// Transaction failed.
    #[snafu(display("Transaction failed: {message}"))]
    Transaction {
        /// Error description.
        message: String,
    },

    /// Table operation failed.
    #[snafu(display("Table operation failed on {table}: {message}"))]
    TableOperation {
        /// Table name.
        table: String,
        /// Error description.
        message: String,
    },

    /// Key encoding error.
    #[snafu(display("Key encoding error: {message}"))]
    KeyEncoding {
        /// Error description.
        message: String,
    },

    /// Snapshot error.
    #[snafu(display("Snapshot error: {message}"))]
    Snapshot {
        /// Error description.
        message: String,
    },

    /// Corruption detected.
    #[snafu(display("Data corruption detected: {message}"))]
    Corruption {
        /// Error description.
        message: String,
    },
}

impl From<StorageError> for LedgerError {
    #[track_caller]
    fn from(err: StorageError) -> Self {
        let loc = std::panic::Location::caller();
        LedgerError::Storage {
            message: err.to_string(),
            location: snafu::Location::new(loc.file(), loc.line(), loc.column()),
        }
    }
}

/// Consensus-specific errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ConsensusError {
    /// Not the leader.
    #[snafu(display("Not the leader, current leader: {leader:?}"))]
    NotLeader {
        /// Current leader node ID, if known.
        leader: Option<String>,
    },

    /// Leader unknown.
    #[snafu(display("Leader unknown, cluster may be electing"))]
    LeaderUnknown,

    /// Proposal failed.
    #[snafu(display("Proposal failed: {message}"))]
    ProposalFailed {
        /// Error description.
        message: String,
    },

    /// Log storage error.
    #[snafu(display("Log storage error: {message}"))]
    LogStorage {
        /// Error description.
        message: String,
    },

    /// State machine error.
    #[snafu(display("State machine error: {message}"))]
    StateMachine {
        /// Error description.
        message: String,
    },

    /// Network error.
    #[snafu(display("Network error communicating with {node}: {message}"))]
    Network {
        /// Target node.
        node: String,
        /// Error description.
        message: String,
    },
}

impl From<ConsensusError> for LedgerError {
    #[track_caller]
    fn from(err: ConsensusError) -> Self {
        let loc = std::panic::Location::caller();
        LedgerError::Consensus {
            message: err.to_string(),
            location: snafu::Location::new(loc.file(), loc.line(), loc.column()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = LedgerError::VaultDiverged {
            vault_id: 42,
            height: 100,
        };
        assert_eq!(err.to_string(), "Vault 42 diverged at height 100");
    }

    #[test]
    fn test_storage_error_conversion() {
        let storage_err = StorageError::DatabaseOpen {
            path: "/tmp/db".to_string(),
            message: "permission denied".to_string(),
        };
        let ledger_err: LedgerError = storage_err.into();
        assert!(matches!(ledger_err, LedgerError::Storage { .. }));
    }
}
