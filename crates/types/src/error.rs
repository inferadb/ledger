//! Error types for InferaDB Ledger using snafu.
//!
//! This module provides a unified error type hierarchy that captures:
//! - Storage errors (inferadb-ledger-store, I/O)
//! - Consensus errors (Raft)
//! - Validation errors (hash mismatches, divergence)
//! - Protocol errors (gRPC, serialization)

use snafu::{Location, Snafu};

use crate::{
    hash::Hash,
    types::{NamespaceId, VaultId},
};

/// Unified result type for ledger operations.
pub type Result<T, E = LedgerError> = std::result::Result<T, E>;

/// Top-level error type for ledger operations.
///
/// # Recovery Guide
///
/// | Variant              | Retryable | Recovery Action                                            |
/// | -------------------- | --------- | ---------------------------------------------------------- |
/// | `Storage`            | Maybe     | Check disk space and I/O health; retry after investigation |
/// | `Consensus`          | Yes       | Retry with backoff; leader election may be in progress     |
/// | `HashMismatch`       | No        | Data corruption detected; trigger integrity check          |
/// | `VaultDiverged`      | No        | Automatic recovery runs; wait for `VaultHealth::Healthy`   |
/// | `VaultUnavailable`   | Yes       | Vault is recovering; retry after short delay               |
/// | `NamespaceNotFound`  | No        | Verify namespace exists via `AdminService::get_namespace`  |
/// | `VaultNotFound`      | No        | Verify vault exists via `AdminService::get_vault`          |
/// | `EntityNotFound`     | No        | Expected for first reads; create the entity first          |
/// | `PreconditionFailed` | No        | Re-read current state and retry with updated condition     |
/// | `AlreadyCommitted`   | No        | Idempotent success; the write was already applied          |
/// | `SequenceViolation`  | No        | Reset client sequence counter from server state            |
/// | `Serialization`      | No        | Bug in codec layer; report as issue                        |
/// | `Config`             | No        | Fix configuration and restart                              |
/// | `Io`                 | Maybe     | Check filesystem permissions and disk health               |
/// | `InvalidArgument`    | No        | Fix the request parameters                                 |
/// | `Internal`           | No        | Unexpected state; report as issue with context             |
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LedgerError {
    /// Storage layer error (disk I/O, page corruption, backend failure).
    ///
    /// **Recovery**: Check disk space, filesystem permissions, and I/O health.
    /// May be retryable if caused by transient I/O pressure.
    #[snafu(display("Storage error at {location}: {message}"))]
    Storage {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Raft consensus error (leader election, log replication, proposal failure).
    ///
    /// **Recovery**: Retry with exponential backoff. The cluster may be
    /// electing a new leader or recovering from a network partition.
    #[snafu(display("Consensus error at {location}: {message}"))]
    Consensus {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Hash verification failed — data integrity violation.
    ///
    /// **Recovery**: Not retryable. Trigger an integrity check via
    /// `AdminService::check_integrity`. This indicates data corruption
    /// or a bug in the hash computation pipeline.
    #[snafu(display("Hash mismatch: expected {expected:02x?}, got {actual:02x?}"))]
    HashMismatch {
        /// Expected hash value.
        expected: Hash,
        /// Actual hash value.
        actual: Hash,
    },

    /// Vault state has diverged from the expected cryptographic commitment.
    ///
    /// **Recovery**: Not retryable directly. The `AutoRecoveryJob` will
    /// attempt automatic recovery. Monitor `VaultHealth` status and wait
    /// for the vault to return to `Healthy`.
    #[snafu(display("Vault {vault_id} diverged at height {height}"))]
    VaultDiverged {
        /// Vault identifier.
        vault_id: VaultId,
        /// Block height where divergence detected.
        height: u64,
    },

    /// Vault is temporarily unavailable (diverged, recovering, or migrating).
    ///
    /// **Recovery**: Retry after a short delay. The vault may be undergoing
    /// automatic recovery or migration. Check vault health status for details.
    #[snafu(display("Vault {vault_id} is unavailable: {reason}"))]
    VaultUnavailable {
        /// Vault identifier.
        vault_id: VaultId,
        /// Reason for unavailability.
        reason: String,
    },

    /// Namespace not found in the cluster.
    ///
    /// **Recovery**: Not retryable. Verify the namespace exists via
    /// `AdminService::get_namespace` or create it with `create_namespace`.
    #[snafu(display("Namespace {namespace_id} not found"))]
    NamespaceNotFound {
        /// Namespace identifier.
        namespace_id: NamespaceId,
    },

    /// Vault not found within the namespace.
    ///
    /// **Recovery**: Not retryable. Verify the vault exists via
    /// `AdminService::get_vault` or create it with `create_vault`.
    #[snafu(display("Vault {vault_id} not found in namespace {namespace_id}"))]
    VaultNotFound {
        /// Namespace identifier.
        namespace_id: NamespaceId,
        /// Vault identifier.
        vault_id: VaultId,
    },

    /// Entity not found at the given key.
    ///
    /// **Recovery**: Not retryable. This is expected for first reads.
    /// Create the entity with a `SetEntity` operation first.
    #[snafu(display("Entity not found: {key}"))]
    EntityNotFound {
        /// Entity key.
        key: String,
    },

    /// Conditional write precondition failed (CAS conflict).
    ///
    /// **Recovery**: Not directly retryable with the same condition. Re-read
    /// the current entity state, update the condition to match, and retry
    /// the write with the new precondition.
    #[snafu(display("Precondition failed for key {key}: {reason}"))]
    PreconditionFailed {
        /// Entity key.
        key: String,
        /// Failure reason.
        reason: String,
    },

    /// Duplicate transaction detected by idempotency check.
    ///
    /// **Recovery**: Not an error — the original write succeeded. Treat as
    /// idempotent success and use the original transaction's result.
    #[snafu(display("Transaction already committed: client={client_id}, sequence={sequence}"))]
    AlreadyCommitted {
        /// Client identifier.
        client_id: String,
        /// Sequence number.
        sequence: u64,
    },

    /// Sequence number violation (out-of-order client request).
    ///
    /// **Recovery**: Not retryable. Reset the client's sequence counter
    /// by querying the server for the last committed sequence number.
    #[snafu(display("Sequence violation for client {client_id}: expected {expected}, got {got}"))]
    SequenceViolation {
        /// Client identifier.
        client_id: String,
        /// Expected sequence number.
        expected: u64,
        /// Actual sequence number received.
        got: u64,
    },

    /// Serialization or deserialization error (postcard codec failure).
    ///
    /// **Recovery**: Not retryable. Indicates a codec bug or data corruption.
    /// Report as an issue with the serialized data context.
    #[snafu(display("Serialization error at {location}: {message}"))]
    Serialization {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Configuration error (invalid value or constraint violation).
    ///
    /// **Recovery**: Not retryable. Fix the configuration value and restart.
    #[snafu(display("Configuration error: {message}"))]
    Config {
        /// Error description.
        message: String,
    },

    /// I/O error (filesystem, network socket).
    ///
    /// **Recovery**: May be retryable if caused by transient filesystem
    /// pressure. Check disk space, permissions, and mount health.
    #[snafu(display("I/O error at {location}: {source}"))]
    Io {
        /// Underlying I/O error.
        source: std::io::Error,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Invalid argument (malformed request parameter).
    ///
    /// **Recovery**: Not retryable. Fix the request parameters and resubmit.
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument {
        /// Error description.
        message: String,
    },

    /// Internal error (unexpected state, invariant violation).
    ///
    /// **Recovery**: Not retryable. This indicates a bug. Collect the error
    /// context (location, message) and report as an issue.
    #[snafu(display("Internal error at {location}: {message}"))]
    Internal {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Storage-specific errors from the embedded B-tree database engine.
///
/// These errors originate in the `store` crate and are wrapped into
/// [`LedgerError::Storage`] when propagated to higher layers.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StorageError {
    /// Database open failed (file not found, permissions, header corruption).
    ///
    /// **Recovery**: Verify the database path exists and has correct permissions.
    /// If the header is corrupted, restore from the most recent backup.
    #[snafu(display("Failed to open database at {path}: {message}"))]
    DatabaseOpen {
        /// Database path.
        path: String,
        /// Error description.
        message: String,
    },

    /// Transaction failed (commit conflict, write lock timeout).
    ///
    /// **Recovery**: Retry the transaction. Only one write transaction can
    /// be active at a time; concurrent writers will fail.
    #[snafu(display("Transaction failed: {message}"))]
    Transaction {
        /// Error description.
        message: String,
    },

    /// Table operation failed (B-tree insert, delete, or lookup error).
    ///
    /// **Recovery**: Check if the table exists and the key/value sizes are
    /// within the page size limit. Large values may require splitting.
    #[snafu(display("Table operation failed on {table}: {message}"))]
    TableOperation {
        /// Table name.
        table: String,
        /// Error description.
        message: String,
    },

    /// Key encoding error (invalid key format for the table schema).
    ///
    /// **Recovery**: Not retryable. Verify the key conforms to the table's
    /// `KeyType` encoding requirements.
    #[snafu(display("Key encoding error: {message}"))]
    KeyEncoding {
        /// Error description.
        message: String,
    },

    /// Snapshot error (creation, compression, or transfer failure).
    ///
    /// **Recovery**: Check disk space for snapshot destination. Snapshot
    /// creation requires approximately 1x the database size in free space.
    #[snafu(display("Snapshot error: {message}"))]
    Snapshot {
        /// Error description.
        message: String,
    },

    /// Data corruption detected (page checksum mismatch, invalid B-tree structure).
    ///
    /// **Recovery**: Not retryable. Restore from the most recent backup.
    /// Report the corruption with page ID and offset for forensic analysis.
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
        let err = LedgerError::VaultDiverged { vault_id: VaultId::new(42), height: 100 };
        assert_eq!(err.to_string(), "Vault vault:42 diverged at height 100");
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
