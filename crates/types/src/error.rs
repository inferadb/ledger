//! Error types for InferaDB Ledger using snafu.
//!
//! Defines a unified error type hierarchy that captures:
//! - Storage errors (inferadb-ledger-store, I/O)
//! - Consensus errors (Raft)
//! - Validation errors (hash mismatches, divergence)
//! - Protocol errors (gRPC, serialization)
//!
//! Each error variant maps to an [`ErrorCode`] with a unique numeric identifier,
//! retryability classification, and suggested recovery action.
//! See [`ErrorCode`] for the full catalog.

use core::fmt;

use snafu::{Location, Snafu};

use crate::{
    hash::Hash,
    types::{NamespaceId, VaultId},
};

/// Unified result type for ledger operations.
pub type Result<T, E = LedgerError> = std::result::Result<T, E>;

/// Machine-readable error codes for programmatic error handling.
///
/// Each error variant across [`LedgerError`], [`StorageError`], and [`ConsensusError`]
/// maps to a unique numeric code. Codes are organized into ranges:
///
/// | Range       | Domain      | Examples                                    |
/// |-------------|-------------|---------------------------------------------|
/// | 1000–1099   | Storage     | Database open, transaction, table ops       |
/// | 1100–1199   | Storage I/O | Corruption, snapshot, key encoding          |
/// | 2000–2099   | Consensus   | Leadership, proposals, log storage          |
/// | 2100–2199   | Consensus   | State machine, network                      |
/// | 3000–3099   | Application | Hash mismatch, vault divergence, not-found  |
/// | 3100–3199   | Application | Precondition, idempotency, sequence         |
/// | 3200–3299   | Application | Serialization, config, I/O, internal        |
///
/// # Wire Format
///
/// Error codes are transmitted as the string representation of their numeric
/// value (e.g., `"1000"`) in gRPC error detail metadata. Use [`ErrorCode::as_u16`]
/// for serialization and [`ErrorCode::from_u16`] for deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    // --- Storage errors (1000–1199) ---
    /// Database file could not be opened.
    StorageDatabaseOpen = 1000,
    /// Transaction commit or lock acquisition failed.
    StorageTransaction = 1001,
    /// B-tree table operation failed.
    StorageTableOperation = 1002,
    /// Key encoding does not match table schema.
    StorageKeyEncoding = 1003,
    /// Snapshot creation, compression, or transfer failed.
    StorageSnapshot = 1100,
    /// Data corruption detected (checksum mismatch, invalid structure).
    StorageCorruption = 1101,

    // --- Consensus errors (2000–2199) ---
    /// Current node is not the Raft leader.
    ConsensusNotLeader = 2000,
    /// Raft cluster leader is unknown (election in progress).
    ConsensusLeaderUnknown = 2001,
    /// Raft proposal was rejected.
    ConsensusProposalFailed = 2002,
    /// Raft log storage error.
    ConsensusLogStorage = 2100,
    /// Raft state machine application error.
    ConsensusStateMachine = 2101,
    /// Network communication error between Raft nodes.
    ConsensusNetwork = 2102,

    // --- Application errors (3000–3299) ---
    /// Wrapper for a storage-layer error at the application level.
    AppStorage = 3000,
    /// Wrapper for a consensus-layer error at the application level.
    AppConsensus = 3001,
    /// Cryptographic hash verification failed.
    AppHashMismatch = 3002,
    /// Vault state diverged from expected commitment.
    AppVaultDiverged = 3003,
    /// Vault temporarily unavailable (recovering or migrating).
    AppVaultUnavailable = 3004,
    /// Namespace not found.
    AppNamespaceNotFound = 3100,
    /// Vault not found.
    AppVaultNotFound = 3101,
    /// Entity not found.
    AppEntityNotFound = 3102,
    /// Conditional write precondition failed (CAS conflict).
    AppPreconditionFailed = 3103,
    /// Duplicate transaction (idempotent success).
    AppAlreadyCommitted = 3104,
    /// Client sequence number violation.
    AppSequenceViolation = 3105,
    /// Serialization or deserialization error.
    AppSerialization = 3200,
    /// Configuration error.
    AppConfig = 3201,
    /// Filesystem or network I/O error.
    AppIo = 3202,
    /// Invalid request argument.
    AppInvalidArgument = 3203,
    /// Internal error (unexpected state, invariant violation).
    AppInternal = 3204,
    /// Namespace resource quota exceeded (storage, vault count, or rate).
    AppQuotaExceeded = 3205,
}

impl ErrorCode {
    /// Returns the numeric code value.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Converts a numeric code to an `ErrorCode`, returning `None` for unknown values.
    #[must_use]
    pub fn from_u16(code: u16) -> Option<Self> {
        match code {
            1000 => Some(Self::StorageDatabaseOpen),
            1001 => Some(Self::StorageTransaction),
            1002 => Some(Self::StorageTableOperation),
            1003 => Some(Self::StorageKeyEncoding),
            1100 => Some(Self::StorageSnapshot),
            1101 => Some(Self::StorageCorruption),
            2000 => Some(Self::ConsensusNotLeader),
            2001 => Some(Self::ConsensusLeaderUnknown),
            2002 => Some(Self::ConsensusProposalFailed),
            2100 => Some(Self::ConsensusLogStorage),
            2101 => Some(Self::ConsensusStateMachine),
            2102 => Some(Self::ConsensusNetwork),
            3000 => Some(Self::AppStorage),
            3001 => Some(Self::AppConsensus),
            3002 => Some(Self::AppHashMismatch),
            3003 => Some(Self::AppVaultDiverged),
            3004 => Some(Self::AppVaultUnavailable),
            3100 => Some(Self::AppNamespaceNotFound),
            3101 => Some(Self::AppVaultNotFound),
            3102 => Some(Self::AppEntityNotFound),
            3103 => Some(Self::AppPreconditionFailed),
            3104 => Some(Self::AppAlreadyCommitted),
            3105 => Some(Self::AppSequenceViolation),
            3200 => Some(Self::AppSerialization),
            3201 => Some(Self::AppConfig),
            3202 => Some(Self::AppIo),
            3203 => Some(Self::AppInvalidArgument),
            3204 => Some(Self::AppInternal),
            3205 => Some(Self::AppQuotaExceeded),
            _ => None,
        }
    }

    /// Whether this error is retryable.
    ///
    /// Retryable errors may succeed on a subsequent attempt, typically after
    /// backoff. Non-retryable errors require corrective action before retrying.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::StorageTransaction
                | Self::StorageSnapshot
                | Self::ConsensusNotLeader
                | Self::ConsensusLeaderUnknown
                | Self::ConsensusProposalFailed
                | Self::ConsensusNetwork
                | Self::AppConsensus
                | Self::AppVaultUnavailable
                | Self::AppIo
                | Self::AppQuotaExceeded
        )
    }

    /// Suggested recovery action for this error code.
    ///
    /// Returns a human-readable string describing what the caller should do
    /// to recover from this error. This guidance is stable and safe to display
    /// in UIs or log to operator dashboards.
    #[must_use]
    pub const fn suggested_action(self) -> &'static str {
        match self {
            Self::StorageDatabaseOpen => {
                "Verify database path exists with correct permissions. Restore from backup if header is corrupted."
            },
            Self::StorageTransaction => {
                "Retry the transaction with backoff. Only one write transaction can be active at a time."
            },
            Self::StorageTableOperation => {
                "Check table exists and key/value sizes are within page size limits."
            },
            Self::StorageKeyEncoding => {
                "Verify the key conforms to the table's key type encoding requirements."
            },
            Self::StorageSnapshot => {
                "Check disk space for snapshot destination. Requires approximately 1x database size in free space."
            },
            Self::StorageCorruption => {
                "Restore from the most recent backup. Report corruption details for forensic analysis."
            },
            Self::ConsensusNotLeader => {
                "Retry with backoff. The request will be forwarded to the current leader."
            },
            Self::ConsensusLeaderUnknown => {
                "Retry with exponential backoff. The cluster may be electing a new leader."
            },
            Self::ConsensusProposalFailed => {
                "Retry with backoff. The proposal may have been rejected during leader transition."
            },
            Self::ConsensusLogStorage => {
                "Check Raft log storage health. This may indicate disk failure on a Raft peer."
            },
            Self::ConsensusStateMachine => {
                "Check state machine health. This indicates an error applying committed entries."
            },
            Self::ConsensusNetwork => {
                "Check network connectivity between Raft peers. Verify firewall rules and DNS resolution."
            },
            Self::AppStorage => {
                "Check disk space, filesystem permissions, and I/O health. May be retryable for transient I/O pressure."
            },
            Self::AppConsensus => {
                "Retry with exponential backoff. The cluster may be electing a new leader."
            },
            Self::AppHashMismatch => {
                "Trigger an integrity check. This indicates data corruption or a hash computation bug."
            },
            Self::AppVaultDiverged => {
                "Automatic recovery is in progress. Wait for vault health to return to Healthy."
            },
            Self::AppVaultUnavailable => {
                "Retry after a short delay. The vault may be recovering or migrating."
            },
            Self::AppNamespaceNotFound => {
                "Verify the namespace exists via AdminService::get_namespace or create it."
            },
            Self::AppVaultNotFound => {
                "Verify the vault exists via AdminService::get_vault or create it."
            },
            Self::AppEntityNotFound => {
                "Expected for first reads. Create the entity with a SetEntity operation."
            },
            Self::AppPreconditionFailed => {
                "Re-read the current entity state and retry with an updated condition."
            },
            Self::AppAlreadyCommitted => {
                "Not an error. The original write succeeded. Treat as idempotent success."
            },
            Self::AppSequenceViolation => {
                "Reset the client sequence counter from the server's last committed sequence."
            },
            Self::AppSerialization => {
                "Codec bug or data corruption. Report as an issue with serialized data context."
            },
            Self::AppConfig => "Fix the configuration value and restart the server.",
            Self::AppIo => {
                "Check disk space, filesystem permissions, and mount health. May be retryable."
            },
            Self::AppInvalidArgument => "Fix the request parameters and resubmit.",
            Self::AppInternal => {
                "Unexpected state or invariant violation. Collect context and report as an issue."
            },
            Self::AppQuotaExceeded => {
                "Namespace resource quota exceeded. Reduce usage or request a quota increase."
            },
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

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
/// | `QuotaExceeded`      | Yes       | Reduce usage or request a namespace quota increase         |
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

impl LedgerError {
    /// Returns the machine-readable error code for this error.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Storage { .. } => ErrorCode::AppStorage,
            Self::Consensus { .. } => ErrorCode::AppConsensus,
            Self::HashMismatch { .. } => ErrorCode::AppHashMismatch,
            Self::VaultDiverged { .. } => ErrorCode::AppVaultDiverged,
            Self::VaultUnavailable { .. } => ErrorCode::AppVaultUnavailable,
            Self::NamespaceNotFound { .. } => ErrorCode::AppNamespaceNotFound,
            Self::VaultNotFound { .. } => ErrorCode::AppVaultNotFound,
            Self::EntityNotFound { .. } => ErrorCode::AppEntityNotFound,
            Self::PreconditionFailed { .. } => ErrorCode::AppPreconditionFailed,
            Self::AlreadyCommitted { .. } => ErrorCode::AppAlreadyCommitted,
            Self::SequenceViolation { .. } => ErrorCode::AppSequenceViolation,
            Self::Serialization { .. } => ErrorCode::AppSerialization,
            Self::Config { .. } => ErrorCode::AppConfig,
            Self::Io { .. } => ErrorCode::AppIo,
            Self::InvalidArgument { .. } => ErrorCode::AppInvalidArgument,
            Self::Internal { .. } => ErrorCode::AppInternal,
        }
    }

    /// Whether this error is retryable.
    ///
    /// Retryable errors may succeed on a subsequent attempt. Delegates to
    /// [`ErrorCode::is_retryable`] for consistency with the wire format.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }

    /// Suggested recovery action for this error.
    ///
    /// Returns a human-readable string describing corrective action.
    /// Delegates to [`ErrorCode::suggested_action`].
    #[must_use]
    pub const fn suggested_action(&self) -> &'static str {
        self.code().suggested_action()
    }
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
        /// Page ID where corruption was detected, if known.
        page_id: Option<u32>,
        /// Table name where corruption was detected, if known.
        table_name: Option<String>,
        /// Expected checksum value, if applicable.
        expected_checksum: Option<u64>,
        /// Actual checksum value, if applicable.
        actual_checksum: Option<u64>,
    },
}

impl StorageError {
    /// Returns the machine-readable error code for this error.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::DatabaseOpen { .. } => ErrorCode::StorageDatabaseOpen,
            Self::Transaction { .. } => ErrorCode::StorageTransaction,
            Self::TableOperation { .. } => ErrorCode::StorageTableOperation,
            Self::KeyEncoding { .. } => ErrorCode::StorageKeyEncoding,
            Self::Snapshot { .. } => ErrorCode::StorageSnapshot,
            Self::Corruption { .. } => ErrorCode::StorageCorruption,
        }
    }

    /// Whether this error is retryable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }

    /// Suggested recovery action for this error.
    #[must_use]
    pub const fn suggested_action(&self) -> &'static str {
        self.code().suggested_action()
    }
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

/// Consensus-specific errors from the Raft layer.
///
/// These errors originate in the `raft` crate and are wrapped into
/// [`LedgerError::Consensus`] when propagated to higher layers.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ConsensusError {
    /// Current node is not the Raft leader and cannot process the request.
    #[snafu(display("Not the leader, current leader: {leader:?}"))]
    NotLeader {
        /// Current leader node ID, if known.
        leader: Option<String>,
    },

    /// Raft cluster leader is unknown, likely during an election.
    #[snafu(display("Leader unknown, cluster may be electing"))]
    LeaderUnknown,

    /// Raft proposal was rejected by the cluster.
    #[snafu(display("Proposal failed: {message}"))]
    ProposalFailed {
        /// Error description.
        message: String,
        /// Reason the proposal was rejected, if available.
        rejection_reason: Option<String>,
        /// Node ID that rejected the proposal, if known.
        rejecting_node_id: Option<String>,
    },

    /// Raft log storage operation failed.
    #[snafu(display("Log storage error: {message}"))]
    LogStorage {
        /// Error description.
        message: String,
    },

    /// Error applying a committed log entry to the state machine.
    #[snafu(display("State machine error: {message}"))]
    StateMachine {
        /// Error description.
        message: String,
    },

    /// Network communication failure with a Raft peer.
    #[snafu(display("Network error communicating with {node}: {message}"))]
    Network {
        /// Target node.
        node: String,
        /// Error description.
        message: String,
    },
}

impl ConsensusError {
    /// Returns the machine-readable error code for this error.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::NotLeader { .. } => ErrorCode::ConsensusNotLeader,
            Self::LeaderUnknown => ErrorCode::ConsensusLeaderUnknown,
            Self::ProposalFailed { .. } => ErrorCode::ConsensusProposalFailed,
            Self::LogStorage { .. } => ErrorCode::ConsensusLogStorage,
            Self::StateMachine { .. } => ErrorCode::ConsensusStateMachine,
            Self::Network { .. } => ErrorCode::ConsensusNetwork,
        }
    }

    /// Whether this error is retryable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }

    /// Suggested recovery action for this error.
    #[must_use]
    pub const fn suggested_action(&self) -> &'static str {
        self.code().suggested_action()
    }
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
    use std::collections::HashSet;

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

    // --- ErrorCode numeric uniqueness ---

    /// Returns all ErrorCode variants.
    fn all_error_codes() -> Vec<ErrorCode> {
        vec![
            ErrorCode::StorageDatabaseOpen,
            ErrorCode::StorageTransaction,
            ErrorCode::StorageTableOperation,
            ErrorCode::StorageKeyEncoding,
            ErrorCode::StorageSnapshot,
            ErrorCode::StorageCorruption,
            ErrorCode::ConsensusNotLeader,
            ErrorCode::ConsensusLeaderUnknown,
            ErrorCode::ConsensusProposalFailed,
            ErrorCode::ConsensusLogStorage,
            ErrorCode::ConsensusStateMachine,
            ErrorCode::ConsensusNetwork,
            ErrorCode::AppStorage,
            ErrorCode::AppConsensus,
            ErrorCode::AppHashMismatch,
            ErrorCode::AppVaultDiverged,
            ErrorCode::AppVaultUnavailable,
            ErrorCode::AppNamespaceNotFound,
            ErrorCode::AppVaultNotFound,
            ErrorCode::AppEntityNotFound,
            ErrorCode::AppPreconditionFailed,
            ErrorCode::AppAlreadyCommitted,
            ErrorCode::AppSequenceViolation,
            ErrorCode::AppSerialization,
            ErrorCode::AppConfig,
            ErrorCode::AppIo,
            ErrorCode::AppInvalidArgument,
            ErrorCode::AppInternal,
            ErrorCode::AppQuotaExceeded,
        ]
    }

    #[test]
    fn test_error_code_numeric_uniqueness() {
        let codes = all_error_codes();
        let mut seen = HashSet::new();
        for code in &codes {
            let numeric = code.as_u16();
            assert!(seen.insert(numeric), "Duplicate error code: {numeric} for {code:?}");
        }
    }

    #[test]
    fn test_error_code_round_trip() {
        for code in all_error_codes() {
            let numeric = code.as_u16();
            let recovered = ErrorCode::from_u16(numeric);
            assert_eq!(
                recovered,
                Some(code),
                "Round-trip failed for {code:?} (numeric: {numeric})"
            );
        }
    }

    #[test]
    fn test_error_code_unknown_value_returns_none() {
        assert_eq!(ErrorCode::from_u16(0), None);
        assert_eq!(ErrorCode::from_u16(9999), None);
        assert_eq!(ErrorCode::from_u16(1500), None);
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::StorageDatabaseOpen.to_string(), "1000");
        assert_eq!(ErrorCode::ConsensusNotLeader.to_string(), "2000");
        assert_eq!(ErrorCode::AppStorage.to_string(), "3000");
    }

    // --- ErrorCode range validation ---

    #[test]
    fn test_storage_codes_in_range() {
        let storage_codes = [
            ErrorCode::StorageDatabaseOpen,
            ErrorCode::StorageTransaction,
            ErrorCode::StorageTableOperation,
            ErrorCode::StorageKeyEncoding,
            ErrorCode::StorageSnapshot,
            ErrorCode::StorageCorruption,
        ];
        for code in storage_codes {
            let n = code.as_u16();
            assert!((1000..2000).contains(&n), "{code:?} ({n}) not in storage range 1000-1999");
        }
    }

    #[test]
    fn test_consensus_codes_in_range() {
        let consensus_codes = [
            ErrorCode::ConsensusNotLeader,
            ErrorCode::ConsensusLeaderUnknown,
            ErrorCode::ConsensusProposalFailed,
            ErrorCode::ConsensusLogStorage,
            ErrorCode::ConsensusStateMachine,
            ErrorCode::ConsensusNetwork,
        ];
        for code in consensus_codes {
            let n = code.as_u16();
            assert!((2000..3000).contains(&n), "{code:?} ({n}) not in consensus range 2000-2999");
        }
    }

    #[test]
    fn test_application_codes_in_range() {
        let app_codes = [
            ErrorCode::AppStorage,
            ErrorCode::AppConsensus,
            ErrorCode::AppHashMismatch,
            ErrorCode::AppVaultDiverged,
            ErrorCode::AppVaultUnavailable,
            ErrorCode::AppNamespaceNotFound,
            ErrorCode::AppVaultNotFound,
            ErrorCode::AppEntityNotFound,
            ErrorCode::AppPreconditionFailed,
            ErrorCode::AppAlreadyCommitted,
            ErrorCode::AppSequenceViolation,
            ErrorCode::AppSerialization,
            ErrorCode::AppConfig,
            ErrorCode::AppIo,
            ErrorCode::AppInvalidArgument,
            ErrorCode::AppInternal,
            ErrorCode::AppQuotaExceeded,
        ];
        for code in app_codes {
            let n = code.as_u16();
            assert!((3000..4000).contains(&n), "{code:?} ({n}) not in application range 3000-3999");
        }
    }

    // --- ErrorCode::is_retryable ---

    #[test]
    fn test_retryable_codes() {
        let retryable = [
            ErrorCode::StorageTransaction,
            ErrorCode::StorageSnapshot,
            ErrorCode::ConsensusNotLeader,
            ErrorCode::ConsensusLeaderUnknown,
            ErrorCode::ConsensusProposalFailed,
            ErrorCode::ConsensusNetwork,
            ErrorCode::AppConsensus,
            ErrorCode::AppVaultUnavailable,
            ErrorCode::AppIo,
            ErrorCode::AppQuotaExceeded,
        ];
        for code in retryable {
            assert!(code.is_retryable(), "{code:?} should be retryable");
        }
    }

    #[test]
    fn test_non_retryable_codes() {
        let non_retryable = [
            ErrorCode::StorageDatabaseOpen,
            ErrorCode::StorageTableOperation,
            ErrorCode::StorageKeyEncoding,
            ErrorCode::StorageCorruption,
            ErrorCode::ConsensusLogStorage,
            ErrorCode::ConsensusStateMachine,
            ErrorCode::AppStorage,
            ErrorCode::AppHashMismatch,
            ErrorCode::AppVaultDiverged,
            ErrorCode::AppNamespaceNotFound,
            ErrorCode::AppVaultNotFound,
            ErrorCode::AppEntityNotFound,
            ErrorCode::AppPreconditionFailed,
            ErrorCode::AppAlreadyCommitted,
            ErrorCode::AppSequenceViolation,
            ErrorCode::AppSerialization,
            ErrorCode::AppConfig,
            ErrorCode::AppInvalidArgument,
            ErrorCode::AppInternal,
        ];
        for code in non_retryable {
            assert!(!code.is_retryable(), "{code:?} should not be retryable");
        }
    }

    // --- ErrorCode::suggested_action ---

    #[test]
    fn test_suggested_action_non_empty() {
        for code in all_error_codes() {
            let action = code.suggested_action();
            assert!(!action.is_empty(), "{code:?} has empty suggested_action");
        }
    }

    // --- LedgerError code mapping ---

    #[test]
    fn test_ledger_error_storage_code() {
        let err = LedgerError::Storage {
            message: "disk full".to_string(),
            location: snafu::Location::new("test.rs", 1, 1),
        };
        assert_eq!(err.code(), ErrorCode::AppStorage);
    }

    #[test]
    fn test_ledger_error_consensus_code() {
        let err = LedgerError::Consensus {
            message: "leader lost".to_string(),
            location: snafu::Location::new("test.rs", 1, 1),
        };
        assert_eq!(err.code(), ErrorCode::AppConsensus);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_ledger_error_hash_mismatch_code() {
        let err = LedgerError::HashMismatch { expected: Hash::default(), actual: Hash::default() };
        assert_eq!(err.code(), ErrorCode::AppHashMismatch);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_ledger_error_vault_unavailable_retryable() {
        let err = LedgerError::VaultUnavailable {
            vault_id: VaultId::new(1),
            reason: "recovering".to_string(),
        };
        assert_eq!(err.code(), ErrorCode::AppVaultUnavailable);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_ledger_error_not_found_not_retryable() {
        let err = LedgerError::NamespaceNotFound { namespace_id: NamespaceId::new(1) };
        assert_eq!(err.code(), ErrorCode::AppNamespaceNotFound);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_ledger_error_invalid_argument_code() {
        let err = LedgerError::InvalidArgument { message: "bad param".to_string() };
        assert_eq!(err.code(), ErrorCode::AppInvalidArgument);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_ledger_error_all_variants_have_codes() {
        // Verify every LedgerError variant maps to a code (compile-time exhaustiveness
        // is enforced by the match, but this tests runtime correctness).
        let variants: Vec<LedgerError> = vec![
            LedgerError::Storage {
                message: String::new(),
                location: snafu::Location::new("", 0, 0),
            },
            LedgerError::Consensus {
                message: String::new(),
                location: snafu::Location::new("", 0, 0),
            },
            LedgerError::HashMismatch { expected: Hash::default(), actual: Hash::default() },
            LedgerError::VaultDiverged { vault_id: VaultId::new(0), height: 0 },
            LedgerError::VaultUnavailable { vault_id: VaultId::new(0), reason: String::new() },
            LedgerError::NamespaceNotFound { namespace_id: NamespaceId::new(0) },
            LedgerError::VaultNotFound {
                namespace_id: NamespaceId::new(0),
                vault_id: VaultId::new(0),
            },
            LedgerError::EntityNotFound { key: String::new() },
            LedgerError::PreconditionFailed { key: String::new(), reason: String::new() },
            LedgerError::AlreadyCommitted { client_id: String::new(), sequence: 0 },
            LedgerError::SequenceViolation { client_id: String::new(), expected: 0, got: 0 },
            LedgerError::Serialization {
                message: String::new(),
                location: snafu::Location::new("", 0, 0),
            },
            LedgerError::Config { message: String::new() },
            LedgerError::Io {
                source: std::io::Error::other("test"),
                location: snafu::Location::new("", 0, 0),
            },
            LedgerError::InvalidArgument { message: String::new() },
            LedgerError::Internal {
                message: String::new(),
                location: snafu::Location::new("", 0, 0),
            },
        ];
        for err in &variants {
            let code = err.code();
            // Every code must have a valid round-trip
            assert!(ErrorCode::from_u16(code.as_u16()).is_some());
        }
    }

    // --- StorageError code mapping ---

    #[test]
    fn test_storage_error_codes() {
        let cases: Vec<(StorageError, ErrorCode)> = vec![
            (
                StorageError::DatabaseOpen { path: String::new(), message: String::new() },
                ErrorCode::StorageDatabaseOpen,
            ),
            (StorageError::Transaction { message: String::new() }, ErrorCode::StorageTransaction),
            (
                StorageError::TableOperation { table: String::new(), message: String::new() },
                ErrorCode::StorageTableOperation,
            ),
            (StorageError::KeyEncoding { message: String::new() }, ErrorCode::StorageKeyEncoding),
            (StorageError::Snapshot { message: String::new() }, ErrorCode::StorageSnapshot),
            (
                StorageError::Corruption {
                    message: String::new(),
                    page_id: None,
                    table_name: None,
                    expected_checksum: None,
                    actual_checksum: None,
                },
                ErrorCode::StorageCorruption,
            ),
        ];
        for (err, expected_code) in cases {
            assert_eq!(err.code(), expected_code, "code mismatch for {err:?}");
        }
    }

    #[test]
    fn test_storage_corruption_with_details() {
        let err = StorageError::Corruption {
            message: "checksum mismatch on page 42".to_string(),
            page_id: Some(42),
            table_name: Some("entities".to_string()),
            expected_checksum: Some(0xDEAD_BEEF),
            actual_checksum: Some(0xCAFE_BABE),
        };
        assert_eq!(err.code(), ErrorCode::StorageCorruption);
        assert!(!err.is_retryable());
        if let StorageError::Corruption {
            page_id,
            table_name,
            expected_checksum,
            actual_checksum,
            ..
        } = &err
        {
            assert_eq!(*page_id, Some(42));
            assert_eq!(table_name.as_deref(), Some("entities"));
            assert_eq!(*expected_checksum, Some(0xDEAD_BEEF));
            assert_eq!(*actual_checksum, Some(0xCAFE_BABE));
        }
    }

    // --- ConsensusError code mapping ---

    #[test]
    fn test_consensus_error_codes() {
        let cases: Vec<(ConsensusError, ErrorCode)> = vec![
            (
                ConsensusError::NotLeader { leader: Some("node-1".to_string()) },
                ErrorCode::ConsensusNotLeader,
            ),
            (ConsensusError::LeaderUnknown, ErrorCode::ConsensusLeaderUnknown),
            (
                ConsensusError::ProposalFailed {
                    message: "rejected".to_string(),
                    rejection_reason: None,
                    rejecting_node_id: None,
                },
                ErrorCode::ConsensusProposalFailed,
            ),
            (ConsensusError::LogStorage { message: String::new() }, ErrorCode::ConsensusLogStorage),
            (
                ConsensusError::StateMachine { message: String::new() },
                ErrorCode::ConsensusStateMachine,
            ),
            (
                ConsensusError::Network { node: String::new(), message: String::new() },
                ErrorCode::ConsensusNetwork,
            ),
        ];
        for (err, expected_code) in cases {
            assert_eq!(err.code(), expected_code, "code mismatch for {err:?}");
        }
    }

    #[test]
    fn test_proposal_failed_with_details() {
        let err = ConsensusError::ProposalFailed {
            message: "quorum not reached".to_string(),
            rejection_reason: Some("log behind".to_string()),
            rejecting_node_id: Some("node-3".to_string()),
        };
        assert_eq!(err.code(), ErrorCode::ConsensusProposalFailed);
        assert!(err.is_retryable());
        if let ConsensusError::ProposalFailed { rejection_reason, rejecting_node_id, .. } = &err {
            assert_eq!(rejection_reason.as_deref(), Some("log behind"));
            assert_eq!(rejecting_node_id.as_deref(), Some("node-3"));
        }
    }

    // --- Retryability consistency ---

    #[test]
    fn test_ledger_error_retryability_matches_code() {
        let err =
            LedgerError::VaultUnavailable { vault_id: VaultId::new(1), reason: "test".to_string() };
        assert_eq!(err.is_retryable(), err.code().is_retryable());

        let err2 = LedgerError::InvalidArgument { message: "bad".to_string() };
        assert_eq!(err2.is_retryable(), err2.code().is_retryable());
    }

    #[test]
    fn test_suggested_action_delegates_to_code() {
        let err = LedgerError::Config { message: "bad".to_string() };
        assert_eq!(err.suggested_action(), err.code().suggested_action());
    }
}
