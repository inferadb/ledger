//! Core type definitions for InferaDB Ledger.
//!
//! These types align with DESIGN.md specifications for:
//! - Identifier types (NamespaceId, VaultId, etc.)
//! - Block and transaction structures
//! - Operations and conditions

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hash::Hash;

// ============================================================================
// Identifier Types
// ============================================================================

/// Generates a newtype wrapper around a numeric type for type-safe identifiers.
///
/// Each generated type provides:
/// - Standard derives: Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord
/// - Serde with `#[serde(transparent)]` for wire format compatibility
/// - `From<inner>` and `Into<inner>` conversions
/// - `Display` with a semantic prefix (e.g., `ns:123`)
/// - `new()` constructor and `value()` accessor
macro_rules! define_id {
    (
        $(#[$meta:meta])*
        $name:ident, $inner:ty, $prefix:expr
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name($inner);

        impl $name {
            /// Creates a new identifier from a raw value.
            #[inline]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw numeric value.
            #[inline]
            pub const fn value(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = <$inner as std::str::FromStr>::Err;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                s.parse::<$inner>().map(Self)
            }
        }
    };
}

define_id!(
    /// Unique identifier for a namespace (organization-level isolation).
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing with
    /// other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `ns:` prefix: `ns:42`.
    NamespaceId, i64, "ns"
);

define_id!(
    /// Unique identifier for a vault within a namespace.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing with
    /// other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `vault:` prefix: `vault:7`.
    VaultId, i64, "vault"
);

define_id!(
    /// Unique identifier for a user in the `_system` namespace.
    ///
    /// Wraps an `i64` with compile-time type safety to prevent mixing with
    /// other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `user:` prefix: `user:1`.
    UserId, i64, "user"
);

define_id!(
    /// Unique identifier for a Raft shard group.
    ///
    /// Wraps a `u32` with compile-time type safety to prevent mixing with
    /// other identifier types.
    ///
    /// # Display
    ///
    /// Formats with `shard:` prefix: `shard:3`.
    ShardId, u32, "shard"
);

/// Transaction identifier (16 bytes, typically UUIDv4).
pub type TxId = [u8; 16];

/// Node identifier in the Raft cluster.
pub type NodeId = String;

/// Client identifier for idempotency tracking.
pub type ClientId = String;

// ============================================================================
// Block Structures
// ============================================================================

/// Block header containing cryptographic chain metadata.
///
/// Per DESIGN.md lines 695-708, block headers are hashed with a fixed 148-byte encoding:
/// height (8) + namespace_id (8) + vault_id (8) + previous_hash (32) + tx_merkle_root (32)
/// + state_root (32) + timestamp_secs (8) + timestamp_nanos (4) + term (8) + committed_index (8)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
pub struct BlockHeader {
    /// Block height (0 for genesis).
    pub height: u64,
    /// Namespace owning this vault.
    #[builder(into)]
    pub namespace_id: NamespaceId,
    /// Vault identifier within the namespace.
    #[builder(into)]
    pub vault_id: VaultId,
    /// Hash of the previous block (ZERO_HASH for genesis).
    pub previous_hash: Hash,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Hash,
    /// State root after applying all transactions.
    pub state_root: Hash,
    /// Block creation timestamp.
    pub timestamp: DateTime<Utc>,
    /// Raft term when this block was committed.
    pub term: u64,
    /// Raft committed index for this block.
    pub committed_index: u64,
}

/// Client-facing block containing a header and transactions for a single vault.
///
/// Clients receive and verify these blocks. Each vault maintains its own
/// independent chain for cryptographic isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultBlock {
    /// Block header with chain metadata (includes namespace_id, vault_id).
    pub header: BlockHeader,
    /// Transactions in this block.
    pub transactions: Vec<Transaction>,
}

impl VaultBlock {
    /// Returns the namespace that owns this vault block.
    #[inline]
    pub fn namespace_id(&self) -> NamespaceId {
        self.header.namespace_id
    }

    /// Returns the vault identifier for this block.
    #[inline]
    pub fn vault_id(&self) -> VaultId {
        self.header.vault_id
    }

    /// Returns the block height in the vault chain.
    #[inline]
    pub fn height(&self) -> u64 {
        self.header.height
    }
}

/// Internal shard block stored on disk, containing entries for multiple vaults.
///
/// Multiple vaults share a single Raft group. Shard blocks are the physical
/// unit of Raft replication; clients never see them directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlock {
    /// Shard group identifier.
    pub shard_id: ShardId,
    /// Monotonic shard-level height.
    pub shard_height: u64,
    /// Hash linking to previous shard block.
    pub previous_shard_hash: Hash,
    /// Entries for each vault modified in this block.
    pub vault_entries: Vec<VaultEntry>,
    /// Block creation timestamp.
    pub timestamp: DateTime<Utc>,
    /// Raft leader that committed this block.
    pub leader_id: NodeId,
    /// Raft term when committed.
    pub term: u64,
    /// Raft committed log index (required per DESIGN.md line 1698).
    pub committed_index: u64,
}

/// Per-vault entry within a ShardBlock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultEntry {
    /// Namespace owning this vault.
    pub namespace_id: NamespaceId,
    /// Vault identifier.
    pub vault_id: VaultId,
    /// Per-vault height (independent of shard height).
    pub vault_height: u64,
    /// Hash of previous vault block.
    pub previous_vault_hash: Hash,
    /// Transactions for this vault.
    pub transactions: Vec<Transaction>,
    /// Merkle root of transactions.
    pub tx_merkle_root: Hash,
    /// State root after applying transactions.
    pub state_root: Hash,
}

/// Accumulated cryptographic commitment for a range of blocks.
///
/// Per DESIGN.md §4.4: Proves snapshot's lineage without requiring full block replay.
/// Enables verification continuity even after transaction body compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChainCommitment {
    /// Sequential hash chain of all block headers in range.
    /// Ensures header ordering is preserved and any tampering invalidates chain.
    pub accumulated_header_hash: Hash,

    /// Merkle root of state_roots in range.
    /// Enables O(log n) proofs that a specific state_root was in the range.
    pub state_root_accumulator: Hash,

    /// Start height of this commitment (inclusive).
    /// 0 for genesis, or previous_snapshot_height + 1.
    pub from_height: u64,

    /// End height of this commitment (inclusive).
    /// This is the snapshot's block height.
    pub to_height: u64,
}

impl ShardBlock {
    /// Converts this ShardBlock to a shard-level BlockHeader for chain commitment computation.
    ///
    /// The shard-level BlockHeader uses:
    /// - `height` = shard_height
    /// - `namespace_id` = 0 (shard-level aggregate)
    /// - `vault_id` = shard_id (for identification)
    /// - `previous_hash` = previous_shard_hash
    /// - `tx_merkle_root` = Merkle root of all vault entry tx_merkle_roots
    /// - `state_root` = Merkle root of all vault entry state_roots
    /// - `timestamp` = block timestamp
    /// - `term`, `committed_index` = Raft consensus state
    ///
    /// This enables computing ChainCommitment over the shard chain for snapshot verification.
    pub fn to_shard_header(&self) -> BlockHeader {
        use crate::merkle::merkle_root;

        let (tx_merkle_root, state_root) = if self.vault_entries.is_empty() {
            (crate::EMPTY_HASH, crate::EMPTY_HASH)
        } else {
            let tx_roots: Vec<_> = self.vault_entries.iter().map(|e| e.tx_merkle_root).collect();
            let state_roots: Vec<_> = self.vault_entries.iter().map(|e| e.state_root).collect();
            (merkle_root(&tx_roots), merkle_root(&state_roots))
        };

        BlockHeader {
            height: self.shard_height,
            namespace_id: NamespaceId::new(0), // Shard-level aggregate, not vault-specific
            vault_id: VaultId::new(self.shard_id.value() as i64),
            previous_hash: self.previous_shard_hash,
            tx_merkle_root,
            state_root,
            timestamp: self.timestamp,
            term: self.term,
            committed_index: self.committed_index,
        }
    }

    /// Extracts a standalone VaultBlock for client verification.
    ///
    /// Clients verify per-vault chains and never see ShardBlock directly.
    /// Per DESIGN.md lines 1749-1753: requires both namespace_id and vault_id
    /// since multiple namespaces can share a shard.
    pub fn extract_vault_block(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
    ) -> Option<VaultBlock> {
        self.vault_entries
            .iter()
            .find(|e| e.namespace_id == namespace_id && e.vault_id == vault_id)
            .map(|e| VaultBlock {
                header: BlockHeader {
                    height: e.vault_height,
                    namespace_id: e.namespace_id,
                    vault_id: e.vault_id,
                    previous_hash: e.previous_vault_hash,
                    tx_merkle_root: e.tx_merkle_root,
                    state_root: e.state_root,
                    timestamp: self.timestamp,
                    term: self.term,
                    committed_index: self.committed_index,
                },
                transactions: e.transactions.clone(),
            })
    }
}

// ============================================================================
// Transaction Structures
// ============================================================================

/// Error during transaction validation.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub))]
pub enum TransactionValidationError {
    /// Actor identifier is empty.
    #[snafu(display("Transaction actor cannot be empty"))]
    EmptyActor,

    /// Operations list is empty.
    #[snafu(display("Transaction must contain at least one operation"))]
    EmptyOperations,

    /// Sequence number must be positive.
    #[snafu(display("Transaction sequence must be positive (got 0)"))]
    ZeroSequence,
}

/// Transaction containing one or more operations.
///
/// Per DESIGN.md lines 196-202.
///
/// Use the builder pattern to construct transactions with validation:
/// ```no_run
/// # use inferadb_ledger_types::types::{Transaction, Operation};
/// # use chrono::Utc;
/// let tx = Transaction::builder()
///     .id([1u8; 16])
///     .client_id("client-123")
///     .sequence(1)
///     .actor("user:alice")
///     .operations(vec![Operation::CreateRelationship {
///         resource: "doc:1".into(),
///         relation: "owner".into(),
///         subject: "user:alice".into(),
///     }])
///     .timestamp(Utc::now())
///     .build()
///     .expect("valid transaction");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique transaction identifier.
    pub id: TxId,
    /// Client identifier for idempotency.
    pub client_id: ClientId,
    /// Monotonic sequence number per client.
    pub sequence: u64,
    /// Actor identifier for audit logging (typically a user or service principal).
    pub actor: String,
    /// Operations to apply atomically.
    pub operations: Vec<Operation>,
    /// Transaction submission timestamp.
    pub timestamp: DateTime<Utc>,
}

#[bon::bon]
impl Transaction {
    /// Creates a new transaction with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `actor` is empty
    /// - `operations` is empty
    /// - `sequence` is zero
    #[builder]
    pub fn new(
        id: TxId,
        #[builder(into)] client_id: ClientId,
        sequence: u64,
        #[builder(into)] actor: String,
        operations: Vec<Operation>,
        timestamp: DateTime<Utc>,
    ) -> Result<Self, TransactionValidationError> {
        snafu::ensure!(!actor.is_empty(), EmptyActorSnafu);
        snafu::ensure!(!operations.is_empty(), EmptyOperationsSnafu);
        snafu::ensure!(sequence > 0, ZeroSequenceSnafu);

        Ok(Self { id, client_id, sequence, actor, operations, timestamp })
    }
}

/// Mutation operations that can be applied to vault state.
///
/// Per DESIGN.md lines 204-214.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Creates a relationship tuple.
    CreateRelationship {
        /// Resource identifier (e.g., "document:123").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (e.g., "user:456").
        subject: String,
    },
    /// Deletes a relationship tuple.
    DeleteRelationship {
        /// Resource identifier.
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
    /// Sets an entity value with optional condition and expiration.
    SetEntity {
        /// Entity key.
        key: String,
        /// Entity value (opaque bytes).
        value: Vec<u8>,
        /// Optional write condition.
        condition: Option<SetCondition>,
        /// Unix timestamp for expiration (0 = never expires).
        expires_at: Option<u64>,
    },
    /// Deletes an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Expires an entity (GC-initiated, distinct from DeleteEntity for audit).
    ExpireEntity {
        /// Entity key that expired.
        key: String,
        /// Unix timestamp when expiration occurred.
        expired_at: u64,
    },
}

/// Conditional write predicates for compare-and-swap operations.
///
/// Per DESIGN.md lines 768-774.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetCondition {
    /// Key must not exist (0x01).
    MustNotExist,
    /// Key must exist (0x02).
    MustExist,
    /// Key version must equal specified value (0x03).
    VersionEquals(u64),
    /// Key value must equal specified bytes (0x04).
    ValueEquals(Vec<u8>),
}

impl SetCondition {
    /// Returns the condition type byte for encoding.
    pub fn type_byte(&self) -> u8 {
        match self {
            SetCondition::MustNotExist => 0x01,
            SetCondition::MustExist => 0x02,
            SetCondition::VersionEquals(_) => 0x03,
            SetCondition::ValueEquals(_) => 0x04,
        }
    }
}

// ============================================================================
// Entity Structures
// ============================================================================

/// Key-value record stored per-vault in the B-tree.
///
/// Each entity has a unique key within its vault, an opaque value,
/// optional TTL expiration, and a monotonic version for optimistic concurrency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entity {
    /// Unique key within the vault, conforming to the validation character whitelist.
    pub key: Vec<u8>,
    /// Opaque value bytes. Interpretation is application-defined.
    pub value: Vec<u8>,
    /// Unix timestamp for expiration (0 = never).
    pub expires_at: u64,
    /// Block height when this entity was last modified.
    pub version: u64,
}

/// Relationship tuple (resource, relation, subject).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    /// Resource identifier (e.g., "doc:123").
    pub resource: String,
    /// Relation name (e.g., "viewer").
    pub relation: String,
    /// Subject identifier (e.g., "user:alice").
    pub subject: String,
}

impl Relationship {
    /// Creates a new authorization tuple linking a resource, relation, and subject.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Encodes relationship as a canonical string key.
    pub fn to_key(&self) -> String {
        format!("rel:{}#{}@{}", self.resource, self.relation, self.subject)
    }
}

// ============================================================================
// Vault Health
// ============================================================================

/// Health status of a vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultHealth {
    /// Vault is operating normally.
    Healthy,
    /// Vault has diverged from expected state.
    Diverged {
        /// Expected state root hash.
        expected: Hash,
        /// Computed state root hash.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
}

// ============================================================================
// Write Result
// ============================================================================

/// Outcome status of a single operation within a write request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteStatus {
    /// Entity/relationship was created.
    Created,
    /// Entity/relationship already existed (idempotent).
    AlreadyExists,
    /// Entity/relationship was updated.
    Updated,
    /// Entity/relationship was deleted.
    Deleted,
    /// Entity/relationship was not found.
    NotFound,
    /// Precondition failed (for conditional writes).
    /// Contains details about the current state for client-side conflict resolution.
    PreconditionFailed {
        /// The key that failed the condition check.
        key: String,
        /// Current version of the entity (block height when last modified), if it exists.
        current_version: Option<u64>,
        /// Current value of the entity, if it exists.
        current_value: Option<Vec<u8>>,
    },
}

/// Aggregate result of a committed write request, including per-operation statuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResult {
    /// Block height where the write was committed.
    pub block_height: u64,
    /// Block hash.
    pub block_hash: Hash,
    /// Status of each operation.
    pub statuses: Vec<WriteStatus>,
}

// ============================================================================
// Raft Node Identifier
// ============================================================================

/// Node identifier in the Raft cluster.
///
/// We use u64 for efficient storage and comparison. The mapping from
/// human-readable node names (e.g., "node-1") to numeric IDs is maintained
/// in the `_system` namespace.
pub type LedgerNodeId = u64;

// ============================================================================
// Block Retention Policy
// ============================================================================

/// Block retention mode for storage/compliance trade-off.
///
/// Per DESIGN.md §4.4: Configurable retention policy determines
/// whether transaction bodies are preserved after snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BlockRetentionMode {
    /// All transaction bodies preserved indefinitely.
    ///
    /// Suitable for audit and compliance requirements where full history
    /// must remain accessible.
    #[default]
    Full,
    /// Transaction bodies removed for blocks older than `retention_blocks` from tip.
    ///
    /// Headers (`state_root`, `tx_merkle_root`) are preserved for verification.
    /// Suitable for high-volume workloads prioritizing storage efficiency.
    Compacted,
}

/// Block retention policy for a vault.
///
/// Controls how long transaction bodies are preserved vs. compacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRetentionPolicy {
    /// Retention mode (Full or Compacted).
    pub mode: BlockRetentionMode,
    /// For COMPACTED mode: blocks newer than tip - retention_blocks keep full transactions.
    /// Ignored for FULL mode. Default: 10000 blocks.
    pub retention_blocks: u64,
}

impl Default for BlockRetentionPolicy {
    fn default() -> Self {
        Self { mode: BlockRetentionMode::Full, retention_blocks: 10_000 }
    }
}

// ============================================================================
// Resource Accounting
// ============================================================================

/// Snapshot of per-namespace resource consumption.
///
/// Used by `QuotaChecker` for enforcement and by operators for
/// capacity planning. All values are point-in-time snapshots from
/// Raft-replicated state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceUsage {
    /// Cumulative estimated storage bytes for this namespace.
    ///
    /// Updated on every committed write via `estimate_write_storage_delta`.
    /// Approximate — does not track exact on-disk overhead.
    pub storage_bytes: u64,
    /// Number of active (non-deleted) vaults in this namespace.
    pub vault_count: u32,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::hash::ZERO_HASH;

    #[test]
    fn test_relationship_to_key() {
        let rel = Relationship::new("doc:123", "viewer", "user:alice");
        assert_eq!(rel.to_key(), "rel:doc:123#viewer@user:alice");
    }

    #[test]
    fn test_set_condition_type_bytes() {
        assert_eq!(SetCondition::MustNotExist.type_byte(), 0x01);
        assert_eq!(SetCondition::MustExist.type_byte(), 0x02);
        assert_eq!(SetCondition::VersionEquals(1).type_byte(), 0x03);
        assert_eq!(SetCondition::ValueEquals(vec![]).type_byte(), 0x04);
    }

    // ========================================================================
    // BlockHeader Builder Tests (TDD)
    // ========================================================================

    #[test]
    fn test_block_header_builder_all_fields() {
        let timestamp = Utc::now();
        let header = BlockHeader::builder()
            .height(100)
            .namespace_id(1)
            .vault_id(2)
            .previous_hash(ZERO_HASH)
            .tx_merkle_root(ZERO_HASH)
            .state_root(ZERO_HASH)
            .timestamp(timestamp)
            .term(5)
            .committed_index(50)
            .build();

        assert_eq!(header.height, 100);
        assert_eq!(header.namespace_id, NamespaceId::new(1));
        assert_eq!(header.vault_id, VaultId::new(2));
        assert_eq!(header.previous_hash, ZERO_HASH);
        assert_eq!(header.tx_merkle_root, ZERO_HASH);
        assert_eq!(header.state_root, ZERO_HASH);
        assert_eq!(header.timestamp, timestamp);
        assert_eq!(header.term, 5);
        assert_eq!(header.committed_index, 50);
    }

    #[test]
    fn test_block_header_builder_genesis_block() {
        let header = BlockHeader::builder()
            .height(0) // Genesis
            .namespace_id(1)
            .vault_id(1)
            .previous_hash(ZERO_HASH) // ZERO_HASH for genesis
            .tx_merkle_root(ZERO_HASH)
            .state_root(ZERO_HASH)
            .timestamp(Utc::now())
            .term(1)
            .committed_index(0)
            .build();

        assert_eq!(header.height, 0);
        assert_eq!(header.previous_hash, ZERO_HASH);
    }

    // ========================================================================
    // Transaction Builder Tests (TDD)
    // ========================================================================

    #[test]
    fn test_transaction_builder_valid() {
        let timestamp = Utc::now();
        let tx = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("user:alice")
            .operations(vec![Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "owner".into(),
                subject: "user:alice".into(),
            }])
            .timestamp(timestamp)
            .build()
            .expect("valid transaction should build");

        assert_eq!(tx.id, [1u8; 16]);
        assert_eq!(tx.client_id, "client-123");
        assert_eq!(tx.sequence, 1);
        assert_eq!(tx.actor, "user:alice");
        assert_eq!(tx.operations.len(), 1);
        assert_eq!(tx.timestamp, timestamp);
    }

    #[test]
    fn test_transaction_builder_empty_actor_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("")
            .operations(vec![Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "owner".into(),
                subject: "user:alice".into(),
            }])
            .timestamp(Utc::now())
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("actor"));
    }

    #[test]
    fn test_transaction_builder_empty_operations_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(1)
            .actor("user:alice")
            .operations(vec![])
            .timestamp(Utc::now())
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("operation"));
    }

    #[test]
    fn test_transaction_builder_zero_sequence_fails() {
        let result = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-123")
            .sequence(0)
            .actor("user:alice")
            .operations(vec![Operation::CreateRelationship {
                resource: "doc:1".into(),
                relation: "owner".into(),
                subject: "user:alice".into(),
            }])
            .timestamp(Utc::now())
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("sequence"));
    }

    #[test]
    fn test_transaction_builder_with_into_for_strings() {
        // Test that #[builder(into)] allows &str for String fields
        let tx = Transaction::builder()
            .id([2u8; 16])
            .client_id("client-456") // &str should work
            .sequence(1)
            .actor("user:bob") // &str should work
            .operations(vec![Operation::SetEntity {
                key: "entity:1".into(),
                value: vec![1, 2, 3],
                condition: None,
                expires_at: None,
            }])
            .timestamp(Utc::now())
            .build()
            .expect("valid transaction with &str");

        assert_eq!(tx.client_id, "client-456");
        assert_eq!(tx.actor, "user:bob");
    }

    #[test]
    fn test_transaction_builder_multiple_operations() {
        let tx = Transaction::builder()
            .id([3u8; 16])
            .client_id("client-789")
            .sequence(5)
            .actor("user:charlie")
            .operations(vec![
                Operation::SetEntity {
                    key: "entity:1".into(),
                    value: vec![1],
                    condition: None,
                    expires_at: None,
                },
                Operation::CreateRelationship {
                    resource: "doc:1".into(),
                    relation: "viewer".into(),
                    subject: "user:charlie".into(),
                },
                Operation::DeleteEntity { key: "entity:old".into() },
            ])
            .timestamp(Utc::now())
            .build()
            .expect("valid transaction with multiple operations");

        assert_eq!(tx.operations.len(), 3);
    }

    // ========================================================================
    // Newtype Identifier Tests
    // ========================================================================

    #[test]
    fn test_namespace_id_new_and_value() {
        let id = NamespaceId::new(42);
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_vault_id_new_and_value() {
        let id = VaultId::new(7);
        assert_eq!(id.value(), 7);
    }

    #[test]
    fn test_user_id_new_and_value() {
        let id = UserId::new(99);
        assert_eq!(id.value(), 99);
    }

    #[test]
    fn test_shard_id_new_and_value() {
        let id = ShardId::new(3);
        assert_eq!(id.value(), 3);
    }

    #[test]
    fn test_namespace_id_display() {
        assert_eq!(format!("{}", NamespaceId::new(123)), "ns:123");
    }

    #[test]
    fn test_vault_id_display() {
        assert_eq!(format!("{}", VaultId::new(456)), "vault:456");
    }

    #[test]
    fn test_user_id_display() {
        assert_eq!(format!("{}", UserId::new(789)), "user:789");
    }

    #[test]
    fn test_shard_id_display() {
        assert_eq!(format!("{}", ShardId::new(10)), "shard:10");
    }

    #[test]
    fn test_namespace_id_from_i64() {
        let id: NamespaceId = 42_i64.into();
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_vault_id_into_i64() {
        let id = VaultId::new(7);
        let raw: i64 = id.into();
        assert_eq!(raw, 7);
    }

    #[test]
    fn test_shard_id_from_u32() {
        let id: ShardId = 5_u32.into();
        assert_eq!(id.value(), 5);
    }

    #[test]
    fn test_shard_id_into_u32() {
        let id = ShardId::new(3);
        let raw: u32 = id.into();
        assert_eq!(raw, 3);
    }

    #[test]
    fn test_id_equality() {
        assert_eq!(NamespaceId::new(1), NamespaceId::new(1));
        assert_ne!(NamespaceId::new(1), NamespaceId::new(2));
    }

    #[test]
    fn test_id_ordering() {
        assert!(NamespaceId::new(1) < NamespaceId::new(2));
        assert!(VaultId::new(10) > VaultId::new(5));
        assert!(ShardId::new(0) < ShardId::new(1));
    }

    #[test]
    fn test_id_hash_map_key() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(NamespaceId::new(1), "org-a");
        map.insert(NamespaceId::new(2), "org-b");
        assert_eq!(map.get(&NamespaceId::new(1)), Some(&"org-a"));
        assert_eq!(map.get(&NamespaceId::new(3)), None);
    }

    #[test]
    fn test_id_copy_semantics() {
        let id = NamespaceId::new(42);
        let id2 = id; // Copy
        assert_eq!(id, id2); // Original still accessible
    }

    #[test]
    fn test_id_serde_roundtrip() {
        let id = NamespaceId::new(42);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "42"); // transparent serialization
        let deserialized: NamespaceId = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, id);
    }

    #[test]
    fn test_vault_id_serde_roundtrip() {
        let id = VaultId::new(7);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "7");
        let deserialized: VaultId = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, id);
    }

    #[test]
    fn test_shard_id_serde_roundtrip() {
        let id = ShardId::new(3);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "3");
        let deserialized: ShardId = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, id);
    }

    #[test]
    fn test_id_negative_values() {
        // IDs can be negative (leader-assigned sequential)
        let id = NamespaceId::new(-1);
        assert_eq!(id.value(), -1);
        assert_eq!(format!("{id}"), "ns:-1");
    }

    #[test]
    fn test_id_zero_value() {
        // Zero has special meaning (system namespace)
        let id = NamespaceId::new(0);
        assert_eq!(id.value(), 0);
        assert_eq!(format!("{id}"), "ns:0");
    }

    #[test]
    fn test_block_header_builder_with_newtype_ids() {
        let header = BlockHeader::builder()
            .height(1)
            .namespace_id(NamespaceId::new(10))
            .vault_id(VaultId::new(20))
            .previous_hash(ZERO_HASH)
            .tx_merkle_root(ZERO_HASH)
            .state_root(ZERO_HASH)
            .timestamp(Utc::now())
            .term(1)
            .committed_index(0)
            .build();

        assert_eq!(header.namespace_id, NamespaceId::new(10));
        assert_eq!(header.vault_id, VaultId::new(20));
    }

    #[test]
    fn test_namespace_usage_default_values() {
        let usage = NamespaceUsage { storage_bytes: 0, vault_count: 0 };
        assert_eq!(usage.storage_bytes, 0);
        assert_eq!(usage.vault_count, 0);
    }

    #[test]
    fn test_namespace_usage_with_data() {
        let usage = NamespaceUsage { storage_bytes: 1_048_576, vault_count: 5 };
        assert_eq!(usage.storage_bytes, 1_048_576);
        assert_eq!(usage.vault_count, 5);
    }

    #[test]
    fn test_namespace_usage_copy_semantics() {
        let a = NamespaceUsage { storage_bytes: 100, vault_count: 3 };
        let b = a; // Copy
        assert_eq!(a, b); // Both accessible — Copy trait
    }
}
