//! Core type definitions for InferaDB Ledger.
//!
//! These types align with DESIGN.md specifications for:
//! - Identifier types (NamespaceId, VaultId, etc.)
//! - Block and transaction structures
//! - Operations and conditions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hash::Hash;

// ============================================================================
// Identifier Types
// ============================================================================

/// Unique identifier for a namespace (organization-level isolation).
pub type NamespaceId = i64;

/// Unique identifier for a vault within a namespace.
pub type VaultId = i64;

/// Unique identifier for a user in the `_system` namespace.
pub type UserId = i64;

/// Unique identifier for a Raft shard group.
pub type ShardId = i32;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block height (0 for genesis).
    pub height: u64,
    /// Namespace owning this vault.
    pub namespace_id: NamespaceId,
    /// Vault identifier within the namespace.
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

/// Client-facing block structure (VaultBlock).
///
/// This is what clients receive and verify. Each vault maintains its own
/// independent chain for cryptographic isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultBlock {
    /// Block header with chain metadata (includes namespace_id, vault_id).
    pub header: BlockHeader,
    /// Transactions in this block.
    pub transactions: Vec<Transaction>,
}

impl VaultBlock {
    /// Get the namespace ID from the header.
    #[inline]
    pub fn namespace_id(&self) -> NamespaceId {
        self.header.namespace_id
    }

    /// Get the vault ID from the header.
    #[inline]
    pub fn vault_id(&self) -> VaultId {
        self.header.vault_id
    }

    /// Get the block height from the header.
    #[inline]
    pub fn height(&self) -> u64 {
        self.header.height
    }
}

/// Internal shard block structure (ShardBlock).
///
/// Multiple vaults share a single Raft group. This is the physical block
/// stored on disk, containing entries for multiple vaults.
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
    /// Convert this ShardBlock to a shard-level BlockHeader for chain commitment computation.
    ///
    /// Create a shard-level BlockHeader for ChainCommitment computation.
    ///
    /// The shard-level BlockHeader uses:
    /// - `height` = shard_height
    /// - `namespace_id` = 0 (shard-level aggregate)
    /// - `vault_id` = shard_id (for identification)
    /// - `previous_hash` = previous_shard_hash
    /// - `tx_merkle_root` = merkle root of all vault entry tx_merkle_roots
    /// - `state_root` = merkle root of all vault entry state_roots
    /// - `timestamp` = block timestamp
    /// - `term`, `committed_index` = Raft consensus state
    ///
    /// This enables computing ChainCommitment over the shard chain for snapshot verification.
    pub fn to_shard_header(&self) -> BlockHeader {
        use crate::merkle::merkle_root;

        let (tx_merkle_root, state_root) = if self.vault_entries.is_empty() {
            (crate::EMPTY_HASH, crate::EMPTY_HASH)
        } else {
            let tx_roots: Vec<_> = self
                .vault_entries
                .iter()
                .map(|e| e.tx_merkle_root)
                .collect();
            let state_roots: Vec<_> = self.vault_entries.iter().map(|e| e.state_root).collect();
            (merkle_root(&tx_roots), merkle_root(&state_roots))
        };

        BlockHeader {
            height: self.shard_height,
            namespace_id: 0, // Shard-level aggregate, not vault-specific
            vault_id: self.shard_id as VaultId,
            previous_hash: self.previous_shard_hash,
            tx_merkle_root,
            state_root,
            timestamp: self.timestamp,
            term: self.term,
            committed_index: self.committed_index,
        }
    }

    /// Extract a standalone VaultBlock for client verification.
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

/// Transaction containing one or more operations.
///
/// Per DESIGN.md lines 196-202.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique transaction identifier.
    pub id: TxId,
    /// Client identifier for idempotency.
    pub client_id: ClientId,
    /// Monotonic sequence number per client.
    pub sequence: u64,
    /// Actor identifier for audit logging (provided by upstream Engine/Control).
    pub actor: String,
    /// Operations to apply atomically.
    pub operations: Vec<Operation>,
    /// Transaction submission timestamp.
    pub timestamp: DateTime<Utc>,
}

/// Operation types per DESIGN.md lines 204-214.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Create a relationship tuple.
    CreateRelationship {
        /// Resource identifier (e.g., "document:123").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (e.g., "user:456").
        subject: String,
    },
    /// Delete a relationship tuple.
    DeleteRelationship {
        /// Resource identifier.
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
    /// Set an entity value with optional condition and expiration.
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
    /// Delete an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Expire an entity (GC-initiated, distinct from DeleteEntity for audit).
    ExpireEntity {
        /// Entity key that expired.
        key: String,
        /// Unix timestamp when expiration occurred.
        expired_at: u64,
    },
}

/// Conditional write conditions per DESIGN.md lines 768-774.
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
    /// Condition type byte for encoding.
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

/// Entity stored in the state layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entity {
    /// Entity key.
    pub key: Vec<u8>,
    /// Entity value.
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
    /// Create a new relationship.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Encode relationship as a canonical string key.
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

/// Result of a write operation.
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

/// Result of processing a write request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResult {
    /// Block height where the write was committed.
    pub block_height: u64,
    /// Block hash.
    pub block_hash: Hash,
    /// Status of each operation.
    pub statuses: Vec<WriteStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
