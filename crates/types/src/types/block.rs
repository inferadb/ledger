//! Block, transaction, entity, relationship, and write result types for the ledger chain.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ClientId, NodeId, OrganizationId, Region, TxId, VaultId, WriteStatus};
use crate::hash::Hash;

// ============================================================================
// Block Structures
// ============================================================================

/// Block header containing cryptographic chain metadata.
///
/// Block headers are hashed with a fixed 148-byte encoding:
/// height (8) + organization (8) + vault (8) + previous_hash (32) + tx_merkle_root (32)
/// + state_root (32) + timestamp_secs (8) + timestamp_nanos (4) + term (8) + committed_index (8)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
pub struct BlockHeader {
    /// Block height (0 for genesis).
    pub height: u64,
    /// Organization owning this vault.
    #[builder(into)]
    pub organization: OrganizationId,
    /// Vault identifier within the organization.
    #[builder(into)]
    pub vault: VaultId,
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
    /// Block header with chain metadata (includes organization, vault).
    pub header: BlockHeader,
    /// Transactions in this block.
    pub transactions: Vec<Transaction>,
}

impl VaultBlock {
    /// Returns the organization that owns this vault block.
    #[inline]
    pub fn organization(&self) -> OrganizationId {
        self.header.organization
    }

    /// Returns the vault identifier for this block.
    #[inline]
    pub fn vault(&self) -> VaultId {
        self.header.vault
    }

    /// Returns the block height in the vault chain.
    #[inline]
    pub fn height(&self) -> u64 {
        self.header.height
    }
}

/// Internal region block stored on disk, containing entries for multiple vaults.
///
/// Multiple vaults share a single Raft group. Region blocks are the physical
/// unit of Raft replication; clients never see them directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionBlock {
    /// Region this block belongs to.
    pub region: Region,
    /// Monotonic region-level height.
    pub region_height: u64,
    /// Hash linking to previous region block.
    pub previous_region_hash: Hash,
    /// Entries for each vault modified in this block.
    pub vault_entries: Vec<VaultEntry>,
    /// Block creation timestamp.
    pub timestamp: DateTime<Utc>,
    /// Raft leader that committed this block.
    pub leader_id: NodeId,
    /// Raft term when committed.
    pub term: u64,
    /// Raft committed log index.
    pub committed_index: u64,
}

/// Per-vault entry within a RegionBlock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultEntry {
    /// Organization owning this vault.
    pub organization: OrganizationId,
    /// Vault identifier.
    pub vault: VaultId,
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
/// Proves snapshot lineage without requiring full block replay.
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

impl RegionBlock {
    /// Converts this [`RegionBlock`] to a region-level [`BlockHeader`] for chain commitment
    /// computation.
    ///
    /// Aggregates vault entry Merkle roots into a single header, enabling
    /// [`ChainCommitment`] computation over the region chain for snapshot verification.
    pub fn to_region_header(&self) -> BlockHeader {
        use crate::merkle::merkle_root;

        let (tx_merkle_root, state_root) = if self.vault_entries.is_empty() {
            (crate::EMPTY_HASH, crate::EMPTY_HASH)
        } else {
            let tx_roots: Vec<_> = self.vault_entries.iter().map(|e| e.tx_merkle_root).collect();
            let state_roots: Vec<_> = self.vault_entries.iter().map(|e| e.state_root).collect();
            (merkle_root(&tx_roots), merkle_root(&state_roots))
        };

        BlockHeader {
            height: self.region_height,
            organization: OrganizationId::new(0), // Region-level aggregate, not vault-specific
            vault: VaultId::new(0),
            previous_hash: self.previous_region_hash,
            tx_merkle_root,
            state_root,
            timestamp: self.timestamp,
            term: self.term,
            committed_index: self.committed_index,
        }
    }

    /// Extracts a standalone VaultBlock for client verification.
    ///
    /// Clients verify per-vault chains and never see [`RegionBlock`] directly.
    /// Requires organization, vault, and vault height to uniquely identify
    /// the entry since multiple organizations can share a region.
    pub fn extract_vault_block(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> Option<VaultBlock> {
        self.vault_entries
            .iter()
            .find(|e| {
                e.organization == organization && e.vault == vault && e.vault_height == vault_height
            })
            .map(|e| VaultBlock {
                header: BlockHeader {
                    height: e.vault_height,
                    organization: e.organization,
                    vault: e.vault,
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
    /// Operations list is empty.
    #[snafu(display("Transaction must contain at least one operation"))]
    EmptyOperations,

    /// Sequence number must be positive.
    #[snafu(display("Transaction sequence must be positive (got 0)"))]
    ZeroSequence,
}

/// Transaction containing one or more operations.
///
/// Use the builder pattern to construct transactions with validation:
/// ```no_run
/// # use inferadb_ledger_types::types::{Transaction, Operation};
/// # use chrono::Utc;
/// let tx = Transaction::builder()
///     .id([1u8; 16])
///     .client_id("client-123")
///     .sequence(1)
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
    /// - `operations` is empty
    /// - `sequence` is zero
    #[builder]
    pub fn new(
        id: TxId,
        #[builder(into)] client_id: ClientId,
        sequence: u64,
        operations: Vec<Operation>,
        timestamp: DateTime<Utc>,
    ) -> Result<Self, TransactionValidationError> {
        snafu::ensure!(!operations.is_empty(), EmptyOperationsSnafu);
        snafu::ensure!(sequence > 0, ZeroSequenceSnafu);

        Ok(Self { id, client_id, sequence, operations, timestamp })
    }
}

/// Mutation operations that can be applied to vault state.
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
        /// Optional Unix timestamp for expiration. `None` means the entry never expires.
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
    /// Unix timestamp for expiration. A value of 0 means the entry never expires.
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
// Write Result
// ============================================================================

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{
        hash::Hash,
        types::{NodeId, OrganizationId, Region, VaultId},
    };

    fn zero_hash() -> Hash {
        [0u8; 32]
    }

    fn make_vault_entry(org: i64, vault: i64, height: u64) -> VaultEntry {
        VaultEntry {
            organization: OrganizationId::new(org),
            vault: VaultId::new(vault),
            vault_height: height,
            previous_vault_hash: zero_hash(),
            transactions: vec![],
            tx_merkle_root: [1u8; 32],
            state_root: [2u8; 32],
        }
    }

    fn make_region_block(entries: Vec<VaultEntry>) -> RegionBlock {
        RegionBlock {
            region: Region::GLOBAL,
            region_height: 1,
            previous_region_hash: zero_hash(),
            vault_entries: entries,
            timestamp: Utc::now(),
            leader_id: NodeId::new("node-1"),
            term: 1,
            committed_index: 10,
        }
    }

    // ── RegionBlock::to_region_header ───────────────────────────────

    #[test]
    fn to_region_header_empty_vault_entries_uses_empty_hash() {
        let block = make_region_block(vec![]);
        let header = block.to_region_header();

        assert_eq!(header.height, 1);
        assert_eq!(header.tx_merkle_root, crate::EMPTY_HASH);
        assert_eq!(header.state_root, crate::EMPTY_HASH);
        assert_eq!(header.organization, OrganizationId::new(0));
        assert_eq!(header.vault, VaultId::new(0));
    }

    #[test]
    fn to_region_header_with_entries_computes_merkle_roots() {
        let block = make_region_block(vec![make_vault_entry(1, 10, 5), make_vault_entry(2, 20, 3)]);
        let header = block.to_region_header();

        assert_eq!(header.height, 1);
        // Non-empty entries should produce non-empty-hash merkle roots.
        assert_ne!(header.tx_merkle_root, crate::EMPTY_HASH);
        assert_ne!(header.state_root, crate::EMPTY_HASH);
        assert_eq!(header.term, 1);
        assert_eq!(header.committed_index, 10);
    }

    // ── RegionBlock::extract_vault_block ────────────────────────────

    #[test]
    fn extract_vault_block_found() {
        let block = make_region_block(vec![make_vault_entry(1, 10, 5), make_vault_entry(2, 20, 3)]);
        let result = block.extract_vault_block(OrganizationId::new(2), VaultId::new(20), 3);
        assert!(result.is_some());
        let vb = result.expect("vault block present");
        assert_eq!(vb.organization(), OrganizationId::new(2));
        assert_eq!(vb.vault(), VaultId::new(20));
        assert_eq!(vb.height(), 3);
    }

    #[test]
    fn extract_vault_block_not_found_wrong_org() {
        let block = make_region_block(vec![make_vault_entry(1, 10, 5)]);
        let result = block.extract_vault_block(OrganizationId::new(99), VaultId::new(10), 5);
        assert!(result.is_none());
    }

    #[test]
    fn extract_vault_block_not_found_wrong_height() {
        let block = make_region_block(vec![make_vault_entry(1, 10, 5)]);
        let result = block.extract_vault_block(OrganizationId::new(1), VaultId::new(10), 999);
        assert!(result.is_none());
    }

    // ── VaultBlock accessors ────────────────────────────────────────

    #[test]
    fn vault_block_accessors() {
        let vb = VaultBlock {
            header: BlockHeader::builder()
                .height(7)
                .organization(OrganizationId::new(3))
                .vault(VaultId::new(42))
                .previous_hash(zero_hash())
                .tx_merkle_root(zero_hash())
                .state_root(zero_hash())
                .timestamp(Utc::now())
                .term(2)
                .committed_index(100)
                .build(),
            transactions: vec![],
        };
        assert_eq!(vb.organization(), OrganizationId::new(3));
        assert_eq!(vb.vault(), VaultId::new(42));
        assert_eq!(vb.height(), 7);
    }

    // ── Transaction builder validation ──────────────────────────────

    #[test]
    fn transaction_builder_rejects_empty_operations() {
        let result = Transaction::builder()
            .id([0u8; 16])
            .client_id("client-1")
            .sequence(1)
            .operations(vec![])
            .timestamp(Utc::now())
            .build();
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(err.to_string().contains("at least one operation"));
    }

    #[test]
    fn transaction_builder_rejects_zero_sequence() {
        let result = Transaction::builder()
            .id([0u8; 16])
            .client_id("client-1")
            .sequence(0)
            .operations(vec![Operation::DeleteEntity { key: "k".to_string() }])
            .timestamp(Utc::now())
            .build();
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn transaction_builder_success() {
        let tx = Transaction::builder()
            .id([1u8; 16])
            .client_id("client-1")
            .sequence(1)
            .operations(vec![Operation::DeleteEntity { key: "k".to_string() }])
            .timestamp(Utc::now())
            .build();
        assert!(tx.is_ok());
        let tx = tx.expect("valid tx");
        assert_eq!(tx.sequence, 1);
        assert_eq!(tx.client_id.value(), "client-1");
    }
}
