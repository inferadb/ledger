use std::collections::HashMap;

use inferadb_ledger_state::system::NamespaceStatus;
use inferadb_ledger_types::{Hash, NamespaceId, Operation, ShardId, Transaction, VaultId};
use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use crate::types::{BlockRetentionPolicy, LedgerNodeId};

// ============================================================================
// Applied State (State Machine)
// ============================================================================

/// Applied state that is tracked for snapshots.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppliedState {
    /// Last applied log ID.
    pub last_applied: Option<LogId<LedgerNodeId>>,
    /// Stored membership configuration.
    pub membership: StoredMembership<LedgerNodeId, openraft::BasicNode>,
    /// Sequence counters for ID generation.
    pub sequences: SequenceCounters,
    /// Per-vault heights for deterministic block heights.
    pub vault_heights: HashMap<(NamespaceId, VaultId), u64>,
    /// Vault health status.
    pub vault_health: HashMap<(NamespaceId, VaultId), VaultHealthStatus>,
    /// Namespace registry (replicated via Raft).
    pub namespaces: HashMap<NamespaceId, NamespaceMeta>,
    /// Vault registry (replicated via Raft).
    pub vaults: HashMap<(NamespaceId, VaultId), VaultMeta>,
    /// Previous vault block hashes (for chaining).
    pub previous_vault_hashes: HashMap<(NamespaceId, VaultId), Hash>,
    /// Current shard height for block creation.
    #[serde(default)]
    pub shard_height: u64,
    /// Previous shard hash for chain continuity.
    #[serde(default)]
    pub previous_shard_hash: Hash,
    /// Per-client last committed sequence numbers (for idempotency recovery).
    /// Key: (namespace_id, vault_id, client_id), Value: last committed sequence.
    #[serde(default)]
    pub client_sequences: HashMap<(NamespaceId, VaultId, String), u64>,
    /// Cumulative estimated storage bytes per namespace.
    ///
    /// Updated on every write (increment by key+value sizes) and delete
    /// (decrement by key size). This is an estimate — not exact byte-level
    /// accounting — but sufficient for quota enforcement and capacity planning.
    ///
    /// Replicated via Raft consensus and survives restarts via snapshot.
    #[serde(default)]
    pub namespace_storage_bytes: HashMap<NamespaceId, u64>,
}

/// Combined snapshot containing both metadata and entity state.
///
/// This is the complete snapshot format that includes:
/// - AppliedState: Raft state machine metadata (vault heights, membership, etc.)
/// - vault_entities: Actual entity data per vault for StateLayer restoration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CombinedSnapshot {
    /// Raft state machine metadata.
    pub applied_state: AppliedState,
    /// Entity data per vault for StateLayer restoration.
    /// Key: vault_id, Value: list of entities
    pub vault_entities: HashMap<VaultId, Vec<inferadb_ledger_types::Entity>>,
}

/// Metadata for a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMeta {
    /// Namespace ID.
    pub namespace_id: NamespaceId,
    /// Human-readable name.
    pub name: String,
    /// Shard hosting this namespace (0 for default).
    pub shard_id: ShardId,
    /// Namespace lifecycle status.
    #[serde(default)]
    pub status: NamespaceStatus,
    /// Target shard for pending migration (only set when status is Migrating).
    #[serde(default)]
    pub pending_shard_id: Option<ShardId>,
    /// Per-namespace resource quota (None means use server default).
    #[serde(default)]
    pub quota: Option<inferadb_ledger_types::config::NamespaceQuota>,
}

/// Metadata for a vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultMeta {
    /// Namespace owning this vault.
    pub namespace_id: NamespaceId,
    /// Vault ID.
    pub vault_id: VaultId,
    /// Human-readable name (optional).
    pub name: Option<String>,
    /// Whether the vault is deleted.
    pub deleted: bool,
    /// Timestamp of last write (Unix seconds).
    #[serde(default)]
    pub last_write_timestamp: u64,
    /// Block retention policy for this vault.
    #[serde(default)]
    pub retention_policy: BlockRetentionPolicy,
}

/// Sequence counters for deterministic ID generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceCounters {
    /// Next namespace ID (starts at 1, 0 = _system).
    pub namespace: NamespaceId,
    /// Next vault ID.
    pub vault: VaultId,
    /// Next user ID.
    pub user: i64,
    /// Next user email ID.
    pub user_email: i64,
    /// Next email verification token ID.
    pub email_verify: i64,
}

impl Default for SequenceCounters {
    fn default() -> Self {
        Self {
            namespace: NamespaceId::new(0),
            vault: VaultId::new(0),
            user: 0,
            user_email: 0,
            email_verify: 0,
        }
    }
}

impl SequenceCounters {
    /// Creates new counters with initial values.
    pub fn new() -> Self {
        Self {
            namespace: NamespaceId::new(1),
            vault: VaultId::new(1),
            user: 1,
            user_email: 1,
            email_verify: 1,
        }
    }

    /// Returns and increment the next namespace ID.
    pub fn next_namespace(&mut self) -> NamespaceId {
        let id = self.namespace;
        self.namespace = NamespaceId::new(id.value() + 1);
        id
    }

    /// Returns and increment the next vault ID.
    pub fn next_vault(&mut self) -> VaultId {
        let id = self.vault;
        self.vault = VaultId::new(id.value() + 1);
        id
    }

    /// Returns and increment the next user ID.
    pub fn next_user(&mut self) -> i64 {
        let id = self.user;
        self.user += 1;
        id
    }

    /// Returns and increment the next user email ID.
    pub fn next_user_email(&mut self) -> i64 {
        let id = self.user_email;
        self.user_email += 1;
        id
    }

    /// Returns and increment the next email verification token ID.
    pub fn next_email_verify(&mut self) -> i64 {
        let id = self.email_verify;
        self.email_verify += 1;
        id
    }
}

/// Selects the least-loaded shard for a new namespace.
///
/// Returns the shard with the fewest active (non-deleted) namespaces.
/// Tie-breaker: lowest shard_id wins.
/// Fallback: shard 0 if no namespaces exist.
pub fn select_least_loaded_shard(namespaces: &HashMap<NamespaceId, NamespaceMeta>) -> ShardId {
    // Count active namespaces per shard
    let mut shard_counts: HashMap<ShardId, usize> = HashMap::new();

    for meta in namespaces.values() {
        if meta.status != NamespaceStatus::Deleted {
            *shard_counts.entry(meta.shard_id).or_insert(0) += 1;
        }
    }

    // If no namespaces exist, default to shard 0
    if shard_counts.is_empty() {
        return ShardId::new(0);
    }

    // Find shard with minimum count, preferring lower shard_id on ties
    shard_counts
        .into_iter()
        .min_by(|(shard_a, count_a), (shard_b, count_b)| {
            count_a.cmp(count_b).then_with(|| shard_a.cmp(shard_b))
        })
        .map(|(shard_id, _)| shard_id)
        .unwrap_or(ShardId::new(0))
}

/// Estimates the net storage delta (in bytes) for a batch of transactions.
///
/// Sets and relationship creates add bytes (key + value sizes).
/// Deletes subtract bytes (key sizes only — we don't know the old value size).
/// Returns a signed value: positive for net growth, negative for net shrinkage.
pub(super) fn estimate_write_storage_delta(transactions: &[Transaction]) -> i64 {
    let mut delta: i64 = 0;
    for tx in transactions {
        for op in &tx.operations {
            match op {
                Operation::SetEntity { key, value, .. } => {
                    delta = delta.saturating_add(key.len() as i64);
                    delta = delta.saturating_add(value.len() as i64);
                },
                Operation::DeleteEntity { key } => {
                    // Subtract estimated key bytes (conservative: we don't know
                    // the value size being removed, only the key)
                    delta = delta.saturating_sub(key.len() as i64);
                },
                Operation::CreateRelationship { resource, relation, subject } => {
                    delta = delta.saturating_add(resource.len() as i64);
                    delta = delta.saturating_add(relation.len() as i64);
                    delta = delta.saturating_add(subject.len() as i64);
                },
                Operation::DeleteRelationship { resource, relation, subject } => {
                    delta = delta.saturating_sub(resource.len() as i64);
                    delta = delta.saturating_sub(relation.len() as i64);
                    delta = delta.saturating_sub(subject.len() as i64);
                },
                Operation::ExpireEntity { key, .. } => {
                    // TTL expiration removes the entity
                    delta = delta.saturating_sub(key.len() as i64);
                },
            }
        }
    }
    delta
}

/// Maximum recovery attempts before requiring manual intervention.
pub const MAX_RECOVERY_ATTEMPTS: u8 = 3;

/// Health status for a vault.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum VaultHealthStatus {
    /// Vault is healthy and accepting writes.
    #[default]
    Healthy,
    /// Vault has diverged and needs recovery.
    Diverged {
        /// Expected state root.
        expected: Hash,
        /// Actual computed state root.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
    /// Vault is currently recovering from divergence.
    Recovering {
        /// When recovery started (Unix timestamp).
        started_at: i64,
        /// Current recovery attempt (1-based, max MAX_RECOVERY_ATTEMPTS).
        attempt: u8,
    },
}
