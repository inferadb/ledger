//! Raft storage implementation using ledger-db.
//!
//! This module provides the persistent storage for Raft log entries,
//! vote state, committed log tracking, and state machine state.
//!
//! We use the deprecated but non-sealed `RaftStorage` trait which combines
//! log storage and state machine into one implementation. The v2 traits
//! (`RaftLogStorage`, `RaftStateMachine`) are sealed in OpenRaft 0.9.
//!
//! Per DESIGN.md, each shard group has its own storage located at:
//! `shards/{shard_id}/raft/log.db`
//!
//! # Lock Ordering Convention
//!
//! To prevent deadlocks, locks in this module must be acquired in the following order:
//!
//! 1. `applied_state` - Raft state machine state
//! 2. `shard_chain` - Shard chain tracking (height + previous hash)
//! 3. `vote_cache`, `last_purged_cache` - Caches (independent, no ordering requirement)
//!
//! The `shard_chain` lock consolidates `shard_height` and `previous_shard_hash`
//! into a single lock to eliminate internal ordering issues.

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use ledger_db::{Database, DatabaseConfig, FileBackend, StorageBackend, tables};
use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftStorage, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use ledger_types::{
    Hash, NamespaceId, Operation, ShardBlock, ShardId, VaultEntry, VaultId, compute_tx_merkle_root,
    decode, encode,
};

use crate::metrics;
use crate::types::{
    BlockRetentionPolicy, LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig,
    SystemRequest,
};

// Re-export storage types used in this module
use ledger_state::system::{NamespaceRegistry, NamespaceStatus, SYSTEM_VAULT_ID, SystemKeys};
use ledger_state::{BlockArchive, StateError, StateLayer};

// ============================================================================
// Metadata Keys
// ============================================================================

// Metadata keys for RaftState table
const KEY_VOTE: &str = "vote";
#[allow(dead_code)] // Reserved for future use with save_committed/read_committed
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_APPLIED_STATE: &str = "applied_state";

// ============================================================================
// Shard Chain State (Lock Consolidated)
// ============================================================================

/// Shard chain tracking state.
///
/// These fields are grouped into a single lock to avoid lock ordering issues.
/// They track the shard-level blockchain state for creating ShardBlocks.
#[derive(Debug, Clone, Copy, Default)]
pub struct ShardChainState {
    /// Current shard height for block creation.
    pub height: u64,
    /// Previous shard hash for chain continuity.
    pub previous_hash: Hash,
}

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
    pub vault_entities: HashMap<VaultId, Vec<ledger_types::Entity>>,
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
    /// Whether the namespace is deleted.
    pub deleted: bool,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceCounters {
    /// Next namespace ID (starts at 1, 0 = _system).
    pub namespace: NamespaceId,
    /// Next vault ID.
    pub vault: VaultId,
    /// Next user ID.
    pub user: i64,
}

impl SequenceCounters {
    /// Create new counters with initial values.
    pub fn new() -> Self {
        Self {
            namespace: 1,
            vault: 1,
            user: 1,
        }
    }

    /// Get and increment the next namespace ID.
    pub fn next_namespace(&mut self) -> NamespaceId {
        let id = self.namespace;
        self.namespace += 1;
        id
    }

    /// Get and increment the next vault ID.
    pub fn next_vault(&mut self) -> VaultId {
        let id = self.vault;
        self.vault += 1;
        id
    }

    /// Get and increment the next user ID.
    pub fn next_user(&mut self) -> i64 {
        let id = self.user;
        self.user += 1;
        id
    }
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

// ============================================================================
// Raft Log Store
// ============================================================================

/// Shared accessor for applied state.
///
/// This allows services to read vault heights and health status
/// without needing direct access to the Raft storage.
#[derive(Clone)]
pub struct AppliedStateAccessor {
    state: Arc<RwLock<AppliedState>>,
}

impl AppliedStateAccessor {
    /// Get the current height for a vault.
    pub fn vault_height(&self, namespace_id: NamespaceId, vault_id: VaultId) -> u64 {
        self.state
            .read()
            .vault_heights
            .get(&(namespace_id, vault_id))
            .copied()
            .unwrap_or(0)
    }

    /// Get the health status for a vault.
    pub fn vault_health(&self, namespace_id: NamespaceId, vault_id: VaultId) -> VaultHealthStatus {
        self.state
            .read()
            .vault_health
            .get(&(namespace_id, vault_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Get all vault heights (for GetTip when no specific vault is requested).
    pub fn all_vault_heights(&self) -> HashMap<(NamespaceId, VaultId), u64> {
        self.state.read().vault_heights.clone()
    }

    /// Get namespace metadata by ID.
    pub fn get_namespace(&self, namespace_id: NamespaceId) -> Option<NamespaceMeta> {
        self.state
            .read()
            .namespaces
            .get(&namespace_id)
            .filter(|ns| !ns.deleted)
            .cloned()
    }

    /// Get namespace metadata by name.
    pub fn get_namespace_by_name(&self, name: &str) -> Option<NamespaceMeta> {
        self.state
            .read()
            .namespaces
            .values()
            .find(|ns| !ns.deleted && ns.name == name)
            .cloned()
    }

    /// List all active namespaces.
    pub fn list_namespaces(&self) -> Vec<NamespaceMeta> {
        self.state
            .read()
            .namespaces
            .values()
            .filter(|ns| !ns.deleted)
            .cloned()
            .collect()
    }

    /// Get vault metadata by ID.
    pub fn get_vault(&self, namespace_id: NamespaceId, vault_id: VaultId) -> Option<VaultMeta> {
        self.state
            .read()
            .vaults
            .get(&(namespace_id, vault_id))
            .filter(|v| !v.deleted)
            .cloned()
    }

    /// List all active vaults in a namespace.
    pub fn list_vaults(&self, namespace_id: NamespaceId) -> Vec<VaultMeta> {
        self.state
            .read()
            .vaults
            .values()
            .filter(|v| v.namespace_id == namespace_id && !v.deleted)
            .cloned()
            .collect()
    }

    /// Get the last committed sequence for a client.
    ///
    /// Returns 0 if no sequence has been committed for this client.
    pub fn client_sequence(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        client_id: &str,
    ) -> u64 {
        self.state
            .read()
            .client_sequences
            .get(&(namespace_id, vault_id, client_id.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Get the current shard height (for snapshot info).
    pub fn shard_height(&self) -> u64 {
        self.state.read().shard_height
    }

    /// Get all vault metadata (for retention policy checks).
    pub fn all_vaults(&self) -> HashMap<(NamespaceId, VaultId), VaultMeta> {
        self.state
            .read()
            .vaults
            .iter()
            .filter(|(_, v)| !v.deleted)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

/// Combined Raft storage backed by Inkwell.
///
/// This implementation stores:
/// - Log entries in the RaftLog table indexed by log index
/// - Vote state (term + voted_for) in RaftState metadata
/// - Committed log ID for recovery
/// - Applied state (state machine) in RaftState metadata
///
/// Additionally, it integrates with:
/// - StateLayer for entity/relationship storage and state root computation
/// - BlockArchive for permanent block storage
///
/// The generic parameter `B` controls the storage backend for StateLayer and
/// BlockArchive. The raft log itself always uses FileBackend for durability.
pub struct RaftLogStore<B: StorageBackend = FileBackend> {
    /// Inkwell database handle for raft log.
    db: Arc<Database<FileBackend>>,
    /// Cached vote state.
    vote_cache: RwLock<Option<Vote<LedgerNodeId>>>,
    /// Cached last purged log ID.
    last_purged_cache: RwLock<Option<LogId<LedgerNodeId>>>,
    /// Applied state (state machine) - shared with accessor.
    applied_state: Arc<RwLock<AppliedState>>,
    /// State layer for entity/relationship storage (shared with read service).
    state_layer: Option<Arc<StateLayer<B>>>,
    /// Block archive for permanent block storage.
    block_archive: Option<Arc<BlockArchive<B>>>,
    /// Shard ID for this Raft group.
    shard_id: ShardId,
    /// Node ID for block metadata.
    node_id: String,
    /// Shard chain state (height and previous hash).
    ///
    /// Consolidated into single lock to avoid lock ordering issues.
    /// See: apply_to_state_machine, restore_from_db
    shard_chain: RwLock<ShardChainState>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Open or create a new log store at the given path.
    ///
    /// This creates a basic log store without StateLayer or BlockArchive integration.
    /// Use `with_state_layer` and `with_block_archive` to add those capabilities.
    /// Page size for Raft log storage.
    ///
    /// Using 16KB pages (vs default 4KB) to allow larger batch sizes.
    /// A batch of 100 operations typically serializes to ~8-12KB with postcard.
    /// Max supported: 64KB. Minimum: 512 bytes (must be power of 2).
    pub const RAFT_PAGE_SIZE: usize = 16 * 1024; // 16KB

    /// Open or create a Raft log storage database.
    ///
    /// New databases are created with 16KB pages to support larger batch sizes.
    /// Existing databases retain their original page size for backwards compatibility.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<LedgerNodeId>> {
        // Try to open existing database, otherwise create new one with larger pages
        let db = if path.as_ref().exists() {
            // Existing database - use whatever page size it was created with
            Database::open(path.as_ref()).map_err(|e| to_storage_error(&e))?
        } else {
            // New database - use larger pages for bigger batch sizes
            let config = DatabaseConfig {
                page_size: Self::RAFT_PAGE_SIZE,
                ..Default::default()
            };
            Database::create_with_config(path.as_ref(), config).map_err(|e| to_storage_error(&e))?
        };

        // Inkwell has fixed tables - no need to create them explicitly

        let store = Self {
            db: Arc::new(db),
            vote_cache: RwLock::new(None),
            last_purged_cache: RwLock::new(None),
            applied_state: Arc::new(RwLock::new(AppliedState {
                sequences: SequenceCounters::new(),
                ..Default::default()
            })),
            state_layer: None,
            block_archive: None,
            shard_id: 0,
            node_id: String::new(),
            shard_chain: RwLock::new(ShardChainState {
                height: 0,
                previous_hash: ledger_types::ZERO_HASH,
            }),
        };

        // Load cached values
        store.load_caches()?;

        Ok(store)
    }

    /// Configure the state layer for transaction application.
    pub fn with_state_layer(mut self, state_layer: Arc<StateLayer<B>>) -> Self {
        self.state_layer = Some(state_layer);
        self
    }

    /// Configure the block archive for permanent block storage.
    pub fn with_block_archive(mut self, block_archive: Arc<BlockArchive<B>>) -> Self {
        self.block_archive = Some(block_archive);
        self
    }

    /// Configure shard metadata.
    pub fn with_shard_config(mut self, shard_id: ShardId, node_id: String) -> Self {
        self.shard_id = shard_id;
        self.node_id = node_id;
        self
    }

    /// Get the current shard height.
    pub fn current_shard_height(&self) -> u64 {
        self.shard_chain.read().height
    }

    /// Get a reference to the state layer (if configured).
    pub fn state_layer(&self) -> Option<&Arc<StateLayer<B>>> {
        self.state_layer.as_ref()
    }

    /// Get a reference to the block archive (if configured).
    pub fn block_archive(&self) -> Option<&Arc<BlockArchive<B>>> {
        self.block_archive.as_ref()
    }

    /// Get an accessor for reading applied state.
    ///
    /// This accessor can be cloned and passed to services that need to read
    /// vault heights and health status.
    pub fn accessor(&self) -> AppliedStateAccessor {
        AppliedStateAccessor {
            state: self.applied_state.clone(),
        }
    }

    /// Load metadata values into caches.
    fn load_caches(&self) -> Result<(), StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        // Load vote
        if let Some(vote_data) = read_txn
            .get::<tables::RaftState>(&KEY_VOTE.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let vote: Vote<LedgerNodeId> = decode(&vote_data).map_err(|e| to_serde_error(&e))?;
            *self.vote_cache.write() = Some(vote);
        }

        // Load last purged
        if let Some(purged_data) = read_txn
            .get::<tables::RaftState>(&KEY_LAST_PURGED.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let purged: LogId<LedgerNodeId> =
                decode(&purged_data).map_err(|e| to_serde_error(&e))?;
            *self.last_purged_cache.write() = Some(purged);
        }

        // Load applied state
        if let Some(state_data) = read_txn
            .get::<tables::RaftState>(&KEY_APPLIED_STATE.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let state: AppliedState = decode(&state_data).map_err(|e| to_serde_error(&e))?;
            // Restore shard chain tracking from persisted state (single lock)
            *self.shard_chain.write() = ShardChainState {
                height: state.shard_height,
                previous_hash: state.previous_shard_hash,
            };
            *self.applied_state.write() = state;
        }

        Ok(())
    }

    /// Get the last log entry.
    fn get_last_entry(
        &self,
    ) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        if let Some((_, entry_data)) = read_txn
            .last::<tables::RaftLog>()
            .map_err(|e| to_storage_error(&e))?
        {
            let entry: Entry<LedgerTypeConfig> =
                decode(&entry_data).map_err(|e| to_serde_error(&e))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Apply a single request and return the response plus optional vault entry.
    ///
    /// For Write requests, this also returns a VaultEntry that should be included
    /// in the ShardBlock. The caller is responsible for collecting these entries
    /// and creating the ShardBlock.
    fn apply_request(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
    ) -> (LedgerResponse, Option<VaultEntry>) {
        match request {
            LedgerRequest::Write {
                namespace_id,
                vault_id,
                transactions,
            } => {
                let key = (*namespace_id, *vault_id);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return (
                        LedgerResponse::Error {
                            message: format!(
                                "Vault {}:{} is diverged and not accepting writes",
                                namespace_id, vault_id
                            ),
                        },
                        None,
                    );
                }

                let current_height = state.vault_heights.get(&key).copied().unwrap_or(0);
                let new_height = current_height + 1;

                // Get previous vault hash (ZERO_HASH for genesis)
                let previous_vault_hash = state
                    .previous_vault_hashes
                    .get(&key)
                    .copied()
                    .unwrap_or(ledger_types::ZERO_HASH);

                // Apply transactions to state layer if configured
                let state_root = if let Some(state_layer) = &self.state_layer {
                    // Collect all operations from all transactions
                    let all_ops: Vec<_> = transactions
                        .iter()
                        .flat_map(|tx| tx.operations.clone())
                        .collect();

                    // Apply operations (StateLayer is internally thread-safe via ledger-db MVCC)
                    if let Err(e) = state_layer.apply_operations(*vault_id, &all_ops, new_height) {
                        // Per DESIGN.md §6.1: On CAS failure, return current state for conflict resolution
                        return match e {
                            StateError::PreconditionFailed {
                                key,
                                current_version,
                                current_value,
                                failed_condition,
                            } => (
                                LedgerResponse::PreconditionFailed {
                                    key,
                                    current_version,
                                    current_value,
                                    failed_condition,
                                },
                                None,
                            ),
                            other => (
                                LedgerResponse::Error {
                                    message: format!("Failed to apply operations: {}", other),
                                },
                                None,
                            ),
                        };
                    }

                    // Compute state root
                    match state_layer.compute_state_root(*vault_id) {
                        Ok(root) => root,
                        Err(e) => {
                            return (
                                LedgerResponse::Error {
                                    message: format!("Failed to compute state root: {}", e),
                                },
                                None,
                            );
                        }
                    }
                } else {
                    // No state layer configured, use placeholder
                    ledger_types::EMPTY_HASH
                };

                // Compute tx merkle root
                let tx_merkle_root = compute_tx_merkle_root(transactions);

                // Update vault height in applied state
                state.vault_heights.insert(key, new_height);

                // Update last write timestamp from latest transaction (deterministic)
                if let Some(last_tx) = transactions.last() {
                    if let Some(vault_meta) = state.vaults.get_mut(&key) {
                        vault_meta.last_write_timestamp = last_tx.timestamp.timestamp() as u64;
                    }
                }

                // Persist client sequences for idempotency recovery.
                // Track the highest sequence number per client for this vault.
                for tx in transactions {
                    let client_key = (*namespace_id, *vault_id, tx.client_id.clone());
                    let current = state
                        .client_sequences
                        .get(&client_key)
                        .copied()
                        .unwrap_or(0);
                    if tx.sequence > current {
                        state.client_sequences.insert(client_key, tx.sequence);
                    }
                }

                // Build VaultEntry for ShardBlock
                let vault_entry = VaultEntry {
                    namespace_id: *namespace_id,
                    vault_id: *vault_id,
                    vault_height: new_height,
                    previous_vault_hash,
                    transactions: transactions.clone(),
                    tx_merkle_root,
                    state_root,
                };

                // Compute block hash from vault entry (for response)
                // We temporarily build a BlockHeader to compute the hash
                let block_hash = self.compute_vault_block_hash(&vault_entry);

                (
                    LedgerResponse::Write {
                        block_height: new_height,
                        block_hash,
                    },
                    Some(vault_entry),
                )
            }

            LedgerRequest::CreateNamespace { name, shard_id } => {
                let namespace_id = state.sequences.next_namespace();
                // Use provided shard_id or default to 0 (system shard)
                let assigned_shard = shard_id.unwrap_or(0);
                state.namespaces.insert(
                    namespace_id,
                    NamespaceMeta {
                        namespace_id,
                        name: name.clone(),
                        shard_id: assigned_shard,
                        deleted: false,
                    },
                );

                // Persist namespace to StateLayer for ShardRouter discovery.
                // This enables the ShardRouter to find the namespace->shard mapping.
                if let Some(state_layer) = &self.state_layer {
                    let registry = NamespaceRegistry {
                        namespace_id,
                        name: name.clone(),
                        shard_id: assigned_shard,
                        member_nodes: state
                            .membership
                            .membership()
                            .nodes()
                            .map(|(id, _)| id.to_string())
                            .collect(),
                        status: NamespaceStatus::Active,
                        config_version: 1,
                        created_at: chrono::Utc::now(),
                    };

                    // Serialize and write to StateLayer
                    if let Ok(value) = encode(&registry) {
                        let key = SystemKeys::namespace_key(namespace_id);
                        let name_index_key = SystemKeys::namespace_name_index_key(name);
                        let ops = vec![
                            Operation::SetEntity {
                                key,
                                value,
                                condition: None,
                                expires_at: None,
                            },
                            Operation::SetEntity {
                                key: name_index_key,
                                value: namespace_id.to_string().into_bytes(),
                                condition: None,
                                expires_at: None,
                            },
                        ];

                        if let Err(e) = state_layer.apply_operations(SYSTEM_VAULT_ID, &ops, 0) {
                            tracing::error!(
                                namespace_id,
                                error = %e,
                                "Failed to persist namespace to StateLayer"
                            );
                        }
                    }
                }

                (
                    LedgerResponse::NamespaceCreated {
                        namespace_id,
                        shard_id: assigned_shard,
                    },
                    None,
                )
            }

            LedgerRequest::CreateVault {
                namespace_id,
                name,
                retention_policy,
            } => {
                let vault_id = state.sequences.next_vault();
                let key = (*namespace_id, vault_id);
                state.vault_heights.insert(key, 0);
                state.vault_health.insert(key, VaultHealthStatus::Healthy);
                state.vaults.insert(
                    key,
                    VaultMeta {
                        namespace_id: *namespace_id,
                        vault_id,
                        name: name.clone(),
                        deleted: false,
                        last_write_timestamp: 0, // No writes yet
                        retention_policy: retention_policy.unwrap_or_default(),
                    },
                );
                (LedgerResponse::VaultCreated { vault_id }, None)
            }

            LedgerRequest::DeleteNamespace { namespace_id } => {
                // Check if namespace has active vaults
                let has_vaults = state
                    .vaults
                    .iter()
                    .any(|((ns, _), v)| *ns == *namespace_id && !v.deleted);

                let response = if has_vaults {
                    LedgerResponse::NamespaceDeleted { success: false }
                } else if let Some(ns) = state.namespaces.get_mut(namespace_id) {
                    ns.deleted = true;
                    LedgerResponse::NamespaceDeleted { success: true }
                } else {
                    LedgerResponse::Error {
                        message: format!("Namespace {} not found", namespace_id),
                    }
                };
                (response, None)
            }

            LedgerRequest::DeleteVault {
                namespace_id,
                vault_id,
            } => {
                let key = (*namespace_id, *vault_id);
                // Mark vault as deleted (keep heights for historical queries)
                let response = if let Some(vault) = state.vaults.get_mut(&key) {
                    vault.deleted = true;
                    LedgerResponse::VaultDeleted { success: true }
                } else {
                    LedgerResponse::Error {
                        message: format!("Vault {}:{} not found", namespace_id, vault_id),
                    }
                };
                (response, None)
            }

            LedgerRequest::UpdateVaultHealth {
                namespace_id,
                vault_id,
                healthy,
                expected_root,
                computed_root,
                diverged_at_height,
                recovery_attempt,
                recovery_started_at,
            } => {
                let key = (*namespace_id, *vault_id);
                if *healthy {
                    // Mark vault as healthy
                    state.vault_health.insert(key, VaultHealthStatus::Healthy);
                    tracing::info!(
                        namespace_id,
                        vault_id,
                        "Vault health updated to Healthy via Raft"
                    );
                } else if let (Some(attempt), Some(started_at)) =
                    (recovery_attempt, recovery_started_at)
                {
                    // Mark vault as recovering
                    state.vault_health.insert(
                        key,
                        VaultHealthStatus::Recovering {
                            started_at: *started_at,
                            attempt: *attempt,
                        },
                    );
                    tracing::info!(
                        namespace_id,
                        vault_id,
                        attempt,
                        "Vault health updated to Recovering via Raft"
                    );
                } else {
                    // Mark vault as diverged
                    let expected = expected_root.unwrap_or(ledger_types::ZERO_HASH);
                    let computed = computed_root.unwrap_or(ledger_types::ZERO_HASH);
                    let at_height = diverged_at_height.unwrap_or(0);
                    state.vault_health.insert(
                        key,
                        VaultHealthStatus::Diverged {
                            expected,
                            computed,
                            at_height,
                        },
                    );
                    tracing::warn!(
                        namespace_id,
                        vault_id,
                        at_height,
                        "Vault health updated to Diverged via Raft"
                    );
                }
                (LedgerResponse::VaultHealthUpdated { success: true }, None)
            }

            LedgerRequest::System(system_request) => {
                let response = match system_request {
                    SystemRequest::CreateUser { name: _, email: _ } => {
                        let user_id = state.sequences.next_user();
                        LedgerResponse::UserCreated { user_id }
                    }
                    SystemRequest::AddNode { .. }
                    | SystemRequest::RemoveNode { .. }
                    | SystemRequest::UpdateNamespaceRouting { .. } => LedgerResponse::Empty,
                };
                (response, None)
            }

            LedgerRequest::BatchWrite { requests } => {
                // Process each request in the batch sequentially, collecting responses.
                // Vault entries are collected and the last one is returned (batches typically
                // target the same vault, so the final block includes all transactions).
                let mut responses = Vec::with_capacity(requests.len());
                let mut last_vault_entry = None;

                for inner_request in requests {
                    let (response, vault_entry) = self.apply_request(inner_request, state);
                    responses.push(response);
                    if vault_entry.is_some() {
                        last_vault_entry = vault_entry;
                    }
                }

                (LedgerResponse::BatchWrite { responses }, last_vault_entry)
            }
        }
    }

    /// Compute a deterministic hash for a vault entry.
    ///
    /// Uses only the cryptographic commitments from the entry, not runtime
    /// metadata like timestamp or proposer. This ensures all Raft nodes
    /// compute the same hash for the same log entry.
    fn compute_vault_block_hash(&self, entry: &VaultEntry) -> Hash {
        ledger_types::vault_entry_hash(entry)
    }

    /// Compute a simple block hash (used in tests).
    #[allow(dead_code)]
    fn compute_block_hash(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        height: u64,
    ) -> Hash {
        use ledger_types::sha256;
        let mut data = Vec::new();
        data.extend_from_slice(&namespace_id.to_le_bytes());
        data.extend_from_slice(&vault_id.to_le_bytes());
        data.extend_from_slice(&height.to_le_bytes());
        sha256(&data)
    }

    /// Persist the applied state.
    fn save_applied_state(&self, state: &AppliedState) -> Result<(), StorageError<LedgerNodeId>> {
        let state_data = encode(state).map_err(|e| to_serde_error(&e))?;
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&KEY_APPLIED_STATE.to_string(), &state_data)
            .map_err(|e| to_storage_error(&e))?;
        write_txn.commit().map_err(|e| to_storage_error(&e))?;
        Ok(())
    }
}

// ============================================================================
// RaftLogReader Implementation
// ============================================================================

impl RaftLogReader<LedgerTypeConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        use std::ops::Bound;

        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        // Convert RangeBounds to start/end indices
        let start_idx = match range.start_bound() {
            Bound::Included(&idx) => idx,
            Bound::Excluded(&idx) => idx.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end_idx = match range.end_bound() {
            Bound::Included(&idx) => Some(idx.saturating_add(1)),
            Bound::Excluded(&idx) => Some(idx),
            Bound::Unbounded => None,
        };

        // Use range iteration from ledger-db
        let start_key = start_idx;
        let end_key = end_idx.unwrap_or(u64::MAX);

        let iter = read_txn
            .range::<tables::RaftLog>(Some(&start_key), Some(&end_key))
            .map_err(|e| to_storage_error(&e))?;

        let mut entries = Vec::new();
        let mut total_bytes = 0usize;
        let deserialize_start = std::time::Instant::now();

        for (_, entry_data) in iter {
            total_bytes += entry_data.len();
            let entry: Entry<LedgerTypeConfig> =
                decode(&entry_data).map_err(|e| to_serde_error(&e))?;
            entries.push(entry);
        }

        // Record batch deserialization metrics
        if !entries.is_empty() {
            let deserialize_secs = deserialize_start.elapsed().as_secs_f64();
            // Record per-entry average to make metrics comparable to encode path
            let per_entry_secs = deserialize_secs / entries.len() as f64;
            metrics::record_postcard_decode(per_entry_secs, "raft_entry");
            metrics::record_serialization_bytes(
                total_bytes / entries.len(),
                "decode",
                "raft_entry",
            );
        }

        Ok(entries)
    }

    /// Get log entries for replication.
    ///
    /// OpenRaft contract: this must not return empty for non-empty range.
    /// If entries are purged, return error to trigger snapshot replication.
    /// If entries are not yet written (race condition), wait for them.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        // Quick path: if range is empty, return empty
        if start >= end {
            return Ok(Vec::new());
        }

        // Retry loop for transient conditions (entries not yet visible)
        // OpenRaft may try to replicate entries before they're fully appended,
        // especially under high concurrent load. We retry with exponential backoff.
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 500; // 5 seconds max wait
        const RETRY_DELAY_MS: u64 = 10;

        loop {
            let entries = self.try_get_log_entries(start..end).await?;

            // Success: got entries
            if !entries.is_empty() {
                return Ok(entries);
            }

            // Empty result - check if purged
            let last_purged = *self.last_purged_cache.read();
            let purged_idx = last_purged.map(|l| l.index).unwrap_or(0);

            // If entries were purged, return error (need snapshot replication)
            if start <= purged_idx {
                return Err(StorageError::IO {
                    source: openraft::StorageIOError::read_logs(openraft::AnyError::error(
                        format!(
                            "Log entries [{}, {}) purged (last_purged={}), need snapshot",
                            start, end, purged_idx
                        ),
                    )),
                });
            }

            // Not purged - might be a race condition, retry
            attempts += 1;
            if attempts >= MAX_ATTEMPTS {
                // Give up and return error - something is seriously wrong
                let last_idx = self
                    .get_last_entry()
                    .ok()
                    .flatten()
                    .map(|e| e.log_id.index)
                    .unwrap_or(0);
                return Err(StorageError::IO {
                    source: openraft::StorageIOError::read_logs(openraft::AnyError::error(
                        format!(
                            "Log entries [{}, {}) not found after {}ms (last={}, purged={})",
                            start,
                            end,
                            attempts * RETRY_DELAY_MS as u32,
                            last_idx,
                            purged_idx
                        ),
                    )),
                });
            }

            // Wait briefly for entries to appear
            tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
        }
    }
}

// ============================================================================
// RaftSnapshotBuilder Implementation
// ============================================================================

/// Snapshot builder for the ledger storage.
pub struct LedgerSnapshotBuilder {
    /// State to snapshot.
    state: AppliedState,
}

impl RaftSnapshotBuilder<LedgerTypeConfig> for LedgerSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let data = encode(&self.state).map_err(|e| to_serde_error(&e))?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.state
                .last_applied
                .as_ref()
                .map(|l| l.index)
                .unwrap_or(0),
            chrono::Utc::now().timestamp()
        );

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.state.last_applied,
                last_membership: self.state.membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ============================================================================
// RaftStorage Implementation (deprecated but non-sealed)
// ============================================================================

#[allow(deprecated)]
impl RaftStorage<LedgerTypeConfig> for RaftLogStore {
    type LogReader = Self;
    type SnapshotBuilder = LedgerSnapshotBuilder;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let last_purged = *self.last_purged_cache.read();
        let last_entry = self.get_last_entry()?;
        let last_log_id = last_entry.map(|e| e.log_id);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            db: Arc::clone(&self.db),
            vote_cache: RwLock::new(*self.vote_cache.read()),
            last_purged_cache: RwLock::new(*self.last_purged_cache.read()),
            applied_state: Arc::clone(&self.applied_state),
            state_layer: self.state_layer.clone(),
            block_archive: self.block_archive.clone(),
            shard_id: self.shard_id,
            node_id: self.node_id.clone(),
            shard_chain: RwLock::new(*self.shard_chain.read()),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let vote_data = encode(vote).map_err(|e| to_serde_error(&e))?;

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&KEY_VOTE.to_string(), &vote_data)
            .map_err(|e| to_storage_error(&e))?;
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.vote_cache.write() = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<LedgerNodeId>>, StorageError<LedgerNodeId>> {
        Ok(*self.vote_cache.read())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<LedgerNodeId>>
    where
        I: IntoIterator<Item = Entry<LedgerTypeConfig>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            tracing::debug!("append_to_log called with empty entries");
            return Ok(());
        }

        let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
        tracing::info!("append_to_log: appending entries {:?}", indices);

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;

        for entry in entries {
            let index = entry.log_id.index;

            // Time postcard serialization (write path hot loop)
            let serialize_start = std::time::Instant::now();
            let entry_data = encode(&entry).map_err(|e| to_serde_error(&e))?;
            let serialize_secs = serialize_start.elapsed().as_secs_f64();

            // Record serialization metrics
            metrics::record_postcard_encode(serialize_secs, "raft_entry");
            metrics::record_serialization_bytes(entry_data.len(), "encode", "raft_entry");

            write_txn
                .insert::<tables::RaftLog>(&index, &entry_data)
                .map_err(|e| to_storage_error(&e))?;
        }

        write_txn.commit().map_err(|e| {
            tracing::error!(
                "append_to_log: commit failed for entries {:?}: {:?}",
                indices,
                e
            );
            to_storage_error(&e)
        })?;

        tracing::info!("append_to_log: committed entries {:?}", indices);
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        // First, collect keys to remove using a read transaction
        let keys_to_remove: Vec<u64> = {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let mut keys = Vec::new();
            let iter = read_txn
                .range::<tables::RaftLog>(Some(&log_id.index), None)
                .map_err(|e| to_storage_error(&e))?;
            for (key_bytes, _) in iter {
                // Key is u64 encoded as big-endian
                if let Ok(arr) = <[u8; 8]>::try_from(key_bytes.as_ref()) {
                    keys.push(u64::from_be_bytes(arr));
                }
            }
            keys
        };

        // Then delete in a write transaction
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        for key in keys_to_remove {
            write_txn
                .delete::<tables::RaftLog>(&key)
                .map_err(|e| to_storage_error(&e))?;
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        // First, collect keys to remove using a read transaction
        let keys_to_remove: Vec<u64> = {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let mut keys = Vec::new();
            // Inclusive end: range from 0 to log_id.index + 1 (exclusive)
            let end_key = log_id.index.saturating_add(1);
            let iter = read_txn
                .range::<tables::RaftLog>(Some(&0u64), Some(&end_key))
                .map_err(|e| to_storage_error(&e))?;
            for (key_bytes, _) in iter {
                // Key is u64 encoded as big-endian
                if let Ok(arr) = <[u8; 8]>::try_from(key_bytes.as_ref()) {
                    keys.push(u64::from_be_bytes(arr));
                }
            }
            keys
        };

        // Then delete in a write transaction
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        for key in keys_to_remove {
            write_txn
                .delete::<tables::RaftLog>(&key)
                .map_err(|e| to_storage_error(&e))?;
        }

        // Save the last purged log ID
        let purged_data = encode(&log_id).map_err(|e| to_serde_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&KEY_LAST_PURGED.to_string(), &purged_data)
            .map_err(|e| to_storage_error(&e))?;

        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.last_purged_cache.write() = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LedgerNodeId>>,
            StoredMembership<LedgerNodeId, openraft::BasicNode>,
        ),
        StorageError<LedgerNodeId>,
    > {
        let state = self.applied_state.read();
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<LedgerTypeConfig>],
    ) -> Result<Vec<LedgerResponse>, StorageError<LedgerNodeId>> {
        let mut responses = Vec::new();
        let mut vault_entries = Vec::new();
        let mut state = self.applied_state.write();

        // Get the committed_index from the last entry (for ShardBlock metadata)
        let committed_index = entries.last().map(|e| e.log_id.index).unwrap_or(0);
        // Term is stored in the leader_id
        let term = entries
            .last()
            .map(|e| e.log_id.leader_id.get_term())
            .unwrap_or(0);

        for entry in entries {
            state.last_applied = Some(entry.log_id);

            let (response, vault_entry) = match &entry.payload {
                EntryPayload::Blank => (LedgerResponse::Empty, None),
                EntryPayload::Normal(request) => self.apply_request(request, &mut state),
                EntryPayload::Membership(membership) => {
                    state.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    (LedgerResponse::Empty, None)
                }
            };

            responses.push(response);

            // Collect vault entries for ShardBlock creation
            if let Some(entry) = vault_entry {
                vault_entries.push(entry);
            }
        }

        // Create and store ShardBlock if we have vault entries
        if !vault_entries.is_empty() {
            let timestamp = chrono::Utc::now();

            // Read shard chain state (single lock acquisition)
            let chain_state = *self.shard_chain.read();
            let new_shard_height = chain_state.height + 1;

            let shard_block = ShardBlock {
                shard_id: self.shard_id,
                shard_height: new_shard_height,
                previous_shard_hash: chain_state.previous_hash,
                vault_entries: vault_entries.clone(),
                timestamp,
                leader_id: self.node_id.clone(),
                term,
                committed_index,
            };

            // Store in block archive if configured
            if let Some(archive) = &self.block_archive {
                if let Err(e) = archive.append_block(&shard_block) {
                    tracing::error!("Failed to store block: {}", e);
                    // Continue - block storage failure is logged but doesn't fail the operation
                }
            }

            // Update previous vault hashes for each entry
            for entry in &vault_entries {
                let vault_block =
                    shard_block.extract_vault_block(entry.namespace_id, entry.vault_id);
                if let Some(vb) = vault_block {
                    let block_hash = ledger_types::hash::block_hash(&vb.header);
                    state
                        .previous_vault_hashes
                        .insert((entry.namespace_id, entry.vault_id), block_hash);
                }
            }

            // Update shard chain tracking (single lock acquisition)
            let shard_hash = ledger_types::sha256(&encode(&shard_block).unwrap_or_default());
            *self.shard_chain.write() = ShardChainState {
                height: new_shard_height,
                previous_hash: shard_hash,
            };

            // Also update AppliedState for snapshot persistence
            state.shard_height = new_shard_height;
            state.previous_shard_hash = shard_hash;
        }

        // Persist state
        drop(state);
        let state_ref = self.applied_state.read().clone();
        self.save_applied_state(&state_ref)?;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LedgerSnapshotBuilder {
            state: self.applied_state.read().clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<LedgerNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<LedgerNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let data = snapshot.into_inner();

        // Try to deserialize as CombinedSnapshot first (new format)
        let combined: CombinedSnapshot = decode(&data).map_err(|e| to_serde_error(&e))?;

        // Restore AppliedState
        *self.applied_state.write() = combined.applied_state.clone();
        self.save_applied_state(&combined.applied_state)?;

        // Restore shard chain tracking from snapshot (single lock)
        *self.shard_chain.write() = ShardChainState {
            height: combined.applied_state.shard_height,
            previous_hash: combined.applied_state.previous_shard_hash,
        };

        // Restore StateLayer entities if StateLayer is configured
        if let Some(state_layer) = &self.state_layer {
            for (vault_id, entities) in &combined.vault_entities {
                for entity in entities {
                    // Convert entity to SetEntity operation
                    let ops = vec![ledger_types::Operation::SetEntity {
                        key: String::from_utf8_lossy(&entity.key).to_string(),
                        value: entity.value.clone(),
                        condition: None, // No condition for snapshot restore
                        expires_at: if entity.expires_at == 0 {
                            None
                        } else {
                            Some(entity.expires_at)
                        },
                    }];
                    // Apply at the entity's version height
                    if let Err(e) = state_layer.apply_operations(*vault_id, &ops, entity.version) {
                        tracing::warn!(
                            vault_id,
                            key = %String::from_utf8_lossy(&entity.key),
                            error = %e,
                            "Failed to restore entity from snapshot"
                        );
                    }
                }
            }
            tracing::info!(
                vault_count = combined.vault_entities.len(),
                "Restored StateLayer from snapshot"
            );
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let state = self.applied_state.read();

        if state.last_applied.is_none() {
            return Ok(None);
        }

        // Collect entities from StateLayer if configured
        // StateLayer is internally thread-safe via ledger-db's MVCC, so no lock needed
        let vault_entities = if let Some(state_layer) = &self.state_layer {
            let mut entities_map = HashMap::new();

            // Get entities for each known vault
            for &(namespace_id, vault_id) in state.vault_heights.keys() {
                // List all entities in this vault (up to 10000 per vault for snapshot)
                match state_layer.list_entities(vault_id, None, None, 10000) {
                    Ok(entities) => {
                        if !entities.is_empty() {
                            entities_map.insert(vault_id, entities);
                            tracing::debug!(
                                namespace_id,
                                vault_id,
                                count = entities_map.get(&vault_id).map(|e| e.len()).unwrap_or(0),
                                "Collected entities for snapshot"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            namespace_id,
                            vault_id,
                            error = %e,
                            "Failed to list entities for snapshot"
                        );
                    }
                }
            }

            entities_map
        } else {
            HashMap::new()
        };

        // Create combined snapshot
        let combined = CombinedSnapshot {
            applied_state: state.clone(),
            vault_entities,
        };

        let data = encode(&combined).map_err(|e| to_serde_error(&e))?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            state.last_applied.as_ref().map(|l| l.index).unwrap_or(0),
            chrono::Utc::now().timestamp()
        );

        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.last_applied,
                last_membership: state.membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

// ============================================================================
// Error Helpers
// ============================================================================

fn to_storage_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

fn to_serde_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;
    use ledger_db::FileBackend;
    use openraft::CommittedLeaderId;
    use tempfile::tempdir;

    /// Helper to create log IDs for tests.
    #[allow(dead_code)]
    fn make_log_id(term: u64, index: u64) -> LogId<LedgerNodeId> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    #[tokio::test]
    async fn test_log_store_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Verify database can be read (tables exist in ledger-db by default)
        let read_txn = store.db.read().expect("begin read");
        // Tables are fixed in ledger-db - just verify we can get a transaction
        let _ = read_txn
            .get::<tables::RaftLog>(&0u64)
            .expect("query RaftLog");
        let _ = read_txn
            .get::<tables::RaftState>(&"test".to_string())
            .expect("query RaftState");
    }

    #[tokio::test]
    async fn test_save_and_read_vote() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let mut store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        let vote = Vote::new(1, 42);
        store.save_vote(&vote).await.expect("save vote");

        let read_vote = store.read_vote().await.expect("read vote");
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn test_sequence_counters() {
        let mut counters = SequenceCounters::new();

        assert_eq!(counters.next_namespace(), 1);
        assert_eq!(counters.next_namespace(), 2);
        assert_eq!(counters.next_vault(), 1);
        assert_eq!(counters.next_vault(), 2);
        assert_eq!(counters.next_user(), 1);
        assert_eq!(counters.next_user(), 2);
    }

    #[tokio::test]
    async fn test_apply_create_namespace() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateNamespace {
            name: "test-ns".to_string(),
            shard_id: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::NamespaceCreated { namespace_id, .. } => {
                assert_eq!(namespace_id, 1);
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultCreated { vault_id } => {
                assert_eq!(vault_id, 1);
                assert_eq!(state.vault_heights.get(&(1, 1)), Some(&0));
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_diverged_vault_rejects_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Mark vault as diverged
        state.vault_health.insert(
            (1, 1),
            VaultHealthStatus::Diverged {
                expected: [1u8; 32],
                computed: [2u8; 32],
                at_height: 10,
            },
        );

        let request = LedgerRequest::Write {
            namespace_id: 1,
            vault_id: 1,
            transactions: vec![],
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("diverged"));
            }
            _ => panic!("expected error response"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_to_healthy() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start with a diverged vault
        state.vault_health.insert(
            (1, 1),
            VaultHealthStatus::Diverged {
                expected: [1u8; 32],
                computed: [2u8; 32],
                at_height: 10,
            },
        );

        // Update to healthy
        let request = LedgerRequest::UpdateVaultHealth {
            namespace_id: 1,
            vault_id: 1,
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            }
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now healthy
        assert_eq!(
            state.vault_health.get(&(1, 1)),
            Some(&VaultHealthStatus::Healthy)
        );
    }

    #[tokio::test]
    async fn test_update_vault_health_to_diverged() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start healthy
        state
            .vault_health
            .insert((1, 1), VaultHealthStatus::Healthy);

        // Update to diverged
        let request = LedgerRequest::UpdateVaultHealth {
            namespace_id: 1,
            vault_id: 1,
            healthy: false,
            expected_root: Some([0xAA; 32]),
            computed_root: Some([0xBB; 32]),
            diverged_at_height: Some(42),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            }
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now diverged with correct values
        match state.vault_health.get(&(1, 1)) {
            Some(VaultHealthStatus::Diverged {
                expected,
                computed,
                at_height,
            }) => {
                assert_eq!(*expected, [0xAA; 32]);
                assert_eq!(*computed, [0xBB; 32]);
                assert_eq!(*at_height, 42);
            }
            _ => panic!("expected Diverged health status"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_to_recovering() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start with a diverged vault
        state.vault_health.insert(
            (1, 1),
            VaultHealthStatus::Diverged {
                expected: [1u8; 32],
                computed: [2u8; 32],
                at_height: 10,
            },
        );

        // Update to recovering
        let request = LedgerRequest::UpdateVaultHealth {
            namespace_id: 1,
            vault_id: 1,
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(1),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            }
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now recovering
        match state.vault_health.get(&(1, 1)) {
            Some(VaultHealthStatus::Recovering { attempt, .. }) => {
                assert_eq!(*attempt, 1);
            }
            _ => panic!("expected Recovering health status"),
        }

        // Test recovery attempt 2 (circuit breaker)
        let request = LedgerRequest::UpdateVaultHealth {
            namespace_id: 1,
            vault_id: 1,
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(2),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            }
            _ => panic!("expected VaultHealthUpdated response"),
        }

        match state.vault_health.get(&(1, 1)) {
            Some(VaultHealthStatus::Recovering { attempt, .. }) => {
                assert_eq!(*attempt, 2);
            }
            _ => panic!("expected Recovering health status with attempt 2"),
        }
    }

    // ========================================================================
    // Determinism Tests (Phase 3.7)
    // ========================================================================
    //
    // These tests verify that the state machine is deterministic - a critical
    // requirement for Raft consensus. All nodes must produce identical state
    // when applying the same log entries.
    //
    // CRITICAL: The state machine must NEVER use:
    // - rand::random() or any RNG
    // - SystemTime::now() for state (only logging)
    // - HashMap iteration order (use BTreeMap for deterministic ordering)
    // - Floating point operations that vary by platform
    // - Any external I/O that could vary between nodes

    /// Verify same input sequence produces identical outputs on independent state machines.
    ///
    /// This is the fundamental Raft invariant: if two nodes apply the same log entries
    /// in the same order, they MUST produce identical state.
    #[tokio::test]
    async fn test_deterministic_apply() {
        // Create two independent state machines (simulating two Raft nodes)
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Same sequence of requests to apply
        let requests = vec![
            LedgerRequest::CreateNamespace {
                name: "acme-corp".to_string(),
                shard_id: None,
            },
            LedgerRequest::CreateNamespace {
                name: "startup-inc".to_string(),
                shard_id: None,
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("production".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("staging".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                namespace_id: 2,
                name: Some("main".to_string()),
                retention_policy: None,
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 2,
                vault_id: 3,
                transactions: vec![],
            },
            LedgerRequest::System(SystemRequest::CreateUser {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
            }),
            LedgerRequest::System(SystemRequest::CreateUser {
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
            }),
        ];

        // Apply to node A
        let mut state_a = store_a.applied_state.write();
        let mut results_a = Vec::new();
        for request in &requests {
            let (response, _) = store_a.apply_request(request, &mut state_a);
            results_a.push(response);
        }
        drop(state_a);

        // Apply to node B
        let mut state_b = store_b.applied_state.write();
        let mut results_b = Vec::new();
        for request in &requests {
            let (response, _) = store_b.apply_request(request, &mut state_b);
            results_b.push(response);
        }
        drop(state_b);

        // Results must be identical
        assert_eq!(
            results_a, results_b,
            "Same inputs must produce identical results on all nodes"
        );

        // Final state must be identical
        let final_state_a = store_a.applied_state.read();
        let final_state_b = store_b.applied_state.read();

        assert_eq!(
            final_state_a.sequences, final_state_b.sequences,
            "Sequence counters must match"
        );
        assert_eq!(
            final_state_a.vault_heights, final_state_b.vault_heights,
            "Vault heights must match"
        );
        assert_eq!(
            final_state_a.vault_health, final_state_b.vault_health,
            "Vault health must match"
        );
    }

    /// Verify ID generation is deterministic across state machines.
    ///
    /// IDs are assigned by the leader during log application. All nodes must
    /// generate the same IDs for the same sequence of requests.
    #[tokio::test]
    async fn test_deterministic_id_generation() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Apply same sequence on both nodes
        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Namespace IDs
        let ns_id_a1 = state_a.sequences.next_namespace();
        let ns_id_a2 = state_a.sequences.next_namespace();
        let ns_id_b1 = state_b.sequences.next_namespace();
        let ns_id_b2 = state_b.sequences.next_namespace();

        assert_eq!(ns_id_a1, ns_id_b1, "First namespace ID must match");
        assert_eq!(ns_id_a2, ns_id_b2, "Second namespace ID must match");

        // Vault IDs
        let vault_id_a1 = state_a.sequences.next_vault();
        let vault_id_a2 = state_a.sequences.next_vault();
        let vault_id_b1 = state_b.sequences.next_vault();
        let vault_id_b2 = state_b.sequences.next_vault();

        assert_eq!(vault_id_a1, vault_id_b1, "First vault ID must match");
        assert_eq!(vault_id_a2, vault_id_b2, "Second vault ID must match");

        // User IDs
        let user_id_a1 = state_a.sequences.next_user();
        let user_id_a2 = state_a.sequences.next_user();
        let user_id_b1 = state_b.sequences.next_user();
        let user_id_b2 = state_b.sequences.next_user();

        assert_eq!(user_id_a1, user_id_b1, "First user ID must match");
        assert_eq!(user_id_a2, user_id_b2, "Second user ID must match");
    }

    /// Verify block hashes are deterministic.
    ///
    /// The same (namespace, vault, height) must always produce the same block hash.
    #[tokio::test]
    async fn test_deterministic_block_hash() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Same inputs must produce same hash
        let hash_a = store_a.compute_block_hash(1, 2, 3);
        let hash_b = store_b.compute_block_hash(1, 2, 3);

        assert_eq!(hash_a, hash_b, "Block hashes must be deterministic");

        // Different inputs must produce different hashes
        let hash_c = store_a.compute_block_hash(1, 2, 4);
        assert_ne!(
            hash_a, hash_c,
            "Different inputs should produce different hashes"
        );
    }

    /// Verify vault height tracking is deterministic.
    ///
    /// Writes to the same vault must increment height consistently across nodes.
    #[tokio::test]
    async fn test_deterministic_vault_heights() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create vault on both nodes
        let create_vault = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: Some("test".to_string()),
            retention_policy: None,
        };
        store_a.apply_request(&create_vault, &mut state_a);
        store_b.apply_request(&create_vault, &mut state_b);

        // Apply multiple writes
        for _ in 0..5 {
            let write = LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            };
            store_a.apply_request(&write, &mut state_a);
            store_b.apply_request(&write, &mut state_b);
        }

        // Heights must match
        assert_eq!(
            state_a.vault_heights.get(&(1, 1)),
            state_b.vault_heights.get(&(1, 1)),
            "Vault heights must be identical after same operations"
        );
        assert_eq!(
            state_a.vault_heights.get(&(1, 1)),
            Some(&5),
            "Height should be 5 after 5 writes"
        );
    }

    /// Verify interleaved operations across multiple vaults are deterministic.
    ///
    /// Real workloads have writes to multiple vaults interleaved. The state
    /// machine must handle this deterministically.
    #[tokio::test]
    async fn test_deterministic_interleaved_operations() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create namespace and vaults
        let requests: Vec<LedgerRequest> = vec![
            LedgerRequest::CreateNamespace {
                name: "ns1".to_string(),
                shard_id: None,
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("vault-a".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("vault-b".to_string()),
                retention_policy: None,
            },
        ];

        for req in &requests {
            store_a.apply_request(req, &mut state_a);
            store_b.apply_request(req, &mut state_b);
        }

        // Interleaved writes to different vaults
        let interleaved: Vec<LedgerRequest> = vec![
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 2,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 2,
                transactions: vec![],
            },
            LedgerRequest::Write {
                namespace_id: 1,
                vault_id: 1,
                transactions: vec![],
            },
        ];

        let mut results_a = Vec::new();
        let mut results_b = Vec::new();

        for req in &interleaved {
            let (response_a, _) = store_a.apply_request(req, &mut state_a);
            let (response_b, _) = store_b.apply_request(req, &mut state_b);
            results_a.push(response_a);
            results_b.push(response_b);
        }

        // Results must match
        assert_eq!(
            results_a, results_b,
            "Interleaved operation results must match"
        );

        // Vault 1: 3 writes, Vault 2: 2 writes
        assert_eq!(state_a.vault_heights.get(&(1, 1)), Some(&3));
        assert_eq!(state_a.vault_heights.get(&(1, 2)), Some(&2));
        assert_eq!(state_a.vault_heights, state_b.vault_heights);
    }

    /// Verify state can be serialized and deserialized deterministically.
    ///
    /// Snapshots must serialize to the same bytes on all nodes for the same state.
    #[tokio::test]
    async fn test_deterministic_state_serialization() {
        let mut state_a = AppliedState {
            sequences: SequenceCounters::new(),
            ..Default::default()
        };
        let mut state_b = AppliedState {
            sequences: SequenceCounters::new(),
            ..Default::default()
        };

        // Apply same mutations
        state_a.sequences.next_namespace();
        state_a.sequences.next_vault();
        state_a.vault_heights.insert((1, 1), 42);

        state_b.sequences.next_namespace();
        state_b.sequences.next_vault();
        state_b.vault_heights.insert((1, 1), 42);

        // Serialize both
        let bytes_a = postcard::to_allocvec(&state_a).expect("serialize a");
        let bytes_b = postcard::to_allocvec(&state_b).expect("serialize b");

        assert_eq!(bytes_a, bytes_b, "Serialized state must be identical");

        // Deserialize and verify
        let restored_a: AppliedState = postcard::from_bytes(&bytes_a).expect("deserialize a");
        let restored_b: AppliedState = postcard::from_bytes(&bytes_b).expect("deserialize b");

        assert_eq!(restored_a.sequences, restored_b.sequences);
        assert_eq!(restored_a.vault_heights, restored_b.vault_heights);
    }

    /// Verify that sequence counters start at well-defined values.
    ///
    /// All nodes must start with the same initial counter values.
    #[test]
    fn test_sequence_counters_initial_values() {
        let counters = SequenceCounters::new();

        // Verify initial values per DESIGN.md:
        // - namespace 0 is reserved for _system
        // - IDs start at 1
        assert_eq!(counters.namespace, 1, "Namespace counter should start at 1");
        assert_eq!(counters.vault, 1, "Vault counter should start at 1");
        assert_eq!(counters.user, 1, "User counter should start at 1");
    }

    // ========================================================================
    // State Machine Integration Tests
    // ========================================================================
    //
    // These tests verify the full state machine flow including StateLayer
    // integration, block creation, and snapshot persistence.

    /// Test that Write with transactions produces a VaultEntry with proper fields.
    ///
    /// This verifies the critical path: Write → apply → VaultEntry creation.
    #[tokio::test]
    async fn test_write_produces_vault_entry() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Setup: create namespace and vault
        store.apply_request(
            &LedgerRequest::CreateNamespace {
                name: "test".to_string(),
                shard_id: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("vault1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Apply a write with transactions
        let tx = ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "test-client".to_string(),
            sequence: 1,
            operations: vec![ledger_types::Operation::SetEntity {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
            actor: "test-actor".to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id: 1,
            vault_id: 1,
            transactions: vec![tx],
        };

        let (response, vault_entry) = store.apply_request(&request, &mut state);

        // Verify response
        match response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 1, "First write should be height 1");
            }
            _ => panic!("Expected Write response"),
        }

        // Verify VaultEntry was created
        let entry = vault_entry.expect("VaultEntry should be created");
        assert_eq!(entry.namespace_id, 1);
        assert_eq!(entry.vault_id, 1);
        assert_eq!(entry.vault_height, 1);
        assert_eq!(entry.transactions.len(), 1);
        // state_root and tx_merkle_root will be ZERO_HASH without StateLayer configured
        // but the structure should be correct
    }

    /// Test that shard_height is tracked in AppliedState for snapshot persistence.
    #[tokio::test]
    async fn test_shard_height_tracked_in_applied_state() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Initial shard height should be 0
        assert_eq!(store.current_shard_height(), 0);

        // After applying entries, shard height should increment
        // Note: full shard height increment requires apply_to_state_machine
        // which creates ShardBlocks. This test verifies the accessor.
        let state = store.applied_state.read();
        assert_eq!(state.shard_height, 0, "Initial shard height should be 0");
    }

    /// Test that AppliedState serialization preserves all fields including shard tracking.
    #[tokio::test]
    async fn test_applied_state_snapshot_round_trip() {
        use openraft::StoredMembership;

        let mut original = AppliedState {
            last_applied: Some(make_log_id(1, 10)),
            membership: StoredMembership::default(),
            sequences: SequenceCounters::new(),
            vault_heights: HashMap::new(),
            vault_health: HashMap::new(),
            previous_vault_hashes: HashMap::new(),
            namespaces: HashMap::new(),
            vaults: HashMap::new(),
            shard_height: 42,
            previous_shard_hash: [0xAB; 32],
            client_sequences: HashMap::new(),
        };

        // Add some data
        original.sequences.next_namespace();
        original.sequences.next_vault();
        original.vault_heights.insert((1, 1), 100);
        original.vault_heights.insert((1, 2), 50);
        original.vault_health.insert(
            (2, 1),
            VaultHealthStatus::Diverged {
                expected: [1u8; 32],
                computed: [2u8; 32],
                at_height: 10,
            },
        );
        original.previous_vault_hashes.insert((1, 1), [0xCD; 32]);
        original.namespaces.insert(
            1,
            NamespaceMeta {
                namespace_id: 1,
                shard_id: 0,
                name: "test-ns".to_string(),
                deleted: false,
            },
        );
        original.vaults.insert(
            (1, 1),
            VaultMeta {
                namespace_id: 1,
                vault_id: 1,
                name: Some("test-vault".to_string()),
                deleted: false,
                last_write_timestamp: 1234567899,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );

        // Serialize and deserialize
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let restored: AppliedState = postcard::from_bytes(&bytes).expect("deserialize");

        // Verify key fields restored
        assert_eq!(restored.sequences, original.sequences);
        assert_eq!(restored.vault_heights, original.vault_heights);
        assert_eq!(restored.vault_health, original.vault_health);
        assert_eq!(
            restored.previous_vault_hashes,
            original.previous_vault_hashes
        );
        assert_eq!(restored.shard_height, 42, "shard_height must be preserved");
        assert_eq!(
            restored.previous_shard_hash, [0xAB; 32],
            "previous_shard_hash must be preserved"
        );
        // Verify namespace and vault counts (HashMaps don't implement PartialEq for complex types)
        assert_eq!(restored.namespaces.len(), 1);
        assert_eq!(restored.vaults.len(), 1);
        assert!(restored.namespaces.contains_key(&1));
        assert!(restored.vaults.contains_key(&(1, 1)));
    }

    /// Test that AppliedStateAccessor provides correct data.
    #[tokio::test]
    async fn test_applied_state_accessor() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let accessor = store.accessor();

        // Setup some state
        {
            let mut state = store.applied_state.write();
            state.vault_heights.insert((1, 1), 42);
            state.vault_heights.insert((1, 2), 100);
            state.shard_height = 99;
            state.namespaces.insert(
                1,
                NamespaceMeta {
                    namespace_id: 1,
                    shard_id: 0,
                    name: "test".to_string(),
                    deleted: false,
                },
            );
        }

        // Test accessor methods
        assert_eq!(accessor.vault_height(1, 1), 42);
        assert_eq!(accessor.vault_height(1, 2), 100);
        assert_eq!(accessor.vault_height(1, 99), 0); // Non-existent returns 0
        assert_eq!(accessor.shard_height(), 99);

        let all_heights = accessor.all_vault_heights();
        assert_eq!(all_heights.len(), 2);
        assert_eq!(all_heights.get(&(1, 1)), Some(&42));

        assert!(accessor.get_namespace(1).is_some());
        assert!(accessor.get_namespace(99).is_none());
    }

    // ========================================================================
    // Snapshot Install Tests
    // ========================================================================
    //
    // These tests verify that snapshot installation correctly restores state,
    // which is critical for follower catch-up and cluster recovery.

    /// Test that snapshot install restores all AppliedState fields.
    ///
    /// This test directly creates a CombinedSnapshot and verifies install_snapshot
    /// correctly restores all state including shard tracking.
    #[tokio::test]
    async fn test_snapshot_install_restores_state() {
        use openraft::{SnapshotMeta, StoredMembership};
        use std::io::Cursor;

        // Build a CombinedSnapshot with realistic data
        let mut applied_state = AppliedState {
            last_applied: Some(make_log_id(1, 100)),
            membership: StoredMembership::default(),
            sequences: SequenceCounters::new(),
            vault_heights: HashMap::new(),
            vault_health: HashMap::new(),
            previous_vault_hashes: HashMap::new(),
            namespaces: HashMap::new(),
            vaults: HashMap::new(),
            shard_height: 55,
            previous_shard_hash: [0xBE; 32],
            client_sequences: HashMap::new(),
        };

        // Add state data
        applied_state.sequences.next_namespace();
        applied_state.sequences.next_namespace();
        applied_state.sequences.next_vault();
        applied_state.vault_heights.insert((1, 1), 42);
        applied_state.vault_heights.insert((1, 2), 100);
        applied_state.namespaces.insert(
            1,
            NamespaceMeta {
                namespace_id: 1,
                shard_id: 0,
                name: "production".to_string(),
                deleted: false,
            },
        );
        applied_state.namespaces.insert(
            2,
            NamespaceMeta {
                namespace_id: 2,
                shard_id: 0,
                name: "staging".to_string(),
                deleted: false,
            },
        );
        applied_state.vaults.insert(
            (1, 1),
            VaultMeta {
                namespace_id: 1,
                vault_id: 1,
                name: Some("main-vault".to_string()),
                deleted: false,
                last_write_timestamp: 1234567890,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );

        let combined = CombinedSnapshot {
            applied_state,
            vault_entities: HashMap::new(),
        };

        let snapshot_data = postcard::to_allocvec(&combined).expect("serialize snapshot");

        // Create target store (simulating a new follower)
        let target_dir = tempdir().expect("create target dir");
        let mut target_store = RaftLogStore::<FileBackend>::open(target_dir.path().join("raft.db"))
            .expect("open target");

        // Verify initial state is empty
        assert_eq!(target_store.current_shard_height(), 0);
        assert!(target_store.applied_state.read().vault_heights.is_empty());

        // Install snapshot on target
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 100)),
            last_membership: StoredMembership::default(),
            snapshot_id: "test-snapshot".to_string(),
        };
        target_store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_data)))
            .await
            .expect("install snapshot");

        // Verify state was restored
        let restored = target_store.applied_state.read();

        // Check sequence counters
        assert_eq!(
            restored.sequences.namespace, 3,
            "namespace counter should be restored"
        );
        assert_eq!(
            restored.sequences.vault, 2,
            "vault counter should be restored"
        );

        // Check vault heights
        assert_eq!(restored.vault_heights.get(&(1, 1)), Some(&42));
        assert_eq!(restored.vault_heights.get(&(1, 2)), Some(&100));

        // Check shard tracking
        assert_eq!(restored.shard_height, 55, "shard_height should be restored");
        assert_eq!(
            restored.previous_shard_hash, [0xBE; 32],
            "previous_shard_hash should be restored"
        );

        // Check namespace registry
        assert_eq!(restored.namespaces.len(), 2);
        let ns1 = restored
            .namespaces
            .get(&1)
            .expect("namespace 1 should exist");
        assert_eq!(ns1.name, "production");

        // Check vault registry
        assert_eq!(restored.vaults.len(), 1);
        let v1 = restored
            .vaults
            .get(&(1, 1))
            .expect("vault (1,1) should exist");
        assert_eq!(v1.name, Some("main-vault".to_string()));

        // Verify the target store's runtime fields are also updated
        drop(restored);
        assert_eq!(target_store.current_shard_height(), 55);
    }

    /// Test that snapshot install on empty store works correctly.
    #[tokio::test]
    async fn test_snapshot_install_on_fresh_node() {
        use openraft::{SnapshotMeta, StoredMembership};
        use std::io::Cursor;

        // Create a minimal CombinedSnapshot
        let combined = CombinedSnapshot {
            applied_state: AppliedState {
                last_applied: Some(make_log_id(2, 50)),
                membership: StoredMembership::default(),
                sequences: SequenceCounters {
                    namespace: 5,
                    vault: 10,
                    user: 3,
                },
                vault_heights: {
                    let mut h = HashMap::new();
                    h.insert((1, 1), 25);
                    h
                },
                vault_health: HashMap::new(),
                previous_vault_hashes: HashMap::new(),
                namespaces: HashMap::new(),
                vaults: HashMap::new(),
                shard_height: 30,
                previous_shard_hash: [0xAA; 32],
                client_sequences: HashMap::new(),
            },
            vault_entities: HashMap::new(),
        };

        let snapshot_data = postcard::to_allocvec(&combined).expect("serialize snapshot");

        // Fresh node
        let dir = tempdir().expect("create dir");
        let mut store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).expect("open");

        // Verify initial state is empty
        assert_eq!(store.current_shard_height(), 0);
        assert!(store.applied_state.read().vault_heights.is_empty());

        // Install snapshot
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(2, 50)),
            last_membership: StoredMembership::default(),
            snapshot_id: "fresh-install".to_string(),
        };
        store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_data)))
            .await
            .expect("install");

        // Verify state
        assert_eq!(store.current_shard_height(), 30);
        assert_eq!(store.applied_state.read().sequences.namespace, 5);
        assert_eq!(store.applied_state.read().sequences.vault, 10);
        assert_eq!(
            store.applied_state.read().vault_heights.get(&(1, 1)),
            Some(&25)
        );
    }

    #[tokio::test]
    async fn test_append_and_read_log_entries() {
        // This test simulates what openraft does during replication
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let mut store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Create 100 log entries (enough to cause multiple leaf nodes in the B-tree)
        let entries: Vec<Entry<LedgerTypeConfig>> = (1..=100u64)
            .map(|i| Entry {
                log_id: make_log_id(1, i),
                payload: EntryPayload::Normal(LedgerRequest::CreateNamespace {
                    name: format!("ns-{}", i),
                    shard_id: None,
                }),
            })
            .collect();

        // Append entries
        store.append_to_log(entries).await.expect("append entries");

        // Get log state
        let log_state = store.get_log_state().await.expect("get log state");
        assert_eq!(log_state.last_log_id.map(|id| id.index), Some(100));

        // Read all entries back (what openraft does during replication)
        let read_entries = store
            .try_get_log_entries(1u64..=100u64)
            .await
            .expect("read entries");

        assert_eq!(
            read_entries.len(),
            100,
            "Expected 100 entries, got {}",
            read_entries.len()
        );

        // Verify each entry exists and has correct index
        for (i, entry) in read_entries.iter().enumerate() {
            let expected_index = (i + 1) as u64;
            assert_eq!(
                entry.log_id.index, expected_index,
                "Entry at position {} has wrong index: expected {}, got {}",
                i, expected_index, entry.log_id.index
            );
        }

        // Test partial range (what openraft does when replicating to a follower)
        let partial = store
            .try_get_log_entries(50u64..=75u64)
            .await
            .expect("read partial");
        assert_eq!(
            partial.len(),
            26,
            "Expected 26 entries, got {}",
            partial.len()
        );
    }
}
