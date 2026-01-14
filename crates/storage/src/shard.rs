//! Shard management for multi-vault state coordination.
//!
//! Per DESIGN.md:
//! - Multiple namespaces share a single Raft group (shard)
//! - Each vault maintains independent cryptographic chain
//! - State root divergence in one vault doesn't cascade to others

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use inkwell::{Database, StorageBackend};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

use ledger_types::{
    EMPTY_HASH, Hash, NamespaceId, ShardBlock, ShardId, VaultHealth, VaultId, ZERO_HASH,
    compute_chain_commitment,
};

use crate::block_archive::BlockArchive;
use crate::bucket::NUM_BUCKETS;
use crate::entity::EntityStore;
use crate::snapshot::{
    Snapshot, SnapshotChainParams, SnapshotManager, SnapshotStateData, VaultSnapshotMeta,
};
use crate::state::StateLayer;

/// Shard manager error types.
#[derive(Debug, Snafu)]
pub enum ShardError {
    #[snafu(display("State layer error: {source}"))]
    State { source: crate::state::StateError },

    #[snafu(display("Block archive error: {source}"))]
    BlockArchive {
        source: crate::block_archive::BlockArchiveError,
    },

    #[snafu(display("Snapshot error: {source}"))]
    Snapshot {
        source: crate::snapshot::SnapshotError,
    },

    #[snafu(display("Storage error: {source}"))]
    Inkwell { source: inkwell::Error },

    #[snafu(display("Entity store error: {source}"))]
    Entity { source: crate::entity::EntityError },

    #[snafu(display("Vault {vault_id} not found"))]
    VaultNotFound { vault_id: VaultId },

    #[snafu(display(
        "State root mismatch for vault {vault_id}: expected {expected:?}, computed {computed:?}"
    ))]
    StateRootMismatch {
        vault_id: VaultId,
        expected: Hash,
        computed: Hash,
    },
}

/// Result type for shard operations.
pub type Result<T> = std::result::Result<T, ShardError>;

/// Compute the hash of a snapshot header for chain linking.
///
/// Used to link snapshots together in a verifiable chain.
fn compute_snapshot_header_hash(header: &crate::snapshot::SnapshotHeader) -> Hash {
    let bytes = postcard::to_allocvec(header).unwrap_or_default();
    ledger_types::sha256(&bytes)
}

/// Per-vault metadata tracked by the shard.
#[derive(Debug, Clone)]
pub struct VaultMeta {
    /// Namespace owning this vault.
    pub namespace_id: NamespaceId,
    /// Current vault height.
    pub height: u64,
    /// Latest state root.
    pub state_root: Hash,
    /// Hash of previous block.
    pub previous_hash: Hash,
    /// Health status.
    pub health: VaultHealth,
}

/// Shard manager coordinating multiple vaults.
///
/// Handles:
/// - State layer management
/// - Block processing and verification
/// - Snapshot creation and restoration
/// - Vault health tracking
pub struct ShardManager<B: StorageBackend> {
    /// Shard identifier.
    shard_id: ShardId,
    /// Shared database.
    db: Arc<Database<B>>,
    /// State layer for K/V operations.
    state: StateLayer<B>,
    /// Block archive.
    blocks: BlockArchive<B>,
    /// Snapshot manager.
    snapshots: SnapshotManager,
    /// Per-vault metadata.
    vault_meta: RwLock<HashMap<VaultId, VaultMeta>>,
    /// Current shard height.
    shard_height: RwLock<u64>,
    /// Genesis block hash (computed from first block, cached for efficiency).
    genesis_hash: RwLock<Option<Hash>>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> ShardManager<B> {
    /// Create a new shard manager.
    pub fn new(
        shard_id: ShardId,
        db: Arc<Database<B>>,
        snapshot_dir: PathBuf,
        max_snapshots: usize,
    ) -> Self {
        Self {
            shard_id,
            db: Arc::clone(&db),
            state: StateLayer::new(Arc::clone(&db)),
            blocks: BlockArchive::new(Arc::clone(&db)),
            snapshots: SnapshotManager::new(snapshot_dir, max_snapshots),
            vault_meta: RwLock::new(HashMap::new()),
            shard_height: RwLock::new(0),
            genesis_hash: RwLock::new(None),
        }
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get the current shard height.
    pub fn shard_height(&self) -> u64 {
        *self.shard_height.read()
    }

    /// Get vault metadata.
    pub fn get_vault_meta(&self, vault_id: VaultId) -> Option<VaultMeta> {
        self.vault_meta.read().get(&vault_id).cloned()
    }

    /// List all vaults in this shard.
    pub fn list_vaults(&self) -> Vec<VaultId> {
        self.vault_meta.read().keys().copied().collect()
    }

    /// Get vault health status.
    pub fn vault_health(&self, vault_id: VaultId) -> Option<VaultHealth> {
        self.vault_meta
            .read()
            .get(&vault_id)
            .map(|m| m.health.clone())
    }

    /// Access the state layer.
    pub fn state(&self) -> &StateLayer<B> {
        &self.state
    }

    /// Access the block archive.
    pub fn blocks(&self) -> &BlockArchive<B> {
        &self.blocks
    }

    /// Register a new vault in this shard.
    pub fn register_vault(&self, namespace_id: NamespaceId, vault_id: VaultId) {
        let mut meta = self.vault_meta.write();
        meta.insert(
            vault_id,
            VaultMeta {
                namespace_id,
                height: 0,
                state_root: EMPTY_HASH,
                previous_hash: ledger_types::ZERO_HASH,
                health: VaultHealth::Healthy,
            },
        );
    }

    /// Apply a shard block.
    ///
    /// This:
    /// 1. Applies operations to state
    /// 2. Computes and verifies state roots
    /// 3. Archives the block
    /// 4. Updates vault metadata
    pub fn apply_block(&self, block: &ShardBlock) -> Result<()> {
        let mut vault_meta = self.vault_meta.write();

        for entry in &block.vault_entries {
            // Apply operations
            let mut dirty_keys = Vec::new();
            for tx in &entry.transactions {
                let _statuses = self
                    .state
                    .apply_operations(entry.vault_id, &tx.operations, entry.vault_height)
                    .context(StateSnafu)?;

                // Track that we have dirty keys
                for op in &tx.operations {
                    match op {
                        ledger_types::Operation::SetEntity { key, .. }
                        | ledger_types::Operation::DeleteEntity { key }
                        | ledger_types::Operation::ExpireEntity { key, .. } => {
                            dirty_keys.push(key.as_bytes().to_vec());
                        }
                        ledger_types::Operation::CreateRelationship {
                            resource,
                            relation,
                            subject,
                        }
                        | ledger_types::Operation::DeleteRelationship {
                            resource,
                            relation,
                            subject,
                        } => {
                            let rel = ledger_types::Relationship::new(resource, relation, subject);
                            dirty_keys.push(rel.to_key().into_bytes());
                        }
                    }
                }
            }

            // Compute and verify state root
            let computed_root = self
                .state
                .compute_state_root(entry.vault_id)
                .context(StateSnafu)?;

            if computed_root != entry.state_root {
                // Mark vault as diverged
                let meta = vault_meta
                    .entry(entry.vault_id)
                    .or_insert_with(|| VaultMeta {
                        namespace_id: entry.namespace_id,
                        height: 0,
                        state_root: EMPTY_HASH,
                        previous_hash: ledger_types::ZERO_HASH,
                        health: VaultHealth::Healthy,
                    });

                meta.health = VaultHealth::Diverged {
                    expected: entry.state_root,
                    computed: computed_root,
                    at_height: entry.vault_height,
                };

                return Err(ShardError::StateRootMismatch {
                    vault_id: entry.vault_id,
                    expected: entry.state_root,
                    computed: computed_root,
                });
            }

            // Update vault metadata
            let meta = vault_meta
                .entry(entry.vault_id)
                .or_insert_with(|| VaultMeta {
                    namespace_id: entry.namespace_id,
                    height: 0,
                    state_root: EMPTY_HASH,
                    previous_hash: ledger_types::ZERO_HASH,
                    health: VaultHealth::Healthy,
                });

            meta.height = entry.vault_height;
            meta.state_root = entry.state_root;
            meta.previous_hash = entry.previous_vault_hash;
        }

        // Archive the block
        self.blocks.append_block(block).context(BlockArchiveSnafu)?;

        // Update shard height
        *self.shard_height.write() = block.shard_height;

        Ok(())
    }

    /// Create a snapshot of current state.
    pub fn create_snapshot(&self) -> Result<PathBuf> {
        let shard_height = self.shard_height();
        let vault_meta = self.vault_meta.read();

        // Collect vault states
        let mut vault_states = Vec::new();
        let mut vault_entities = HashMap::new();

        for (&vault_id, meta) in vault_meta.iter() {
            // Get bucket roots
            let bucket_roots = self
                .state
                .get_bucket_roots(vault_id)
                .unwrap_or([EMPTY_HASH; NUM_BUCKETS]);

            // Collect entities (this is expensive but necessary for snapshot)
            let txn = self.db.read().context(InkwellSnafu)?;
            let entities =
                EntityStore::list_in_vault(&txn, vault_id, usize::MAX, 0).context(EntitySnafu)?;

            vault_states.push(VaultSnapshotMeta::new(
                vault_id,
                meta.height,
                meta.state_root,
                bucket_roots,
                entities.len() as u64,
            ));

            vault_entities.insert(vault_id, entities);
        }

        let state_data = SnapshotStateData { vault_entities };

        // Compute chain commitment for snapshot verification
        let chain_params = self.compute_chain_params(shard_height)?;

        let snapshot = Snapshot::new(
            self.shard_id,
            shard_height,
            vault_states,
            state_data,
            chain_params,
        )
        .context(SnapshotSnafu)?;

        self.snapshots.save(&snapshot).context(SnapshotSnafu)
    }

    /// Compute chain parameters for a new snapshot.
    ///
    /// This computes the ChainCommitment covering all blocks since the previous
    /// snapshot, linking the new snapshot into the verification chain.
    fn compute_chain_params(&self, shard_height: u64) -> Result<SnapshotChainParams> {
        // Get genesis hash (cached for efficiency)
        let genesis_hash = self.get_or_compute_genesis_hash()?;

        // Find previous snapshot
        let existing_snapshots = self.snapshots.list_snapshots().context(SnapshotSnafu)?;
        let (previous_snapshot_height, previous_snapshot_hash, from_height) =
            if let Some(&prev_height) = existing_snapshots.last() {
                // Load previous snapshot to get its header hash
                let prev_snapshot = self.snapshots.load(prev_height).context(SnapshotSnafu)?;
                let prev_hash = compute_snapshot_header_hash(&prev_snapshot.header);
                (Some(prev_height), Some(prev_hash), prev_height + 1)
            } else {
                // First snapshot - start from block 1
                (None, None, 1)
            };

        // Load block headers from (from_height..=shard_height)
        let blocks = self
            .blocks
            .read_range(from_height, shard_height)
            .context(BlockArchiveSnafu)?;

        let headers: Vec<_> = blocks.iter().map(|b| b.to_shard_header()).collect();

        // Compute chain commitment
        let chain_commitment = compute_chain_commitment(&headers, from_height, shard_height);

        Ok(SnapshotChainParams {
            genesis_hash,
            previous_snapshot_height,
            previous_snapshot_hash,
            chain_commitment,
        })
    }

    /// Get or compute the genesis hash for this shard.
    ///
    /// The genesis hash is the block hash of the first block (height 1).
    /// It's cached after first computation for efficiency.
    fn get_or_compute_genesis_hash(&self) -> Result<Hash> {
        // Check cache first
        if let Some(hash) = *self.genesis_hash.read() {
            return Ok(hash);
        }

        // Compute from first block
        let genesis = match self.blocks.read_block(1) {
            Ok(block) => {
                let header = block.to_shard_header();
                ledger_types::hash::block_hash(&header)
            }
            Err(_) => {
                // No blocks yet - use ZERO_HASH as placeholder
                ZERO_HASH
            }
        };

        // Cache the result
        *self.genesis_hash.write() = Some(genesis);
        Ok(genesis)
    }

    /// Restore from a snapshot.
    pub fn restore_from_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let mut vault_meta = self.vault_meta.write();
        vault_meta.clear();

        // Restore vault metadata and commitments
        for vault_state in &snapshot.header.vault_states {
            vault_meta.insert(
                vault_state.vault_id,
                VaultMeta {
                    namespace_id: 0, // Would need to be stored in snapshot
                    height: vault_state.vault_height,
                    state_root: vault_state.state_root,
                    previous_hash: ledger_types::ZERO_HASH, // Would need snapshot
                    health: VaultHealth::Healthy,
                },
            );

            // Restore bucket roots for state commitment
            if let Some(bucket_roots) = vault_state.bucket_roots_array() {
                self.state
                    .load_vault_commitment(vault_state.vault_id, bucket_roots);
            }
        }

        // Restore entities
        let mut txn = self.db.write().context(InkwellSnafu)?;

        for (&vault_id, entities) in &snapshot.state.vault_entities {
            for entity in entities {
                EntityStore::set(&mut txn, vault_id, entity).context(EntitySnafu)?;
            }
        }

        txn.commit().context(InkwellSnafu)?;

        // Update shard height
        *self.shard_height.write() = snapshot.header.shard_height;

        Ok(())
    }

    /// Recover from snapshot + block replay.
    pub fn recover(&self) -> Result<()> {
        // Load latest snapshot
        if let Some(snapshot) = self.snapshots.load_latest().context(SnapshotSnafu)? {
            self.restore_from_snapshot(&snapshot)?;

            // Replay blocks after snapshot
            let start_height = snapshot.shard_height() + 1;
            if let Some((_, latest)) = self.blocks.height_range().context(BlockArchiveSnafu)? {
                for height in start_height..=latest {
                    let block = self.blocks.read_block(height).context(BlockArchiveSnafu)?;
                    self.apply_block(&block)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::engine::InMemoryStorageEngine;
    use chrono::Utc;
    use ledger_types::{Operation, Transaction, VaultEntry};
    use tempfile::TempDir;

    fn create_test_manager() -> (ShardManager<inkwell::InMemoryBackend>, TempDir) {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TempDir::new().expect("create temp dir");

        let manager = ShardManager::new(1, engine.db(), temp.path().join("snapshots"), 3);

        (manager, temp)
    }

    #[test]
    fn test_register_vault() {
        let (manager, _temp) = create_test_manager();

        manager.register_vault(1, 1);
        manager.register_vault(1, 2);

        let vaults = manager.list_vaults();
        assert_eq!(vaults.len(), 2);
        assert!(vaults.contains(&1));
        assert!(vaults.contains(&2));
    }

    #[test]
    fn test_apply_block() {
        let (manager, _temp) = create_test_manager();
        manager.register_vault(1, 1);

        // Create a block with a SetEntity operation
        let tx = Transaction {
            id: [0u8; 16],
            client_id: "client-1".to_string(),
            sequence: 1,
            actor: "user:admin".to_string(),
            operations: vec![Operation::SetEntity {
                key: "test_key".to_string(),
                value: b"test_value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: Utc::now(),
        };

        // Compute expected state root
        let _ = manager
            .state
            .apply_operations(1, &tx.operations, 1)
            .expect("apply");
        let expected_root = manager.state.compute_state_root(1).expect("compute root");

        // Reset state and apply via block
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TempDir::new().expect("create temp dir");
        let manager = ShardManager::new(1, engine.db(), temp.path().join("snapshots"), 3);
        manager.register_vault(1, 1);

        let block = ShardBlock {
            shard_id: 1,
            shard_height: 1,
            previous_shard_hash: ledger_types::ZERO_HASH,
            vault_entries: vec![VaultEntry {
                namespace_id: 1,
                vault_id: 1,
                vault_height: 1,
                previous_vault_hash: ledger_types::ZERO_HASH,
                transactions: vec![tx],
                tx_merkle_root: [0u8; 32],
                state_root: expected_root,
            }],
            timestamp: Utc::now(),
            leader_id: "node-1".to_string(),
            term: 1,
            committed_index: 1,
        };

        manager.apply_block(&block).expect("apply block");

        // Verify vault metadata updated
        let meta = manager.get_vault_meta(1).expect("vault meta");
        assert_eq!(meta.height, 1);
        assert_eq!(meta.state_root, expected_root);
    }

    #[test]
    fn test_state_root_mismatch() {
        let (manager, _temp) = create_test_manager();
        manager.register_vault(1, 1);

        let tx = Transaction {
            id: [0u8; 16],
            client_id: "client-1".to_string(),
            sequence: 1,
            actor: "user:admin".to_string(),
            operations: vec![Operation::SetEntity {
                key: "key".to_string(),
                value: b"value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: Utc::now(),
        };

        // Block with wrong state root
        let block = ShardBlock {
            shard_id: 1,
            shard_height: 1,
            previous_shard_hash: ledger_types::ZERO_HASH,
            vault_entries: vec![VaultEntry {
                namespace_id: 1,
                vault_id: 1,
                vault_height: 1,
                previous_vault_hash: ledger_types::ZERO_HASH,
                transactions: vec![tx],
                tx_merkle_root: [0u8; 32],
                state_root: [42u8; 32], // Wrong!
            }],
            timestamp: Utc::now(),
            leader_id: "node-1".to_string(),
            term: 1,
            committed_index: 1,
        };

        let result = manager.apply_block(&block);
        assert!(matches!(result, Err(ShardError::StateRootMismatch { .. })));

        // Vault should be marked diverged
        let health = manager.vault_health(1).expect("health");
        assert!(matches!(health, VaultHealth::Diverged { .. }));
    }

    #[test]
    fn test_snapshot_and_restore() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TempDir::new().expect("create temp dir");
        let manager = ShardManager::new(1, engine.db(), temp.path().join("snapshots"), 3);

        manager.register_vault(1, 1);

        // Add some data
        let ops = vec![Operation::SetEntity {
            key: "key1".to_string(),
            value: b"value1".to_vec(),
            condition: None,
            expires_at: None,
        }];
        manager.state.apply_operations(1, &ops, 1).expect("apply");
        manager.state.compute_state_root(1).expect("compute root");

        // Update vault meta
        {
            let mut meta = manager.vault_meta.write();
            if let Some(m) = meta.get_mut(&1) {
                m.height = 1;
            }
        }

        // Create snapshot
        let snapshot_path = manager.create_snapshot().expect("create snapshot");
        assert!(snapshot_path.exists());

        // Create new manager and restore
        let engine2 = InMemoryStorageEngine::open().expect("open engine");
        let manager2 = ShardManager::new(1, engine2.db(), temp.path().join("snapshots"), 3);

        let snapshot = Snapshot::read_from_file(&snapshot_path).expect("read snapshot");
        manager2.restore_from_snapshot(&snapshot).expect("restore");

        // Verify data restored
        let entity = manager2.state.get_entity(1, b"key1").expect("get entity");
        assert!(entity.is_some());
        assert_eq!(entity.unwrap().value, b"value1");
    }
}
