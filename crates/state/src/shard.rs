//! Shard management for multi-vault state coordination.
//!
//! - Multiple vaults (potentially from different organizations) share a single Raft group (shard)
//! - Each vault maintains independent cryptographic chain
//! - State root divergence in one vault doesn't cascade to others

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use inferadb_ledger_store::{Database, StorageBackend};
use inferadb_ledger_types::{
    EMPTY_HASH, Hash, OrganizationId, Region, RegionBlock, UserId, VaultHealth, VaultId, ZERO_HASH,
    compute_chain_commitment, encode,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

use crate::{
    block_archive::BlockArchive,
    bucket::NUM_BUCKETS,
    entity::EntityStore,
    snapshot::{
        Snapshot, SnapshotChainParams, SnapshotManager, SnapshotStateData, VaultSnapshotMeta,
    },
    state::StateLayer,
};

/// Errors returned by [`ShardManager`] operations.
#[derive(Debug, Snafu)]
pub enum ShardError {
    /// State layer operation failed.
    #[snafu(display("State layer error: {source}"))]
    State {
        source: crate::state::StateError,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Block archive operation failed.
    #[snafu(display("Block archive error: {source}"))]
    BlockArchive {
        source: crate::block_archive::BlockArchiveError,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Snapshot operation failed.
    #[snafu(display("Snapshot error: {source}"))]
    Snapshot {
        source: crate::snapshot::SnapshotError,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Store {
        source: inferadb_ledger_store::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Entity store operation failed.
    #[snafu(display("Entity store error: {source}"))]
    Entity {
        source: crate::entity::EntityError,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Requested vault does not exist in this region.
    #[snafu(display("Vault {vault} not found"))]
    VaultNotFound { vault: VaultId },

    /// Computed state root does not match expected value from block.
    #[snafu(display(
        "State root mismatch for vault {vault}: expected {expected:?}, computed {computed:?}"
    ))]
    StateRootMismatch { vault: VaultId, expected: Hash, computed: Hash },
}

/// Result type for shard operations.
pub type Result<T> = std::result::Result<T, ShardError>;

/// Computes the hash of a snapshot header for chain linking.
fn compute_snapshot_header_hash(header: &crate::snapshot::SnapshotHeader) -> Hash {
    let bytes = encode(header).unwrap_or_default();
    inferadb_ledger_types::sha256(&bytes)
}

/// Per-vault metadata tracked by the shard.
#[derive(Debug, Clone)]
pub struct VaultMeta {
    /// Organization owning this vault (`OrganizationId`).
    pub organization: OrganizationId,
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
    /// Region this shard manager belongs to.
    region: Region,
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
    /// Current region height.
    region_height: RwLock<u64>,
    /// Genesis block hash (computed from first block, cached for efficiency).
    genesis_hash: RwLock<Option<Hash>>,
    /// User IDs whose subject keys have been destroyed via crypto-shredding.
    /// Included in every snapshot for erasure guarantee survival.
    erased_users: RwLock<HashSet<UserId>>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> ShardManager<B> {
    /// Creates a new shard manager.
    pub fn new(
        region: Region,
        db: Arc<Database<B>>,
        snapshot_dir: PathBuf,
        max_snapshots: usize,
    ) -> Self {
        Self {
            region,
            db: Arc::clone(&db),
            state: StateLayer::new(Arc::clone(&db)),
            blocks: BlockArchive::new(Arc::clone(&db)),
            snapshots: SnapshotManager::new(snapshot_dir, max_snapshots),
            vault_meta: RwLock::new(HashMap::new()),
            region_height: RwLock::new(0),
            genesis_hash: RwLock::new(None),
            erased_users: RwLock::new(HashSet::new()),
        }
    }

    /// Returns the region this shard manager belongs to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the current region height.
    pub fn region_height(&self) -> u64 {
        *self.region_height.read()
    }

    /// Records a user as erased via crypto-shredding.
    ///
    /// The user ID is added to the erasure tombstone set, which is included in
    /// every subsequent snapshot. This ensures erasure survives snapshot restore
    /// even if the Raft log has been compacted past the erasure's log index.
    pub fn add_erased_user(&self, user_id: UserId) {
        self.erased_users.write().insert(user_id);
    }

    /// Returns a snapshot of the current erasure tombstone set.
    pub fn erased_users(&self) -> HashSet<UserId> {
        self.erased_users.read().clone()
    }

    /// Replaces the in-memory erased users set.
    ///
    /// Call after state restoration to populate the tombstone set from
    /// persisted erasure audit records.
    pub fn set_erased_users(&self, erased: HashSet<UserId>) {
        *self.erased_users.write() = erased;
    }

    /// Returns vault metadata.
    pub fn get_vault_meta(&self, vault: VaultId) -> Option<VaultMeta> {
        self.vault_meta.read().get(&vault).cloned()
    }

    /// Lists all vaults in this region.
    pub fn list_vaults(&self) -> Vec<VaultId> {
        self.vault_meta.read().keys().copied().collect()
    }

    /// Returns vault health status.
    pub fn vault_health(&self, vault: VaultId) -> Option<VaultHealth> {
        self.vault_meta.read().get(&vault).map(|m| m.health.clone())
    }

    /// Returns a reference to the state layer.
    pub fn state(&self) -> &StateLayer<B> {
        &self.state
    }

    /// Returns a reference to the block archive.
    pub fn blocks(&self) -> &BlockArchive<B> {
        &self.blocks
    }

    /// Registers a new vault in this region.
    pub fn register_vault(&self, organization: OrganizationId, vault: VaultId) {
        let mut meta = self.vault_meta.write();
        meta.insert(
            vault,
            VaultMeta {
                organization,
                height: 0,
                state_root: EMPTY_HASH,
                previous_hash: inferadb_ledger_types::ZERO_HASH,
                health: VaultHealth::Healthy,
            },
        );
    }

    /// Applies a region block.
    ///
    /// This:
    /// 1. Applies operations to state
    /// 2. Computes and verifies state roots
    /// 3. Archives the block
    /// 4. Updates vault metadata
    ///
    /// # Errors
    ///
    /// Returns `ShardError::State` if applying operations or computing state root fails.
    /// Returns `ShardError::StateRootMismatch` if the computed state root differs from the
    /// block's expected root.
    /// Returns `ShardError::BlockArchive` if archiving the block fails.
    pub fn apply_block(&self, block: &RegionBlock) -> Result<()> {
        let mut vault_meta = self.vault_meta.write();

        for entry in &block.vault_entries {
            // Apply operations — dirty key tracking and bucket marking are handled
            // internally by apply_operations.
            for tx in &entry.transactions {
                let _statuses = self
                    .state
                    .apply_operations(entry.vault, &tx.operations, entry.vault_height)
                    .context(StateSnafu)?;
            }

            // Compute and verify state root
            let computed_root = self.state.compute_state_root(entry.vault).context(StateSnafu)?;

            if computed_root != entry.state_root {
                // Mark vault as diverged
                let meta = vault_meta.entry(entry.vault).or_insert_with(|| VaultMeta {
                    organization: entry.organization,
                    height: 0,
                    state_root: EMPTY_HASH,
                    previous_hash: inferadb_ledger_types::ZERO_HASH,
                    health: VaultHealth::Healthy,
                });

                meta.health = VaultHealth::Diverged {
                    expected: entry.state_root,
                    computed: computed_root,
                    at_height: entry.vault_height,
                };

                return Err(ShardError::StateRootMismatch {
                    vault: entry.vault,
                    expected: entry.state_root,
                    computed: computed_root,
                });
            }

            // Update vault metadata
            let meta = vault_meta.entry(entry.vault).or_insert_with(|| VaultMeta {
                organization: entry.organization,
                height: 0,
                state_root: EMPTY_HASH,
                previous_hash: inferadb_ledger_types::ZERO_HASH,
                health: VaultHealth::Healthy,
            });

            meta.height = entry.vault_height;
            meta.state_root = entry.state_root;
            meta.previous_hash = entry.previous_vault_hash;
        }

        // Archive the block
        self.blocks.append_block(block).context(BlockArchiveSnafu)?;

        // Update region height
        *self.region_height.write() = block.region_height;

        Ok(())
    }

    /// Creates a snapshot of current state.
    ///
    /// # Errors
    ///
    /// Returns `ShardError::Store` if reading entities from the database fails.
    /// Returns `ShardError::Entity` if entity listing fails.
    /// Returns `ShardError::Snapshot` if snapshot creation or saving fails.
    /// Returns `ShardError::BlockArchive` if computing chain parameters fails.
    pub fn create_snapshot(&self) -> Result<PathBuf> {
        let region_height = self.region_height();
        let vault_meta = self.vault_meta.read();

        // Collect vault states
        let mut vault_states = Vec::new();
        let mut vault_entities = HashMap::new();

        for (&vault_id, meta) in vault_meta.iter() {
            // Get bucket roots
            let bucket_roots =
                self.state.get_bucket_roots(vault_id).unwrap_or([EMPTY_HASH; NUM_BUCKETS]);

            // Collect entities (this is expensive but necessary for snapshot)
            let txn = self.db.read().context(StoreSnafu)?;
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
        let chain_params = self.compute_chain_params(region_height)?;

        let snapshot =
            Snapshot::new(self.region, region_height, vault_states, state_data, chain_params)
                .context(SnapshotSnafu)?;

        self.snapshots.save(&snapshot).context(SnapshotSnafu)
    }

    /// Computes chain parameters for a new snapshot.
    ///
    /// This computes the ChainCommitment covering all blocks since the previous
    /// snapshot, linking the new snapshot into the verification chain.
    fn compute_chain_params(&self, region_height: u64) -> Result<SnapshotChainParams> {
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

        // Load block headers from (from_height..=region_height)
        let blocks =
            self.blocks.read_range(from_height, region_height).context(BlockArchiveSnafu)?;

        let headers: Vec<_> = blocks.iter().map(|b| b.to_region_header()).collect();

        // Compute chain commitment
        let chain_commitment = compute_chain_commitment(&headers, from_height, region_height);

        Ok(SnapshotChainParams {
            genesis_hash,
            previous_snapshot_height,
            previous_snapshot_hash,
            chain_commitment,
            erased_users: self.erased_users.read().clone(),
        })
    }

    /// Returns or computes the genesis hash for this region.
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
                let header = block.to_region_header();
                inferadb_ledger_types::hash::block_hash(&header)
            },
            Err(_) => {
                // No blocks yet - use ZERO_HASH as placeholder
                ZERO_HASH
            },
        };

        // Cache the result
        *self.genesis_hash.write() = Some(genesis);
        Ok(genesis)
    }

    /// Restores from a snapshot.
    ///
    /// # Errors
    ///
    /// Returns `ShardError::Store` if the write transaction or commit fails.
    /// Returns `ShardError::Entity` if restoring any entity fails.
    pub fn restore_from_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let mut vault_meta = self.vault_meta.write();
        vault_meta.clear();

        // Restore vault metadata and commitments
        for vault_state in &snapshot.header.vault_states {
            vault_meta.insert(
                vault_state.vault,
                VaultMeta {
                    organization: OrganizationId::new(0), // Would need to be stored in snapshot
                    height: vault_state.vault_height,
                    state_root: vault_state.state_root,
                    previous_hash: inferadb_ledger_types::ZERO_HASH, // Would need snapshot
                    health: VaultHealth::Healthy,
                },
            );

            // Restore bucket roots for state commitment
            if let Some(bucket_roots) = vault_state.bucket_roots_array() {
                self.state.load_vault_commitment(vault_state.vault, bucket_roots);
            }
        }

        // Restore entities
        let mut txn = self.db.write().context(StoreSnafu)?;

        for (&vault_id, entities) in &snapshot.state.vault_entities {
            for entity in entities {
                EntityStore::set(&mut txn, vault_id, entity).context(EntitySnafu)?;
            }
        }

        txn.commit().context(StoreSnafu)?;

        // Update region height
        *self.region_height.write() = snapshot.header.region_height;

        Ok(())
    }

    /// Recovers from snapshot + block replay.
    ///
    /// # Errors
    ///
    /// Returns `ShardError::Snapshot` if loading the latest snapshot fails.
    /// Returns `ShardError::BlockArchive` if reading blocks for replay fails.
    /// Returns any `ShardError` variant if restoring from the snapshot or replaying
    /// blocks fails.
    pub fn recover(&self) -> Result<()> {
        // Load latest snapshot
        if let Some(snapshot) = self.snapshots.load_latest().context(SnapshotSnafu)? {
            self.restore_from_snapshot(&snapshot)?;

            // Replay blocks after snapshot
            let start_height = snapshot.region_height() + 1;
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
    use chrono::Utc;
    use inferadb_ledger_test_utils::TestDir;
    use inferadb_ledger_types::{ClientId, NodeId, Operation, Transaction, VaultEntry};

    use super::*;
    use crate::engine::InMemoryStorageEngine;

    fn create_test_manager() -> (ShardManager<inferadb_ledger_store::InMemoryBackend>, TestDir) {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TestDir::new();

        let manager = ShardManager::new(Region::GLOBAL, engine.db(), temp.join("snapshots"), 3);

        (manager, temp)
    }

    #[test]
    fn test_register_vault() {
        let (manager, _temp) = create_test_manager();

        manager.register_vault(OrganizationId::new(1), VaultId::new(1));
        manager.register_vault(OrganizationId::new(1), VaultId::new(2));

        let vaults = manager.list_vaults();
        assert_eq!(vaults.len(), 2);
        assert!(vaults.contains(&VaultId::new(1)));
        assert!(vaults.contains(&VaultId::new(2)));
    }

    #[test]
    fn test_apply_block() {
        let (manager, _temp) = create_test_manager();
        manager.register_vault(OrganizationId::new(1), VaultId::new(1));

        // Create a block with a SetEntity operation
        let tx = Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("client-1"),
            sequence: 1,
            operations: vec![Operation::SetEntity {
                key: "test_key".to_string(),
                value: b"test_value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: Utc::now(),
        };

        // Compute expected state root
        let _ = manager.state.apply_operations(VaultId::new(1), &tx.operations, 1).expect("apply");
        let expected_root =
            manager.state.compute_state_root(VaultId::new(1)).expect("compute root");

        // Reset state and apply via block
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TestDir::new();
        let manager = ShardManager::new(Region::GLOBAL, engine.db(), temp.join("snapshots"), 3);
        manager.register_vault(OrganizationId::new(1), VaultId::new(1));

        let block = RegionBlock {
            region: Region::GLOBAL,
            region_height: 1,
            previous_region_hash: inferadb_ledger_types::ZERO_HASH,
            vault_entries: vec![VaultEntry {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 1,
                previous_vault_hash: inferadb_ledger_types::ZERO_HASH,
                transactions: vec![tx],
                tx_merkle_root: [0u8; 32],
                state_root: expected_root,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            }],
            timestamp: Utc::now(),
            leader_id: NodeId::new("node-1"),
            term: 1,
            committed_index: 1,
        };

        manager.apply_block(&block).expect("apply block");

        // Verify vault metadata updated
        let meta = manager.get_vault_meta(VaultId::new(1)).expect("vault meta");
        assert_eq!(meta.height, 1);
        assert_eq!(meta.state_root, expected_root);
    }

    #[test]
    fn test_state_root_mismatch() {
        let (manager, _temp) = create_test_manager();
        manager.register_vault(OrganizationId::new(1), VaultId::new(1));

        let tx = Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("client-1"),
            sequence: 1,
            operations: vec![Operation::SetEntity {
                key: "key".to_string(),
                value: b"value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: Utc::now(),
        };

        // Block with wrong state root
        let block = RegionBlock {
            region: Region::GLOBAL,
            region_height: 1,
            previous_region_hash: inferadb_ledger_types::ZERO_HASH,
            vault_entries: vec![VaultEntry {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 1,
                previous_vault_hash: inferadb_ledger_types::ZERO_HASH,
                transactions: vec![tx],
                tx_merkle_root: [0u8; 32],
                state_root: [42u8; 32], // Wrong!,

                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            }],
            timestamp: Utc::now(),
            leader_id: NodeId::new("node-1"),
            term: 1,
            committed_index: 1,
        };

        let result = manager.apply_block(&block);
        assert!(matches!(result, Err(ShardError::StateRootMismatch { .. })));

        // Vault should be marked diverged
        let health = manager.vault_health(VaultId::new(1)).expect("health");
        assert!(matches!(health, VaultHealth::Diverged { .. }));
    }

    #[test]
    fn test_snapshot_and_restore() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let temp = TestDir::new();
        let manager = ShardManager::new(Region::GLOBAL, engine.db(), temp.join("snapshots"), 3);

        manager.register_vault(OrganizationId::new(1), VaultId::new(1));

        // Add some data
        let ops = vec![Operation::SetEntity {
            key: "key1".to_string(),
            value: b"value1".to_vec(),
            condition: None,
            expires_at: None,
        }];
        manager.state.apply_operations(VaultId::new(1), &ops, 1).expect("apply");
        manager.state.compute_state_root(VaultId::new(1)).expect("compute root");

        // Update vault meta
        {
            let mut meta = manager.vault_meta.write();
            if let Some(m) = meta.get_mut(&VaultId::new(1)) {
                m.height = 1;
            }
        }

        // Create snapshot
        let snapshot_path = manager.create_snapshot().expect("create snapshot");
        assert!(snapshot_path.exists());

        // Create new manager and restore
        let engine2 = InMemoryStorageEngine::open().expect("open engine");
        let manager2 =
            ShardManager::new(Region::GLOBAL, engine2.db(), temp.path().join("snapshots"), 3);

        let snapshot = Snapshot::read_from_file(&snapshot_path).expect("read snapshot");
        manager2.restore_from_snapshot(&snapshot).expect("restore");

        // Verify data restored
        let entity = manager2.state.get_entity(VaultId::new(1), b"key1").expect("get entity");
        assert!(entity.is_some());
        assert_eq!(entity.unwrap().value, b"value1");
    }
}
