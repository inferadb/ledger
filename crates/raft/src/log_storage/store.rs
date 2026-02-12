use std::{path::Path, sync::Arc};

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::{Database, DatabaseConfig, FileBackend, StorageBackend, tables};
use inferadb_ledger_types::{ShardId, decode, encode};
use openraft::{Entry, LogId, StorageError, Vote};
use parking_lot::RwLock;
use tokio::sync::broadcast;

use super::{
    KEY_APPLIED_STATE, KEY_LAST_PURGED, KEY_VOTE, ShardChainState,
    accessor::AppliedStateAccessor,
    raft_impl::{to_serde_error, to_storage_error},
    types::{AppliedState, SequenceCounters},
};
use crate::types::{LedgerNodeId, LedgerTypeConfig};

/// Combined Raft storage.
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
    /// Database handle for raft log.
    pub(super) db: Arc<Database<FileBackend>>,
    /// Cached vote state.
    pub(super) vote_cache: RwLock<Option<Vote<LedgerNodeId>>>,
    /// Cached last purged log ID.
    pub(super) last_purged_cache: RwLock<Option<LogId<LedgerNodeId>>>,
    /// Applied state (state machine) - shared with accessor.
    pub(super) applied_state: Arc<RwLock<AppliedState>>,
    /// State layer for entity/relationship storage (shared with read service).
    pub(super) state_layer: Option<Arc<StateLayer<B>>>,
    /// Block archive for permanent block storage.
    pub(super) block_archive: Option<Arc<BlockArchive<B>>>,
    /// Shard ID for this Raft group.
    pub(super) shard_id: ShardId,
    /// Node ID for block metadata.
    pub(super) node_id: String,
    /// Shard chain state (height and previous hash).
    ///
    /// Consolidated into single lock to avoid lock ordering issues.
    /// See: apply_to_state_machine, restore_from_db
    pub(super) shard_chain: RwLock<ShardChainState>,
    /// Block announcement broadcast channel for real-time block notifications.
    ///
    /// When set, announcements are broadcast after each successful block commit.
    /// Receivers subscribe via `WatchBlocks` gRPC streaming endpoint.
    pub(super) block_announcements: Option<broadcast::Sender<BlockAnnouncement>>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Opens or creates a new log store at the given path.
    ///
    /// This creates a basic log store without StateLayer or BlockArchive integration.
    /// Use `with_state_layer` and `with_block_archive` to add those capabilities.
    /// Page size for Raft log storage.
    ///
    /// Using 16KB pages (vs default 4KB) to allow larger batch sizes.
    /// A batch of 100 operations typically serializes to ~8-12KB with postcard.
    /// Max supported: 64KB. Minimum: 512 bytes (must be power of 2).
    pub const RAFT_PAGE_SIZE: usize = 16 * 1024; // 16KB

    /// Opens or creates a Raft log storage database.
    ///
    /// New databases are created with 16KB pages to support larger batch sizes.
    /// Existing databases retain their original page size for backwards compatibility.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the database file cannot be opened or created,
    /// or if the cached vote/purge metadata cannot be loaded.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<LedgerNodeId>> {
        // Try to open existing database, otherwise create new one with larger pages
        let db = if path.as_ref().exists() {
            // Existing database - use whatever page size it was created with
            Database::open(path.as_ref()).map_err(|e| to_storage_error(&e))?
        } else {
            // New database - use larger pages for bigger batch sizes
            let config = DatabaseConfig { page_size: Self::RAFT_PAGE_SIZE, ..Default::default() };
            Database::create_with_config(path.as_ref(), config).map_err(|e| to_storage_error(&e))?
        };

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
            shard_id: ShardId::new(0),
            node_id: String::new(),
            shard_chain: RwLock::new(ShardChainState {
                height: 0,
                previous_hash: inferadb_ledger_types::ZERO_HASH,
            }),
            block_announcements: None,
        };

        // Load cached values
        store.load_caches()?;

        Ok(store)
    }

    /// Configures the state layer for transaction application.
    pub fn with_state_layer(mut self, state_layer: Arc<StateLayer<B>>) -> Self {
        self.state_layer = Some(state_layer);
        self
    }

    /// Configures the block archive for permanent block storage.
    pub fn with_block_archive(mut self, block_archive: Arc<BlockArchive<B>>) -> Self {
        self.block_archive = Some(block_archive);
        self
    }

    /// Configures shard metadata.
    pub fn with_shard_config(mut self, shard_id: ShardId, node_id: String) -> Self {
        self.shard_id = shard_id;
        self.node_id = node_id;
        self
    }

    /// Configures the block announcements broadcast channel.
    ///
    /// When set, the log store will broadcast `BlockAnnouncement` messages
    /// after each successful block commit in `apply_to_state_machine`.
    pub fn with_block_announcements(
        mut self,
        sender: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        self.block_announcements = Some(sender);
        self
    }

    /// Returns a reference to the block announcements sender (if configured).
    pub fn block_announcements(&self) -> Option<&broadcast::Sender<BlockAnnouncement>> {
        self.block_announcements.as_ref()
    }

    /// Returns the current shard height.
    pub fn current_shard_height(&self) -> u64 {
        self.shard_chain.read().height
    }

    /// Returns a reference to the state layer (if configured).
    pub fn state_layer(&self) -> Option<&Arc<StateLayer<B>>> {
        self.state_layer.as_ref()
    }

    /// Returns a reference to the block archive (if configured).
    pub fn block_archive(&self) -> Option<&Arc<BlockArchive<B>>> {
        self.block_archive.as_ref()
    }

    /// Returns an accessor for reading applied state.
    ///
    /// This accessor can be cloned and passed to services that need to read
    /// vault heights and health status.
    pub fn accessor(&self) -> AppliedStateAccessor {
        AppliedStateAccessor { state: self.applied_state.clone() }
    }

    /// Checks if this log store has been previously initialized.
    ///
    /// Returns `true` if a vote has been saved, indicating that Raft consensus
    /// has been started at some point. Used for auto-detection of whether to
    /// bootstrap a new cluster or resume an existing one.
    pub fn is_initialized(&self) -> bool {
        self.vote_cache.read().is_some()
    }

    /// Loads metadata values into caches.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the underlying database read or
    /// deserialization of cached vote/purge metadata fails.
    pub(super) fn load_caches(&self) -> Result<(), StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        if let Some(vote_data) = read_txn
            .get::<tables::RaftState>(&KEY_VOTE.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let vote: Vote<LedgerNodeId> = decode(&vote_data).map_err(|e| to_serde_error(&e))?;
            *self.vote_cache.write() = Some(vote);
        }

        if let Some(purged_data) = read_txn
            .get::<tables::RaftState>(&KEY_LAST_PURGED.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let purged: LogId<LedgerNodeId> =
                decode(&purged_data).map_err(|e| to_serde_error(&e))?;
            *self.last_purged_cache.write() = Some(purged);
        }

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

    /// Returns the last log entry.
    pub(super) fn get_last_entry(
        &self,
    ) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        if let Some((_, entry_data)) =
            read_txn.last::<tables::RaftLog>().map_err(|e| to_storage_error(&e))?
        {
            let entry: Entry<LedgerTypeConfig> =
                decode(&entry_data).map_err(|e| to_serde_error(&e))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Persist the applied state.
    pub(super) fn save_applied_state(
        &self,
        state: &AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let state_data = encode(state).map_err(|e| to_serde_error(&e))?;
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&KEY_APPLIED_STATE.to_string(), &state_data)
            .map_err(|e| to_storage_error(&e))?;
        write_txn.commit().map_err(|e| to_storage_error(&e))?;
        Ok(())
    }
}
