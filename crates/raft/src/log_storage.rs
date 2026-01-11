//! Raft log storage implementation using redb.
//!
//! This module provides the persistent storage for Raft log entries,
//! vote state, and committed log tracking.
//!
//! Per DESIGN.md, each shard group has its own log storage located at:
//! `shards/{shard_id}/raft/log.redb`

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftTypeConfig, StorageError, Vote};
use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::types::{LedgerNodeId, LedgerTypeConfig};

// ============================================================================
// Table Definitions
// ============================================================================

/// Table storing Raft log entries.
/// Key: log index (u64)
/// Value: serialized Entry<LedgerTypeConfig>
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// Table storing metadata.
/// Key: metadata key (str)
/// Value: serialized metadata value
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

// Metadata keys
const KEY_VOTE: &str = "vote";
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";

// ============================================================================
// Log Storage
// ============================================================================

/// Raft log storage backed by redb.
///
/// This implementation stores:
/// - Log entries in a redb table indexed by log index
/// - Vote state (term + voted_for) in metadata
/// - Committed log ID for recovery
/// - Last purged log ID for compaction tracking
pub struct RaftLogStore {
    /// redb database handle.
    db: Arc<Database>,
    /// Cached vote state (for fast reads).
    vote_cache: RwLock<Option<Vote<LedgerNodeId>>>,
    /// Cached committed log ID.
    committed_cache: RwLock<Option<LogId<LedgerNodeId>>>,
    /// Cached last purged log ID.
    last_purged_cache: RwLock<Option<LogId<LedgerNodeId>>>,
}

impl RaftLogStore {
    /// Open or create a new log store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<LedgerNodeId>> {
        let db = Database::create(path.as_ref()).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            )
        })?;

        // Ensure tables exist
        let write_txn = db.begin_write().map_err(to_storage_error)?;
        {
            let _log_table = write_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;
            let _meta_table = write_txn.open_table(META_TABLE).map_err(to_storage_error)?;
        }
        write_txn.commit().map_err(to_storage_error)?;

        let store = Self {
            db: Arc::new(db),
            vote_cache: RwLock::new(None),
            committed_cache: RwLock::new(None),
            last_purged_cache: RwLock::new(None),
        };

        // Load cached values
        store.load_caches()?;

        Ok(store)
    }

    /// Load metadata values into caches.
    fn load_caches(&self) -> Result<(), StorageError<LedgerNodeId>> {
        let read_txn = self.db.begin_read().map_err(to_storage_error)?;
        let meta_table = read_txn.open_table(META_TABLE).map_err(to_storage_error)?;

        // Load vote
        if let Some(vote_data) = meta_table.get(KEY_VOTE).map_err(to_storage_error)? {
            let vote: Vote<LedgerNodeId> =
                bincode::deserialize(vote_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            *self.vote_cache.write() = Some(vote);
        }

        // Load committed
        if let Some(committed_data) = meta_table.get(KEY_COMMITTED).map_err(to_storage_error)? {
            let committed: LogId<LedgerNodeId> =
                bincode::deserialize(committed_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            *self.committed_cache.write() = Some(committed);
        }

        // Load last purged
        if let Some(purged_data) = meta_table.get(KEY_LAST_PURGED).map_err(to_storage_error)? {
            let purged: LogId<LedgerNodeId> =
                bincode::deserialize(purged_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            *self.last_purged_cache.write() = Some(purged);
        }

        Ok(())
    }

    /// Get an entry by log index.
    fn get_entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.begin_read().map_err(to_storage_error)?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;

        if let Some(entry_data) = log_table.get(index).map_err(to_storage_error)? {
            let entry: Entry<LedgerTypeConfig> =
                bincode::deserialize(entry_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Get the last log entry.
    fn get_last_entry(&self) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.begin_read().map_err(to_storage_error)?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;

        if let Some((_, entry_data)) = log_table.last().map_err(to_storage_error)? {
            let entry: Entry<LedgerTypeConfig> =
                bincode::deserialize(entry_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
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
        let read_txn = self.db.begin_read().map_err(to_storage_error)?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;

        let mut entries = Vec::new();

        for result in log_table.range(range).map_err(to_storage_error)? {
            let (_, entry_data) = result.map_err(to_storage_error)?;
            let entry: Entry<LedgerTypeConfig> =
                bincode::deserialize(entry_data.value()).map_err(|e| to_serde_error(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<LedgerNodeId>>, StorageError<LedgerNodeId>> {
        Ok(*self.vote_cache.read())
    }
}

// ============================================================================
// RaftLogStorage Implementation
// ============================================================================

impl RaftLogStorage<LedgerTypeConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Clone the Arc for the reader
        Self {
            db: Arc::clone(&self.db),
            vote_cache: RwLock::new(*self.vote_cache.read()),
            committed_cache: RwLock::new(*self.committed_cache.read()),
            last_purged_cache: RwLock::new(*self.last_purged_cache.read()),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let last_purged = *self.last_purged_cache.read();
        let last_entry = self.get_last_entry()?;
        let last_log_id = last_entry.map(|e| *e.get_log_id());

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<LedgerNodeId>) -> Result<(), StorageError<LedgerNodeId>> {
        let vote_data = bincode::serialize(vote).map_err(|e| to_serde_error(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(to_storage_error)?;
        {
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(to_storage_error)?;
            meta_table
                .insert(KEY_VOTE, vote_data.as_slice())
                .map_err(to_storage_error)?;
        }
        write_txn.commit().map_err(to_storage_error)?;

        *self.vote_cache.write() = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<LedgerNodeId>>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let write_txn = self.db.begin_write().map_err(to_storage_error)?;
        {
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(to_storage_error)?;
            if let Some(ref log_id) = committed {
                let data = bincode::serialize(log_id).map_err(|e| to_serde_error(e.to_string()))?;
                meta_table
                    .insert(KEY_COMMITTED, data.as_slice())
                    .map_err(to_storage_error)?;
            }
        }
        write_txn.commit().map_err(to_storage_error)?;

        *self.committed_cache.write() = committed;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<LedgerTypeConfig>,
    ) -> Result<(), StorageError<LedgerNodeId>>
    where
        I: IntoIterator<Item = Entry<LedgerTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let write_txn = self.db.begin_write().map_err(to_storage_error)?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;

            for entry in entries {
                let index = entry.get_log_id().index;
                let entry_data = bincode::serialize(&entry).map_err(|e| to_serde_error(e.to_string()))?;
                log_table
                    .insert(index, entry_data.as_slice())
                    .map_err(to_storage_error)?;
            }
        }
        write_txn.commit().map_err(to_storage_error)?;

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<LedgerNodeId>) -> Result<(), StorageError<LedgerNodeId>> {
        let write_txn = self.db.begin_write().map_err(to_storage_error)?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;

            // Remove all entries with index > log_id.index
            let keys_to_remove: Vec<u64> = log_table
                .range((log_id.index + 1)..)
                .map_err(to_storage_error)?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(to_storage_error)?;

            for key in keys_to_remove {
                log_table.remove(key).map_err(to_storage_error)?;
            }
        }
        write_txn.commit().map_err(to_storage_error)?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<LedgerNodeId>) -> Result<(), StorageError<LedgerNodeId>> {
        let write_txn = self.db.begin_write().map_err(to_storage_error)?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(to_storage_error)?;
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(to_storage_error)?;

            // Remove all entries with index <= log_id.index
            let keys_to_remove: Vec<u64> = log_table
                .range(..=log_id.index)
                .map_err(to_storage_error)?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(to_storage_error)?;

            for key in keys_to_remove {
                log_table.remove(key).map_err(to_storage_error)?;
            }

            // Update last_purged
            let purged_data = bincode::serialize(&log_id).map_err(|e| to_serde_error(e.to_string()))?;
            meta_table
                .insert(KEY_LAST_PURGED, purged_data.as_slice())
                .map_err(to_storage_error)?;
        }
        write_txn.commit().map_err(to_storage_error)?;

        *self.last_purged_cache.write() = Some(log_id);
        Ok(())
    }
}

// ============================================================================
// Error Helpers
// ============================================================================

fn to_storage_error<E: std::error::Error + Send + Sync + 'static>(
    e: E,
) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    )
}

fn to_serde_error(msg: String) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::new(std::io::ErrorKind::InvalidData, msg),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::EntryPayload;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_log_store_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let store = RaftLogStore::open(&path).expect("open store");

        // Verify tables were created
        let read_txn = store.db.begin_read().expect("begin read");
        let _log_table = read_txn.open_table(LOG_TABLE).expect("open log table");
        let _meta_table = read_txn.open_table(META_TABLE).expect("open meta table");
    }

    #[tokio::test]
    async fn test_save_and_read_vote() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let mut store = RaftLogStore::open(&path).expect("open store");

        let vote = Vote::new(1, 42);
        store.save_vote(&vote).await.expect("save vote");

        let read_vote = store.read_vote().await.expect("read vote");
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn test_append_and_read_entries() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let mut store = RaftLogStore::open(&path).expect("open store");

        // Create entries
        let entries = vec![
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 1)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 2)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 3)),
        ];

        // Create a callback
        let (tx, rx) = tokio::sync::oneshot::channel();
        let callback = IOFlushed::new(Some(tx));

        store.append(entries, callback).await.expect("append entries");

        // Wait for callback
        rx.await.expect("callback result").expect("callback success");

        // Read entries
        let read_entries = store.try_get_log_entries(1..4).await.expect("read entries");
        assert_eq!(read_entries.len(), 3);
    }

    #[tokio::test]
    async fn test_truncate() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let mut store = RaftLogStore::open(&path).expect("open store");

        // Append entries
        let entries = vec![
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 1)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 2)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 3)),
        ];

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let callback = IOFlushed::new(Some(tx));
        store.append(entries, callback).await.expect("append entries");

        // Truncate at index 1 (removes entries 2 and 3)
        store.truncate(LogId::new(1, 0, 1)).await.expect("truncate");

        // Verify only entry 1 remains
        let read_entries = store.try_get_log_entries(1..4).await.expect("read entries");
        assert_eq!(read_entries.len(), 1);
        assert_eq!(read_entries[0].get_log_id().index, 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let mut store = RaftLogStore::open(&path).expect("open store");

        // Append entries
        let entries = vec![
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 1)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 2)),
            Entry::<LedgerTypeConfig>::new_blank(LogId::new(1, 0, 3)),
        ];

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let callback = IOFlushed::new(Some(tx));
        store.append(entries, callback).await.expect("append entries");

        // Purge up to index 2 (removes entries 1 and 2)
        store.purge(LogId::new(1, 0, 2)).await.expect("purge");

        // Verify only entry 3 remains
        let read_entries = store.try_get_log_entries(1..4).await.expect("read entries");
        assert_eq!(read_entries.len(), 1);
        assert_eq!(read_entries[0].get_log_id().index, 3);

        // Verify last_purged is updated
        let state = store.get_log_state().await.expect("get log state");
        assert_eq!(state.last_purged_log_id, Some(LogId::new(1, 0, 2)));
    }
}
