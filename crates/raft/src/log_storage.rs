//! Raft storage implementation using redb.
//!
//! This module provides the persistent storage for Raft log entries,
//! vote state, committed log tracking, and state machine state.
//!
//! We use the deprecated but non-sealed `RaftStorage` trait which combines
//! log storage and state machine into one implementation. The v2 traits
//! (`RaftLogStorage`, `RaftStateMachine`) are sealed in OpenRaft 0.9.
//!
//! Per DESIGN.md, each shard group has its own storage located at:
//! `shards/{shard_id}/raft/log.redb`

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftStorage, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use ledger_types::{Hash, NamespaceId, VaultId};

use crate::types::{
    LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig, SystemRequest,
};

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
#[allow(dead_code)] // Reserved for future use with save_committed/read_committed
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_APPLIED_STATE: &str = "applied_state";

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

/// Health status for a vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultHealthStatus {
    /// Vault is healthy and accepting writes.
    Healthy,
    /// Vault has diverged and is in recovery.
    Diverged {
        /// Expected state root.
        expected: Hash,
        /// Actual computed state root.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
}

impl Default for VaultHealthStatus {
    fn default() -> Self {
        Self::Healthy
    }
}

// ============================================================================
// Raft Log Store
// ============================================================================

/// Combined Raft storage backed by redb.
///
/// This implementation stores:
/// - Log entries in a redb table indexed by log index
/// - Vote state (term + voted_for) in metadata
/// - Committed log ID for recovery
/// - Applied state (state machine) in metadata
pub struct RaftLogStore {
    /// redb database handle.
    db: Arc<Database>,
    /// Cached vote state.
    vote_cache: RwLock<Option<Vote<LedgerNodeId>>>,
    /// Cached last purged log ID.
    last_purged_cache: RwLock<Option<LogId<LedgerNodeId>>>,
    /// Applied state (state machine).
    applied_state: RwLock<AppliedState>,
}

impl RaftLogStore {
    /// Open or create a new log store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<LedgerNodeId>> {
        let db = Database::create(path.as_ref()).map_err(|e| to_storage_error(&e))?;

        // Ensure tables exist
        let write_txn = db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let _log_table = write_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;
            let _meta_table = write_txn.open_table(META_TABLE).map_err(|e| to_storage_error(&e))?;
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        let store = Self {
            db: Arc::new(db),
            vote_cache: RwLock::new(None),
            last_purged_cache: RwLock::new(None),
            applied_state: RwLock::new(AppliedState {
                sequences: SequenceCounters::new(),
                ..Default::default()
            }),
        };

        // Load cached values
        store.load_caches()?;

        Ok(store)
    }

    /// Load metadata values into caches.
    fn load_caches(&self) -> Result<(), StorageError<LedgerNodeId>> {
        let read_txn = self.db.begin_read().map_err(|e| to_storage_error(&e))?;
        let meta_table = read_txn.open_table(META_TABLE).map_err(|e| to_storage_error(&e))?;

        // Load vote
        if let Some(vote_data) = meta_table.get(KEY_VOTE).map_err(|e| to_storage_error(&e))? {
            let vote: Vote<LedgerNodeId> =
                bincode::deserialize(vote_data.value()).map_err(|e| to_serde_error(&e))?;
            *self.vote_cache.write() = Some(vote);
        }

        // Load last purged
        if let Some(purged_data) = meta_table.get(KEY_LAST_PURGED).map_err(|e| to_storage_error(&e))? {
            let purged: LogId<LedgerNodeId> =
                bincode::deserialize(purged_data.value()).map_err(|e| to_serde_error(&e))?;
            *self.last_purged_cache.write() = Some(purged);
        }

        // Load applied state
        if let Some(state_data) = meta_table.get(KEY_APPLIED_STATE).map_err(|e| to_storage_error(&e))? {
            let state: AppliedState =
                bincode::deserialize(state_data.value()).map_err(|e| to_serde_error(&e))?;
            *self.applied_state.write() = state;
        }

        Ok(())
    }

    /// Get the last log entry.
    fn get_last_entry(&self) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.begin_read().map_err(|e| to_storage_error(&e))?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;

        if let Some((_, entry_data)) = log_table.last().map_err(|e| to_storage_error(&e))? {
            let entry: Entry<LedgerTypeConfig> =
                bincode::deserialize(entry_data.value()).map_err(|e| to_serde_error(&e))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Apply a single request and return the response.
    fn apply_request(&self, request: &LedgerRequest, state: &mut AppliedState) -> LedgerResponse {
        match request {
            LedgerRequest::Write {
                namespace_id,
                vault_id,
                transactions: _,
            } => {
                let key = (*namespace_id, *vault_id);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return LedgerResponse::Error {
                        message: format!(
                            "Vault {}:{} is diverged and not accepting writes",
                            namespace_id, vault_id
                        ),
                    };
                }

                let current_height = state.vault_heights.get(&key).copied().unwrap_or(0);
                let new_height = current_height + 1;
                let block_hash = self.compute_block_hash(*namespace_id, *vault_id, new_height);
                state.vault_heights.insert(key, new_height);

                LedgerResponse::Write {
                    block_height: new_height,
                    block_hash,
                }
            }

            LedgerRequest::CreateNamespace { name: _ } => {
                let namespace_id = state.sequences.next_namespace();
                LedgerResponse::NamespaceCreated { namespace_id }
            }

            LedgerRequest::CreateVault {
                namespace_id,
                name: _,
            } => {
                let vault_id = state.sequences.next_vault();
                let key = (*namespace_id, vault_id);
                state.vault_heights.insert(key, 0);
                state.vault_health.insert(key, VaultHealthStatus::Healthy);
                LedgerResponse::VaultCreated { vault_id }
            }

            LedgerRequest::DeleteNamespace { namespace_id: _ } => {
                // TODO: Check if namespace has vaults, reject if so
                // For now, just mark as deleted
                LedgerResponse::NamespaceDeleted { success: true }
            }

            LedgerRequest::DeleteVault {
                namespace_id,
                vault_id,
            } => {
                // Remove vault from tracking
                let key = (*namespace_id, *vault_id);
                state.vault_heights.remove(&key);
                state.vault_health.remove(&key);
                LedgerResponse::VaultDeleted { success: true }
            }

            LedgerRequest::System(system_request) => match system_request {
                SystemRequest::CreateUser { name: _, email: _ } => {
                    let user_id = state.sequences.next_user();
                    LedgerResponse::UserCreated { user_id }
                }
                SystemRequest::AddNode { .. }
                | SystemRequest::RemoveNode { .. }
                | SystemRequest::UpdateNamespaceRouting { .. } => LedgerResponse::Empty,
            },
        }
    }

    /// Compute a simple block hash.
    fn compute_block_hash(&self, namespace_id: NamespaceId, vault_id: VaultId, height: u64) -> Hash {
        use ledger_types::sha256;
        let mut data = Vec::new();
        data.extend_from_slice(&namespace_id.to_le_bytes());
        data.extend_from_slice(&vault_id.to_le_bytes());
        data.extend_from_slice(&height.to_le_bytes());
        sha256(&data)
    }

    /// Persist the applied state.
    fn save_applied_state(&self, state: &AppliedState) -> Result<(), StorageError<LedgerNodeId>> {
        let state_data = bincode::serialize(state).map_err(|e| to_serde_error(&e))?;
        let write_txn = self.db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(|e| to_storage_error(&e))?;
            meta_table
                .insert(KEY_APPLIED_STATE, state_data.as_slice())
                .map_err(|e| to_storage_error(&e))?;
        }
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
        let read_txn = self.db.begin_read().map_err(|e| to_storage_error(&e))?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;

        let mut entries = Vec::new();
        for result in log_table.range(range).map_err(|e| to_storage_error(&e))? {
            let (_, entry_data) = result.map_err(|e| to_storage_error(&e))?;
            let entry: Entry<LedgerTypeConfig> =
                bincode::deserialize(entry_data.value()).map_err(|e| to_serde_error(&e))?;
            entries.push(entry);
        }

        Ok(entries)
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
        let data = bincode::serialize(&self.state).map_err(|e| to_serde_error(&e))?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.state.last_applied.as_ref().map(|l| l.index).unwrap_or(0),
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

    async fn get_log_state(&mut self) -> Result<LogState<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
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
            applied_state: RwLock::new(self.applied_state.read().clone()),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<LedgerNodeId>) -> Result<(), StorageError<LedgerNodeId>> {
        let vote_data = bincode::serialize(vote).map_err(|e| to_serde_error(&e))?;

        let write_txn = self.db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(|e| to_storage_error(&e))?;
            meta_table
                .insert(KEY_VOTE, vote_data.as_slice())
                .map_err(|e| to_storage_error(&e))?;
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.vote_cache.write() = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<LedgerNodeId>>, StorageError<LedgerNodeId>> {
        Ok(*self.vote_cache.read())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<LedgerNodeId>>
    where
        I: IntoIterator<Item = Entry<LedgerTypeConfig>> + OptionalSend,
    {
        let write_txn = self.db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;

            for entry in entries {
                let index = entry.log_id.index;
                let entry_data = bincode::serialize(&entry).map_err(|e| to_serde_error(&e))?;
                log_table
                    .insert(index, entry_data.as_slice())
                    .map_err(|e| to_storage_error(&e))?;
            }
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let write_txn = self.db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;

            // Delete logs from log_id.index onwards (inclusive)
            let keys_to_remove: Vec<u64> = log_table
                .range(log_id.index..)
                .map_err(|e| to_storage_error(&e))?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| to_storage_error(&e))?;

            for key in keys_to_remove {
                log_table.remove(key).map_err(|e| to_storage_error(&e))?;
            }
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let write_txn = self.db.begin_write().map_err(|e| to_storage_error(&e))?;
        {
            let mut log_table = write_txn.open_table(LOG_TABLE).map_err(|e| to_storage_error(&e))?;
            let mut meta_table = write_txn.open_table(META_TABLE).map_err(|e| to_storage_error(&e))?;

            let keys_to_remove: Vec<u64> = log_table
                .range(..=log_id.index)
                .map_err(|e| to_storage_error(&e))?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| to_storage_error(&e))?;

            for key in keys_to_remove {
                log_table.remove(key).map_err(|e| to_storage_error(&e))?;
            }

            let purged_data = bincode::serialize(&log_id).map_err(|e| to_serde_error(&e))?;
            meta_table
                .insert(KEY_LAST_PURGED, purged_data.as_slice())
                .map_err(|e| to_storage_error(&e))?;
        }
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
        let mut state = self.applied_state.write();

        for entry in entries {
            state.last_applied = Some(entry.log_id);

            let response = match &entry.payload {
                EntryPayload::Blank => LedgerResponse::Empty,
                EntryPayload::Normal(request) => self.apply_request(request, &mut state),
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
                    LedgerResponse::Empty
                }
            };

            responses.push(response);
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
        let new_state: AppliedState = bincode::deserialize(&data).map_err(|e| to_serde_error(&e))?;

        *self.applied_state.write() = new_state.clone();
        self.save_applied_state(&new_state)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let state = self.applied_state.read();

        if state.last_applied.is_none() {
            return Ok(None);
        }

        let data = bincode::serialize(&*state).map_err(|e| to_serde_error(&e))?;

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
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    )
}

fn to_serde_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let path = dir.path().join("raft_log.redb");

        let store = RaftLogStore::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateNamespace {
            name: "test-ns".to_string(),
        };

        let response = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::NamespaceCreated { namespace_id } => {
                assert_eq!(namespace_id, 1);
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.redb");

        let store = RaftLogStore::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: Some("test-vault".to_string()),
        };

        let response = store.apply_request(&request, &mut state);

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
        let path = dir.path().join("raft_log.redb");

        let store = RaftLogStore::open(&path).expect("open store");
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

        let response = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("diverged"));
            }
            _ => panic!("expected error response"),
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

        let store_a = RaftLogStore::open(dir_a.path().join("raft_log.redb")).expect("open store a");
        let store_b = RaftLogStore::open(dir_b.path().join("raft_log.redb")).expect("open store b");

        // Same sequence of requests to apply
        let requests = vec![
            LedgerRequest::CreateNamespace {
                name: "acme-corp".to_string(),
            },
            LedgerRequest::CreateNamespace {
                name: "startup-inc".to_string(),
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("production".to_string()),
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("staging".to_string()),
            },
            LedgerRequest::CreateVault {
                namespace_id: 2,
                name: Some("main".to_string()),
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
            results_a.push(store_a.apply_request(request, &mut state_a));
        }
        drop(state_a);

        // Apply to node B
        let mut state_b = store_b.applied_state.write();
        let mut results_b = Vec::new();
        for request in &requests {
            results_b.push(store_b.apply_request(request, &mut state_b));
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

        let store_a = RaftLogStore::open(dir_a.path().join("raft_log.redb")).expect("open store a");
        let store_b = RaftLogStore::open(dir_b.path().join("raft_log.redb")).expect("open store b");

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

        let store_a = RaftLogStore::open(dir_a.path().join("raft_log.redb")).expect("open store a");
        let store_b = RaftLogStore::open(dir_b.path().join("raft_log.redb")).expect("open store b");

        // Same inputs must produce same hash
        let hash_a = store_a.compute_block_hash(1, 2, 3);
        let hash_b = store_b.compute_block_hash(1, 2, 3);

        assert_eq!(hash_a, hash_b, "Block hashes must be deterministic");

        // Different inputs must produce different hashes
        let hash_c = store_a.compute_block_hash(1, 2, 4);
        assert_ne!(hash_a, hash_c, "Different inputs should produce different hashes");
    }

    /// Verify vault height tracking is deterministic.
    ///
    /// Writes to the same vault must increment height consistently across nodes.
    #[tokio::test]
    async fn test_deterministic_vault_heights() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::open(dir_a.path().join("raft_log.redb")).expect("open store a");
        let store_b = RaftLogStore::open(dir_b.path().join("raft_log.redb")).expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create vault on both nodes
        let create_vault = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: Some("test".to_string()),
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

        let store_a = RaftLogStore::open(dir_a.path().join("raft_log.redb")).expect("open store a");
        let store_b = RaftLogStore::open(dir_b.path().join("raft_log.redb")).expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create namespace and vaults
        let requests: Vec<LedgerRequest> = vec![
            LedgerRequest::CreateNamespace {
                name: "ns1".to_string(),
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("vault-a".to_string()),
            },
            LedgerRequest::CreateVault {
                namespace_id: 1,
                name: Some("vault-b".to_string()),
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
            results_a.push(store_a.apply_request(req, &mut state_a));
            results_b.push(store_b.apply_request(req, &mut state_b));
        }

        // Results must match
        assert_eq!(results_a, results_b, "Interleaved operation results must match");

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
        let bytes_a = bincode::serialize(&state_a).expect("serialize a");
        let bytes_b = bincode::serialize(&state_b).expect("serialize b");

        assert_eq!(bytes_a, bytes_b, "Serialized state must be identical");

        // Deserialize and verify
        let restored_a: AppliedState = bincode::deserialize(&bytes_a).expect("deserialize a");
        let restored_b: AppliedState = bincode::deserialize(&bytes_b).expect("deserialize b");

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
}
