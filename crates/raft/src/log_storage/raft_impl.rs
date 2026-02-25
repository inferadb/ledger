//! OpenRaft trait implementations for [`RaftLogStore`].
//!
//! Implements [`RaftLogReader`], [`RaftStorage`], and [`RaftSnapshotBuilder`]
//! using the B-tree-backed storage engine.

use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::{FileBackend, TableId, tables};
use inferadb_ledger_types::{decode, encode, events::EventConfig};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftStorage, SnapshotMeta, StorageError,
    StoredMembership, Vote,
    storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot},
};
use parking_lot::RwLock;

use super::{
    ShardChainState,
    store::RaftLogStore,
    types::{AppliedState, AppliedStateCore, PendingExternalWrites},
};
use crate::{
    metrics,
    snapshot::{
        SNAPSHOT_TABLE_IDS, SnapshotError, SnapshotReader, SnapshotWriter, SyncSnapshotReader,
    },
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Version sentinel prepended to the serialized `AppliedStateCore` in the
/// `RaftState` B+ tree table. Matches `RaftLogStore::STATE_CORE_VERSION`.
const STATE_CORE_VERSION_SENTINEL: [u8; 2] = [0x00, 0x01];

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

        // Use range iteration from inferadb-ledger-store
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

    /// Returns log entries for replication.
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
            let purged_idx = last_purged.map_or(0, |l| l.index);

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
                let last_idx = self.get_last_entry().ok().flatten().map_or(0, |e| e.log_id.index);
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

/// Builds Raft snapshots by streaming B-tree tables into compressed snapshot files.
///
/// Holds shared references to the database and state layer so that
/// `build_snapshot()` can open a `ReadTransaction` for a consistent
/// point-in-time view — no `AppliedState` clone needed, no in-memory
/// state lock acquired.
pub struct LedgerSnapshotBuilder {
    /// Shared B-tree database (log + state tables).
    db: Arc<inferadb_ledger_store::Database<FileBackend>>,
    /// State layer for entity/relationship storage (used during snapshot entity
    /// restoration via `StateLayer::restore_entity()`).
    #[allow(dead_code)]
    state_layer: Option<Arc<inferadb_ledger_state::StateLayer<FileBackend>>>,
    /// Events database for apply-phase event snapshot (separate from main db).
    events_db: Option<Arc<EventsDatabase<FileBackend>>>,
    /// Event config for snapshot event limits.
    event_config: Option<EventConfig>,
}

impl RaftSnapshotBuilder<LedgerTypeConfig> for LedgerSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        // Extract org IDs and timestamp for per-org event range scans.
        let (org_ids, last_ts) = extract_snapshot_event_context(&self.db)?;

        // Collect events synchronously before entering async code.
        // EventsDatabase contains raw pointers (!Send), so the reference
        // must not live across await points.
        let event_entries = collect_snapshot_events(
            self.events_db.as_deref(),
            self.event_config.as_ref(),
            &org_ids,
            last_ts,
        );

        let (file, meta) = write_snapshot_to_file(&self.db, event_entries)
            .await
            .map_err(|e| snapshot_to_storage_error(&e))?;

        Ok(Snapshot { meta, snapshot: Box::new(file) })
    }
}

// ============================================================================
// Shared snapshot creation helper
// ============================================================================

/// Creates a complete snapshot file and returns the open file handle plus metadata.
///
/// Separates sync B-tree reads from async file I/O for `Send` safety:
/// 1. (sync) Open `ReadTransaction`, read `AppliedStateCore` + table + entity data
/// 2. (async) Write all pre-collected data to the snapshot file via `SnapshotWriter`
///
/// All data comes from a single `ReadTransaction` for transactional consistency —
/// no in-memory `AppliedState` lock needed.
///
/// Event entries must be collected by the caller before invoking this function,
/// because `EventsDatabase` contains raw pointers (`!Send`). Passing pre-collected
/// `Vec<Vec<u8>>` keeps this async function's future `Send`-safe.
async fn write_snapshot_to_file(
    db: &inferadb_ledger_store::Database<FileBackend>,
    event_entries: Vec<Vec<u8>>,
) -> Result<(tokio::fs::File, SnapshotMeta<LedgerNodeId, openraft::BasicNode>), SnapshotError> {
    // --- Sync phase: collect all data from a single ReadTransaction ---

    let (core_bytes, last_applied, membership, snapshot_id, table_data, entity_entries) = {
        let read_txn = db.read().map_err(|e| SnapshotError::InvalidEntry {
            reason: format!("Failed to open read transaction: {e}"),
        })?;

        // 1. Read AppliedStateCore from the RaftState table
        let state_data = read_txn
            .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
            .map_err(|e| SnapshotError::InvalidEntry {
            reason: format!("Failed to read AppliedStateCore from RaftState: {e}"),
        })?;

        let (core, core_bytes) = match state_data {
            Some(data) if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL => {
                // New format: version sentinel + postcard(AppliedStateCore)
                let core: AppliedStateCore =
                    decode(&data[2..]).map_err(|e| SnapshotError::InvalidEntry {
                        reason: format!("Failed to decode AppliedStateCore: {e}"),
                    })?;
                let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to re-encode AppliedStateCore: {e}"),
                })?;
                (core, core_bytes)
            },
            Some(data) => {
                // Old format: full AppliedState blob (pre-migration)
                let state: AppliedState =
                    decode(&data).map_err(|e| SnapshotError::InvalidEntry {
                        reason: format!("Failed to decode old-format AppliedState: {e}"),
                    })?;
                let core = AppliedStateCore::from(&state);
                let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to encode AppliedStateCore: {e}"),
                })?;
                (core, core_bytes)
            },
            None => {
                // Fresh database — empty state
                let core = AppliedStateCore {
                    last_applied: None,
                    membership: StoredMembership::default(),
                    shard_height: 0,
                    previous_shard_hash: inferadb_ledger_types::Hash::default(),
                    last_applied_timestamp_ns: 0,
                };
                let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to encode default AppliedStateCore: {e}"),
                })?;
                (core, core_bytes)
            },
        };

        let snapshot_id = format!(
            "snapshot-{}-{}",
            core.last_applied.as_ref().map_or(0, |l| l.index),
            chrono::Utc::now().timestamp()
        );

        // 2. Collect all table data from the same ReadTransaction
        let mut tables = Vec::with_capacity(SNAPSHOT_TABLE_IDS.len());
        for &table_id_u8 in SNAPSHOT_TABLE_IDS {
            let table_id = TableId::from_u8(table_id_u8)
                .ok_or(SnapshotError::UnknownTableId { table_id: table_id_u8 })?;
            let entries = iter_table_raw(&read_txn, table_id)?;
            tables.push((table_id_u8, entries));
        }

        // Collect entities
        let entities = iter_table_raw(&read_txn, TableId::Entities)?;

        // ReadTransaction is dropped here — no longer held across awaits
        (core_bytes, core.last_applied, core.membership.clone(), snapshot_id, tables, entities)
    };

    // --- Async phase: write all collected data to the snapshot file ---

    let std_file = tempfile::tempfile().map_err(|e| SnapshotError::Io {
        source: e,
        location: snafu::Location::new(file!(), line!(), 0),
    })?;
    let file = tokio::fs::File::from_std(std_file);
    let mut writer = SnapshotWriter::new(file);

    // Header
    writer.write_header(&core_bytes).await?;

    // Table sections
    writer.write_table_count(table_data.len() as u32).await?;
    for (table_id_u8, entries) in &table_data {
        writer.write_table_header(*table_id_u8, entries.len() as u32).await?;
        for (key, value) in entries {
            writer.write_table_entry(key, value).await?;
        }
    }

    // Entity section
    writer.write_entity_count(entity_entries.len() as u64).await?;
    for (key, value) in &entity_entries {
        writer.write_entity_entry(key, value).await?;
    }

    // Event section
    writer.write_event_count(event_entries.len() as u64).await?;
    for entry_bytes in &event_entries {
        writer.write_event_entry(entry_bytes).await?;
    }

    // Finalize: flush zstd stream and write checksum footer
    let mut file = writer.finish().await?;

    // Seek to start for reading
    use tokio::io::AsyncSeekExt;
    file.seek(std::io::SeekFrom::Start(0)).await.map_err(|e| SnapshotError::Io {
        source: e,
        location: snafu::Location::new(file!(), line!(), 0),
    })?;

    let meta = SnapshotMeta { last_log_id: last_applied, last_membership: membership, snapshot_id };

    tracing::info!(
        last_applied = ?last_applied,
        table_count = table_data.len(),
        entity_count = entity_entries.len(),
        event_count = event_entries.len(),
        "Snapshot file created"
    );

    Ok((file, meta))
}

/// Iterates all entries in a table by its runtime `TableId`, returning raw
/// pre-encoded `(key, value)` byte pairs.
///
/// This uses the typed `iter` method for each known table ID. The iterator
/// always yields `(Vec<u8>, Vec<u8>)` regardless of the table's declared
/// key/value types, because `TableIterator::Item = (Vec<u8>, Vec<u8>)`.
fn iter_table_raw(
    read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
    table_id: TableId,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SnapshotError> {
    // Macro to reduce boilerplate: each table type has the same iterator pattern
    macro_rules! collect_table {
        ($table:ty) => {{
            let iter = read_txn.iter::<$table>().map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to iterate table {}: {e}", table_id as u8),
            })?;
            Ok(iter.collect())
        }};
    }

    match table_id {
        TableId::Entities => collect_table!(tables::Entities),
        TableId::Relationships => collect_table!(tables::Relationships),
        TableId::ObjIndex => collect_table!(tables::ObjIndex),
        TableId::SubjIndex => collect_table!(tables::SubjIndex),
        TableId::Blocks => collect_table!(tables::Blocks),
        TableId::VaultBlockIndex => collect_table!(tables::VaultBlockIndex),
        TableId::RaftLog => collect_table!(tables::RaftLog),
        TableId::RaftState => collect_table!(tables::RaftState),
        TableId::VaultMeta => collect_table!(tables::VaultMeta),
        TableId::OrganizationMeta => collect_table!(tables::OrganizationMeta),
        TableId::Sequences => collect_table!(tables::Sequences),
        TableId::ClientSequences => collect_table!(tables::ClientSequences),
        TableId::CompactionMeta => collect_table!(tables::CompactionMeta),
        TableId::OrganizationSlugIndex => collect_table!(tables::OrganizationSlugIndex),
        TableId::VaultSlugIndex => collect_table!(tables::VaultSlugIndex),
        TableId::VaultHeights => collect_table!(tables::VaultHeights),
        TableId::VaultHashes => collect_table!(tables::VaultHashes),
        TableId::VaultHealth => collect_table!(tables::VaultHealth),
    }
}

/// Extracts organization IDs and the last applied deterministic timestamp from
/// the database. Used by snapshot creation to parameterize event collection.
fn extract_snapshot_event_context(
    db: &inferadb_ledger_store::Database<FileBackend>,
) -> Result<(Vec<inferadb_ledger_types::OrganizationId>, i64), StorageError<LedgerNodeId>> {
    let read_txn = db.read().map_err(|e| to_storage_error(&e))?;

    // Extract org IDs from OrganizationMeta table
    let org_iter = read_txn.iter::<tables::OrganizationMeta>().map_err(|e| to_storage_error(&e))?;
    let org_ids: Vec<_> = org_iter
        .filter_map(|(key, _)| {
            <i64 as inferadb_ledger_store::Key>::decode(&key)
                .map(inferadb_ledger_types::OrganizationId::new)
        })
        .collect();

    // Extract last_applied_timestamp_ns from AppliedStateCore
    let timestamp_ns = read_txn
        .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
        .map_err(|e| to_storage_error(&e))?
        .and_then(|data| {
            if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL {
                decode::<AppliedStateCore>(&data[2..]).ok()
            } else {
                None
            }
        })
        .map_or(0, |core| core.last_applied_timestamp_ns);

    Ok((org_ids, timestamp_ns))
}

/// Collects apply-phase events from events.db for snapshot inclusion.
///
/// Uses per-organization range scans with a timestamp cutoff derived from
/// the last applied entry's deterministic timestamp. This avoids the
/// sort-then-truncate pattern that loads all events into memory.
///
/// Returns serialized event entries (already postcard-encoded `EventEntry`
/// bytes from the B-tree values). Capped by `EventConfig::max_snapshot_events`.
/// Returns an empty vec if events are not configured or scanning fails
/// (best-effort).
fn collect_snapshot_events(
    events_db: Option<&EventsDatabase<FileBackend>>,
    event_config: Option<&EventConfig>,
    org_ids: &[inferadb_ledger_types::OrganizationId],
    last_applied_timestamp_ns: i64,
) -> Vec<Vec<u8>> {
    let Some(edb) = events_db else {
        return Vec::new();
    };
    let max_events = event_config.map_or(10_000, |c| c.max_snapshot_events);
    let ttl_days = event_config.map_or(90, |c| c.default_ttl_days);

    // Compute cutoff: last_applied_timestamp - TTL.
    // Events older than the TTL would be GC'd shortly after the follower joins,
    // so excluding them from the snapshot avoids unnecessary transfer.
    let ttl_ns = i64::from(ttl_days) * 86_400 * 1_000_000_000;
    let cutoff_ns = last_applied_timestamp_ns.saturating_sub(ttl_ns).max(0) as u64;

    let read_txn = match edb.read() {
        Ok(txn) => txn,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to open events.db for snapshot");
            return Vec::new();
        },
    };

    let entries = match inferadb_ledger_state::EventStore::scan_apply_phase_ranged(
        &read_txn, org_ids, cutoff_ns, max_events,
    ) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to scan events for snapshot");
            return Vec::new();
        },
    };

    if !entries.is_empty() {
        tracing::debug!(
            count = entries.len(),
            max = max_events,
            org_count = org_ids.len(),
            cutoff_ns,
            "Collected apply-phase events for snapshot via range scan"
        );
    }

    entries
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

        Ok(LogState { last_purged_log_id: last_purged, last_log_id })
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
            block_announcements: self.block_announcements.clone(),
            event_writer: None,
            client_sequence_eviction: self.client_sequence_eviction.clone(),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let vote_data = encode(vote).map_err(|e| to_serde_error(&e))?;

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&super::KEY_VOTE.to_string(), &vote_data)
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
            tracing::error!("append_to_log: commit failed for entries {:?}: {:?}", indices, e);
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
            write_txn.delete::<tables::RaftLog>(&key).map_err(|e| to_storage_error(&e))?;
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
            write_txn.delete::<tables::RaftLog>(&key).map_err(|e| to_storage_error(&e))?;
        }

        // Save the last purged log ID
        let purged_data = encode(&log_id).map_err(|e| to_serde_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&super::KEY_LAST_PURGED.to_string(), &purged_data)
            .map_err(|e| to_storage_error(&e))?;

        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.last_purged_cache.write() = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<LedgerNodeId>>, StoredMembership<LedgerNodeId, openraft::BasicNode>),
        StorageError<LedgerNodeId>,
    > {
        let state = self.applied_state.read();
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<LedgerTypeConfig>],
    ) -> Result<Vec<crate::types::LedgerResponse>, StorageError<LedgerNodeId>> {
        let _span = tracing::info_span!(
            "apply_to_state_machine",
            entry_count = entries.len(),
            shard_id = self.shard_id.value(),
        )
        .entered();

        let mut responses = Vec::new();
        let mut vault_entries = Vec::new();
        let mut state = self.applied_state.write();

        // Event accumulation — deterministic timestamp from leader's proposal.
        // All replicas apply with the same timestamp, guaranteeing byte-identical
        // event storage, B+ tree keys, and pagination cursors across the cluster.
        let block_timestamp = entries
            .last()
            .and_then(|e| match &e.payload {
                EntryPayload::Normal(payload) => Some(payload.proposed_at),
                _ => None,
            })
            .unwrap_or_else(chrono::Utc::now);
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let mut pending = PendingExternalWrites::default();
        let ttl_days = self.event_writer.as_ref().map_or(0, |ew| ew.config().default_ttl_days);

        // Get the committed_index from the last entry (for ShardBlock metadata)
        let committed_index = entries.last().map_or(0, |e| e.log_id.index);
        // Term is stored in the leader_id
        let term = entries.last().map(|e| e.log_id.leader_id.get_term()).unwrap_or(0);

        for entry in entries {
            state.last_applied = Some(entry.log_id);

            let (response, vault_entry) = match &entry.payload {
                EntryPayload::Blank => (crate::types::LedgerResponse::Empty, None),
                EntryPayload::Normal(payload) => self.apply_request_with_events(
                    &payload.request,
                    &mut state,
                    block_timestamp,
                    &mut op_index,
                    &mut events,
                    ttl_days,
                    &mut pending,
                ),
                EntryPayload::Membership(membership) => {
                    state.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    (crate::types::LedgerResponse::Empty, None)
                },
            };

            responses.push(response);

            // Collect vault entries for ShardBlock creation
            if let Some(entry) = vault_entry {
                vault_entries.push(entry);
            }
        }

        // Create and store ShardBlock if we have vault entries
        if !vault_entries.is_empty() {
            let timestamp = block_timestamp;

            // Read shard chain state (single lock acquisition)
            let chain_state = *self.shard_chain.read();
            let new_shard_height = chain_state.height + 1;

            let shard_block = inferadb_ledger_types::ShardBlock {
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
            if let Some(archive) = &self.block_archive
                && let Err(e) = archive.append_block(&shard_block)
            {
                tracing::error!("Failed to store block: {}", e);
                // Continue - block storage failure is logged but doesn't fail the operation
            }

            // Broadcast block announcements for real-time subscribers
            if let Some(sender) = &self.block_announcements {
                for entry in &vault_entries {
                    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
                    let announcement = inferadb_ledger_proto::proto::BlockAnnouncement {
                        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                            slug: state
                                .id_to_slug
                                .get(&entry.organization_id)
                                .map_or(entry.organization_id.value() as u64, |s| s.value()),
                        }),
                        vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                            slug: state
                                .vault_id_to_slug
                                .get(&entry.vault_id)
                                .map_or(0, |s| s.value()),
                        }),
                        height: entry.vault_height,
                        block_hash: Some(inferadb_ledger_proto::proto::Hash {
                            value: block_hash.to_vec(),
                        }),
                        state_root: Some(inferadb_ledger_proto::proto::Hash {
                            value: entry.state_root.to_vec(),
                        }),
                        timestamp: Some(prost_types::Timestamp {
                            seconds: timestamp.timestamp(),
                            nanos: timestamp.timestamp_subsec_nanos() as i32,
                        }),
                    };
                    // Ignore send errors - no receivers is valid (fire-and-forget)
                    let _ = sender.send(announcement);
                    tracing::debug!(
                        organization_id = entry.organization_id.value(),
                        vault_id = entry.vault_id.value(),
                        height = entry.vault_height,
                        "Block announcement broadcast"
                    );
                }
            }

            // Update previous vault hashes for each entry
            for entry in &vault_entries {
                let vault_block =
                    shard_block.extract_vault_block(entry.organization_id, entry.vault_id);
                if let Some(vb) = vault_block {
                    let block_hash = inferadb_ledger_types::hash::block_hash(&vb.header);
                    let key = (entry.organization_id, entry.vault_id);
                    state.previous_vault_hashes.insert(key, block_hash);
                    pending.vault_hashes.push((key, block_hash));
                }
            }

            // Update shard chain tracking (single lock acquisition)
            let shard_hash =
                inferadb_ledger_types::sha256(&encode(&shard_block).unwrap_or_default());
            *self.shard_chain.write() =
                ShardChainState { height: new_shard_height, previous_hash: shard_hash };

            // Also update AppliedState for snapshot persistence
            state.shard_height = new_shard_height;
            state.previous_shard_hash = shard_hash;
        }

        // Client sequence TTL eviction — deterministic from log index.
        //
        // Triggered when any entry in this batch has an index that is a
        // multiple of eviction_interval. This guarantees identical eviction
        // behavior across replicas regardless of how openraft batches entries.
        let eviction_interval = self.client_sequence_eviction.eviction_interval;
        let eviction_ttl = self.client_sequence_eviction.ttl_seconds;
        let should_evict = eviction_interval > 0
            && entries.iter().any(|e| e.log_id.index % eviction_interval == 0);

        if should_evict {
            let proposed_at_secs = block_timestamp.timestamp();
            // Collect expired entries: deterministic sort by composite key encoding
            // ensures identical deletion order across replicas (HashMap iteration is
            // non-deterministic in Rust).
            let mut expired_keys: Vec<(
                (inferadb_ledger_types::OrganizationId, inferadb_ledger_types::VaultId, String),
                Vec<u8>,
            )> = state
                .client_sequences
                .iter()
                .filter(|(_, entry)| {
                    // Saturating subtraction: if proposed_at < last_seen (clock
                    // regression after leader change), treat delta as 0 — skip eviction
                    // rather than risk spurious deletes.
                    proposed_at_secs.saturating_sub(entry.last_seen) > eviction_ttl
                })
                .map(|(key, _)| {
                    let bytes_key =
                        PendingExternalWrites::client_sequence_key(key.0, key.1, key.2.as_bytes());
                    (key.clone(), bytes_key)
                })
                .collect();

            // Sort by composite bytes key (matches B+ tree ordering)
            expired_keys.sort_by(|a, b| a.1.cmp(&b.1));

            for (map_key, bytes_key) in expired_keys {
                state.client_sequences.remove(&map_key);
                pending.client_sequences_deleted.push(bytes_key);
            }
        }

        // Record deterministic timestamp for snapshot event collection.
        // This ensures two snapshots of the same state produce identical event
        // sets regardless of wall-clock time.
        state.last_applied_timestamp_ns = block_timestamp.timestamp_nanos_opt().unwrap_or(0);

        // Snapshot sequence counters into pending (once per batch, not per-operation)
        pending
            .sequences
            .push(("organization".to_string(), state.sequences.organization.value() as u64));
        pending.sequences.push(("vault".to_string(), state.sequences.vault.value() as u64));
        pending.sequences.push(("user".to_string(), state.sequences.user as u64));
        pending.sequences.push(("user_email".to_string(), state.sequences.user_email as u64));
        pending.sequences.push(("email_verify".to_string(), state.sequences.email_verify as u64));

        // Persist core state blob + external table writes atomically
        self.save_state_core(&state, &pending)?;
        drop(state);

        // Write accumulated events to events.db (best-effort: log failures but
        // don't fail the Raft apply — events are audit data, not consensus-critical).
        if !events.is_empty()
            && let Some(ew) = &self.event_writer
        {
            match ew.write_events(&events) {
                Ok(count) if count > 0 => {
                    tracing::debug!(
                        written = count,
                        total = events.len(),
                        "Apply-phase events persisted"
                    );
                },
                Ok(_) => {}, // All filtered by config — no-op
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        event_count = events.len(),
                        "Failed to persist apply-phase events"
                    );
                },
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LedgerSnapshotBuilder {
            db: Arc::clone(&self.db),
            state_layer: self.state_layer.as_ref().map(Arc::clone),
            events_db: self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db())),
            event_config: self.event_writer.as_ref().map(|ew| ew.config().clone()),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<LedgerNodeId>> {
        // Create a temp file for receiving snapshot data from the leader.
        // openraft will write the streamed chunks into this file, then pass
        // it to install_snapshot().
        let std_file = tempfile::tempfile().map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                e,
            )
        })?;
        let file = tokio::fs::File::from_std(std_file);
        Ok(Box::new(file))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<LedgerNodeId, openraft::BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        use tokio::io::AsyncSeekExt;

        let mut file = *snapshot;

        // =============================================================
        // Async phase: verify SHA-256 checksum over compressed bytes
        // =============================================================
        let file_size = file
            .seek(std::io::SeekFrom::End(0))
            .await
            .map_err(|e| to_io_storage_error(e, "seek to end"))?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| to_io_storage_error(e, "seek to start"))?;

        SnapshotReader::verify_checksum(&mut file, file_size)
            .await
            .map_err(|e| snapshot_to_storage_error(&e))?;

        // Convert to std::fs::File for synchronous streaming
        let std_file = file.into_std().await;
        let compressed_size = file_size - crate::snapshot::CHECKSUM_SIZE as u64;

        // =============================================================
        // Sync phase: stream decompressed data directly into B-trees
        // =============================================================
        // Using block_in_place because:
        // 1. WriteTransaction is !Send (RefCell/raw pointers)
        // 2. Sync zstd::Decoder avoids async/sync boundary issues
        // 3. No staging Vecs — entries stream directly into WriteTransaction

        let db = &self.db;
        let event_writer_ref = self.event_writer.as_ref();

        let (core, table_count, entity_count, event_count) =
            tokio::task::block_in_place(|| -> Result<_, StorageError<LedgerNodeId>> {
                use std::io::{BufReader, Read, Seek, SeekFrom};

                let mut file = std_file;
                file.seek(SeekFrom::Start(0))
                    .map_err(|e| to_io_storage_error(e, "sync seek to start"))?;

                // Create sync zstd decoder over the compressed portion
                let reader = BufReader::new(file.take(compressed_size));
                let mut decoder = zstd::stream::read::Decoder::new(reader)
                    .map_err(|e| to_io_storage_error(e, "zstd decoder init"))?;

                // Read header → AppliedStateCore
                let core_bytes = SyncSnapshotReader::read_header(&mut decoder)
                    .map_err(|e| snapshot_to_storage_error(&e))?;
                let core: AppliedStateCore = decode(&core_bytes).map_err(|e| to_serde_error(&e))?;

                // Open WriteTransaction — all table + entity writes go here
                let mut write_txn = db.write().map_err(|e| to_storage_error(&e))?;

                // Stream table entries directly into WriteTransaction
                let table_count = SyncSnapshotReader::read_u32(&mut decoder)
                    .map_err(|e| snapshot_to_storage_error(&e))?;

                for _ in 0..table_count {
                    let (table_id_u8, entry_count) =
                        SyncSnapshotReader::read_table_header(&mut decoder)
                            .map_err(|e| snapshot_to_storage_error(&e))?;

                    let table_id = TableId::from_u8(table_id_u8).ok_or_else(|| {
                        snapshot_to_storage_error(&SnapshotError::UnknownTableId {
                            table_id: table_id_u8,
                        })
                    })?;

                    for _ in 0..entry_count {
                        let (key, value) = SyncSnapshotReader::read_kv_entry(&mut decoder)
                            .map_err(|e| snapshot_to_storage_error(&e))?;
                        write_txn
                            .insert_raw(table_id, &key, &value)
                            .map_err(|e| to_storage_error(&e))?;
                    }
                }

                // Stream entity entries directly into WriteTransaction
                let entity_count = SyncSnapshotReader::read_u64(&mut decoder)
                    .map_err(|e| snapshot_to_storage_error(&e))?;

                for _ in 0..entity_count {
                    let (key, value) = SyncSnapshotReader::read_kv_entry(&mut decoder)
                        .map_err(|e| snapshot_to_storage_error(&e))?;
                    inferadb_ledger_state::StateLayer::restore_entity(&mut write_txn, &key, &value)
                        .map_err(|e| to_storage_error(&e))?;
                }

                // Persist the AppliedStateCore blob with version sentinel
                let core_data = encode(&core).map_err(|e| to_serde_error(&e))?;
                let mut state_data =
                    Vec::with_capacity(STATE_CORE_VERSION_SENTINEL.len() + core_data.len());
                state_data.extend_from_slice(&STATE_CORE_VERSION_SENTINEL);
                state_data.extend_from_slice(&core_data);
                write_txn
                    .insert::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string(), &state_data)
                    .map_err(|e| to_storage_error(&e))?;

                // Atomic commit — either all writes are visible or none
                write_txn.commit().map_err(|e| to_storage_error(&e))?;

                // Best-effort event restoration (separate database)
                // Stream events directly into events.db WriteTransaction — no staging Vec
                let event_count = SyncSnapshotReader::read_u64(&mut decoder)
                    .map_err(|e| snapshot_to_storage_error(&e))?;

                if event_count > 0
                    && let Some(ew) = event_writer_ref
                {
                    match ew.events_db().write() {
                        Ok(mut event_txn) => {
                            let mut written = 0usize;
                            for _ in 0..event_count {
                                match SyncSnapshotReader::read_event_entry(&mut decoder) {
                                    Ok(entry_bytes) => {
                                        match decode::<inferadb_ledger_types::events::EventEntry>(
                                            &entry_bytes,
                                        ) {
                                            Ok(entry) => {
                                                if inferadb_ledger_state::EventStore::write(
                                                    &mut event_txn,
                                                    &entry,
                                                )
                                                .is_ok()
                                                {
                                                    written += 1;
                                                }
                                            },
                                            Err(e) => {
                                                tracing::warn!(
                                                    error = %e,
                                                    "Failed to decode event entry from snapshot"
                                                );
                                            },
                                        }
                                    },
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to read event entry from snapshot"
                                        );
                                        break;
                                    },
                                }
                            }
                            if let Err(e) = event_txn.commit() {
                                tracing::warn!(
                                    error = %e,
                                    "Failed to commit restored events from snapshot"
                                );
                            } else {
                                tracing::info!(
                                    count = written,
                                    total = event_count,
                                    "Restored apply-phase events from snapshot"
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Failed to open write transaction for event snapshot restore"
                            );
                        },
                    }
                }

                Ok((core, table_count, entity_count, event_count))
            })?;

        // =============================================================
        // Restore in-memory state from the committed tables
        // =============================================================
        let loaded_state = self.load_applied_state_from_db(&core)?;
        *self.applied_state.write() = loaded_state;

        // Restore shard chain tracking
        *self.shard_chain.write() =
            ShardChainState { height: core.shard_height, previous_hash: core.previous_shard_hash };

        tracing::info!(
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            table_count,
            entity_count,
            event_count,
            "Snapshot installed (streaming)"
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        // Check whether any state has been applied by reading from the database.
        // This avoids acquiring the in-memory AppliedState lock.
        {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let has_state = read_txn
                .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
                .map_err(|e| to_storage_error(&e))?
                .is_some();
            if !has_state {
                return Ok(None);
            }
        }

        // Extract org IDs and timestamp for per-org event range scans.
        let (org_ids, last_ts) = extract_snapshot_event_context(&self.db)?;

        // Collect events synchronously — EventsDatabase contains raw pointers
        // (!Send), so the Arc<EventsDatabase> reference must not live across
        // await points.
        let event_entries = {
            let events_db = self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db()));
            let event_config = self.event_writer.as_ref().map(|ew| ew.config().clone());
            collect_snapshot_events(events_db.as_deref(), event_config.as_ref(), &org_ids, last_ts)
        };

        let (file, meta) = write_snapshot_to_file(&self.db, event_entries)
            .await
            .map_err(|e| snapshot_to_storage_error(&e))?;

        Ok(Some(Snapshot { meta, snapshot: Box::new(file) }))
    }
}

// ============================================================================
// Helpers
// ============================================================================

impl RaftLogStore {
    /// Reconstructs the full `AppliedState` from an `AppliedStateCore` blob and
    /// the externalized B-tree tables. Used during snapshot installation to
    /// rebuild the in-memory state after table data has been committed.
    fn load_applied_state_from_db(
        &self,
        core: &AppliedStateCore,
    ) -> Result<AppliedState, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        let mut state = AppliedState {
            last_applied: core.last_applied,
            membership: core.membership.clone(),
            shard_height: core.shard_height,
            previous_shard_hash: core.previous_shard_hash,
            last_applied_timestamp_ns: core.last_applied_timestamp_ns,
            ..Default::default()
        };

        // Load organizations
        let org_iter =
            read_txn.iter::<tables::OrganizationMeta>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in org_iter {
            if let Some(org_id_val) = inferadb_ledger_store::Key::decode(&key_bytes) {
                let org_id = inferadb_ledger_types::OrganizationId::new(org_id_val);
                if let Ok(meta) = decode::<super::types::OrganizationMeta>(&value_bytes) {
                    state.id_to_slug.insert(org_id, meta.slug);
                    state.slug_index.insert(meta.slug, org_id);
                    state.organization_storage_bytes.insert(org_id, meta.storage_bytes);
                    state.organizations.insert(org_id, meta);
                }
            }
        }

        // Load vaults
        let vault_iter = read_txn.iter::<tables::VaultMeta>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in vault_iter {
            if let Some(vault_id_val) = <i64 as inferadb_ledger_store::Key>::decode(&key_bytes) {
                let vault_id = inferadb_ledger_types::VaultId::new(vault_id_val);
                if let Ok(meta) = decode::<super::types::VaultMeta>(&value_bytes) {
                    state.vault_id_to_slug.insert(vault_id, meta.slug);
                    state.vault_slug_index.insert(meta.slug, vault_id);
                    state.vaults.insert((meta.organization_id, vault_id), meta);
                }
            }
        }

        // Load sequences
        let seq_iter = read_txn.iter::<tables::Sequences>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in seq_iter {
            if let (Some(name), Some(val)) = (
                <String as inferadb_ledger_store::Key>::decode(&key_bytes),
                <u64 as inferadb_ledger_store::Value>::decode(&value_bytes),
            ) {
                match name.as_str() {
                    "organization" => {
                        state.sequences.organization =
                            inferadb_ledger_types::OrganizationId::new(val as i64);
                    },
                    "vault" => {
                        state.sequences.vault = inferadb_ledger_types::VaultId::new(val as i64);
                    },
                    "user" => state.sequences.user = val as i64,
                    "user_email" => state.sequences.user_email = val as i64,
                    "email_verify" => state.sequences.email_verify = val as i64,
                    _ => {},
                }
            }
        }

        // Load vault heights (values are postcard-encoded u64, not raw BE)
        let height_iter =
            read_txn.iter::<tables::VaultHeights>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in height_iter {
            if key_bytes.len() == 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                if let Ok(height) = decode::<u64>(&value_bytes) {
                    state.vault_heights.insert((org_id, vault_id), height);
                }
            }
        }

        // Load vault hashes
        let hash_iter = read_txn.iter::<tables::VaultHashes>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in hash_iter {
            if key_bytes.len() == 16 && value_bytes.len() == 32 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                let hash: inferadb_ledger_types::Hash =
                    value_bytes.as_slice().try_into().unwrap_or([0; 32]);
                state.previous_vault_hashes.insert((org_id, vault_id), hash);
            }
        }

        // Load vault health
        let health_iter =
            read_txn.iter::<tables::VaultHealth>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in health_iter {
            if key_bytes.len() == 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                if let Ok(status) = decode::<super::types::VaultHealthStatus>(&value_bytes) {
                    state.vault_health.insert((org_id, vault_id), status);
                }
            }
        }

        // Load client sequences
        let cs_iter =
            read_txn.iter::<tables::ClientSequences>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in cs_iter {
            if key_bytes.len() >= 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                let client_id = String::from_utf8_lossy(&key_bytes[16..]).to_string();
                if let Ok(entry) = decode::<super::types::ClientSequenceEntry>(&value_bytes) {
                    state.client_sequences.insert((org_id, vault_id, client_id), entry);
                }
            }
        }

        // Load slug indexes
        let slug_iter =
            read_txn.iter::<tables::OrganizationSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in slug_iter {
            if let (Some(slug_val), Some(org_id_val)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<i64>(&value_bytes).ok(),
            ) {
                let slug = inferadb_ledger_types::OrganizationSlug::new(slug_val);
                let org_id = inferadb_ledger_types::OrganizationId::new(org_id_val);
                state.slug_index.insert(slug, org_id);
                state.id_to_slug.insert(org_id, slug);
            }
        }

        let vault_slug_iter =
            read_txn.iter::<tables::VaultSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in vault_slug_iter {
            if let (Some(slug_val), Some(vault_id_val)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<i64>(&value_bytes).ok(),
            ) {
                let slug = inferadb_ledger_types::VaultSlug::new(slug_val);
                let vault_id = inferadb_ledger_types::VaultId::new(vault_id_val);
                state.vault_slug_index.insert(slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, slug);
            }
        }

        Ok(state)
    }
}

// ============================================================================
// Error Helpers
// ============================================================================

pub(super) fn to_storage_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

pub(super) fn to_serde_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

fn snapshot_to_storage_error(e: &SnapshotError) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Snapshot(None),
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

fn to_io_storage_error(e: std::io::Error, context: &str) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Snapshot(None),
        openraft::ErrorVerb::Read,
        std::io::Error::other(format!("{context}: {e}")),
    )
}
