//! Events storage layer for the audit event system.
//!
//! Provides [`EventStore`] for low-level CRUD on the `Events` B+ tree table,
//! and [`EventsDatabase`] as a managed wrapper around the dedicated `events.db`
//! database file. The Events table lives in a separate database from `state.db`
//! to avoid write lock contention between handler-phase event writes and
//! Raft apply-phase state mutations.

use std::{path::Path, sync::Arc};

use inferadb_ledger_store::{
    Database, FileBackend, InMemoryBackend, ReadTransaction, StorageBackend, Table, TableId,
    WriteTransaction,
};
use inferadb_ledger_types::{
    CodecError, OrganizationId,
    events::{EventEntry, EventMeta},
};
use snafu::{ResultExt, Snafu};

use crate::events_keys::{
    encode_event_index_key, encode_event_index_value, encode_event_key, org_prefix,
    primary_key_from_index_value,
};

/// Events table: stores audit event entries in a dedicated `events.db` database.
///
/// Uses `TableId::Entities` (value 0) as the table slot — safe because
/// the Events table is the sole occupant of its own [`Database`] instance.
pub struct Events;

impl Table for Events {
    const ID: TableId = TableId::Entities;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Secondary index for O(log n) event lookups by ID.
///
/// Maps `(org_id, event_id)` → `(timestamp_ns, event_hash)`. Given these
/// components, the full 24-byte primary key can be reconstructed for a
/// direct `Events` table lookup. Two B+ tree lookups total: index + primary.
///
/// Uses `TableId::Relationships` (value 1) — unused in `events.db`.
pub struct EventIndex;

impl Table for EventIndex {
    const ID: TableId = TableId::Relationships;
    type KeyType = Vec<u8>;
    type ValueType = Vec<u8>;
}

/// Errors returned by [`EventStore`] operations.
#[derive(Debug, Snafu)]
pub enum EventStoreError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        /// The underlying store error.
        source: inferadb_ledger_store::Error,
        /// Source code location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Serialization or deserialization failed.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The codec error.
        source: CodecError,
        /// Source code location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for event store operations.
pub type Result<T> = std::result::Result<T, EventStoreError>;

/// Low-level event storage operations on raw transactions.
///
/// Stateless — all operations take a transaction reference. Follows the
/// [`EntityStore`](crate::EntityStore) pattern.
pub struct EventStore;

impl EventStore {
    /// Writes an event entry to the Events table.
    ///
    /// The key is derived from the entry's `organization_id`, `timestamp`,
    /// and `event_id` fields.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Codec` if serialization fails.
    /// Returns `EventStoreError::Storage` if the write transaction fails.
    pub fn write<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        entry: &EventEntry,
    ) -> Result<()> {
        let timestamp_ns = entry.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
        let key = encode_event_key(entry.organization_id, timestamp_ns, &entry.event_id);
        let value = inferadb_ledger_types::encode(entry).context(CodecSnafu)?;
        txn.insert::<Events>(&key, &value).context(StorageSnafu)?;

        // Maintain secondary index: (org_id, event_id) → (timestamp_ns, event_hash)
        let idx_key = encode_event_index_key(entry.organization_id, &entry.event_id);
        let idx_val = encode_event_index_value(timestamp_ns, &entry.event_id);
        txn.insert::<EventIndex>(&idx_key, &idx_val).context(StorageSnafu)?;

        Ok(())
    }

    /// Retrieves a single event by its components.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if the read transaction fails.
    /// Returns `EventStoreError::Codec` if deserialization fails.
    pub fn get<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        org_id: OrganizationId,
        timestamp_ns: u64,
        event_id: &[u8; 16],
    ) -> Result<Option<EventEntry>> {
        let key = encode_event_key(org_id, timestamp_ns, event_id);
        match txn.get::<Events>(&key).context(StorageSnafu)? {
            Some(data) => {
                let entry: EventEntry = inferadb_ledger_types::decode(&data).context(CodecSnafu)?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Retrieves a single event by organization ID and event ID using the
    /// secondary index for O(log n) lookup.
    ///
    /// Performs two B+ tree lookups:
    /// 1. `EventIndex`: `(org_id, event_id)` → `(timestamp_ns, event_hash)`
    /// 2. `Events`: reconstructed primary key → `EventEntry`
    ///
    /// Returns `None` if the index entry is missing or if the primary entry
    /// has been GC'd (orphaned index entry).
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if either read fails.
    /// Returns `EventStoreError::Codec` if deserialization fails.
    pub fn get_by_id<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        org_id: OrganizationId,
        event_id: &[u8; 16],
    ) -> Result<Option<EventEntry>> {
        // Step 1: Index lookup
        let idx_key = encode_event_index_key(org_id, event_id);
        let idx_val = match txn.get::<EventIndex>(&idx_key).context(StorageSnafu)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Step 2: Reconstruct primary key and fetch
        let primary_key = primary_key_from_index_value(org_id, &idx_val);
        match txn.get::<Events>(&primary_key).context(StorageSnafu)? {
            Some(data) => {
                let entry: EventEntry = inferadb_ledger_types::decode(&data).context(CodecSnafu)?;
                Ok(Some(entry))
            },
            // Orphaned index entry — primary was GC'd
            None => Ok(None),
        }
    }

    /// Lists events for an organization within a time range, with cursor-based pagination.
    ///
    /// Returns entries in chronological order (ascending timestamp). The optional
    /// `after_key` parameter is an opaque 24-byte resume cursor from a previous call.
    ///
    /// Returns `(entries, next_cursor)` where `next_cursor` is `Some` if more
    /// results are available.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if the read transaction or iterator fails.
    /// Returns `EventStoreError::Codec` if deserialization of any entry fails.
    pub fn list<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        org_id: OrganizationId,
        start_ns: u64,
        end_ns: u64,
        limit: usize,
        after_key: Option<&[u8]>,
    ) -> Result<(Vec<EventEntry>, Option<Vec<u8>>)> {
        let start_key = match after_key {
            Some(cursor) => {
                // Resume after the cursor key — increment last byte to get exclusive start
                let mut resume = cursor.to_vec();
                // Increment the key to skip the cursor entry itself
                increment_key(&mut resume);
                resume
            },
            None => {
                let prefix = crate::events_keys::org_time_prefix(org_id, start_ns);
                prefix.to_vec()
            },
        };

        let end_key = crate::events_keys::org_time_prefix(org_id, end_ns);
        let org_bytes = org_prefix(org_id);

        // Fetch limit + 1 to detect if more results exist
        let fetch_limit = limit.saturating_add(1);

        let iter =
            txn.range::<Events>(Some(&start_key), Some(&end_key.to_vec())).context(StorageSnafu)?;

        let mut entries = Vec::with_capacity(limit);
        let mut last_key: Option<Vec<u8>> = None;

        for (key_bytes, value_bytes) in iter {
            // Ensure we're still within the target org
            if key_bytes.len() < 8 || key_bytes[..8] != org_bytes[..] {
                break;
            }

            if entries.len() >= fetch_limit {
                break;
            }

            let entry: EventEntry =
                inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;
            last_key = Some(key_bytes);
            entries.push(entry);
        }

        if entries.len() > limit {
            // More results available — return the cursor for the last included entry
            entries.truncate(limit);
            let cursor = last_key.map(|_| {
                // The cursor is the key of the last entry we're returning
                let last_entry = &entries[limit - 1];
                let ts = last_entry.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
                encode_event_key(last_entry.organization_id, ts, &last_entry.event_id)
            });
            Ok((entries, cursor))
        } else {
            Ok((entries, None))
        }
    }

    /// Deletes expired event entries using the collect-then-delete pattern.
    ///
    /// Scans all entries using thin deserialization ([`EventMeta`]) to check
    /// `expires_at`. Expired entries are fully deserialized to extract
    /// `event_id` and `organization_id` for secondary index cleanup.
    ///
    /// Returns the number of entries deleted.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if read/write operations fail.
    /// Returns `EventStoreError::Codec` if deserialization fails.
    pub fn delete_expired<B: StorageBackend>(
        read_txn: &ReadTransaction<'_, B>,
        write_txn: &mut WriteTransaction<'_, B>,
        now_unix: u64,
        max_batch: usize,
    ) -> Result<usize> {
        let mut expired: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        let iter = read_txn.iter::<Events>().context(StorageSnafu)?;

        for (key_bytes, value_bytes) in iter {
            if expired.len() >= max_batch {
                break;
            }

            // Thin deserialization — reads emission (field 1) + expires_at (field 2)
            let meta: EventMeta =
                inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;

            if meta.expires_at > 0 && meta.expires_at < now_unix {
                expired.push((key_bytes, value_bytes));
            }
        }

        let deleted = expired.len();
        for (key, value_bytes) in expired {
            // Full deserialization to get event_id + organization_id for index cleanup
            let entry: EventEntry =
                inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;

            write_txn.delete::<Events>(&key).context(StorageSnafu)?;

            // Remove secondary index entry
            let idx_key = encode_event_index_key(entry.organization_id, &entry.event_id);
            write_txn.delete::<EventIndex>(&idx_key).context(StorageSnafu)?;
        }

        Ok(deleted)
    }

    /// Counts entries for a specific organization.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if the iterator or read transaction fails.
    pub fn count<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        org_id: OrganizationId,
    ) -> Result<u64> {
        let prefix = org_prefix(org_id);
        let mut count = 0u64;

        let iter = txn.iter::<Events>().context(StorageSnafu)?;

        for (key_bytes, _) in iter {
            if key_bytes.len() < 8 {
                continue;
            }
            if key_bytes[..8] < prefix[..] {
                continue;
            }
            if key_bytes[..8] > prefix[..] {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Scans all apply-phase events for inclusion in Raft snapshots.
    ///
    /// Uses thin deserialization ([`EmissionMeta`]) to read only the `emission`
    /// discriminant (~1–2 bytes) for each event, skipping full deserialization
    /// of handler-phase events (~55% of total). Apply-phase events are then
    /// fully deserialized, sorted newest-first, and truncated to `max_entries`.
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if the iterator fails.
    /// Returns `EventStoreError::Codec` if deserialization fails.
    pub fn scan_apply_phase<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        max_entries: usize,
    ) -> Result<Vec<inferadb_ledger_types::events::EventEntry>> {
        use inferadb_ledger_types::events::{EmissionMeta, EventEmission};

        let iter = txn.iter::<Events>().context(StorageSnafu)?;

        let mut entries: Vec<inferadb_ledger_types::events::EventEntry> = Vec::new();

        for (_key_bytes, value_bytes) in iter {
            // Thin deserialization — reads only the emission discriminant (~1–2 bytes)
            let meta: EmissionMeta =
                inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;

            if matches!(meta.emission, EventEmission::ApplyPhase) {
                // Full deserialization only for apply-phase events
                let entry: inferadb_ledger_types::events::EventEntry =
                    inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;
                entries.push(entry);
            }
        }

        // Sort by timestamp descending (most recent first) to keep newest entries
        entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        // Keep only the most recent `max_entries`
        entries.truncate(max_entries);

        Ok(entries)
    }

    /// Scans apply-phase events using per-organization range scans with a
    /// timestamp cutoff.
    ///
    /// For each organization, performs a B-tree range scan starting at the
    /// cutoff timestamp (inclusive) through the organization's maximum key.
    /// This avoids loading all events into memory — only events newer than
    /// the cutoff are read.
    ///
    /// Returns serialized event entries (postcard-encoded `EventEntry`).
    /// Total results are capped at `max_entries`.
    ///
    /// # Arguments
    ///
    /// * `txn` — Read transaction on the events database
    /// * `org_ids` — Organization IDs to scan (determines per-org range bounds)
    /// * `cutoff_timestamp_ns` — Minimum timestamp (nanoseconds since epoch); events older than
    ///   this are excluded. Use `0` to include all events.
    /// * `max_entries` — Global cap on total returned events
    ///
    /// # Errors
    ///
    /// Returns `EventStoreError::Storage` if any iterator fails.
    /// Returns `EventStoreError::Codec` if deserialization fails.
    pub fn scan_apply_phase_ranged<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        org_ids: &[OrganizationId],
        cutoff_timestamp_ns: u64,
        max_entries: usize,
    ) -> Result<Vec<Vec<u8>>> {
        use inferadb_ledger_types::events::{EmissionMeta, EventEmission};

        let mut result = Vec::new();

        for &org_id in org_ids {
            if result.len() >= max_entries {
                break;
            }

            // Range: [org_id ++ cutoff_ts ++ 0x00..00, org_id ++ u64::MAX ++ u64::MAX]
            let start = crate::events_keys::org_time_prefix(org_id, cutoff_timestamp_ns).to_vec();
            let end = {
                let mut e = crate::events_keys::org_time_prefix(org_id, u64::MAX).to_vec();
                // Extend to full 24-byte key with max hash for inclusive upper bound
                e.extend_from_slice(&u64::MAX.to_be_bytes());
                e
            };

            let iter = txn.range::<Events>(Some(&start), Some(&end)).context(StorageSnafu)?;

            let org_bytes = org_prefix(org_id);

            for (key_bytes, value_bytes) in iter {
                if result.len() >= max_entries {
                    break;
                }

                // Ensure we're still within the target org
                if key_bytes.len() < 8 || key_bytes[..8] != org_bytes[..] {
                    break;
                }

                // Thin deserialization — reads only the emission discriminant
                let meta: EmissionMeta =
                    inferadb_ledger_types::decode(&value_bytes).context(CodecSnafu)?;

                if matches!(meta.emission, EventEmission::ApplyPhase) {
                    result.push(value_bytes);
                }
            }
        }

        Ok(result)
    }
}

/// Increments a byte slice as a big-endian integer (for exclusive cursor starts).
fn increment_key(key: &mut Vec<u8>) {
    for byte in key.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return;
        }
        *byte = 0;
    }
    // Overflow — all bytes were 0xFF. Insert a leading 0x01.
    key.insert(0, 1);
}

// ============================================================================
// EventsDatabase wrapper
// ============================================================================

/// Errors returned by [`EventsDatabase`] operations.
#[derive(Debug, Snafu)]
pub enum EventsDatabaseError {
    /// Failed to open or create the events database file.
    #[snafu(display("Failed to open events database at {path}: {source}"))]
    Open {
        /// Filesystem path that failed to open.
        path: String,
        /// The underlying store error.
        source: inferadb_ledger_store::Error,
    },
}

/// Managed wrapper around the dedicated `events.db` database.
///
/// Wraps a [`Database`] instance with convenience methods for read/write
/// transactions. The events database is separate from `state.db` — no write
/// lock contention with [`StateLayer`](crate::StateLayer).
///
/// Generic over [`StorageBackend`] to support both file-backed (production)
/// and in-memory (testing) backends.
pub struct EventsDatabase<B: StorageBackend> {
    db: Arc<Database<B>>,
}

impl EventsDatabase<FileBackend> {
    /// Opens or creates the events database at `{data_dir}/events.db`.
    ///
    /// # Errors
    ///
    /// Returns `EventsDatabaseError::Open` if the database cannot be opened
    /// or created.
    pub fn open(data_dir: &Path) -> std::result::Result<Self, EventsDatabaseError> {
        let path = data_dir.join("events.db");
        let db =
            if path.exists() { Database::open(&path) } else { Database::create(&path) }.map_err(
                |e| EventsDatabaseError::Open { path: path.display().to_string(), source: e },
            )?;

        Ok(Self { db: Arc::new(db) })
    }
}

impl EventsDatabase<InMemoryBackend> {
    /// Creates an in-memory events database for testing.
    ///
    /// # Errors
    ///
    /// Returns `EventsDatabaseError::Open` if the in-memory database cannot
    /// be created.
    pub fn open_in_memory() -> std::result::Result<Self, EventsDatabaseError> {
        let db = Database::open_in_memory()
            .map_err(|e| EventsDatabaseError::Open { path: ":memory:".to_string(), source: e })?;

        Ok(Self { db: Arc::new(db) })
    }
}

impl<B: StorageBackend> EventsDatabase<B> {
    /// Begins a read transaction on the events database.
    ///
    /// # Errors
    ///
    /// Returns the store error if the read transaction cannot be started.
    pub fn read(&self) -> inferadb_ledger_store::Result<ReadTransaction<'_, B>> {
        self.db.read()
    }

    /// Begins a write transaction on the events database.
    ///
    /// # Errors
    ///
    /// Returns the store error if the write transaction cannot be started.
    pub fn write(&self) -> inferadb_ledger_store::Result<WriteTransaction<'_, B>> {
        self.db.write()
    }

    /// Returns a shared reference to the underlying database.
    pub fn db(&self) -> &Arc<Database<B>> {
        &self.db
    }
}

impl<B: StorageBackend> Clone for EventsDatabase<B> {
    fn clone(&self) -> Self {
        Self { db: Arc::clone(&self.db) }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use inferadb_ledger_types::events::{EventAction, EventEmission, EventOutcome, EventScope};

    use super::*;

    fn make_entry(
        org_id: i64,
        event_id: [u8; 16],
        timestamp_secs: i64,
        expires_at: u64,
    ) -> EventEntry {
        EventEntry {
            expires_at,
            event_id,
            source_service: "ledger".to_string(),
            event_type: "ledger.test.event".to_string(),
            timestamp: Utc.timestamp_opt(timestamp_secs, 0).unwrap(),
            scope: EventScope::Organization,
            action: EventAction::WriteCommitted,
            emission: EventEmission::ApplyPhase,
            principal: "test-user".to_string(),
            organization_id: OrganizationId::new(org_id),
            organization_slug: None,
            vault_slug: None,
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            block_height: None,
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    #[test]
    fn write_and_get_roundtrip() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let entry = make_entry(1, [1u8; 16], 1_700_000_000, 0);
        let ts_ns = entry.timestamp.timestamp_nanos_opt().unwrap() as u64;

        {
            let mut txn = events_db.write().expect("write txn");
            EventStore::write(&mut txn, &entry).expect("write event");
            txn.commit().expect("commit");
        }

        {
            let txn = events_db.read().expect("read txn");
            let got = EventStore::get(&txn, OrganizationId::new(1), ts_ns, &[1u8; 16])
                .expect("get")
                .expect("should exist");
            assert_eq!(got.event_id, entry.event_id);
            assert_eq!(got.source_service, "ledger");
            assert_eq!(got.principal, "test-user");
        }
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let txn = events_db.read().expect("read txn");
        let result = EventStore::get(&txn, OrganizationId::new(1), 0, &[0u8; 16]).expect("get");
        assert!(result.is_none());
    }

    #[test]
    fn list_chronological_order() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let org_id = OrganizationId::new(1);

        // Write entries with deliberately out-of-order timestamps
        {
            let mut txn = events_db.write().expect("write txn");
            let timestamps = [1_700_000_003i64, 1_700_000_001, 1_700_000_002];
            for (i, &ts) in timestamps.iter().enumerate() {
                let mut entry = make_entry(1, [0u8; 16], ts, 0);
                entry.event_id = [i as u8; 16];
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        {
            let txn = events_db.read().expect("read txn");
            let (entries, cursor) =
                EventStore::list(&txn, org_id, 0, u64::MAX, 10, None).expect("list");

            assert_eq!(entries.len(), 3);
            assert!(cursor.is_none(), "no more results");

            // Verify ascending timestamp order
            assert!(entries[0].timestamp <= entries[1].timestamp);
            assert!(entries[1].timestamp <= entries[2].timestamp);
        }
    }

    #[test]
    fn list_with_pagination() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let org_id = OrganizationId::new(1);

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..5u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        // Page 1: first 2 entries
        let txn = events_db.read().expect("read txn");
        let (page1, cursor1) =
            EventStore::list(&txn, org_id, 0, u64::MAX, 2, None).expect("list page 1");
        assert_eq!(page1.len(), 2);
        assert!(cursor1.is_some(), "should have more");

        // Page 2: next 2 entries
        let (page2, cursor2) = EventStore::list(&txn, org_id, 0, u64::MAX, 2, cursor1.as_deref())
            .expect("list page 2");
        assert_eq!(page2.len(), 2);
        assert!(cursor2.is_some(), "should have more");

        // Page 3: last entry
        let (page3, cursor3) = EventStore::list(&txn, org_id, 0, u64::MAX, 2, cursor2.as_deref())
            .expect("list page 3");
        assert_eq!(page3.len(), 1);
        assert!(cursor3.is_none(), "no more");

        // Verify no duplicates across pages
        let all_ids: Vec<[u8; 16]> =
            page1.iter().chain(page2.iter()).chain(page3.iter()).map(|e| e.event_id).collect();
        assert_eq!(all_ids.len(), 5);
        for (i, id) in all_ids.iter().enumerate() {
            for (j, other) in all_ids.iter().enumerate() {
                if i != j {
                    assert_ne!(id, other, "duplicate event ID at positions {i} and {j}");
                }
            }
        }
    }

    #[test]
    fn organization_isolation() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            // 10 events for org A
            for i in 0..10u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write org A");
            }
            // 10 events for org B
            for i in 0..10u8 {
                let entry = make_entry(2, [100 + i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write org B");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");

        // Org A query should return zero org B events
        let (org_a_events, _) =
            EventStore::list(&txn, OrganizationId::new(1), 0, u64::MAX, 100, None)
                .expect("list org A");
        assert_eq!(org_a_events.len(), 10);
        for e in &org_a_events {
            assert_eq!(e.organization_id, OrganizationId::new(1));
        }

        // Org B query should return zero org A events
        let (org_b_events, _) =
            EventStore::list(&txn, OrganizationId::new(2), 0, u64::MAX, 100, None)
                .expect("list org B");
        assert_eq!(org_b_events.len(), 10);
        for e in &org_b_events {
            assert_eq!(e.organization_id, OrganizationId::new(2));
        }
    }

    #[test]
    fn system_events_isolated() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let system_org = OrganizationId::new(0);
        let regular_org = OrganizationId::new(42);

        {
            let mut txn = events_db.write().expect("write txn");
            // System event
            let mut sys_entry = make_entry(0, [1u8; 16], 1_700_000_000, 0);
            sys_entry.scope = EventScope::System;
            sys_entry.action = EventAction::OrganizationCreated;
            EventStore::write(&mut txn, &sys_entry).expect("write system");

            // Regular org event
            let org_entry = make_entry(42, [2u8; 16], 1_700_000_000, 0);
            EventStore::write(&mut txn, &org_entry).expect("write org");
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");

        let (system_events, _) =
            EventStore::list(&txn, system_org, 0, u64::MAX, 100, None).expect("list system");
        assert_eq!(system_events.len(), 1);
        assert_eq!(system_events[0].organization_id, system_org);

        let (org_events, _) =
            EventStore::list(&txn, regular_org, 0, u64::MAX, 100, None).expect("list org");
        assert_eq!(org_events.len(), 1);
        assert_eq!(org_events[0].organization_id, regular_org);
    }

    #[test]
    fn empty_org_scan_returns_empty() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let txn = events_db.read().expect("read txn");
        let (entries, cursor) =
            EventStore::list(&txn, OrganizationId::new(999), 0, u64::MAX, 100, None).expect("list");
        assert!(entries.is_empty());
        assert!(cursor.is_none());
    }

    #[test]
    fn count_consistent_with_list() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let org_id = OrganizationId::new(5);

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..7u8 {
                let entry = make_entry(5, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, org_id).expect("count");
        let (entries, _) = EventStore::list(&txn, org_id, 0, u64::MAX, 100, None).expect("list");
        assert_eq!(count, entries.len() as u64);
    }

    #[test]
    fn count_after_deletion() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let org_id = OrganizationId::new(1);

        {
            let mut txn = events_db.write().expect("write txn");
            // Write 3 entries, 2 expired
            let e1 = make_entry(1, [1u8; 16], 1_700_000_000, 100); // expired
            let e2 = make_entry(1, [2u8; 16], 1_700_000_001, 200); // expired
            let e3 = make_entry(1, [3u8; 16], 1_700_000_002, 0); // no expiry
            EventStore::write(&mut txn, &e1).expect("write");
            EventStore::write(&mut txn, &e2).expect("write");
            EventStore::write(&mut txn, &e3).expect("write");
            txn.commit().expect("commit");
        }

        // Delete expired (now_unix > both expires_at values)
        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 300, 100).expect("gc");
            assert_eq!(deleted, 2);
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, org_id).expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_expired_respects_batch_limit() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..10u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 50);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        // Delete with batch limit of 3
        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 100, 3).expect("gc");
            assert_eq!(deleted, 3, "should respect batch limit");
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 7, "7 entries remain after deleting 3");
    }

    #[test]
    fn delete_expired_preserves_non_expired() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            let expired = make_entry(1, [1u8; 16], 1_700_000_000, 50); // expired
            let future = make_entry(1, [2u8; 16], 1_700_000_001, 999); // not yet expired
            let no_expiry = make_entry(1, [3u8; 16], 1_700_000_002, 0); // no expiry
            EventStore::write(&mut txn, &expired).expect("write");
            EventStore::write(&mut txn, &future).expect("write");
            EventStore::write(&mut txn, &no_expiry).expect("write");
            txn.commit().expect("commit");
        }

        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 100, 100).expect("gc");
            assert_eq!(deleted, 1, "only the expired entry");
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 2, "future + no_expiry remain");
    }

    #[test]
    fn delete_expired_zero_expires_at_never_deleted() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..5u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, u64::MAX, 100).expect("gc");
            assert_eq!(deleted, 0);
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 5);
    }

    #[test]
    fn mixed_batch_gc() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");

            // 3 expired, 2 future, 2 no-expiry
            let entries = vec![
                make_entry(1, [1u8; 16], 1_700_000_001, 10),  // expired
                make_entry(1, [2u8; 16], 1_700_000_002, 20),  // expired
                make_entry(1, [3u8; 16], 1_700_000_003, 30),  // expired
                make_entry(1, [4u8; 16], 1_700_000_004, 500), // future
                make_entry(1, [5u8; 16], 1_700_000_005, 600), // future
                make_entry(1, [6u8; 16], 1_700_000_006, 0),   // no expiry
                make_entry(1, [7u8; 16], 1_700_000_007, 0),   // no expiry
            ];
            for entry in &entries {
                EventStore::write(&mut txn, entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 100, 100).expect("gc");
            assert_eq!(deleted, 3);
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 4);
    }

    #[test]
    fn scan_apply_phase_filters_handler_events() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");

            // Apply-phase event
            let apply_entry = make_entry(1, [1u8; 16], 1_700_000_000, 0);
            EventStore::write(&mut txn, &apply_entry).expect("write apply");

            // Handler-phase event
            let mut handler_entry = make_entry(1, [2u8; 16], 1_700_000_001, 0);
            handler_entry.emission = EventEmission::HandlerPhase { node_id: 1 };
            EventStore::write(&mut txn, &handler_entry).expect("write handler");

            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 100).expect("scan");

        assert_eq!(results.len(), 1, "only apply-phase event should be included");
        assert_eq!(results[0].event_id, [1u8; 16]);
        assert!(matches!(results[0].emission, EventEmission::ApplyPhase));
    }

    #[test]
    fn scan_apply_phase_respects_max_entries() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..10u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 3).expect("scan");

        assert_eq!(results.len(), 3, "should truncate to max_entries");
        // Should keep the 3 newest (highest timestamps)
        assert!(results[0].timestamp >= results[1].timestamp, "sorted newest first");
        assert!(results[1].timestamp >= results[2].timestamp, "sorted newest first");
    }

    #[test]
    fn scan_apply_phase_empty_db() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 100).expect("scan");
        assert!(results.is_empty());
    }

    #[test]
    fn scan_apply_phase_cross_org() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            // Events across different orgs
            let e1 = make_entry(1, [1u8; 16], 1_700_000_000, 0);
            let e2 = make_entry(2, [2u8; 16], 1_700_000_001, 0);
            let e3 = make_entry(3, [3u8; 16], 1_700_000_002, 0);
            EventStore::write(&mut txn, &e1).expect("write");
            EventStore::write(&mut txn, &e2).expect("write");
            EventStore::write(&mut txn, &e3).expect("write");
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 100).expect("scan");

        assert_eq!(results.len(), 3, "should scan across all organizations");
        // All three orgs should be represented
        let org_ids: Vec<_> = results.iter().map(|e| e.organization_id).collect();
        assert!(org_ids.contains(&OrganizationId::new(1)));
        assert!(org_ids.contains(&OrganizationId::new(2)));
        assert!(org_ids.contains(&OrganizationId::new(3)));
    }

    #[test]
    fn events_database_clone_shares_state() {
        let db1 = EventsDatabase::open_in_memory().expect("open");
        let db2 = db1.clone();

        // Write via db1
        {
            let mut txn = db1.write().expect("write txn");
            let entry = make_entry(1, [1u8; 16], 1_700_000_000, 0);
            EventStore::write(&mut txn, &entry).expect("write");
            txn.commit().expect("commit");
        }

        // Read via db2
        {
            let txn = db2.read().expect("read txn");
            let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
            assert_eq!(count, 1, "clone should share same database");
        }
    }

    #[test]
    fn list_time_range_filtering() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let org_id = OrganizationId::new(1);

        {
            let mut txn = events_db.write().expect("write txn");
            // Events at t=1000s, t=2000s, t=3000s
            for (i, ts) in [1000i64, 2000, 3000].iter().enumerate() {
                let entry = make_entry(1, [i as u8; 16], *ts, 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");

        // Full range
        let (all, _) = EventStore::list(&txn, org_id, 0, u64::MAX, 100, None).expect("list all");
        assert_eq!(all.len(), 3);

        // Only middle entry (start_ns inclusive of 2000s, end_ns exclusive of 3000s)
        let start_ns = 2000u64 * 1_000_000_000;
        let end_ns = 3000u64 * 1_000_000_000;
        let (mid, _) =
            EventStore::list(&txn, org_id, start_ns, end_ns, 100, None).expect("list mid");
        assert_eq!(mid.len(), 1);
        assert_eq!(mid[0].timestamp.timestamp(), 2000);
    }

    /// Verifies GC (`delete_expired`) works correctly after the `EventEntry` field
    /// reorder. `EventMeta` now reads `emission` (field 1) + `expires_at` (field 2),
    /// so the GC must still correctly identify expired entries.
    #[test]
    fn delete_expired_works_after_field_reorder() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");

            // Expired apply-phase event
            let e1 = make_entry(1, [1u8; 16], 1_700_000_000, 50);
            EventStore::write(&mut txn, &e1).expect("write");

            // Expired handler-phase event
            let mut e2 = make_entry(1, [2u8; 16], 1_700_000_001, 50);
            e2.emission = EventEmission::HandlerPhase { node_id: 1 };
            EventStore::write(&mut txn, &e2).expect("write");

            // Non-expired event
            let e3 = make_entry(1, [3u8; 16], 1_700_000_002, 0);
            EventStore::write(&mut txn, &e3).expect("write");

            txn.commit().expect("commit");
        }

        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 100, 100).expect("gc");
            assert_eq!(deleted, 2, "both expired entries should be deleted");
            write_txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 1, "only non-expired entry remains");
    }

    /// 50 apply-phase + 50 handler-phase events: scan returns exactly 50 apply-phase.
    #[test]
    fn scan_apply_phase_50_apply_50_handler() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");

            for i in 0..50u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write apply");
            }

            for i in 50..100u8 {
                let mut entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                entry.emission = EventEmission::HandlerPhase { node_id: 1 };
                EventStore::write(&mut txn, &entry).expect("write handler");
            }

            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 1000).expect("scan");

        assert_eq!(results.len(), 50, "exactly 50 apply-phase events");
        for entry in &results {
            assert!(
                matches!(entry.emission, EventEmission::ApplyPhase),
                "all entries must be apply-phase"
            );
        }
    }

    /// scan_apply_phase with max_entries truncation returns newest N sorted desc.
    #[test]
    fn scan_apply_phase_truncates_to_newest() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..20u8 {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let results = EventStore::scan_apply_phase(&txn, 5).expect("scan");

        assert_eq!(results.len(), 5, "truncated to max_entries");

        // Newest first
        for window in results.windows(2) {
            assert!(window[0].timestamp >= window[1].timestamp, "must be sorted newest-first");
        }

        // The 5 newest timestamps correspond to i=15..19
        let min_ts = chrono::TimeZone::timestamp_opt(&Utc, 1_700_000_015, 0).unwrap().timestamp();
        for entry in &results {
            assert!(
                entry.timestamp.timestamp() >= min_ts,
                "truncated entries should be the 5 newest"
            );
        }
    }

    // ====================================================================
    // EventIndex / get_by_id tests
    // ====================================================================

    #[test]
    fn get_by_id_returns_written_event() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let entry = make_entry(1, [10u8; 16], 1_700_000_000, 0);

        {
            let mut txn = events_db.write().expect("write txn");
            EventStore::write(&mut txn, &entry).expect("write");
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let found = EventStore::get_by_id(&txn, OrganizationId::new(1), &[10u8; 16])
            .expect("get_by_id")
            .expect("should exist");

        assert_eq!(found.event_id, [10u8; 16]);
        assert_eq!(found.organization_id, OrganizationId::new(1));
        assert_eq!(found.source_service, "ledger");
    }

    #[test]
    fn get_by_id_nonexistent_returns_none() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let txn = events_db.read().expect("read txn");
        let result =
            EventStore::get_by_id(&txn, OrganizationId::new(1), &[99u8; 16]).expect("get_by_id");
        assert!(result.is_none());
    }

    #[test]
    fn get_by_id_wrong_org_returns_none() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let entry = make_entry(1, [10u8; 16], 1_700_000_000, 0);

        {
            let mut txn = events_db.write().expect("write txn");
            EventStore::write(&mut txn, &entry).expect("write");
            txn.commit().expect("commit");
        }

        // Look up with wrong org
        let txn = events_db.read().expect("read txn");
        let result =
            EventStore::get_by_id(&txn, OrganizationId::new(999), &[10u8; 16]).expect("get_by_id");
        assert!(result.is_none(), "different org should not find the event");
    }

    #[test]
    fn get_by_id_finds_all_n_events() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let n = 50u8;

        {
            let mut txn = events_db.write().expect("write txn");
            for i in 0..n {
                let entry = make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0);
                EventStore::write(&mut txn, &entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        for i in 0..n {
            let found = EventStore::get_by_id(&txn, OrganizationId::new(1), &[i; 16])
                .expect("get_by_id")
                .expect("should exist");
            assert_eq!(found.event_id, [i; 16]);
        }
    }

    #[test]
    fn gc_removes_index_entries() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            let e1 = make_entry(1, [1u8; 16], 1_700_000_000, 100); // expired
            let e2 = make_entry(1, [2u8; 16], 1_700_000_001, 200); // expired
            let e3 = make_entry(1, [3u8; 16], 1_700_000_002, 0); // no expiry
            EventStore::write(&mut txn, &e1).expect("write");
            EventStore::write(&mut txn, &e2).expect("write");
            EventStore::write(&mut txn, &e3).expect("write");
            txn.commit().expect("commit");
        }

        // All three should be findable before GC
        {
            let txn = events_db.read().expect("read txn");
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[1u8; 16])
                    .expect("get_by_id")
                    .is_some()
            );
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[2u8; 16])
                    .expect("get_by_id")
                    .is_some()
            );
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[3u8; 16])
                    .expect("get_by_id")
                    .is_some()
            );
        }

        // GC expired entries
        {
            let read_txn = events_db.read().expect("read txn");
            let mut write_txn = events_db.write().expect("write txn");
            let deleted =
                EventStore::delete_expired(&read_txn, &mut write_txn, 300, 100).expect("gc");
            assert_eq!(deleted, 2);
            write_txn.commit().expect("commit");
        }

        // GC'd entries should be gone from index too
        {
            let txn = events_db.read().expect("read txn");
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[1u8; 16])
                    .expect("get_by_id")
                    .is_none(),
                "GC'd event should not be in index"
            );
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[2u8; 16])
                    .expect("get_by_id")
                    .is_none(),
                "GC'd event should not be in index"
            );
            // Non-expired entry survives
            assert!(
                EventStore::get_by_id(&txn, OrganizationId::new(1), &[3u8; 16])
                    .expect("get_by_id")
                    .is_some()
            );
        }
    }

    #[test]
    fn snapshot_restore_rebuilds_index() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        // Write events to the "source" database
        let entries: Vec<EventEntry> =
            (0..10u8).map(|i| make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), 0)).collect();

        {
            let mut txn = events_db.write().expect("write txn");
            for entry in &entries {
                EventStore::write(&mut txn, entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        // Simulate snapshot: collect all entries via scan_apply_phase
        let snapshot_entries = {
            let txn = events_db.read().expect("read txn");
            EventStore::scan_apply_phase(&txn, 100).expect("scan")
        };

        // "Restore" to a fresh database via write()
        let restored_db = EventsDatabase::open_in_memory().expect("open");
        {
            let mut txn = restored_db.write().expect("write txn");
            for entry in &snapshot_entries {
                EventStore::write(&mut txn, entry).expect("write");
            }
            txn.commit().expect("commit");
        }

        // Verify all events are findable via get_by_id on the restored DB
        let txn = restored_db.read().expect("read txn");
        for i in 0..10u8 {
            let found = EventStore::get_by_id(&txn, OrganizationId::new(1), &[i; 16])
                .expect("get_by_id")
                .expect("should exist after restore");
            assert_eq!(found.event_id, [i; 16]);
        }
    }

    #[test]
    fn index_atomicity_with_primary() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        // Write without committing — then drop the transaction
        {
            let mut txn = events_db.write().expect("write txn");
            let entry = make_entry(1, [50u8; 16], 1_700_000_000, 0);
            EventStore::write(&mut txn, &entry).expect("write");
            // Drop without commit — implicit abort
        }

        // Neither primary nor index should contain the entry
        let txn = events_db.read().expect("read txn");
        let result =
            EventStore::get_by_id(&txn, OrganizationId::new(1), &[50u8; 16]).expect("get_by_id");
        assert!(result.is_none(), "aborted txn should not write index entry");

        let ts_ns =
            Utc.timestamp_opt(1_700_000_000, 0).unwrap().timestamp_nanos_opt().unwrap() as u64;
        let primary =
            EventStore::get(&txn, OrganizationId::new(1), ts_ns, &[50u8; 16]).expect("get");
        assert!(primary.is_none(), "aborted txn should not write primary entry");
    }

    #[test]
    fn get_by_id_multi_org_isolation() {
        let events_db = EventsDatabase::open_in_memory().expect("open");

        {
            let mut txn = events_db.write().expect("write txn");
            // Same event_id in different orgs
            let e1 = make_entry(1, [99u8; 16], 1_700_000_000, 0);
            let e2 = make_entry(2, [99u8; 16], 1_700_000_001, 0);
            EventStore::write(&mut txn, &e1).expect("write");
            EventStore::write(&mut txn, &e2).expect("write");
            txn.commit().expect("commit");
        }

        let txn = events_db.read().expect("read txn");
        let found1 = EventStore::get_by_id(&txn, OrganizationId::new(1), &[99u8; 16])
            .expect("get_by_id")
            .expect("org 1 should find");
        let found2 = EventStore::get_by_id(&txn, OrganizationId::new(2), &[99u8; 16])
            .expect("get_by_id")
            .expect("org 2 should find");

        assert_eq!(found1.organization_id, OrganizationId::new(1));
        assert_eq!(found2.organization_id, OrganizationId::new(2));
    }
}
