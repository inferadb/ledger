//! Garbage collection for expired event entries.
//!
//! Periodically scans the `events.db` database and deletes entries whose
//! `expires_at` timestamp has passed. Unlike [`TtlGarbageCollector`] (which
//! proposes deletions through Raft consensus), events GC operates directly
//! on the local database — no Raft coordination needed.
//!
//! Runs on **all nodes**: apply-phase events are identical across replicas
//! (deterministic) and expire at the same threshold; handler-phase events
//! are node-local. Each node independently GCs its own `events.db`.
//!
//! Uses thin [`EventMeta`] deserialization (first field only) during scans
//! to avoid full [`EventEntry`] decode overhead.
//!
//! [`TtlGarbageCollector`]: crate::TtlGarbageCollector
//! [`EventMeta`]: inferadb_ledger_types::events::EventMeta
//! [`EventEntry`]: inferadb_ledger_types::events::EventEntry

use std::{sync::Arc, time::Duration};

use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::events::EventConfig;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::metrics;

/// Default interval between GC cycles (5 minutes).
///
/// Longer than entity TTL GC (60s) because audit events have day-scale TTLs
/// and the table is append-mostly.
const GC_INTERVAL: Duration = Duration::from_secs(300);

/// Maximum expired entries to delete per cycle.
///
/// Prevents oversized write transactions from blocking other writes.
const MAX_BATCH_SIZE: usize = 5000;

/// Background garbage collector for expired event entries.
///
/// Scans all entries in `events.db`, deserializes only [`EventMeta`] (the
/// `expires_at` field), and deletes expired entries in batches. Entries with
/// `expires_at = 0` (no expiry) are never deleted.
///
/// [`EventMeta`]: inferadb_ledger_types::events::EventMeta
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct EventsGarbageCollector<B: StorageBackend + 'static> {
    /// Handle to the dedicated events database.
    events_db: Arc<EventsDatabase<B>>,
    /// Event configuration (checked for `enabled` flag).
    config: EventConfig,
    /// Interval between GC cycles.
    #[builder(default = GC_INTERVAL)]
    interval: Duration,
    /// Maximum expired entries to delete per cycle.
    #[builder(default = MAX_BATCH_SIZE)]
    max_batch_size: usize,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> EventsGarbageCollector<B> {
    /// Runs a single GC cycle, deleting expired event entries.
    ///
    /// Returns the number of entries deleted, or an error description on failure.
    fn run_cycle(&self) -> Result<usize, String> {
        if !self.config.enabled {
            debug!("Events GC skipped (events disabled)");
            return Ok(0);
        }

        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let read_txn = self.events_db.read().map_err(|e| format!("read transaction: {e}"))?;
        let mut write_txn =
            self.events_db.write().map_err(|e| format!("write transaction: {e}"))?;

        let deleted = inferadb_ledger_state::EventStore::delete_expired(
            &read_txn,
            &mut write_txn,
            now_unix,
            self.max_batch_size,
        )
        .map_err(|e| format!("delete_expired: {e}"))?;

        // Drop the read transaction before committing the write
        drop(read_txn);

        if deleted > 0 {
            write_txn.commit().map_err(|e| format!("commit: {e}"))?;
            info!(deleted, "Events GC cycle deleted expired entries");
        }

        Ok(deleted)
    }

    /// Starts the garbage collector as a background tokio task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;

                // Update watchdog heartbeat
                if let Some(ref handle) = self.watchdog_handle {
                    handle.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }

                let start = std::time::Instant::now();
                match self.run_cycle() {
                    Ok(deleted) => {
                        let duration = start.elapsed().as_secs_f64();
                        metrics::record_events_gc_cycle_duration(duration);
                        metrics::record_events_gc_cycle("success");
                        if deleted > 0 {
                            metrics::record_events_gc_entries_deleted(deleted as u64);
                        }
                    },
                    Err(e) => {
                        let duration = start.elapsed().as_secs_f64();
                        metrics::record_events_gc_cycle_duration(duration);
                        metrics::record_events_gc_cycle("failure");
                        warn!(error = %e, "Events GC cycle failed");
                    },
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use inferadb_ledger_state::{EventStore, EventsDatabase};
    use inferadb_ledger_types::{
        OrganizationId,
        events::{EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope},
    };

    use super::*;

    fn make_entry(org_id: i64, event_id: [u8; 16], ts_secs: i64, expires_at: u64) -> EventEntry {
        EventEntry {
            expires_at,
            event_id,
            source_service: "ledger".to_string(),
            event_type: "ledger.test.event".to_string(),
            timestamp: Utc.timestamp_opt(ts_secs, 0).unwrap(),
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

    fn now_unix() -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
    }

    fn write_entries(
        db: &EventsDatabase<inferadb_ledger_store::InMemoryBackend>,
        entries: &[EventEntry],
    ) {
        let mut txn = db.write().unwrap();
        for entry in entries {
            EventStore::write(&mut txn, entry).unwrap();
        }
        txn.commit().unwrap();
    }

    fn count_all(db: &EventsDatabase<inferadb_ledger_store::InMemoryBackend>) -> u64 {
        // Count entries across a wide org range
        let txn = db.read().unwrap();
        let mut total = 0u64;
        for org_id in 0..=10 {
            total += EventStore::count(&txn, OrganizationId::new(org_id)).unwrap();
        }
        total
    }

    fn default_config() -> EventConfig {
        EventConfig::builder().build().unwrap()
    }

    #[test]
    fn gc_deletes_expired_entries() {
        let db = EventsDatabase::open_in_memory().unwrap();
        let now = now_unix();

        // Write entries that expired 1 hour ago
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_000, now - 3600),
            make_entry(1, [2u8; 16], 1_700_000_001, now - 7200),
            make_entry(1, [3u8; 16], 1_700_000_002, now - 1800),
        ];
        write_entries(&db, &entries);
        assert_eq!(count_all(&db), 3);

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(count_all(&db), 0);
    }

    #[test]
    fn gc_preserves_non_expired_entries() {
        let db = EventsDatabase::open_in_memory().unwrap();
        let now = now_unix();

        // Write entries that expire in the future
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_000, now + 3600),
            make_entry(1, [2u8; 16], 1_700_000_001, now + 86400),
        ];
        write_entries(&db, &entries);
        assert_eq!(count_all(&db), 2);

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(count_all(&db), 2);
    }

    #[test]
    fn gc_never_deletes_no_expiry_entries() {
        let db = EventsDatabase::open_in_memory().unwrap();

        // expires_at = 0 means no expiry
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_000, 0),
            make_entry(1, [2u8; 16], 1_700_000_001, 0),
        ];
        write_entries(&db, &entries);
        assert_eq!(count_all(&db), 2);

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(count_all(&db), 2);
    }

    #[test]
    fn gc_batch_size_limit_respected() {
        let db = EventsDatabase::open_in_memory().unwrap();
        let now = now_unix();

        // Write 10 expired entries
        let entries: Vec<EventEntry> = (0..10u8)
            .map(|i| {
                let mut id = [0u8; 16];
                id[0] = i;
                make_entry(1, id, 1_700_000_000 + i64::from(i), now - 3600)
            })
            .collect();
        write_entries(&db, &entries);
        assert_eq!(count_all(&db), 10);

        // Set max_batch_size to 3
        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .max_batch_size(3)
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(count_all(&db), 7);

        // Run again — another 3
        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(count_all(&db), 4);
    }

    #[test]
    fn gc_mixed_batch_only_deletes_expired() {
        let db = EventsDatabase::open_in_memory().unwrap();
        let now = now_unix();

        let entries = vec![
            // Expired
            make_entry(1, [1u8; 16], 1_700_000_000, now - 3600),
            make_entry(1, [2u8; 16], 1_700_000_001, now - 100),
            // Not expired
            make_entry(1, [3u8; 16], 1_700_000_002, now + 3600),
            // No expiry
            make_entry(1, [4u8; 16], 1_700_000_003, 0),
            // Expired
            make_entry(2, [5u8; 16], 1_700_000_004, now - 60),
        ];
        write_entries(&db, &entries);
        assert_eq!(count_all(&db), 5);

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 3); // Only the 3 expired entries
        assert_eq!(count_all(&db), 2); // Future + no-expiry survive
    }

    #[test]
    fn gc_skips_when_disabled() {
        let db = EventsDatabase::open_in_memory().unwrap();
        let now = now_unix();

        let entries = vec![make_entry(1, [1u8; 16], 1_700_000_000, now - 3600)];
        write_entries(&db, &entries);

        let disabled_config = EventConfig::builder().enabled(false).build().unwrap();

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(disabled_config)
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(count_all(&db), 1); // Entry preserved
    }

    #[test]
    fn gc_empty_database_succeeds() {
        let db = EventsDatabase::open_in_memory().unwrap();

        let gc = EventsGarbageCollector::builder()
            .events_db(Arc::new(db.clone()))
            .config(default_config())
            .build();

        let deleted = gc.run_cycle().unwrap();
        assert_eq!(deleted, 0);
    }
}
