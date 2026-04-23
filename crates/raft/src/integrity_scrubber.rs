//! Background integrity scrubber job.
//!
//! Periodically verifies page checksums and B-tree structural invariants
//! to detect silent data corruption (bit rot). Runs on **all nodes** (leader
//! and followers) because scrubbing is read-only — each node independently
//! verifies its own local storage. Uses progressive scanning — each cycle
//! checks a percentage of total pages, advancing a cursor that wraps around
//! the full page space.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{Database, IntegrityScrubber, StorageBackend};
use inferadb_ledger_types::{VaultId, config::IntegrityConfig, trace_context::TraceContext};
use parking_lot::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::metrics::{record_integrity_errors, record_integrity_pages_checked};

/// Per-DB scan cursor key.
///
/// Vault DBs are addressed by `Vault(VaultId)`; the four per-org DBs
/// (`raft.db`, `blocks.db`, `events.db`, `_meta.db`) are addressed by
/// dedicated variants so each owns its own cursor and a per-DB
/// progressive scan never interferes with another DB's progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ScanTarget {
    Vault(VaultId),
    Raft,
    Blocks,
    Events,
    Meta,
}

impl ScanTarget {
    fn label(&self) -> String {
        match self {
            Self::Vault(v) => format!("vault-{}", v.value()),
            Self::Raft => "raft".to_string(),
            Self::Blocks => "blocks".to_string(),
            Self::Events => "events".to_string(),
            Self::Meta => "meta".to_string(),
        }
    }
}

/// Default scrub interval (1 hour).
const DEFAULT_SCRUB_INTERVAL: Duration = Duration::from_secs(3600);

/// Default percentage of pages to check per cycle.
const DEFAULT_PAGES_PER_CYCLE_PERCENT: f64 = 1.0;

/// Background job that progressively scrubs page integrity.
///
/// Each cycle iterates every materialised vault DB plus the per-org
/// `raft.db`, `blocks.db`, `events.db`, and `_meta.db` (whichever are
/// configured), and runs a per-DB progressive checksum sweep:
///
/// 1. Computes which page range to scrub for that DB based on its own cursor position
/// 2. Wraps sync page I/O in `spawn_blocking` to avoid blocking Tokio
/// 3. Verifies checksums for those pages
/// 4. Periodically runs full B-tree invariant checks (when the per-DB cursor wraps)
/// 5. Records metrics and advances the per-DB cursor
///
/// Per-DB cursors give corruption isolation: a checksum failure in
/// one vault does not cascade into the per-DB scan progress of any
/// other DB.
///
/// Runs on **all nodes** — scrubbing is read-only, so every node
/// independently detects corruption in its own local storage.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct IntegrityScrubberJob<B: StorageBackend + 'static> {
    /// State layer providing access to per-vault databases.
    state: Arc<StateLayer<B>>,
    /// Per-org `raft.db` handle (the Raft log + applied state).
    /// Optional because some test harnesses construct the scrubber
    /// without the full per-org DB set.
    #[builder(default)]
    raft_db: Option<Arc<Database<B>>>,
    /// Per-org `blocks.db` handle (the block chain).
    #[builder(default)]
    blocks_db: Option<Arc<Database<B>>>,
    /// Per-org `events.db` handle (the audit stream). `None` when the
    /// region is configured without an events writer.
    #[builder(default)]
    events_db: Option<Arc<Database<B>>>,
    /// Per-org `_meta.db` handle (applied-durable sentinel +
    /// coordination state).
    #[builder(default)]
    meta_db: Option<Arc<Database<B>>>,
    /// Interval between scrub cycles.
    #[builder(default = DEFAULT_SCRUB_INTERVAL)]
    interval: Duration,
    /// Percentage of pages to check per cycle.
    #[builder(default = DEFAULT_PAGES_PER_CYCLE_PERCENT)]
    pages_per_cycle_percent: f64,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<AtomicU64>>,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl<B: StorageBackend + 'static> IntegrityScrubberJob<B> {
    /// Creates from a configuration struct.
    pub fn from_config(
        state: Arc<StateLayer<B>>,
        config: &IntegrityConfig,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            state,
            raft_db: None,
            blocks_db: None,
            events_db: None,
            meta_db: None,
            interval: Duration::from_secs(config.scrub_interval_secs),
            pages_per_cycle_percent: config.pages_per_cycle_percent,
            watchdog_handle: None,
            cancellation_token,
        }
    }

    /// Collects every database handle this job should scrub on this
    /// cycle: each materialised vault DB plus the per-org control DBs.
    fn scan_targets(&self) -> Vec<(ScanTarget, Arc<Database<B>>)> {
        let mut targets: Vec<(ScanTarget, Arc<Database<B>>)> = self
            .state
            .live_vault_dbs()
            .into_iter()
            .map(|(v, db)| (ScanTarget::Vault(v), db))
            .collect();
        if let Some(db) = self.raft_db.as_ref() {
            targets.push((ScanTarget::Raft, Arc::clone(db)));
        }
        if let Some(db) = self.blocks_db.as_ref() {
            targets.push((ScanTarget::Blocks, Arc::clone(db)));
        }
        if let Some(db) = self.events_db.as_ref() {
            targets.push((ScanTarget::Events, Arc::clone(db)));
        }
        if let Some(db) = self.meta_db.as_ref() {
            targets.push((ScanTarget::Meta, Arc::clone(db)));
        }
        targets
    }

    /// Runs a single scrub cycle across every materialised vault DB
    /// + the per-org control DBs.
    ///
    /// Per-DB cursors are stored in a shared `HashMap` keyed by
    /// [`ScanTarget`]. Newly-materialised vaults pick up a fresh
    /// cursor on their first sweep.
    async fn run_cycle(&self, cursors: &Mutex<HashMap<ScanTarget, u64>>) {
        let mut job = crate::logging::JobContext::new("integrity_scrub", None);
        let trace_ctx = TraceContext::new();

        let targets = self.scan_targets();
        if targets.is_empty() {
            debug!(trace_id = %trace_ctx.trace_id, "Skipping scrub (no targets)");
            self.heartbeat();
            return;
        }

        let mut cycle_total_checked: u64 = 0;
        let mut cycle_checksum_errors: u64 = 0;
        let mut cycle_structural_errors: u64 = 0;

        for (target, db) in targets {
            let total_pages = db.total_page_count();
            if total_pages == 0 {
                continue;
            }

            // Compute how many pages to check this cycle for this DB.
            let pages_to_check =
                ((total_pages as f64 * self.pages_per_cycle_percent / 100.0).ceil() as u64).max(1);

            // Get current cursor for this DB and advance.
            let start = {
                let mut map = cursors.lock();
                let cursor = map.entry(target).or_insert(0);
                let s = *cursor % total_pages;
                *cursor = (s + pages_to_check) % total_pages;
                s
            };
            let check_btree = start + pages_to_check >= total_pages;
            let page_ids: Vec<u64> =
                (0..pages_to_check).map(|i| (start + i) % total_pages).collect();

            // Offload synchronous page verification.
            let target_label = target.label();
            let scrub_result = tokio::task::spawn_blocking(move || {
                let scrubber = IntegrityScrubber::new(&db);
                let checksum_result = scrubber.verify_page_checksums(&page_ids);
                let structural_result = if check_btree {
                    scrubber.verify_btree_invariants()
                } else {
                    Default::default()
                };
                (checksum_result, structural_result)
            })
            .await;

            let (checksum_result, structural_result) = match scrub_result {
                Ok(results) => results,
                Err(e) => {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        target = %target_label,
                        error = %e,
                        "Integrity scrub spawn_blocking panicked"
                    );
                    job.set_failure();
                    continue;
                },
            };

            let total_checked = checksum_result.pages_checked + structural_result.pages_checked;
            let total_checksum_errors = checksum_result.checksum_errors;
            let total_structural_errors =
                checksum_result.structural_errors + structural_result.structural_errors;

            cycle_total_checked = cycle_total_checked.saturating_add(total_checked);
            cycle_checksum_errors = cycle_checksum_errors.saturating_add(total_checksum_errors);
            cycle_structural_errors =
                cycle_structural_errors.saturating_add(total_structural_errors);

            let has_errors = total_checksum_errors > 0 || total_structural_errors > 0;
            if has_errors {
                for err in checksum_result.errors.iter().chain(structural_result.errors.iter()) {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        target = %target_label,
                        page_id = err.page_id,
                        table = ?err.table_name,
                        error = %err.description,
                        "Integrity error detected"
                    );
                }
            }
        }

        // Record aggregate metrics for the cycle.
        record_integrity_pages_checked(cycle_total_checked);
        if cycle_checksum_errors > 0 {
            record_integrity_errors("checksum", cycle_checksum_errors);
            job.set_failure();
        }
        if cycle_structural_errors > 0 {
            record_integrity_errors("structural", cycle_structural_errors);
            job.set_failure();
        }
        job.record_items(cycle_total_checked);

        if cycle_checksum_errors > 0 || cycle_structural_errors > 0 {
            warn!(
                trace_id = %trace_ctx.trace_id,
                pages_checked = cycle_total_checked,
                checksum_errors = cycle_checksum_errors,
                structural_errors = cycle_structural_errors,
                "Integrity scrub cycle completed with errors"
            );
        } else if cycle_total_checked > 0 {
            debug!(
                trace_id = %trace_ctx.trace_id,
                pages_checked = cycle_total_checked,
                "Integrity scrub cycle complete (no errors)"
            );
        }

        self.heartbeat();
    }

    /// Updates the watchdog heartbeat timestamp.
    fn heartbeat(&self) {
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(crate::graceful_shutdown::watchdog_now_nanos(), Ordering::Relaxed);
        }
    }

    /// Starts the integrity scrubber background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let cursors: Mutex<HashMap<ScanTarget, u64>> = Mutex::new(HashMap::new());
            let mut ticker = interval(self.interval);

            info!(
                "Integrity scrubber started (interval={:?}, pages_per_cycle={}%)",
                self.interval, self.pages_per_cycle_percent
            );

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.run_cycle(&cursors).await;
                    }
                    _ = self.cancellation_token.cancelled() => {
                        info!("IntegrityScrubberJob shutting down");
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_store::{Database, tables::Entities};

    use super::*;

    fn create_test_db() -> Arc<Database<inferadb_ledger_store::InMemoryBackend>> {
        let db = Arc::new(Database::open_in_memory().unwrap());
        // Insert some data
        {
            let mut txn = db.write().unwrap();
            for i in 0..50u32 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i}").into_bytes();
                txn.insert::<Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }
        db
    }

    #[test]
    fn test_progressive_cursor_advancement() {
        let db = create_test_db();
        let total_pages = db.total_page_count();
        let cursor = AtomicU64::new(0);

        // Simulate pages_per_cycle_percent = 10%
        let pages_per_cycle = ((total_pages as f64 * 10.0 / 100.0).ceil() as u64).max(1);

        // First cycle starts at 0
        let start = cursor.load(Ordering::Relaxed) % total_pages;
        assert_eq!(start, 0);

        // Advance cursor
        cursor.store((start + pages_per_cycle) % total_pages, Ordering::Relaxed);
        let next_start = cursor.load(Ordering::Relaxed);
        assert_eq!(next_start, pages_per_cycle % total_pages);
    }

    #[test]
    fn test_cursor_wraps_around() {
        let cursor = AtomicU64::new(0);
        let total_pages: u64 = 10;
        let pages_per_cycle: u64 = 3;

        // Advance through 4 cycles (covers 12 pages with wrap)
        for cycle in 0..4 {
            let start = cursor.load(Ordering::Relaxed) % total_pages;
            cursor.store((start + pages_per_cycle) % total_pages, Ordering::Relaxed);

            match cycle {
                0 => assert_eq!(start, 0),
                1 => assert_eq!(start, 3),
                2 => assert_eq!(start, 6),
                3 => assert_eq!(start, 9),
                _ => {},
            }
        }
    }

    #[test]
    fn test_from_config() {
        let config = IntegrityConfig::default();
        assert_eq!(config.scrub_interval_secs, 3600);
        assert!((config.pages_per_cycle_percent - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_page_ids_generation_with_wrap() {
        let total_pages: u64 = 10;
        let start: u64 = 8;
        let pages_to_check: u64 = 4;

        let page_ids: Vec<u64> = (0..pages_to_check).map(|i| (start + i) % total_pages).collect();

        assert_eq!(page_ids, vec![8, 9, 0, 1]);
    }

    #[test]
    fn test_scrubber_on_clean_database() {
        let db = create_test_db();
        let scrubber = IntegrityScrubber::new(&*db);

        // Full checksum scan
        let total = db.total_page_count();
        let all_pages: Vec<u64> = (0..total).collect();
        let result = scrubber.verify_page_checksums(&all_pages);
        assert!(result.pages_checked > 0);
        assert_eq!(result.checksum_errors, 0);

        // B-tree invariant scan
        let result = scrubber.verify_btree_invariants();
        assert!(result.pages_checked > 0);
        assert_eq!(result.structural_errors, 0);
    }

    #[test]
    fn test_scrubber_detects_corruption() {
        let db = create_test_db();
        let roots = db.table_root_pages();
        let root = roots[inferadb_ledger_store::tables::TableId::Entities as usize];
        assert!(root > 0);

        // Corrupt the root page
        let mut page = db.read_raw_page(root).unwrap();
        page.data[20] ^= 0xFF;
        db.write_raw_page_for_test(root, &page.data).unwrap();

        let scrubber = IntegrityScrubber::new(&*db);
        let result = scrubber.verify_page_checksums(&[root]);
        assert_eq!(result.checksum_errors, 1);
    }
}
