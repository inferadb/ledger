//! Background integrity scrubber job.
//!
//! Periodically verifies page checksums and B-tree structural invariants
//! to detect silent data corruption (bit rot). Only runs on the leader node.
//! Uses progressive scanning — each cycle checks a percentage of total pages,
//! advancing a cursor that wraps around the full page space.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{IntegrityScrubber, StorageBackend};
use inferadb_ledger_types::config::IntegrityConfig;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
        record_integrity_errors, record_integrity_pages_checked, record_integrity_scan_duration,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Default scrub interval (1 hour).
const DEFAULT_SCRUB_INTERVAL: Duration = Duration::from_secs(3600);

/// Default percentage of pages to check per cycle.
const DEFAULT_PAGES_PER_CYCLE_PERCENT: f64 = 1.0;

/// Background job that progressively scrubs page integrity.
///
/// Each cycle:
/// 1. Checks if this node is the leader
/// 2. Computes which page range to scrub based on the cursor position
/// 3. Verifies checksums for those pages
/// 4. Periodically runs full B-tree invariant checks
/// 5. Records metrics and advances the cursor
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct IntegrityScrubberJob<B: StorageBackend + 'static> {
    /// The Raft instance (for leader check).
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer providing database access.
    state: Arc<StateLayer<B>>,
    /// Interval between scrub cycles.
    #[builder(default = DEFAULT_SCRUB_INTERVAL)]
    interval: Duration,
    /// Percentage of pages to check per cycle.
    #[builder(default = DEFAULT_PAGES_PER_CYCLE_PERCENT)]
    pages_per_cycle_percent: f64,
}

impl<B: StorageBackend + 'static> IntegrityScrubberJob<B> {
    /// Create from a configuration struct.
    pub fn from_config(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        config: &IntegrityConfig,
    ) -> Self {
        Self {
            raft,
            node_id,
            state,
            interval: Duration::from_secs(config.scrub_interval_secs),
            pages_per_cycle_percent: config.pages_per_cycle_percent,
        }
    }

    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Run a single scrub cycle.
    ///
    /// Scrubs a range of pages starting from the current cursor position,
    /// then verifies B-tree invariants.
    fn run_cycle(&self, cursor: &AtomicU64) {
        if !self.is_leader() {
            debug!("Skipping integrity scrub cycle (not leader)");
            return;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();

        let db = self.state.database();
        let total_pages = db.total_page_count();

        if total_pages == 0 {
            debug!(trace_id = %trace_ctx.trace_id, "Skipping scrub (no pages)");
            return;
        }

        // Compute how many pages to check this cycle
        let pages_to_check =
            ((total_pages as f64 * self.pages_per_cycle_percent / 100.0).ceil() as u64).max(1);

        // Get current cursor and compute page range
        let start = cursor.load(Ordering::Relaxed) % total_pages;
        let page_ids: Vec<u64> = (0..pages_to_check).map(|i| (start + i) % total_pages).collect();

        // Advance cursor for next cycle
        cursor.store((start + pages_to_check) % total_pages, Ordering::Relaxed);

        let scrubber = IntegrityScrubber::new(db);

        // Phase 1: Checksum verification
        let checksum_result = scrubber.verify_page_checksums(&page_ids);

        // Phase 2: B-tree structural invariants (only when cursor wraps around)
        let structural_result = if start + pages_to_check >= total_pages {
            // Full wrap — run structural checks
            scrubber.verify_btree_invariants()
        } else {
            Default::default()
        };

        let total_checked = checksum_result.pages_checked + structural_result.pages_checked;
        let total_checksum_errors = checksum_result.checksum_errors;
        let total_structural_errors =
            checksum_result.structural_errors + structural_result.structural_errors;

        // Record metrics
        let duration = cycle_start.elapsed().as_secs_f64();
        record_integrity_pages_checked(total_checked);
        if total_checksum_errors > 0 {
            record_integrity_errors("checksum", total_checksum_errors);
        }
        if total_structural_errors > 0 {
            record_integrity_errors("structural", total_structural_errors);
        }
        record_integrity_scan_duration(duration);

        // Background job observability
        let has_errors = total_checksum_errors > 0 || total_structural_errors > 0;
        record_background_job_duration("integrity_scrub", duration);
        record_background_job_run(
            "integrity_scrub",
            if has_errors { "failure" } else { "success" },
        );
        record_background_job_items("integrity_scrub", total_checked);

        // Log results
        if has_errors {
            for err in checksum_result.errors.iter().chain(structural_result.errors.iter()) {
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    page_id = err.page_id,
                    table = ?err.table_name,
                    error = %err.description,
                    "Integrity error detected"
                );
            }
            warn!(
                trace_id = %trace_ctx.trace_id,
                pages_checked = total_checked,
                checksum_errors = total_checksum_errors,
                structural_errors = total_structural_errors,
                "Integrity scrub cycle completed with errors"
            );
        } else if total_checked > 0 {
            debug!(
                trace_id = %trace_ctx.trace_id,
                pages_checked = total_checked,
                "Integrity scrub cycle complete (no errors)"
            );
        }
    }

    /// Start the integrity scrubber background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let cursor = AtomicU64::new(0);
            let mut ticker = interval(self.interval);

            info!(
                "Integrity scrubber started (interval={:?}, pages_per_cycle={}%)",
                self.interval, self.pages_per_cycle_percent
            );

            loop {
                ticker.tick().await;
                self.run_cycle(&cursor);
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
    fn test_background_job_metrics_emitted_on_scrub() {
        // Verify that scrubber metric recording functions accept expected arguments
        // Without a recorder, these are no-ops — the test confirms the function
        // signatures match what run_cycle() calls.
        use crate::metrics::{
            record_background_job_duration, record_background_job_items, record_background_job_run,
        };

        // Simulate success path
        record_background_job_duration("integrity_scrub", 0.05);
        record_background_job_run("integrity_scrub", "success");
        record_background_job_items("integrity_scrub", 10);

        // Simulate failure path (corruption detected)
        record_background_job_duration("integrity_scrub", 0.12);
        record_background_job_run("integrity_scrub", "failure");
        record_background_job_items("integrity_scrub", 5);
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
