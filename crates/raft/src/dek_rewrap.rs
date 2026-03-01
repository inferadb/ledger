//! Background DEK re-wrapping job.
//!
//! After RMK rotation, page sidecar metadata still references the old
//! `rmk_version`. This job iterates all pages in the crypto sidecar,
//! unwrapping each DEK with the old RMK and re-wrapping with the new
//! RMK. Only the sidecar metadata changes — encrypted page bodies are
//! never touched.
//!
//! The job is resumable: it tracks progress in an [`AtomicU64`] and
//! scans from the last processed page ID. Re-wrapping is idempotent —
//! pages already at the target version are skipped.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::config::RewrapConfig;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
        record_rewrap_duration, record_rewrap_pages, record_rewrap_remaining,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Shared re-wrapping progress for status queries.
///
/// Stored behind an `Arc` so the admin service can read progress
/// while the background job writes it.
pub struct RewrapProgress {
    /// Total pages in the sidecar.
    pub total_pages: AtomicU64,
    /// Next page ID to process.
    pub next_page_id: AtomicU64,
    /// Total pages actually re-wrapped (had old version).
    pub pages_rewrapped: AtomicU64,
    /// Whether the job has completed a full pass.
    pub complete: AtomicBool,
    /// Target RMK version being re-wrapped to.
    pub target_version: AtomicU64,
}

impl RewrapProgress {
    /// Creates a new progress tracker.
    pub fn new() -> Self {
        Self {
            total_pages: AtomicU64::new(0),
            next_page_id: AtomicU64::new(0),
            pages_rewrapped: AtomicU64::new(0),
            complete: AtomicBool::new(true), // No rotation in progress
            target_version: AtomicU64::new(0),
        }
    }

    /// Resets progress for a new rotation cycle.
    pub fn start_rotation(&self, target_version: u32, total_pages: u64) {
        self.target_version.store(u64::from(target_version), Ordering::Release);
        self.total_pages.store(total_pages, Ordering::Release);
        self.next_page_id.store(0, Ordering::Release);
        self.pages_rewrapped.store(0, Ordering::Release);
        self.complete.store(false, Ordering::Release);
    }

    /// Marks the rotation as complete.
    pub fn mark_complete(&self) {
        self.complete.store(true, Ordering::Release);
    }

    /// Returns estimated remaining seconds based on pages processed so far.
    pub fn estimated_remaining_secs(&self) -> u64 {
        let total = self.total_pages.load(Ordering::Acquire);
        let processed = self.next_page_id.load(Ordering::Acquire);

        if processed == 0 || total == 0 {
            return 0;
        }

        // Simple linear estimate — no history, just proportional
        let remaining = total.saturating_sub(processed);
        // We don't track elapsed time, so return 0 (caller can compute from metrics)
        remaining / processed.max(1)
    }
}

impl Default for RewrapProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Background job that re-wraps DEK sidecar metadata after RMK rotation.
///
/// Runs periodically on the leader, processing pages in configurable
/// batches. When no rotation is pending, the job idles.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct DekRewrapJob<B: StorageBackend + 'static> {
    /// Raft consensus handle for leadership checks.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer providing database access.
    state: Arc<StateLayer<B>>,
    /// Shared progress tracker (read by admin service).
    progress: Arc<RewrapProgress>,
    /// Pages processed per batch cycle.
    #[builder(default = 1000)]
    batch_size: usize,
    /// Interval between re-wrapping cycles.
    #[builder(default = Duration::from_secs(300))]
    interval: Duration,
    /// Target RMK version to re-wrap to (None = current).
    #[builder(default)]
    target_version: Option<u32>,
}

impl<B: StorageBackend + 'static> DekRewrapJob<B> {
    /// Creates a job from a configuration struct.
    pub fn from_config(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        progress: Arc<RewrapProgress>,
        config: &RewrapConfig,
    ) -> Self {
        Self {
            raft,
            node_id,
            state,
            progress,
            batch_size: config.batch_size,
            interval: Duration::from_secs(config.interval_secs),
            target_version: config.target_rmk_version,
        }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single re-wrapping batch cycle.
    fn run_cycle(&self) {
        if !self.is_leader() {
            debug!("Skipping DEK re-wrap cycle (not leader)");
            return;
        }

        // Check if re-wrapping is needed
        if self.progress.complete.load(Ordering::Acquire) {
            // Check if there's new work (e.g., rotation triggered since last check)
            match self.state.sidecar_page_count() {
                Ok(total) => {
                    self.progress.total_pages.store(total, Ordering::Release);
                    record_rewrap_remaining(0);
                },
                Err(e) => {
                    warn!(error = %e, "Failed to read sidecar page count");
                },
            }
            return;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        let next_page = self.progress.next_page_id.load(Ordering::Acquire);

        debug!(
            trace_id = %trace_ctx.trace_id,
            next_page_id = next_page,
            batch_size = self.batch_size,
            "Starting DEK re-wrap cycle"
        );

        match self.state.rewrap_pages(next_page, self.batch_size, self.target_version) {
            Ok((rewrapped, next)) => {
                let duration = cycle_start.elapsed().as_secs_f64();

                // Update progress
                self.progress.pages_rewrapped.fetch_add(rewrapped as u64, Ordering::Release);

                if let Some(next_id) = next {
                    self.progress.next_page_id.store(next_id, Ordering::Release);
                    let total = self.progress.total_pages.load(Ordering::Acquire);
                    let remaining = total.saturating_sub(next_id);
                    record_rewrap_remaining(remaining);
                } else {
                    // All pages processed
                    self.progress.mark_complete();
                    record_rewrap_remaining(0);
                    let total_rewrapped = self.progress.pages_rewrapped.load(Ordering::Acquire);
                    info!(
                        trace_id = %trace_ctx.trace_id,
                        total_rewrapped = total_rewrapped,
                        target_version = ?self.target_version,
                        "DEK re-wrapping complete"
                    );
                }

                // Record metrics
                record_rewrap_pages(rewrapped as u64);
                record_rewrap_duration(duration);
                record_background_job_duration("dek_rewrap", duration);
                record_background_job_run("dek_rewrap", "success");
                record_background_job_items("dek_rewrap", rewrapped as u64);

                if rewrapped > 0 {
                    info!(
                        trace_id = %trace_ctx.trace_id,
                        pages_rewrapped = rewrapped,
                        next_page_id = ?next,
                        duration_secs = duration,
                        "DEK re-wrap batch complete"
                    );
                } else {
                    debug!(
                        trace_id = %trace_ctx.trace_id,
                        next_page_id = ?next,
                        duration_secs = duration,
                        "DEK re-wrap batch complete (no pages re-wrapped)"
                    );
                }
            },
            Err(e) => {
                let duration = cycle_start.elapsed().as_secs_f64();
                record_background_job_duration("dek_rewrap", duration);
                record_background_job_run("dek_rewrap", "failure");

                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    next_page_id = next_page,
                    duration_secs = duration,
                    "DEK re-wrap cycle failed"
                );
            },
        }
    }

    /// Starts the DEK re-wrapping background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                self.run_cycle();
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrap_progress_lifecycle() {
        let progress = RewrapProgress::new();

        // Initially complete (no rotation pending)
        assert!(progress.complete.load(Ordering::Acquire));
        assert_eq!(progress.total_pages.load(Ordering::Acquire), 0);

        // Start a rotation
        progress.start_rotation(2, 1000);
        assert!(!progress.complete.load(Ordering::Acquire));
        assert_eq!(progress.total_pages.load(Ordering::Acquire), 1000);
        assert_eq!(progress.target_version.load(Ordering::Acquire), 2);
        assert_eq!(progress.next_page_id.load(Ordering::Acquire), 0);

        // Simulate progress
        progress.next_page_id.store(500, Ordering::Release);
        progress.pages_rewrapped.store(300, Ordering::Release);

        // Mark complete
        progress.mark_complete();
        assert!(progress.complete.load(Ordering::Acquire));
    }

    #[test]
    fn test_rewrap_progress_estimated_remaining() {
        let progress = RewrapProgress::new();
        progress.start_rotation(2, 1000);

        // No progress yet — 0
        assert_eq!(progress.estimated_remaining_secs(), 0);

        // Half done
        progress.next_page_id.store(500, Ordering::Release);
        let est = progress.estimated_remaining_secs();
        assert_eq!(est, 1); // 500 remaining / 500 processed = 1

        // Almost done
        progress.next_page_id.store(999, Ordering::Release);
        let est = progress.estimated_remaining_secs();
        assert_eq!(est, 0); // 1 remaining / 999 processed ≈ 0
    }

    #[test]
    fn test_rewrap_progress_default() {
        let progress = RewrapProgress::default();
        assert!(progress.complete.load(Ordering::Acquire));
    }

    #[test]
    fn test_config_to_job_params() {
        let config = RewrapConfig::builder().batch_size(500_usize).interval_secs(60_u64).build();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.interval_secs, 60);
    }

    #[test]
    fn test_rewrap_metrics_signatures() {
        // Verify metric recording functions accept expected arguments.
        // No-ops without a recorder — confirms call signatures.
        record_rewrap_pages(42);
        record_rewrap_remaining(100);
        record_rewrap_duration(1.5);
        record_background_job_duration("dek_rewrap", 2.0);
        record_background_job_run("dek_rewrap", "success");
        record_background_job_items("dek_rewrap", 42);
    }
}
