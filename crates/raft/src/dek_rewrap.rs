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
    time::{Duration, Instant, SystemTime},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::config::RewrapConfig;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    consensus_handle::ConsensusHandle,
    metrics::{record_rewrap_pages, record_rewrap_remaining},
    trace_context::TraceContext,
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
    /// Wall-clock start time as milliseconds since UNIX epoch.
    started_at_millis: AtomicU64,
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
            started_at_millis: AtomicU64::new(0),
        }
    }

    /// Returns current time as milliseconds since UNIX epoch.
    fn now_millis() -> u64 {
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
    }

    /// Resets progress for a new rotation cycle.
    pub fn start_rotation(&self, target_version: u32, total_pages: u64) {
        self.target_version.store(u64::from(target_version), Ordering::Release);
        self.total_pages.store(total_pages, Ordering::Release);
        self.next_page_id.store(0, Ordering::Release);
        self.pages_rewrapped.store(0, Ordering::Release);
        self.started_at_millis.store(Self::now_millis(), Ordering::Release);
        self.complete.store(false, Ordering::Release);
    }

    /// Marks the rotation as complete.
    pub fn mark_complete(&self) {
        self.complete.store(true, Ordering::Release);
    }

    /// Returns estimated remaining seconds based on pages processed and elapsed time.
    pub fn estimated_remaining_secs(&self) -> u64 {
        let total = self.total_pages.load(Ordering::Acquire);
        let processed = self.next_page_id.load(Ordering::Acquire);
        let started_at = self.started_at_millis.load(Ordering::Acquire);

        if processed == 0 || total == 0 || started_at == 0 {
            return 0;
        }

        let elapsed_ms = Self::now_millis().saturating_sub(started_at);
        if elapsed_ms == 0 {
            return 0;
        }

        let remaining = total.saturating_sub(processed);
        // rate = processed / elapsed_ms, ETA = remaining / rate = remaining * elapsed_ms /
        // processed
        remaining.saturating_mul(elapsed_ms) / (processed * 1000)
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
    /// Consensus handle for leadership checks.
    handle: Arc<ConsensusHandle>,
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
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<B>>,
        progress: Arc<RewrapProgress>,
        config: &RewrapConfig,
    ) -> Self {
        Self {
            handle,
            state,
            progress,
            batch_size: config.batch_size,
            interval: Duration::from_secs(config.interval_secs),
            target_version: config.target_rmk_version,
        }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        self.handle.is_leader()
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

        let mut job = crate::logging::JobContext::new("dek_rewrap", None);
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
                job.record_items(rewrapped as u64);

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
                job.set_failure();

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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
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
    fn test_rewrap_progress_estimated_remaining_no_progress() {
        let progress = RewrapProgress::new();
        progress.start_rotation(2, 1000);

        // No progress yet — 0
        assert_eq!(progress.estimated_remaining_secs(), 0);
    }

    #[test]
    fn test_rewrap_progress_estimated_remaining_uses_elapsed_time() {
        let progress = RewrapProgress::new();

        // Simulate a rotation that started 10 seconds ago with 500 of 1000 pages done.
        let ten_secs_ago = RewrapProgress::now_millis() - 10_000;
        progress.total_pages.store(1000, Ordering::Release);
        progress.next_page_id.store(500, Ordering::Release);
        progress.started_at_millis.store(ten_secs_ago, Ordering::Release);
        progress.complete.store(false, Ordering::Release);

        // Rate = 500 pages / 10s = 50 pages/s, remaining = 500, ETA = 500/50 = 10s
        let est = progress.estimated_remaining_secs();
        assert_eq!(est, 10);
    }

    #[test]
    fn test_rewrap_progress_estimated_remaining_almost_done() {
        let progress = RewrapProgress::new();

        let five_secs_ago = RewrapProgress::now_millis() - 5_000;
        progress.total_pages.store(1000, Ordering::Release);
        progress.next_page_id.store(999, Ordering::Release);
        progress.started_at_millis.store(five_secs_ago, Ordering::Release);
        progress.complete.store(false, Ordering::Release);

        // Rate = 999 pages / 5s, remaining = 1, ETA ≈ 0s
        let est = progress.estimated_remaining_secs();
        assert_eq!(est, 0);
    }

    #[test]
    fn test_config_to_job_params() {
        let config =
            RewrapConfig::builder().batch_size(500_usize).interval_secs(60_u64).build().unwrap();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.interval_secs, 60);
    }
}
