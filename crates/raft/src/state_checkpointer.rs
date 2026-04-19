//! Background state-DB checkpointer.
//!
//! Drives [`Database::sync_state`] on a time / apply-count / dirty-page
//! trigger policy so that state-DB durability is amortized across many
//! in-memory commits. The apply path uses `WriteTransaction::commit_in_memory`
//! (landed in Sprint 1B2 Task 1A), so writes visible to in-process readers
//! may be ahead of the dual-slot on-disk pointer until the next checkpoint.
//! This task narrows that gap.
//!
//! Fire policy (any one triggers a checkpoint):
//!
//! 1. **Time** — more than `interval_ms` elapsed since the last checkpoint.
//! 2. **Applies** — >= `applies_threshold` applies since the last checkpoint.
//! 3. **Dirty pages** — `Database::cache_dirty_page_count()` >= `dirty_pages_threshold`
//!    (backpressure against unbounded cache growth).
//!
//! Thresholds are read from `RuntimeConfigHandle` on every wake-up so live
//! `UpdateConfig` RPCs take effect on the next tick without restarting the
//! task.
//!
//! The task is purely a scheduler: the heavy lifting (`flush_pages` +
//! `persist_state_to_disk`) runs inside `Database::sync_state` on a
//! `spawn_blocking` thread. `sync_state` errors are logged and surfaced via
//! metrics; internal trigger counters are not advanced so the next wake-up
//! retries against the same (or newer) in-memory state.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::config::CheckpointConfig;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{metrics, runtime_config::RuntimeConfigHandle};

/// Floor on the internal poll cadence so sub-50ms `interval_ms` settings do
/// not spin the task at the tokio timer's minimum resolution.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Cap on the internal poll cadence so live `UpdateConfig` RPCs take effect
/// within one second even when the previously-loaded `interval_ms` was large.
/// The time trigger itself still fires on the config's `interval_ms` via
/// `last_checkpoint_at`; the cap only bounds how often the task wakes up
/// to re-read the config and sample dirty-page counters.
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Trigger label for the time-based threshold.
const TRIGGER_TIME: &str = "time";
/// Trigger label for the apply-count threshold.
const TRIGGER_APPLIES: &str = "applies";
/// Trigger label for the dirty-page threshold.
const TRIGGER_DIRTY: &str = "dirty";
/// Status label for successful checkpoints.
const STATUS_OK: &str = "ok";
/// Status label for failed checkpoints.
const STATUS_ERROR: &str = "error";

/// Background task that periodically drives [`Database::sync_state`] on the
/// state DB, amortizing dual-slot `persist_state_to_disk` fsyncs.
///
/// Constructed via [`StateCheckpointer::from_config`] and started with
/// [`StateCheckpointer::start`], which returns a `JoinHandle` that completes
/// once the supplied `CancellationToken` is cancelled.
pub struct StateCheckpointer {
    /// State DB handle. The checkpointer calls `sync_state` on this.
    db: Arc<Database<FileBackend>>,
    /// Live runtime-config handle. Re-read on every wake-up so live
    /// `UpdateConfig` RPCs take effect on the next tick.
    runtime_config: RuntimeConfigHandle,
    /// Watch channel delivering the current applied index. The task wakes on
    /// `changed()` notifications in addition to the time tick; channel values
    /// are sampled via `borrow()` on wake-up.
    applied_rx: watch::Receiver<u64>,
    /// Cancellation token that terminates the task.
    cancellation_token: CancellationToken,
    /// Region label used on emitted Prometheus metrics.
    region: String,
    /// Applied index observed at the most recent successful checkpoint.
    /// Used to compute the apply-count trigger.
    applies_at_last_checkpoint: AtomicU64,
    /// Wall-clock `Instant` of the most recent successful checkpoint. Used
    /// to compute the time-based trigger.
    last_checkpoint_at: Mutex<Instant>,
}

impl StateCheckpointer {
    /// Builds a checkpointer bound to the given state DB, runtime config,
    /// applied-index channel, and cancellation token.
    ///
    /// `region` is used as the label for emitted Prometheus metrics
    /// (`ledger_state_*{region=...}`).
    #[must_use]
    pub fn from_config(
        db: Arc<Database<FileBackend>>,
        runtime_config: RuntimeConfigHandle,
        applied_rx: watch::Receiver<u64>,
        cancellation_token: CancellationToken,
        region: impl Into<String>,
    ) -> Self {
        let initial_applied = *applied_rx.borrow();
        Self {
            db,
            runtime_config,
            applied_rx,
            cancellation_token,
            region: region.into(),
            applies_at_last_checkpoint: AtomicU64::new(initial_applied),
            last_checkpoint_at: Mutex::new(Instant::now()),
        }
    }

    /// Reads the current checkpoint config off the runtime handle.
    ///
    /// Falls back to `CheckpointConfig::default()` if `state_checkpoint` is
    /// unset so the checkpointer still has sane thresholds even when live
    /// config has been reset. Matches the "graceful degradation" pattern used
    /// by the rate limiter.
    fn current_config(&self) -> CheckpointConfig {
        self.runtime_config.load().state_checkpoint.clone().unwrap_or_default()
    }

    /// Decides whether the trigger policy warrants a checkpoint right now.
    ///
    /// Returns `Some(trigger_label)` with the **most severe** condition
    /// currently met (priority: `dirty` > `applies` > `time`). Returns
    /// `None` if none apply.
    ///
    /// Priority is a deterministic tie-breaker for metric labelling only —
    /// the checkpoint itself is the same work regardless of which label the
    /// counter gets incremented under.
    fn should_checkpoint(
        &self,
        config: &CheckpointConfig,
        latest_applied: u64,
        dirty_pages: u64,
    ) -> Option<&'static str> {
        if dirty_pages >= config.dirty_pages_threshold {
            return Some(TRIGGER_DIRTY);
        }
        let applies_since_last =
            latest_applied.saturating_sub(self.applies_at_last_checkpoint.load(Ordering::Relaxed));
        if applies_since_last >= config.applies_threshold {
            return Some(TRIGGER_APPLIES);
        }
        let elapsed = self.last_checkpoint_at.lock().elapsed();
        if elapsed >= Duration::from_millis(config.interval_ms) {
            return Some(TRIGGER_TIME);
        }
        None
    }

    /// Samples live gauges at a wake-up. These are emitted regardless of
    /// whether a checkpoint fires so operators can see accumulator trends.
    fn emit_live_gauges(&self, latest_applied: u64, dirty_pages: u64, cache_len: u64) {
        let applies_since_last =
            latest_applied.saturating_sub(self.applies_at_last_checkpoint.load(Ordering::Relaxed));
        metrics::set_state_applies_since_checkpoint(&self.region, applies_since_last);
        metrics::set_state_dirty_pages(&self.region, dirty_pages);
        metrics::set_state_page_cache_len(&self.region, cache_len);
    }

    /// Executes a single checkpoint. On success, advances the trigger
    /// accumulators (`applies_at_last_checkpoint`, `last_checkpoint_at`)
    /// and emits success metrics. On failure, logs a warning, emits an
    /// error counter increment, and **leaves accumulators unchanged** so
    /// the next wake-up retries. `sync_state`'s own contract preserves
    /// `last_synced_snapshot_id` and `pending_frees` on error, so retry is
    /// safe.
    async fn do_checkpoint(&self, trigger: &'static str, latest_applied_at_start: u64) {
        // Compute how many entries this checkpoint is flushing before we
        // reset the accumulator. Used in both the success debug! and the
        // failure warn! so operators can see the magnitude of the work
        // (or the stalled work) regardless of outcome.
        let prior_applies = self.applies_at_last_checkpoint.load(Ordering::Relaxed);
        let applies_since_last = latest_applied_at_start.saturating_sub(prior_applies);

        let start = Instant::now();
        let result = Arc::clone(&self.db).sync_state().await;
        let duration = start.elapsed();
        let duration_secs = duration.as_secs_f64();

        match result {
            Ok(()) => {
                // Advance internal accumulators only on success.
                self.applies_at_last_checkpoint.store(latest_applied_at_start, Ordering::Relaxed);
                *self.last_checkpoint_at.lock() = Instant::now();

                metrics::record_state_checkpoint(&self.region, trigger, STATUS_OK, duration_secs);
                metrics::set_state_last_synced_snapshot_id(
                    &self.region,
                    self.db.last_synced_snapshot_id(),
                );
                if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    metrics::set_state_checkpoint_last_timestamp(&self.region, now.as_secs_f64());
                }

                let dirty_pages = self.db.cache_dirty_page_count() as u64;
                debug!(
                    trigger,
                    duration_ms = duration.as_millis() as u64,
                    applies_since_last,
                    dirty_pages,
                    region = %self.region,
                    "state checkpoint complete"
                );
            },
            Err(e) => {
                metrics::record_state_checkpoint(
                    &self.region,
                    trigger,
                    STATUS_ERROR,
                    duration_secs,
                );
                warn!(
                    error = %e,
                    trigger,
                    duration_ms = duration.as_millis() as u64,
                    applies_since_last,
                    region = %self.region,
                    "state checkpoint failed; leaving accumulators untouched so the next tick retries"
                );
            },
        }
    }

    /// Starts the checkpointer task.
    ///
    /// Returns a `JoinHandle` that completes once the cancellation token is
    /// triggered. The task owns `self` for its lifetime.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let initial = self.current_config();
            info!(
                interval_ms = initial.interval_ms,
                applies_threshold = initial.applies_threshold,
                dirty_pages_threshold = initial.dirty_pages_threshold,
                region = %self.region,
                "StateCheckpointer starting"
            );

            let mut applied_rx = self.applied_rx.clone();
            // Tracks whether the applied-index watch sender is still live.
            // If the sender is dropped without first cancelling the token
            // (shutdown bug, test harness drop, etc.), `changed()` returns
            // `Err` immediately on every poll — left enabled, the select
            // would busy-loop and starve the time / dirty-page triggers.
            // Flipping this guard to `false` disables the arm via the
            // `if` clause on the select branch; the time + cancellation
            // arms keep the task functional as a fallback.
            let mut applied_rx_alive = true;

            loop {
                let config = self.current_config();
                let poll_interval = Duration::from_millis(config.interval_ms / 4)
                    .max(MIN_POLL_INTERVAL)
                    .min(MAX_POLL_INTERVAL);

                let cancelled = tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => false,
                    res = applied_rx.changed(), if applied_rx_alive => {
                        if res.is_err() {
                            warn!(
                                region = %self.region,
                                "applied-index watch sender dropped; \
                                 falling back to time-trigger only"
                            );
                            applied_rx_alive = false;
                        }
                        false
                    },
                    _ = self.cancellation_token.cancelled() => true,
                };

                if cancelled {
                    info!(region = %self.region, "StateCheckpointer shutting down");
                    break;
                }

                self.tick(&config).await;
            }
        })
    }

    /// Performs one wake-up pass: sample state, emit gauges, decide, maybe
    /// checkpoint. Extracted so tests can drive the decision logic without
    /// spinning the tokio select loop.
    async fn tick(&self, config: &CheckpointConfig) {
        let latest_applied = *self.applied_rx.borrow();
        let dirty_pages = self.db.cache_dirty_page_count() as u64;
        let cache_len = self.db.stats().cached_pages as u64;

        self.emit_live_gauges(latest_applied, dirty_pages, cache_len);

        if let Some(trigger) = self.should_checkpoint(config, latest_applied, dirty_pages) {
            self.do_checkpoint(trigger, latest_applied).await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_store::tables::Entities;
    use inferadb_ledger_types::config::RuntimeConfig;
    use tempfile::TempDir;

    use super::*;

    /// Builds a file-backed state DB in a tempdir. The tempdir is returned
    /// so its lifetime spans the test.
    fn new_test_db() -> (TempDir, Arc<Database<FileBackend>>) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("state.db");
        let db = Arc::new(Database::create(&path).expect("create db"));
        (dir, db)
    }

    /// Dirties a page in the state DB via `commit_in_memory`.
    fn commit_in_memory_one(db: &Arc<Database<FileBackend>>, key: &[u8], value: &[u8]) {
        let mut txn = db.write().expect("open write txn");
        txn.insert::<Entities>(&key.to_vec(), &value.to_vec()).expect("insert");
        txn.commit_in_memory().expect("commit_in_memory");
    }

    fn new_checkpointer(
        db: Arc<Database<FileBackend>>,
        cfg: CheckpointConfig,
        applied: u64,
    ) -> (StateCheckpointer, RuntimeConfigHandle, CancellationToken, watch::Sender<u64>) {
        let runtime_config = RuntimeConfigHandle::new(RuntimeConfig {
            state_checkpoint: Some(cfg),
            ..RuntimeConfig::default()
        });
        let (tx, rx) = watch::channel(applied);
        let token = CancellationToken::new();
        let cp = StateCheckpointer::from_config(
            db,
            runtime_config.clone(),
            rx,
            token.clone(),
            "test-region",
        );
        (cp, runtime_config, token, tx)
    }

    // ── Pure trigger-policy tests ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_time_fires_after_interval() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);
        // Back-date last_checkpoint_at so the time trigger qualifies.
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 0), Some(TRIGGER_TIME));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_applies_fires_after_threshold() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(100)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);
        // Baseline applies = 0 (set in from_config); latest_applied = 100 is >= threshold.
        assert_eq!(cp.should_checkpoint(&cfg, 100, 0), Some(TRIGGER_APPLIES));
        // 99 is under threshold.
        assert_eq!(cp.should_checkpoint(&cfg, 99, 0), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_dirty_fires_after_threshold() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(10)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 10), Some(TRIGGER_DIRTY));
        assert_eq!(cp.should_checkpoint(&cfg, 0, 9), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_returns_none_when_all_under_threshold() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 0), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn triggers_are_prioritized_deterministically_when_multiple_fire() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        // All three conditions qualify — dirty wins.
        assert_eq!(cp.should_checkpoint(&cfg, 100, 100), Some(TRIGGER_DIRTY));
        // dirty below threshold → applies wins.
        assert_eq!(cp.should_checkpoint(&cfg, 100, 0), Some(TRIGGER_APPLIES));
        // dirty + applies below threshold → time wins.
        let cfg2 = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        assert_eq!(cp.should_checkpoint(&cfg2, 0, 0), Some(TRIGGER_TIME));
    }

    // ── Integration-style tests (spawn the task) ────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_checkpoint_on_time_tick() {
        let (_dir, db) = new_test_db();
        // Dirty one page in memory so there's something to checkpoint.
        commit_in_memory_one(&db, b"k1", b"v1");
        assert_eq!(db.last_synced_snapshot_id(), 0);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, token, _tx) = new_checkpointer(Arc::clone(&db), cfg, 0);

        let handle = cp.start();

        // Wait for at least one time-tick to fire a checkpoint.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if db.last_synced_snapshot_id() > 0 {
                break;
            }
        }

        assert!(
            db.last_synced_snapshot_id() > 0,
            "expected checkpoint to advance last_synced_snapshot_id from zero"
        );

        token.cancel();
        handle.await.expect("task join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_stops_the_task() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::default();
        let (cp, _rc, token, _tx) = new_checkpointer(Arc::clone(&db), cfg, 0);

        let handle = cp.start();
        tokio::time::sleep(Duration::from_millis(80)).await;
        token.cancel();
        // Should return promptly.
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("task did not exit within 2s")
            .expect("task join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_drop_does_not_spin() {
        // Regression test: dropping the applied-index watch sender used to
        // make `applied_rx.changed()` return `Err` immediately on every
        // poll, busy-looping the select and starving the time / dirty
        // triggers. The select arm is now guarded by `applied_rx_alive`,
        // which flips to `false` on `Err` so the time + cancellation arms
        // remain functional.
        let (_dir, db) = new_test_db();
        // Use sane thresholds so the time arm doesn't fire constantly while
        // we wait — we're testing the loop behaviour, not the trigger.
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, token, tx) = new_checkpointer(Arc::clone(&db), cfg, 0);

        let handle = cp.start();

        // Drop the sender: the applied-index arm should go silent, but the
        // task should keep ticking on the time arm and remain cancellable.
        drop(tx);
        tokio::time::sleep(Duration::from_millis(200)).await;

        token.cancel();
        // Task should exit promptly once cancelled — if the loop were
        // spinning, it would still exit, but if cancellation were starved
        // the timeout would fire.
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("task did not exit within 1s after sender drop + cancel")
            .expect("task join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_config_update_takes_effect_on_next_tick() {
        let (_dir, db) = new_test_db();
        // Start with thresholds so high nothing ever fires.
        let initial = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, runtime, token, _tx) = new_checkpointer(Arc::clone(&db), initial, 0);
        // Dirty a page so a checkpoint would advance snapshot id if one fires.
        commit_in_memory_one(&db, b"k1", b"v1");
        assert_eq!(db.last_synced_snapshot_id(), 0);

        let handle = cp.start();

        // Let a few ticks pass with the huge thresholds; nothing should fire.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            db.last_synced_snapshot_id(),
            0,
            "no checkpoint should have fired with huge thresholds"
        );

        // Swap in a tight time-trigger config.
        let tight = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        runtime.store(RuntimeConfig { state_checkpoint: Some(tight), ..RuntimeConfig::default() });

        // The poll interval is bounded by MAX_POLL_INTERVAL (1s), so the
        // next wake-up re-reads the tightened config within ~1s and the
        // following ticks fire checkpoints at ~50ms cadence.
        let mut observed = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if db.last_synced_snapshot_id() > 0 {
                observed = true;
                break;
            }
        }
        assert!(observed, "expected next tick to pick up the tightened config");

        token.cancel();
        handle.await.expect("task join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn applies_count_resets_on_successful_checkpoint_only() {
        let (_dir, db) = new_test_db();
        commit_in_memory_one(&db, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);

        // Synthesize applied_index advance *before* a successful checkpoint.
        tx.send(42).expect("watch send");

        // Fire a time-triggered checkpoint manually via tick().
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        cp.tick(&cfg).await;

        assert_eq!(
            cp.applies_at_last_checkpoint.load(Ordering::Relaxed),
            42,
            "successful checkpoint must advance applies_at_last_checkpoint to latest applied"
        );
        assert!(
            db.last_synced_snapshot_id() > 0,
            "successful checkpoint must have advanced the synced snapshot id"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tick_does_nothing_when_nothing_qualifies() {
        let (_dir, db) = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(Arc::clone(&db), cfg.clone(), 0);

        cp.tick(&cfg).await;
        assert_eq!(db.last_synced_snapshot_id(), 0);
        assert_eq!(cp.applies_at_last_checkpoint.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn current_config_falls_back_to_default_when_unset() {
        let (_dir, db) = new_test_db();
        let runtime_config = RuntimeConfigHandle::new(RuntimeConfig::default());
        let (_tx, rx) = watch::channel(0u64);
        let token = CancellationToken::new();
        let cp = StateCheckpointer::from_config(
            Arc::clone(&db),
            runtime_config,
            rx,
            token,
            "test-region",
        );

        let cfg = cp.current_config();
        assert_eq!(cfg, CheckpointConfig::default());
    }
}
