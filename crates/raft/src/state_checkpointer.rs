//! Background state-DB checkpointer.
//!
//! Drives [`Database::sync_state`] on a time / apply-count / dirty-page
//! trigger policy so that state-DB durability is amortized across many
//! in-memory commits. The apply path uses `WriteTransaction::commit_in_memory`,
//! so writes visible to in-process readers may be ahead of the dual-slot
//! on-disk pointer until the next checkpoint. This task narrows that gap.
//!
//! Each region has up to **four** `Database<FileBackend>` handles that go
//! lazy: `state.db` (entity/relationship tables, owned by `StateLayer`),
//! `raft.db` (`KEY_APPLIED_STATE` + Raft log, owned by
//! `RaftLogStore`), `blocks.db` (historical block archive, owned by
//! `BlockArchive`), and `events.db` (audit events, owned by
//! `EventsDatabase` — optional: some regions are configured without events).
//! All flipped DBs receive `commit_in_memory` commits on every applied
//! batch, so the checkpointer syncs them concurrently on every fire.
//! Missing the raft.db sync would cause `applied_durable = 0` to be read on
//! clean-shutdown restart (`KEY_APPLIED_STATE` never reaches disk), forcing
//! a full WAL replay — see the follow-up in the commit-durability audit.
//! Missing blocks.db or events.db would leave their dirty pages
//! accumulating unbounded in memory between ticks.
//!
//! Fire policy (any one triggers a checkpoint):
//!
//! 1. **Time** — more than `interval_ms` elapsed since the last checkpoint.
//! 2. **Applies** — >= `applies_threshold` applies since the last checkpoint.
//! 3. **Dirty pages** — `max(cache_dirty_page_count)` across all 4 DBs (state, raft, blocks, events
//!    — whichever are configured) >= `dirty_pages_threshold`. Under ingest-heavy / write-light
//!    workloads, only events.db (or only blocks.db under write-heavy / read-light workloads) may be
//!    dirty; using state.db alone as the proxy would fail to fire until the time trigger elapsed,
//!    letting the non-state DBs accumulate pages. One atomic read per DB per wake-up is trivially
//!    cheap.
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
use inferadb_ledger_types::{OrganizationId, config::CheckpointConfig};
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
    /// State DB handle. The checkpointer calls `sync_state` on this DB and
    /// contributes its dirty-page count to the `dirty_pages_threshold`
    /// trigger's `max()` computation across all configured DBs.
    state_db: Arc<Database<FileBackend>>,
    /// Raft DB handle (the `raft.db` file that owns `KEY_APPLIED_STATE` +
    /// the Raft log). Synced concurrently with `state_db` so clean-shutdown
    /// restarts read a non-zero `applied_durable` and skip WAL replay.
    /// See the follow-up in the commit-durability audit.
    raft_db: Arc<Database<FileBackend>>,
    /// Blocks DB handle (the `blocks.db` file that owns the region's block
    /// archive). `BlockArchive::append_block` uses `commit_in_memory`; this
    /// handle is synced concurrently alongside state.db + raft.db + events.db
    /// so dirty apply-phase block pages reach disk on the checkpoint cadence.
    blocks_db: Arc<Database<FileBackend>>,
    /// Events DB handle (the `events.db` file that owns apply-phase audit
    /// events). `None` for regions configured without an events writer
    /// (test fixtures, historical GLOBAL-only configurations) — the
    /// checkpointer silently skips events.db in that case.
    /// `EventWriter::write_events` uses `commit_in_memory`; when present,
    /// this handle must be synced to prevent apply-phase event pages from
    /// accumulating unbounded between ticks.
    events_db: Option<Arc<Database<FileBackend>>>,
    /// Meta DB handle (`_meta.db`) — the per-organization coordinator
    /// introduced by Slice 1 of per-vault consensus. Owns the
    /// `_meta:last_applied` crash-recovery sentinel.
    ///
    /// **Strict ordering invariant:** meta.db must sync **after** state.db
    /// / raft.db / blocks.db / events.db on every tick. Inverting the
    /// order would allow the sentinel on disk to advance past entity data
    /// that has not yet reached the dual-slot on-disk pointer — a
    /// correctness bug on crash-recovery. `do_checkpoint` enforces this
    /// explicitly: the first `tokio::join!` covers the four entity/state
    /// DBs, then meta.db is synced separately, after they all succeed.
    meta_db: Arc<Database<FileBackend>>,
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
    /// Organization-id label used on emitted Prometheus metrics, pre-
    /// stringified from the owning `OrganizationGroup`'s
    /// [`OrganizationId`]. The data-region group emits `"0"`; per-
    /// organization groups emit the organization's id so dashboards can
    /// split checkpoint cadence per organization.
    shard: String,
    /// Applied index observed at the most recent successful checkpoint.
    /// Used to compute the apply-count trigger.
    applies_at_last_checkpoint: AtomicU64,
    /// Wall-clock `Instant` of the most recent successful checkpoint. Used
    /// to compute the time-based trigger.
    last_checkpoint_at: Mutex<Instant>,
}

impl StateCheckpointer {
    /// Builds a checkpointer bound to the given per-region database handles,
    /// runtime config, applied-index channel, and cancellation token.
    ///
    /// `state_db`, `raft_db`, and `blocks_db` are required — every region
    /// in production has all three. `events_db` is `Option` because a region
    /// may be configured without an events writer (test fixtures, historical
    /// GLOBAL-only configurations); when `None`, the checkpointer silently
    /// omits events.db from its sync set and from the `max()` dirty-page
    /// trigger.
    ///
    /// `region` and `shard` are used as the labels for emitted Prometheus
    /// metrics (`ledger_state_*{region=..., organization_id=...}`). The
    /// data-region group's checkpointer emits `"0"`; per-organization
    /// group checkpointers emit the organization's id, so dashboards can
    /// split cadence per organization without any metric-schema change.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        state_db: Arc<Database<FileBackend>>,
        raft_db: Arc<Database<FileBackend>>,
        blocks_db: Arc<Database<FileBackend>>,
        events_db: Option<Arc<Database<FileBackend>>>,
        meta_db: Arc<Database<FileBackend>>,
        runtime_config: RuntimeConfigHandle,
        applied_rx: watch::Receiver<u64>,
        cancellation_token: CancellationToken,
        region: impl Into<String>,
        organization_id: OrganizationId,
    ) -> Self {
        let initial_applied = *applied_rx.borrow();
        Self {
            state_db,
            raft_db,
            blocks_db,
            events_db,
            meta_db,
            runtime_config,
            applied_rx,
            cancellation_token,
            region: region.into(),
            shard: organization_id.value().to_string(),
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
        metrics::set_state_applies_since_checkpoint(&self.region, &self.shard, applies_since_last);
        metrics::set_state_dirty_pages(&self.region, &self.shard, dirty_pages);
        metrics::set_state_page_cache_len(&self.region, &self.shard, cache_len);
    }

    /// Returns the peak `cache_dirty_page_count()` across every configured
    /// DB (state, raft, blocks, events when Some, and meta). The trigger
    /// must fire on whichever DB is under pressure, not solely state.db,
    /// because ingest-heavy workloads can dirty events.db while state.db
    /// stays clean. meta.db contributes per-entry sentinel writes and must
    /// be part of the `max()` so the checkpointer catches per-tick
    /// sentinel pressure too.
    fn max_dirty_pages(&self) -> u64 {
        let state = self.state_db.cache_dirty_page_count() as u64;
        let raft = self.raft_db.cache_dirty_page_count() as u64;
        let blocks = self.blocks_db.cache_dirty_page_count() as u64;
        let events =
            self.events_db.as_ref().map(|db| db.cache_dirty_page_count() as u64).unwrap_or(0);
        let meta = self.meta_db.cache_dirty_page_count() as u64;
        state.max(raft).max(blocks).max(events).max(meta)
    }

    /// Executes a single checkpoint. On success, advances the trigger
    /// accumulators (`applies_at_last_checkpoint`, `last_checkpoint_at`)
    /// and emits success metrics. On failure, logs a warning per failing
    /// DB, emits an error counter increment, and **leaves accumulators
    /// unchanged** so the next wake-up retries. `sync_state`'s own
    /// contract preserves `last_synced_snapshot_id` and `pending_frees`
    /// on error, so retry is safe.
    ///
    /// Sync semantics:
    ///
    /// 1. **Phase A** — state.db / raft.db / blocks.db / events.db (when
    ///    present) run concurrently via `tokio::join!`. These are the
    ///    entity-data stores; they must reach disk before the sentinel
    ///    that references them advances.
    /// 2. **Phase B** — meta.db is synced **after** Phase A completes.
    ///    meta.db owns the `_meta:last_applied` sentinel; landing it on
    ///    disk before the entity data would allow a post-crash boot to
    ///    observe a sentinel that references writes still trapped in the
    ///    page cache.
    ///
    /// A single-DB failure in Phase A does not short-circuit the tick —
    /// the remaining Phase A DBs still get their sync so each flush
    /// narrows the crash gap. Accumulators advance only when **every**
    /// configured DB's sync succeeded, keeping the lock-step policy the
    /// 2-DB version established. If any Phase A DB fails, meta.db is
    /// still synced — the strict ordering holds because entity data that
    /// failed to reach disk won't be referenced by the sentinel (the
    /// apply path commits state.db first, then meta.db; a stale sentinel
    /// simply points at the prior-apply snapshot).
    async fn do_checkpoint(&self, trigger: &'static str, latest_applied_at_start: u64) {
        // Compute how many entries this checkpoint is flushing before we
        // reset the accumulator. Used in both the success debug! and the
        // failure warn! so operators can see the magnitude of the work
        // (or the stalled work) regardless of outcome.
        let prior_applies = self.applies_at_last_checkpoint.load(Ordering::Relaxed);
        let applies_since_last = latest_applied_at_start.saturating_sub(prior_applies);

        let start = Instant::now();

        // Phase A: sync every entity/state DB concurrently. state.db owns
        // the entity/relationship tables, raft.db owns `KEY_APPLIED_STATE`,
        // blocks.db owns the historical block archive, events.db (when
        // configured) owns apply-phase audit events. The blocks.db +
        // events.db apply-path commits use `commit_in_memory`; all of them
        // must reach disk for a clean-shutdown restart to find a consistent
        // on-disk world and skip WAL replay.
        //
        // The match on `events_db` lets us emit a 3-arm join when events
        // is absent — this preserves the "configured DBs only" invariant
        // so a None events_db doesn't contribute a spurious Ok(()) to the
        // all-ok check.
        let (state_result, raft_result, blocks_result, events_result) = match &self.events_db {
            Some(events_db) => {
                let (s, r, b, e) = tokio::join!(
                    Arc::clone(&self.state_db).sync_state(),
                    Arc::clone(&self.raft_db).sync_state(),
                    Arc::clone(&self.blocks_db).sync_state(),
                    Arc::clone(events_db).sync_state(),
                );
                (s, r, b, Some(e))
            },
            None => {
                let (s, r, b) = tokio::join!(
                    Arc::clone(&self.state_db).sync_state(),
                    Arc::clone(&self.raft_db).sync_state(),
                    Arc::clone(&self.blocks_db).sync_state(),
                );
                (s, r, b, None)
            },
        };

        // Phase B: sync meta.db **after** the Phase A entity DBs have
        // resolved. This is the strict-ordering invariant — the sentinel
        // on disk must never race ahead of the entity data it references.
        let meta_result = Arc::clone(&self.meta_db).sync_state().await;

        let duration = start.elapsed();
        let duration_secs = duration.as_secs_f64();

        let all_ok = state_result.is_ok()
            && raft_result.is_ok()
            && blocks_result.is_ok()
            && events_result.as_ref().is_none_or(|r| r.is_ok())
            && meta_result.is_ok();

        // Log per-DB failures separately so operators can tell which slot
        // lagged. We continue on a single-DB failure because syncing the
        // remaining DBs still narrows the crash gap meaningfully.
        if let Err(ref e) = state_result {
            warn!(
                error = %e,
                trigger,
                db = "state",
                region = %self.region,
                "state checkpoint sync failed; leaving accumulators untouched so the next tick retries"
            );
        }
        if let Err(ref e) = raft_result {
            warn!(
                error = %e,
                trigger,
                db = "raft",
                region = %self.region,
                "state checkpoint sync failed; leaving accumulators untouched so the next tick retries"
            );
        }
        if let Err(ref e) = blocks_result {
            warn!(
                error = %e,
                trigger,
                db = "blocks",
                region = %self.region,
                "state checkpoint sync failed; leaving accumulators untouched so the next tick retries"
            );
        }
        if let Some(Err(ref e)) = events_result {
            warn!(
                error = %e,
                trigger,
                db = "events",
                region = %self.region,
                "state checkpoint sync failed; leaving accumulators untouched so the next tick retries"
            );
        }
        if let Err(ref e) = meta_result {
            warn!(
                error = %e,
                trigger,
                db = "meta",
                region = %self.region,
                "state checkpoint sync failed; leaving accumulators untouched so the next tick retries"
            );
        }

        if all_ok {
            // Advance internal accumulators only when EVERY configured DB
            // synced. If any lags, next tick must retry so the slots stay
            // in lock-step.
            self.applies_at_last_checkpoint.store(latest_applied_at_start, Ordering::Relaxed);
            *self.last_checkpoint_at.lock() = Instant::now();

            metrics::record_state_checkpoint(
                &self.region,
                &self.shard,
                trigger,
                STATUS_OK,
                duration_secs,
            );
            metrics::set_state_last_synced_snapshot_id(
                &self.region,
                &self.shard,
                self.state_db.last_synced_snapshot_id(),
            );
            if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                metrics::set_state_checkpoint_last_timestamp(
                    &self.region,
                    &self.shard,
                    now.as_secs_f64(),
                );
            }

            let dirty_pages = self.max_dirty_pages();
            debug!(
                trigger,
                duration_ms = duration.as_millis() as u64,
                applies_since_last,
                dirty_pages,
                events_enabled = self.events_db.is_some(),
                region = %self.region,
                "state checkpoint complete (state.db + raft.db + blocks.db + events.db if enabled, then meta.db)"
            );
        } else {
            metrics::record_state_checkpoint(
                &self.region,
                &self.shard,
                trigger,
                STATUS_ERROR,
                duration_secs,
            );
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
        // The dirty-page trigger reads `max()` across every configured DB
        // (state, raft, blocks, events when present). Using state.db alone
        // as a proxy fails under ingest-heavy workloads where only
        // events.db is dirty; `max()` ensures operator-tuned thresholds
        // apply to whichever DB is under pressure.
        let dirty_pages = self.max_dirty_pages();
        // Cache-length gauge continues to report state.db — it's the
        // operator-facing "how large is the working set" metric; per-DB
        // breakdown is a separate observability concern and is not
        // introduced by this task.
        let cache_len = self.state_db.stats().cached_pages as u64;

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

    /// Aggregated tempdir-backed database handles used across tests.
    ///
    /// `events` is `Option` so tests can exercise both the 3-DB (no events
    /// writer) and 4-DB (events writer present) paths through the
    /// checkpointer. The tempdir is kept alive for the test's lifetime.
    struct TestDbs {
        _dir: TempDir,
        state: Arc<Database<FileBackend>>,
        raft: Arc<Database<FileBackend>>,
        blocks: Arc<Database<FileBackend>>,
        events: Option<Arc<Database<FileBackend>>>,
        meta: Arc<Database<FileBackend>>,
    }

    /// Builds file-backed state + raft + blocks + meta DBs in a tempdir.
    /// No events DB — matches regions configured without an events writer.
    fn new_test_db() -> TestDbs {
        let dir = TempDir::new().expect("tempdir");
        let state =
            Arc::new(Database::create(dir.path().join("state.db")).expect("create state db"));
        let raft = Arc::new(Database::create(dir.path().join("raft.db")).expect("create raft db"));
        let blocks =
            Arc::new(Database::create(dir.path().join("blocks.db")).expect("create blocks db"));
        let meta =
            Arc::new(Database::create(dir.path().join("_meta.db")).expect("create meta db"));
        TestDbs { _dir: dir, state, raft, blocks, events: None, meta }
    }

    /// Builds file-backed state + raft + blocks + events + meta DBs in a
    /// tempdir. Matches the production region configuration where every DB
    /// is present.
    fn new_test_db_with_events() -> TestDbs {
        let dir = TempDir::new().expect("tempdir");
        let state =
            Arc::new(Database::create(dir.path().join("state.db")).expect("create state db"));
        let raft = Arc::new(Database::create(dir.path().join("raft.db")).expect("create raft db"));
        let blocks =
            Arc::new(Database::create(dir.path().join("blocks.db")).expect("create blocks db"));
        let events =
            Arc::new(Database::create(dir.path().join("events.db")).expect("create events db"));
        let meta =
            Arc::new(Database::create(dir.path().join("_meta.db")).expect("create meta db"));
        TestDbs { _dir: dir, state, raft, blocks, events: Some(events), meta }
    }

    /// Dirties a page in the given DB via `commit_in_memory`.
    fn commit_in_memory_one(db: &Arc<Database<FileBackend>>, key: &[u8], value: &[u8]) {
        let mut txn = db.write().expect("open write txn");
        txn.insert::<Entities>(&key.to_vec(), &value.to_vec()).expect("insert");
        txn.commit_in_memory().expect("commit_in_memory");
    }

    fn new_checkpointer(
        dbs: &TestDbs,
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
            Arc::clone(&dbs.state),
            Arc::clone(&dbs.raft),
            Arc::clone(&dbs.blocks),
            dbs.events.as_ref().map(Arc::clone),
            Arc::clone(&dbs.meta),
            runtime_config.clone(),
            rx,
            token.clone(),
            "test-region",
            OrganizationId::new(0),
        );
        (cp, runtime_config, token, tx)
    }

    // ── Pure trigger-policy tests ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_time_fires_after_interval() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);
        // Back-date last_checkpoint_at so the time trigger qualifies.
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 0), Some(TRIGGER_TIME));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_applies_fires_after_threshold() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(100)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);
        // Baseline applies = 0 (set in from_config); latest_applied = 100 is >= threshold.
        assert_eq!(cp.should_checkpoint(&cfg, 100, 0), Some(TRIGGER_APPLIES));
        // 99 is under threshold.
        assert_eq!(cp.should_checkpoint(&cfg, 99, 0), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_dirty_fires_after_threshold() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(10)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 10), Some(TRIGGER_DIRTY));
        assert_eq!(cp.should_checkpoint(&cfg, 0, 9), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_checkpoint_returns_none_when_all_under_threshold() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);
        assert_eq!(cp.should_checkpoint(&cfg, 0, 0), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn triggers_are_prioritized_deterministically_when_multiple_fire() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);
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
        let dbs = new_test_db();
        // Dirty one page in memory so there's something to checkpoint.
        commit_in_memory_one(&dbs.state, b"k1", b"v1");
        assert_eq!(dbs.state.last_synced_snapshot_id(), 0);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let state_handle = Arc::clone(&dbs.state);
        let (cp, _rc, token, _tx) = new_checkpointer(&dbs, cfg, 0);

        let handle = cp.start();

        // Wait for at least one time-tick to fire a checkpoint.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if state_handle.last_synced_snapshot_id() > 0 {
                break;
            }
        }

        assert!(
            state_handle.last_synced_snapshot_id() > 0,
            "expected checkpoint to advance state.db last_synced_snapshot_id from zero"
        );

        token.cancel();
        handle.await.expect("task join");
    }

    /// Regression test: the checkpointer must sync raft.db alongside
    /// state.db on every fire. Without this, a
    /// clean-shutdown restart reads `applied_durable = 0` from raft.db
    /// and forces a full WAL replay even though state.db is caught up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_advances_both_state_db_and_raft_db() {
        let dbs = new_test_db();
        // Dirty a page on BOTH DBs — mirrors the per-batch apply pattern
        // where state.db gets entity writes and raft.db gets the
        // KEY_APPLIED_STATE write.
        commit_in_memory_one(&dbs.state, b"k1", b"v1");
        commit_in_memory_one(&dbs.raft, b"k1", b"v1");
        assert_eq!(dbs.state.last_synced_snapshot_id(), 0);
        assert_eq!(dbs.raft.last_synced_snapshot_id(), 0);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let state_handle = Arc::clone(&dbs.state);
        let raft_handle = Arc::clone(&dbs.raft);
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        // Fire a time-triggered checkpoint manually via tick() so we don't
        // rely on task scheduling.
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        cp.tick(&cfg).await;

        assert!(
            state_handle.last_synced_snapshot_id() > 0,
            "state.db last_synced_snapshot_id must advance on checkpoint"
        );
        assert!(
            raft_handle.last_synced_snapshot_id() > 0,
            "raft.db last_synced_snapshot_id must advance on checkpoint \
             (missing this caused full WAL replay after clean shutdown)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_stops_the_task() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::default();
        let (cp, _rc, token, _tx) = new_checkpointer(&dbs, cfg, 0);

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
        let dbs = new_test_db();
        // Use sane thresholds so the time arm doesn't fire constantly while
        // we wait — we're testing the loop behaviour, not the trigger.
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, token, tx) = new_checkpointer(&dbs, cfg, 0);

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
        let dbs = new_test_db();
        // Start with thresholds so high nothing ever fires.
        let initial = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let state_handle = Arc::clone(&dbs.state);
        let (cp, runtime, token, _tx) = new_checkpointer(&dbs, initial, 0);
        // Dirty a page so a checkpoint would advance snapshot id if one fires.
        commit_in_memory_one(&state_handle, b"k1", b"v1");
        assert_eq!(state_handle.last_synced_snapshot_id(), 0);

        let handle = cp.start();

        // Let a few ticks pass with the huge thresholds; nothing should fire.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            state_handle.last_synced_snapshot_id(),
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
            if state_handle.last_synced_snapshot_id() > 0 {
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
        let dbs = new_test_db();
        commit_in_memory_one(&dbs.state, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let state_handle = Arc::clone(&dbs.state);
        let (cp, _rc, _tok, tx) = new_checkpointer(&dbs, cfg.clone(), 0);

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
            state_handle.last_synced_snapshot_id() > 0,
            "successful checkpoint must have advanced the synced snapshot id"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tick_does_nothing_when_nothing_qualifies() {
        let dbs = new_test_db();
        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let state_handle = Arc::clone(&dbs.state);
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        cp.tick(&cfg).await;
        assert_eq!(state_handle.last_synced_snapshot_id(), 0);
        assert_eq!(cp.applies_at_last_checkpoint.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn current_config_falls_back_to_default_when_unset() {
        let dbs = new_test_db();
        let runtime_config = RuntimeConfigHandle::new(RuntimeConfig::default());
        let (_tx, rx) = watch::channel(0u64);
        let token = CancellationToken::new();
        let cp = StateCheckpointer::from_config(
            Arc::clone(&dbs.state),
            Arc::clone(&dbs.raft),
            Arc::clone(&dbs.blocks),
            dbs.events.as_ref().map(Arc::clone),
            Arc::clone(&dbs.meta),
            runtime_config,
            rx,
            token,
            "test-region",
            OrganizationId::new(0),
        );

        let cfg = cp.current_config();
        assert_eq!(cfg, CheckpointConfig::default());
    }

    /// Slice 1 durability-ordering gate: meta.db must advance alongside
    /// every other DB on a successful checkpoint. This is the fundament
    /// of the crash-recovery contract — on restart, the sentinel in
    /// meta.db is the anchor `replay_crash_gap` uses to skip already-
    /// applied entries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_advances_meta_db_after_entity_dbs() {
        let dbs = new_test_db_with_events();
        let events_handle =
            Arc::clone(dbs.events.as_ref().expect("events db configured by helper"));
        commit_in_memory_one(&dbs.state, b"k1", b"v1");
        commit_in_memory_one(&dbs.raft, b"k1", b"v1");
        commit_in_memory_one(&dbs.blocks, b"k1", b"v1");
        commit_in_memory_one(&events_handle, b"k1", b"v1");
        commit_in_memory_one(&dbs.meta, b"sentinel", b"log_id_1");
        let state_handle = Arc::clone(&dbs.state);
        let raft_handle = Arc::clone(&dbs.raft);
        let blocks_handle = Arc::clone(&dbs.blocks);
        let meta_handle = Arc::clone(&dbs.meta);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        cp.tick(&cfg).await;

        assert!(state_handle.last_synced_snapshot_id() > 0, "state.db must advance");
        assert!(raft_handle.last_synced_snapshot_id() > 0, "raft.db must advance");
        assert!(blocks_handle.last_synced_snapshot_id() > 0, "blocks.db must advance");
        assert!(events_handle.last_synced_snapshot_id() > 0, "events.db must advance");
        assert!(
            meta_handle.last_synced_snapshot_id() > 0,
            "meta.db must advance — Slice 1 strict-ordering gate"
        );
    }

    /// `max()` trigger must include meta.db — dirty pages in meta.db alone
    /// must fire the dirty-pages trigger.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_max_trigger_fires_on_meta_db_pressure() {
        let dbs = new_test_db_with_events();
        commit_in_memory_one(&dbs.meta, b"sentinel", b"log_id_1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        assert_eq!(dbs.state.cache_dirty_page_count() as u64, 0);
        assert_eq!(dbs.raft.cache_dirty_page_count() as u64, 0);
        assert_eq!(dbs.blocks.cache_dirty_page_count() as u64, 0);
        assert!(dbs.meta.cache_dirty_page_count() as u64 >= 1);
        assert_eq!(cp.should_checkpoint(&cfg, 0, cp.max_dirty_pages()), Some(TRIGGER_DIRTY));
    }

    // ── 4-DB coverage ─────────────────────────────

    /// All 4 configured DBs (state + raft + blocks + events) must have
    /// their `last_synced_snapshot_id` advance when the checkpointer
    /// fires. The 2-DB regression test above is preserved; this one adds
    /// events.db + blocks.db coverage.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_advances_all_four_dbs() {
        let dbs = new_test_db_with_events();
        let events_handle =
            Arc::clone(dbs.events.as_ref().expect("events db configured by helper"));
        // Dirty a page on every DB — mirrors the apply pattern where
        // state.db, raft.db, blocks.db, and events.db all receive
        // `commit_in_memory` writes on an applied batch.
        commit_in_memory_one(&dbs.state, b"k1", b"v1");
        commit_in_memory_one(&dbs.raft, b"k1", b"v1");
        commit_in_memory_one(&dbs.blocks, b"k1", b"v1");
        commit_in_memory_one(&events_handle, b"k1", b"v1");
        let state_handle = Arc::clone(&dbs.state);
        let raft_handle = Arc::clone(&dbs.raft);
        let blocks_handle = Arc::clone(&dbs.blocks);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        // Fire a time-triggered checkpoint manually via tick().
        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        cp.tick(&cfg).await;

        assert!(state_handle.last_synced_snapshot_id() > 0, "state.db must advance");
        assert!(raft_handle.last_synced_snapshot_id() > 0, "raft.db must advance");
        assert!(blocks_handle.last_synced_snapshot_id() > 0, "blocks.db must advance");
        assert!(events_handle.last_synced_snapshot_id() > 0, "events.db must advance");
    }

    /// A region configured without an events writer (3-DB case) must
    /// still advance the snapshot id for state + raft + blocks, and the
    /// absence of events_db must NOT prevent the accumulator from
    /// advancing (the `all_ok` check must treat None as vacuously ok).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_advances_three_dbs_when_events_absent() {
        let dbs = new_test_db();
        assert!(dbs.events.is_none(), "helper guarantees events = None");
        commit_in_memory_one(&dbs.state, b"k1", b"v1");
        commit_in_memory_one(&dbs.raft, b"k1", b"v1");
        commit_in_memory_one(&dbs.blocks, b"k1", b"v1");
        let state_handle = Arc::clone(&dbs.state);
        let raft_handle = Arc::clone(&dbs.raft);
        let blocks_handle = Arc::clone(&dbs.blocks);

        let cfg = CheckpointConfig::builder()
            .interval_ms(50)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1_000_000)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        *cp.last_checkpoint_at.lock() = Instant::now() - Duration::from_millis(500);
        cp.tick(&cfg).await;

        assert!(state_handle.last_synced_snapshot_id() > 0, "state.db must advance");
        assert!(raft_handle.last_synced_snapshot_id() > 0, "raft.db must advance");
        assert!(blocks_handle.last_synced_snapshot_id() > 0, "blocks.db must advance");
        // Accumulator must have advanced — proves the "events = None
        // counts as ok" branch of `all_ok` works.
        assert_eq!(
            cp.applies_at_last_checkpoint.load(Ordering::Relaxed),
            0,
            "no applies advance — but a successful 3-DB sync still updates the accumulator"
        );
    }

    /// `max()` trigger: dirtying ONLY blocks.db past the threshold must
    /// fire the dirty-page trigger. The `max()` across all four DBs
    /// catches cases a state.db-only sample would miss.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_max_trigger_fires_on_blocks_db_pressure() {
        let dbs = new_test_db_with_events();
        // Dirty blocks.db only.
        commit_in_memory_one(&dbs.blocks, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            // Dirty threshold is small enough that a single commit-in-memory
            // page lands us above threshold.
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        // Verify the sampled max reflects the blocks-db pressure and that
        // state.db alone would NOT have fired (it's clean).
        assert_eq!(dbs.state.cache_dirty_page_count() as u64, 0);
        assert!(dbs.blocks.cache_dirty_page_count() as u64 >= 1);
        assert!(cp.max_dirty_pages() >= 1);
        assert_eq!(cp.should_checkpoint(&cfg, 0, cp.max_dirty_pages()), Some(TRIGGER_DIRTY));
    }

    /// `max()` trigger: dirtying ONLY events.db past the threshold must
    /// fire the dirty-page trigger. Critical under ingest-heavy
    /// workloads where `IngestExternalEvents` touches only events.db.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_max_trigger_fires_on_events_db_pressure() {
        let dbs = new_test_db_with_events();
        let events_handle =
            Arc::clone(dbs.events.as_ref().expect("events db configured by helper"));
        // Dirty events.db only.
        commit_in_memory_one(&events_handle, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        assert_eq!(dbs.state.cache_dirty_page_count() as u64, 0);
        assert_eq!(dbs.raft.cache_dirty_page_count() as u64, 0);
        assert_eq!(dbs.blocks.cache_dirty_page_count() as u64, 0);
        assert!(events_handle.cache_dirty_page_count() as u64 >= 1);
        assert!(cp.max_dirty_pages() >= 1);
        assert_eq!(cp.should_checkpoint(&cfg, 0, cp.max_dirty_pages()), Some(TRIGGER_DIRTY));
    }

    /// `max()` trigger: dirtying ONLY raft.db past the threshold must
    /// fire — symmetric coverage so no DB can silently drop out of the
    /// trigger computation via a refactor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_max_trigger_fires_on_raft_db_pressure() {
        let dbs = new_test_db_with_events();
        commit_in_memory_one(&dbs.raft, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        assert_eq!(dbs.state.cache_dirty_page_count() as u64, 0);
        assert!(dbs.raft.cache_dirty_page_count() as u64 >= 1);
        assert_eq!(cp.should_checkpoint(&cfg, 0, cp.max_dirty_pages()), Some(TRIGGER_DIRTY));
    }

    /// `max()` trigger: state.db pressure alone also fires — parity
    /// check that the single-DB case still triggers the checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_max_trigger_fires_on_state_db_pressure() {
        let dbs = new_test_db_with_events();
        commit_in_memory_one(&dbs.state, b"k1", b"v1");

        let cfg = CheckpointConfig::builder()
            .interval_ms(60_000)
            .applies_threshold(1_000_000)
            .dirty_pages_threshold(1)
            .build()
            .unwrap();
        let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg.clone(), 0);

        assert!(dbs.state.cache_dirty_page_count() as u64 >= 1);
        assert_eq!(cp.should_checkpoint(&cfg, 0, cp.max_dirty_pages()), Some(TRIGGER_DIRTY));
    }

    // -----------------------------------------------------------------------
    // Apply-accumulator property tests
    // -----------------------------------------------------------------------

    /// Accumulator-reset invariant covered as a proptest.
    ///
    /// `applies_since_last = latest_applied - applies_at_last_checkpoint` must
    /// always be `>= 0` and may only decrease when a checkpoint succeeds. The
    /// existing `applies_count_resets_on_successful_checkpoint_only` unit test
    /// covers one concrete sequence; this proptest sweeps arbitrary
    /// (advance, succeeds) event streams and asserts the invariant holds
    /// across every observable point.
    ///
    /// The property is exercised against the real `StateCheckpointer`
    /// accumulator fields by directly invoking the success branch of
    /// `do_checkpoint`'s accounting (`applies_at_last_checkpoint.store(...)`)
    /// and skipping the store on failure — i.e. simulating the outcome of
    /// `sync_state` without actually calling it. This keeps the proptest
    /// cheap enough to run at the workspace default 256 cases.
    mod proptest_accumulator_invariant {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            // 64 cases balances search breadth against the per-case cost of
            // spinning up a tempdir-backed pair of file DBs plus a tokio
            // current-thread runtime. 256 (workspace default) takes ~3s;
            // 64 keeps this test under ~1s of lib-test wall clock.
            #![proptest_config(ProptestConfig::with_cases(64))]

            #[test]
            fn accumulator_resets_only_on_success(
                events in proptest::collection::vec(
                    (0u64..1000u64, any::<bool>()),
                    1..=50,
                ),
            ) {
                // Spin up a checkpointer bound to a pair of FileBackend DBs.
                // We never call sync_state, so no dual-slot I/O happens —
                // only the in-memory accumulator is touched.
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime");
                rt.block_on(async move {
                    let dbs = new_test_db();
                    let cfg = CheckpointConfig::builder()
                        .interval_ms(60_000)
                        .applies_threshold(1)
                        .dirty_pages_threshold(1_000_000)
                        .build()
                        .unwrap();
                    let (cp, _rc, _tok, _tx) = new_checkpointer(&dbs, cfg, 0);

                    // Reference model: `latest_applied` and `applies_at_last_checkpoint`.
                    let mut latest: u64 = 0;
                    let mut ref_at_last: u64 = 0;
                    let mut prev_diff: u64 = 0;

                    for (advance, succeeds) in events {
                        // Apply the advance.
                        latest = latest.saturating_add(advance);

                        let diff_before_checkpoint =
                            latest.saturating_sub(cp.applies_at_last_checkpoint.load(Ordering::Relaxed));
                        // `applies_since_last` must be monotonically non-negative; saturating_sub
                        // guarantees this trivially, but the assertion documents intent.
                        prop_assert!(
                            diff_before_checkpoint == latest.saturating_sub(ref_at_last),
                            "checkpointer accumulator must match reference model"
                        );

                        // Simulate the success branch of do_checkpoint: on success,
                        // advance applies_at_last_checkpoint to the observed latest.
                        // On failure, leave it alone — exactly what do_checkpoint does.
                        if succeeds {
                            cp.applies_at_last_checkpoint.store(latest, Ordering::Relaxed);
                            ref_at_last = latest;
                        }

                        let diff_after =
                            latest.saturating_sub(cp.applies_at_last_checkpoint.load(Ordering::Relaxed));
                        prop_assert_eq!(
                            diff_after,
                            latest.saturating_sub(ref_at_last),
                            "diff after step must equal reference model"
                        );

                        // The accumulator may only decrease on success. On failure,
                        // it must be >= prev_diff (advance can only grow the gap).
                        if !succeeds {
                            prop_assert!(
                                diff_after >= prev_diff,
                                "failure-only step must never shrink applies_since_last ({} < {})",
                                diff_after, prev_diff
                            );
                        }
                        prev_diff = diff_after;

                        // Global invariant: the accumulator never exceeds latest.
                        prop_assert!(
                            cp.applies_at_last_checkpoint.load(Ordering::Relaxed) <= latest,
                            "applies_at_last_checkpoint may never exceed latest_applied"
                        );
                    }

                    Ok::<(), proptest::test_runner::TestCaseError>(())
                })?;
            }
        }
    }
}
