//! Transaction batching for improved write throughput.
//!
//! Transaction batching coalesces multiple writes into single Raft proposals
//! to reduce consensus round-trips and improve throughput.
//!
//! ## Architecture
//!
//! The `BatchWriter` collects incoming writes and flushes them either:
//! - When the batch reaches `max_batch_size`
//! - When `batch_timeout` elapses since the first pending write
//!
//! Each caller receives a `oneshot::Receiver` that resolves when their
//! write is committed (or fails).

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_types::config::BatchConfig;
use parking_lot::Mutex;
use tokio::{
    sync::{Semaphore, oneshot},
    time::interval,
};
use tracing::{debug, info, instrument, warn};

use crate::{
    metrics,
    types::{LedgerRequest, LedgerResponse},
};

/// Configuration for the batch writer.
///
/// ## Tuning tradeoffs
///
/// Batching amortizes the cost of a single WAL `fsync` across many proposals.
/// Under concurrent load the bottleneck is fsync-per-commit, so defaults favor
/// throughput over single-client latency:
///
/// - Larger `max_batch_size` lets concurrent bursts coalesce into one fsync.
/// - Longer `batch_timeout` gives proposals more time to accumulate before the timer-triggered
///   flush caps added latency under single-client load.
/// - Longer `tick_interval` reduces the probability that an isolated proposal is flushed on its own
///   inside a sub-millisecond tick window.
#[derive(Debug, Clone, bon::Builder)]
pub struct BatchWriterConfig {
    /// Maximum number of writes to batch together.
    ///
    /// Upper bound on a single batch. Once reached, `should_flush` returns
    /// true immediately regardless of `batch_timeout`. Caps memory growth
    /// under bursty concurrent load; also caps the size of a single Raft
    /// proposal. Default raised to 2000 alongside the static
    /// [`BatchConfig`][inferadb_ledger_types::config::BatchConfig] default —
    /// under `barrier` WAL sync the cap, not fsync latency, becomes the
    /// binding constraint on amortization.
    #[builder(default = 2000)]
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing a partial batch.
    ///
    /// Caps added latency when proposals arrive one at a time. Under
    /// single-client load this is the effective per-request overhead before
    /// the fsync fires; under concurrent load `max_batch_size` usually wins
    /// first.
    #[builder(default = Duration::from_millis(10))]
    pub batch_timeout: Duration,
    /// Interval for checking batch timeout.
    ///
    /// The `run` loop wakes on this cadence to re-evaluate `should_flush`.
    /// Shorter intervals mean tighter timeout granularity but burn more CPU
    /// and — historically, when eager-commit flushed on queue-drain — caused
    /// isolated proposals to fsync on their own. 5ms is a throughput-biased
    /// compromise; lower it if single-client latency SLOs are tight.
    #[builder(default = Duration::from_millis(5))]
    pub tick_interval: Duration,
    /// Maximum number of batches in-flight concurrently.
    ///
    /// `run()` spawns each drained batch as an independent tokio task and
    /// bounds the spawn count with a semaphore of this size. Values > 1
    /// pipeline batches through Raft (propose + commit + apply + response)
    /// — the BatchWriter keeps forming new batches while earlier ones are
    /// still applying. Raft preserves log ordering regardless of in-flight
    /// count (the engine serializes `propose` calls by log index). Default:
    /// 8. Set to 1 to restore the pre-pipeline serial behaviour.
    #[builder(default = 8)]
    pub max_in_flight_batches: usize,
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Minimum tick interval derived from a [`BatchConfig`].
///
/// Guards against degenerate cases where `batch_timeout` is very small (or zero)
/// and `batch_timeout / 2` rounds down to a sub-millisecond tick that burns CPU
/// without meaningfully tightening timeout granularity.
const DERIVED_TICK_INTERVAL_MIN: Duration = Duration::from_millis(1);

/// Maximum tick interval derived from a [`BatchConfig`].
///
/// Caps `batch_timeout / 2` when operators configure a very long `batch_timeout`
/// — above 10ms the loop is no longer "timeout-granularity" bound, it's just a
/// background poller, and a longer tick adds latency without throughput gain.
const DERIVED_TICK_INTERVAL_MAX: Duration = Duration::from_millis(10);

impl From<&BatchConfig> for BatchWriterConfig {
    /// Converts the operator-visible [`BatchConfig`] into the runtime-internal
    /// [`BatchWriterConfig`] consumed by [`BatchWriter`].
    ///
    /// `max_batch_size` and `batch_timeout` are copied verbatim. `BatchConfig`
    /// does not expose `tick_interval` — it's an internal polling knob — so it
    /// is derived as `batch_timeout / 2`, clamped to
    /// `[DERIVED_TICK_INTERVAL_MIN, DERIVED_TICK_INTERVAL_MAX]`. With the
    /// default `batch_timeout` of 10ms this produces a 5ms tick, matching the
    /// [`BatchWriterConfig::default()`] value.
    fn from(config: &BatchConfig) -> Self {
        let derived_tick =
            (config.batch_timeout / 2).clamp(DERIVED_TICK_INTERVAL_MIN, DERIVED_TICK_INTERVAL_MAX);
        Self::builder()
            .max_batch_size(config.max_batch_size)
            .batch_timeout(config.batch_timeout)
            .tick_interval(derived_tick)
            .max_in_flight_batches(config.max_in_flight_batches)
            .build()
    }
}

/// A pending write waiting to be batched.
struct PendingWrite {
    /// The write request.
    request: LedgerRequest,
    /// Channel to send the result.
    response_tx: oneshot::Sender<Result<LedgerResponse, BatchError>>,
    /// When this write was queued.
    queued_at: Instant,
}

/// Error type for batch operations.
#[derive(Debug, Clone)]
pub enum BatchError {
    /// The batch was dropped before completion.
    Dropped,
    /// Raft consensus failed.
    RaftError(String),
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::Dropped => write!(f, "batch dropped"),
            BatchError::RaftError(e) => write!(f, "raft error: {}", e),
            BatchError::Internal(e) => write!(f, "internal error: {}", e),
        }
    }
}

impl std::error::Error for BatchError {}

/// Shared state for the batch writer.
struct BatchState {
    /// Pending writes.
    pending: VecDeque<PendingWrite>,
    /// Time when the first pending write was queued.
    first_pending_at: Option<Instant>,
}

impl BatchState {
    fn new() -> Self {
        Self { pending: VecDeque::new(), first_pending_at: None }
    }

    fn push(&mut self, write: PendingWrite) {
        if self.first_pending_at.is_none() {
            self.first_pending_at = Some(write.queued_at);
        }
        self.pending.push_back(write);
    }

    fn take_batch(&mut self) -> Vec<PendingWrite> {
        self.first_pending_at = None;
        self.pending.drain(..).collect()
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    #[allow(dead_code)] // utility for batch queue inspection
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Checks if the batch should be flushed.
    ///
    /// Returns true if:
    /// 1. Batch size reached `max_batch_size` (short-circuit to cap batch size / memory /
    ///    single-proposal payload), OR
    /// 2. `batch_timeout` elapsed since the first pending write arrived.
    ///
    /// There is deliberately no "queue drained" predicate: under concurrent
    /// load the queue is drained on every tick (clients wait on prior
    /// responses before submitting), so a drain-based trigger fires per-tick
    /// and defeats batching. Timer-only flushing amortizes WAL fsyncs across
    /// concurrent bursts; `batch_timeout` caps added latency for single-client
    /// workloads.
    fn should_flush(&self, config: &BatchWriterConfig) -> bool {
        // Always flush if we hit max batch size.
        if self.pending.len() >= config.max_batch_size {
            return true;
        }

        // Flush if batch timeout elapsed.
        if let Some(first_at) = self.first_pending_at
            && first_at.elapsed() >= config.batch_timeout
        {
            return true;
        }

        false
    }
}

/// A handle for submitting writes to the batch writer.
#[derive(Clone)]
pub struct BatchWriterHandle {
    state: Arc<Mutex<BatchState>>,
    config: BatchWriterConfig,
}

impl BatchWriterHandle {
    /// Submit a write to be batched.
    ///
    /// Returns a receiver that will receive the result when the write is committed.
    pub fn submit(
        &self,
        request: LedgerRequest,
    ) -> oneshot::Receiver<Result<LedgerResponse, BatchError>> {
        let (tx, rx) = oneshot::channel();

        let write = PendingWrite { request, response_tx: tx, queued_at: Instant::now() };

        let should_flush = {
            let mut state = self.state.lock();
            state.push(write);
            state.should_flush(&self.config)
        };

        if should_flush {
            debug!("Batch threshold reached, flush will occur on next tick");
        }

        rx
    }

    /// Returns the current number of pending writes.
    pub fn pending_count(&self) -> usize {
        self.state.lock().len()
    }
}

/// Batch writer that coalesces individual write proposals into batches for Raft consensus.
pub struct BatchWriter<F>
where
    F: Fn(
            Vec<LedgerRequest>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<LedgerResponse>, String>>
        + Send
        + Sync
        + 'static,
{
    state: Arc<Mutex<BatchState>>,
    config: BatchWriterConfig,
    /// Function to submit batched requests to Raft.
    submit_fn: F,
    /// Region identifier for metric labels.
    region: String,
}

impl<F> BatchWriter<F>
where
    F: Fn(
            Vec<LedgerRequest>,
        ) -> futures::future::BoxFuture<'static, Result<Vec<LedgerResponse>, String>>
        + Send
        + Sync
        + 'static,
{
    /// Creates a new batch writer.
    pub fn new(config: BatchWriterConfig, submit_fn: F, region: impl Into<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(BatchState::new())),
            config,
            submit_fn,
            region: region.into(),
        }
    }

    /// Returns a handle for submitting writes.
    pub fn handle(&self) -> BatchWriterHandle {
        BatchWriterHandle { state: self.state.clone(), config: self.config.clone() }
    }

    /// Runs the batch writer loop.
    ///
    /// This should be spawned as a background task. Runs an infinite loop
    /// that processes batches on each tick until the task is cancelled or
    /// the process exits.
    ///
    /// # Pipelining
    ///
    /// Each drained batch is handed off to a spawned task (gated by a
    /// semaphore sized `max_in_flight_batches`) rather than awaited inline.
    /// This lets the run loop keep forming and proposing new batches while
    /// earlier batches are still committing + applying. Raft serializes
    /// `propose` calls internally by log index, so ordering is preserved
    /// regardless of spawn order. Setting `max_in_flight_batches = 1`
    /// restores the pre-pipeline serial behaviour — used by
    /// [`test_utils::config::test_batch_config`] for deterministic tests
    /// that assume strict before/after apply ordering.
    #[instrument(skip(self))]
    pub async fn run(self) {
        let mut ticker = interval(self.config.tick_interval);
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight_batches.max(1)));
        let this = Arc::new(self);

        info!(
            max_batch_size = this.config.max_batch_size,
            batch_timeout_ms = this.config.batch_timeout.as_millis(),
            tick_interval_us = this.config.tick_interval.as_micros(),
            max_in_flight_batches = this.config.max_in_flight_batches,
            "Starting batch writer"
        );

        loop {
            ticker.tick().await;

            let batch = {
                let mut state = this.state.lock();

                // Emit batch queue depth gauge for SLI monitoring.
                metrics::set_batch_queue_depth(state.pending.len(), &this.region);

                if state.should_flush(&this.config) { Some(state.take_batch()) } else { None }
            };

            if let Some(batch) = batch {
                // Bounded pipelining: block the tick until a permit frees up so
                // the queue cannot accumulate indefinitely if Raft apply falls
                // behind. Permits are owned by the spawned task and released
                // once the batch's responses are distributed.
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_closed) => break,
                };
                let this = this.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    this.flush_batch(batch).await;
                });
            }
        }
    }

    /// Flushes a batch of writes.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            batch_size = batch.len(),
            region = %self.region,
        )
    )]
    async fn flush_batch(&self, batch: Vec<PendingWrite>) {
        let batch_size = batch.len();
        if batch_size == 0 {
            return;
        }

        let start = Instant::now();
        debug!(batch_size, "Flushing batch");

        // Record batch metrics.
        metrics::record_batch_coalesce(batch_size);

        // Destructure batch into requests and response senders (zero clones).
        let (requests, senders): (Vec<LedgerRequest>, Vec<_>) =
            batch.into_iter().map(|w| (w.request, w.response_tx)).unzip();

        // Submit to Raft.
        let result = (self.submit_fn)(requests).await;

        let latency = start.elapsed().as_secs_f64();
        metrics::record_batch_flush(latency);

        // Distribute results to waiters.
        match result {
            Ok(responses) => {
                if responses.len() != batch_size {
                    warn!(expected = batch_size, got = responses.len(), "Response count mismatch");
                }

                for (sender, response) in senders.into_iter().zip(responses) {
                    let _ = sender.send(Ok(response));
                }

                info!(batch_size, latency_ms = latency * 1000.0, "Batch flushed successfully");
            },
            Err(e) => {
                warn!(error = %e, batch_size, "Batch flush failed");
                for sender in senders {
                    let _ = sender.send(Err(BatchError::RaftError(e.clone())));
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use inferadb_ledger_types::{OrganizationId, VaultId};

    use super::*;

    fn org(n: i64) -> OrganizationId {
        OrganizationId::new(n)
    }

    fn vault(n: i64) -> VaultId {
        VaultId::new(n)
    }

    fn make_request(organization: OrganizationId, vault: VaultId) -> LedgerRequest {
        LedgerRequest::Write {
            organization,
            vault,
            transactions: vec![],
            idempotency_key: [0; 16],
            request_hash: 0,
        }
    }

    fn make_response(block_height: u64) -> LedgerResponse {
        LedgerResponse::Write { block_height, block_hash: [0u8; 32], assigned_sequence: 1 }
    }

    #[tokio::test]
    async fn test_batch_writer_handle() {
        let config = BatchWriterConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_millis(100),
            tick_interval: Duration::from_millis(10),
        max_in_flight_batches: 1,
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(
            config,
            move |requests| {
                let count = call_count_clone.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(requests
                        .into_iter()
                        .enumerate()
                        .map(|(i, _)| make_response(i as u64))
                        .collect())
                })
            },
            "global",
        );

        let handle = writer.handle();

        // Spawn the writer
        let writer_task = tokio::spawn(writer.run());

        // Submit some writes
        let rx1 = handle.submit(make_request(org(1), vault(1)));
        let rx2 = handle.submit(make_request(org(1), vault(2)));
        let rx3 = handle.submit(make_request(org(1), vault(3)));

        // Wait for batch timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Check results
        let result1 = rx1.await;
        assert!(result1.is_ok());

        let result2 = rx2.await;
        assert!(result2.is_ok());

        let result3 = rx3.await;
        assert!(result3.is_ok());

        // Should have been batched together (1 call)
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        writer_task.abort();
    }

    #[tokio::test]
    async fn test_batch_flushes_on_size() {
        let config = BatchWriterConfig {
            max_batch_size: 3,
            batch_timeout: Duration::from_secs(10), // Long timeout
            tick_interval: Duration::from_millis(1),
        max_in_flight_batches: 1,
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(
            config,
            move |requests| {
                let count = call_count_clone.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(requests
                        .into_iter()
                        .enumerate()
                        .map(|(i, _)| make_response(i as u64))
                        .collect())
                })
            },
            "global",
        );

        let handle = writer.handle();
        let writer_task = tokio::spawn(writer.run());

        // Submit exactly max_batch_size writes
        let rx1 = handle.submit(make_request(org(1), vault(1)));
        let rx2 = handle.submit(make_request(org(1), vault(2)));
        let rx3 = handle.submit(make_request(org(1), vault(3)));

        // Should flush immediately due to size
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(rx1.await.is_ok());
        assert!(rx2.await.is_ok());
        assert!(rx3.await.is_ok());

        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        writer_task.abort();
    }

    #[test]
    fn test_batch_state() {
        let config = BatchWriterConfig {
            max_batch_size: 2,
            batch_timeout: Duration::from_millis(10),
            tick_interval: Duration::from_millis(1),
        max_in_flight_batches: 1,
        };

        let mut state = BatchState::new();
        assert!(state.is_empty());
        assert!(!state.should_flush(&config));

        // Add one write
        let (tx1, _rx1) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(org(1), vault(1)),
            response_tx: tx1,
            queued_at: Instant::now(),
        });
        assert_eq!(state.len(), 1);
        assert!(!state.should_flush(&config)); // Not at max size yet

        // Add another to reach max size
        let (tx2, _rx2) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(org(1), vault(2)),
            response_tx: tx2,
            queued_at: Instant::now(),
        });
        assert_eq!(state.len(), 2);
        assert!(state.should_flush(&config)); // Now at max size

        // Take the batch
        let batch = state.take_batch();
        assert_eq!(batch.len(), 2);
        assert!(state.is_empty());
    }

    /// Timer-only flushing: a lone pending proposal waits for `batch_timeout`
    /// before fsync'ing, and additional concurrent proposals accumulate into
    /// the same batch.
    #[test]
    fn test_timer_only_flush_does_not_fire_on_drain() {
        let config = BatchWriterConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_secs(10), // Long timeout
            tick_interval: Duration::from_millis(1),
        max_in_flight_batches: 1,
        };

        let mut state = BatchState::new();

        // Add one write.
        let (tx1, _rx1) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(org(1), vault(1)),
            response_tx: tx1,
            queued_at: Instant::now(),
        });

        // With only timer-based flushing, a single pending write does NOT
        // flush until `batch_timeout` elapses — even though the queue has
        // "drained" (no new arrivals). Under concurrent load, clients wait
        // on responses before submitting, so the drained-queue predicate
        // fires on every tick and forces one-proposal-per-fsync behavior.
        assert!(!state.should_flush(&config));

        // Add another write — still under max_batch_size, still under
        // batch_timeout, so still don't flush.
        let (tx2, _rx2) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(org(1), vault(2)),
            response_tx: tx2,
            queued_at: Instant::now(),
        });
        assert!(!state.should_flush(&config));
    }

    /// Flushing on timeout — once `batch_timeout` elapses since the first
    /// pending write, `should_flush` returns true even if the batch is well
    /// below `max_batch_size`.
    #[tokio::test]
    async fn test_batch_timeout_flushes() {
        let config = BatchWriterConfig {
            max_batch_size: 100,                      // High limit
            batch_timeout: Duration::from_millis(20), // Short timeout
            tick_interval: Duration::from_millis(5),
        max_in_flight_batches: 1,
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(
            config,
            move |requests| {
                let count = call_count_clone.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(requests
                        .into_iter()
                        .enumerate()
                        .map(|(i, _)| make_response(i as u64))
                        .collect())
                })
            },
            "global",
        );

        let handle = writer.handle();
        let writer_task = tokio::spawn(writer.run());

        // Submit a single write.
        let rx1 = handle.submit(make_request(org(1), vault(1)));

        // Wait for the timeout to elapse plus a few tick intervals.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Timer-based flush should have fired.
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "batch_timeout should have triggered flush"
        );

        let result = rx1.await;
        assert!(result.is_ok());

        writer_task.abort();
    }

    // =========================================================================
    // From<&BatchConfig> tests (operator config → runtime config)
    // =========================================================================

    // `BatchConfig::builder().build()` returns `Result`; these tests unwrap
    // with `.expect()` because each input is a literal known to validate.
    #[allow(clippy::expect_used)]
    mod from_batch_config {
        use super::*;

        #[test]
        fn defaults_match_batch_writer_config_defaults() {
            // Default BatchConfig (post-Fix-#1: 500 / 10ms) should produce a
            // BatchWriterConfig matching its own defaults (500 / 10ms / 5ms).
            let src = BatchConfig::default();
            let dst = BatchWriterConfig::from(&src);
            assert_eq!(dst.max_batch_size, src.max_batch_size);
            assert_eq!(dst.batch_timeout, src.batch_timeout);
            assert_eq!(dst.tick_interval, Duration::from_millis(5));
        }

        #[test]
        fn custom_timeout_uses_half_as_tick() {
            // batch_timeout / 2 falls inside [1ms, 10ms] — use it directly.
            let src = BatchConfig::builder()
                .max_batch_size(200)
                .batch_timeout(Duration::from_millis(16))
                .build()
                .expect("valid batch config");
            let dst = BatchWriterConfig::from(&src);
            assert_eq!(dst.max_batch_size, 200);
            assert_eq!(dst.batch_timeout, Duration::from_millis(16));
            assert_eq!(dst.tick_interval, Duration::from_millis(8));
        }

        #[test]
        fn clamps_tick_lower_bound() {
            // batch_timeout / 2 < 1ms should be clamped up to 1ms.
            let src = BatchConfig::builder()
                .batch_timeout(Duration::from_micros(500))
                .build()
                .expect("valid batch config");
            let dst = BatchWriterConfig::from(&src);
            assert_eq!(dst.tick_interval, DERIVED_TICK_INTERVAL_MIN);
        }

        #[test]
        fn clamps_tick_upper_bound() {
            // batch_timeout / 2 > 10ms should be clamped down to 10ms.
            let src = BatchConfig::builder()
                .batch_timeout(Duration::from_millis(100))
                .build()
                .expect("valid batch config");
            let dst = BatchWriterConfig::from(&src);
            assert_eq!(dst.tick_interval, DERIVED_TICK_INTERVAL_MAX);
        }
    }

    // =========================================================================
    // Proptest: BatchWriter preserves FIFO submission order
    // =========================================================================
    //
    // `BatchState::should_flush` dropped `eager_commit` and retuned defaults.
    // The existing `test_batch_state` and
    // `test_timer_only_flush_does_not_fire_on_drain` cover size/timer triggers;
    // this proptest pins the orthogonal invariant the apply pipeline depends
    // on: when N proposals are submitted serially via
    // `BatchWriterHandle::submit`, the flusher delivers them to the `submit_fn`
    // in the same order. A regression here would silently reorder log entries
    // in a Raft proposal and corrupt state-machine determinism across replicas.
    //
    // Runtime: 32 cases × one 2-worker tokio runtime + one short batch-timeout
    // wait per case. Each case caps at 50 proposals — keeps total proptest wall
    // clock under a couple of seconds for the full libtest run.
    //
    // `.expect()` appears inside the proptest case body for tokio runtime
    // construction and oneshot receive — both are infrastructure failures that
    // are legitimate test panics, not Result handling deficiencies.
    #[allow(clippy::expect_used)]
    mod proptest_fifo {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use proptest::prelude::*;

        use super::*;

        fn make_indexed_request(idx: u64) -> LedgerRequest {
            LedgerRequest::Write {
                organization: OrganizationId::new(idx as i64),
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
            }
        }

        // Static counter so each proptest case uses distinct organization ids
        // across cases — belt-and-braces in case a case's observed vector is
        // leaked into the next case via Arc sharing (it isn't, but this makes
        // the invariant trivially true).
        static CASE_BASE: AtomicUsize = AtomicUsize::new(1);

        proptest! {
            // 32 cases is the same cadence as `proptest_flush_correctness` in
            // event_writer.rs — each case spins a fresh tokio runtime, so we
            // keep iteration count low. The FIFO invariant is a single
            // logical property: more cases would shrink tighter but not
            // broaden coverage.
            #![proptest_config(ProptestConfig::with_cases(32))]

            /// Property: for any serial submission of 1..=50 proposals to
            /// a single `BatchWriterHandle`, the flusher's `submit_fn`
            /// observes them in the exact submission order.
            #[test]
            fn batch_writer_preserves_proposal_order(n in 1u64..=50u64) {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    let config = BatchWriterConfig {
                        max_batch_size: 64,
                        batch_timeout: Duration::from_millis(20),
                        tick_interval: Duration::from_millis(2),
                    max_in_flight_batches: 1,
                    };

                    // Capturing `submit_fn` — records each request's
                    // organization id (used by the test as the submission
                    // index) in the order the flusher hands them over.
                    // BatchWriter pipes batches through this closure in
                    // `take_batch()` order, which is push order in the
                    // internal VecDeque, which is `submit` order.
                    let observed: Arc<Mutex<Vec<u64>>> =
                        Arc::new(Mutex::new(Vec::new()));
                    let observed_clone = Arc::clone(&observed);

                    let submit_fn = move |requests: Vec<LedgerRequest>| {
                        let observed = Arc::clone(&observed_clone);
                        let responses: Vec<LedgerResponse> = requests
                            .iter()
                            .enumerate()
                            .map(|(i, _)| LedgerResponse::Write {
                                block_height: i as u64,
                                block_hash: [0u8; 32],
                                assigned_sequence: 1,
                            })
                            .collect();

                        for req in &requests {
                            if let LedgerRequest::Write { organization, .. } = req {
                                observed.lock().push(organization.value() as u64);
                            }
                        }

                        Box::pin(async move { Ok(responses) })
                            as futures::future::BoxFuture<
                                'static,
                                Result<Vec<LedgerResponse>, String>,
                            >
                    };

                    let writer = BatchWriter::new(config, submit_fn, "proptest");
                    let handle = writer.handle();
                    let writer_task = tokio::spawn(writer.run());

                    let base =
                        CASE_BASE.fetch_add(100, Ordering::SeqCst) as u64;
                    let expected: Vec<u64> =
                        (0..n).map(|i| base + i).collect();

                    // Serial submission: each call returns an oneshot rx
                    // we hold to completion. Because submission is serial
                    // from a single task, the mutex-guarded VecDeque
                    // preserves push order by construction — the property
                    // under test is that `run()`'s take_batch + submit_fn
                    // chain preserves that order too.
                    let mut rxs = Vec::with_capacity(n as usize);
                    for idx in &expected {
                        rxs.push(handle.submit(make_indexed_request(*idx)));
                    }

                    // Await each rx in submission order — this also
                    // guarantees the flusher has drained every batch
                    // before we inspect `observed`.
                    for rx in rxs {
                        let _ = rx.await.expect("oneshot");
                    }

                    writer_task.abort();

                    let actual = observed.lock().clone();
                    prop_assert_eq!(
                        actual.len(),
                        expected.len(),
                        "flusher should observe every submitted proposal exactly once"
                    );
                    prop_assert_eq!(
                        actual,
                        expected,
                        "BatchWriter must preserve serial submission order"
                    );

                    Ok(())
                }).expect("proptest case body")
            }
        }
    }
}
