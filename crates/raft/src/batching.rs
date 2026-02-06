//! Transaction batching for improved write throughput.
//!
//! Per DESIGN.md §6.3: Transaction batching coalesces multiple writes into
//! single Raft proposals to reduce consensus round-trips and improve throughput.
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

use parking_lot::Mutex;
use tokio::{sync::oneshot, time::interval};
use tracing::{debug, info, instrument, warn};

use crate::{
    metrics,
    types::{LedgerRequest, LedgerResponse},
};

/// Configuration for the batch writer.
#[derive(Debug, Clone, bon::Builder)]
pub struct BatchConfig {
    /// Maximum number of writes to batch together.
    #[builder(default = 100)]
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing a batch.
    #[builder(default = Duration::from_millis(2))]
    pub batch_timeout: Duration,
    /// Interval for checking batch timeout.
    #[builder(default = Duration::from_micros(500))]
    pub tick_interval: Duration,
    /// Commit immediately when queue drains to zero.
    ///
    /// Per DESIGN.md §6.3: When true (default), flushes the batch as soon as
    /// the incoming queue is empty rather than waiting for `batch_timeout`.
    /// This optimizes latency for interactive workloads.
    ///
    /// Set to false for batch import workloads where throughput is more
    /// important than latency.
    #[builder(default = true)]
    pub eager_commit: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(2),
            tick_interval: Duration::from_micros(500),
            eager_commit: true,
        }
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
    /// Time when the last write was added (for eager commit detection).
    last_write_at: Option<Instant>,
    /// Number of pending writes at last tick (for eager commit detection).
    pending_at_last_tick: usize,
}

impl BatchState {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            first_pending_at: None,
            last_write_at: None,
            pending_at_last_tick: 0,
        }
    }

    fn push(&mut self, write: PendingWrite) {
        if self.first_pending_at.is_none() {
            self.first_pending_at = Some(write.queued_at);
        }
        self.last_write_at = Some(write.queued_at);
        self.pending.push_back(write);
    }

    fn take_batch(&mut self) -> Vec<PendingWrite> {
        self.first_pending_at = None;
        self.last_write_at = None;
        self.pending_at_last_tick = 0;
        self.pending.drain(..).collect()
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    #[allow(dead_code)] // utility for batch queue inspection
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Check if the batch should be flushed.
    ///
    /// Returns true if:
    /// 1. Batch size reached max_batch_size
    /// 2. Batch timeout elapsed since first pending write
    /// 3. Eager commit enabled AND queue has drained (no new writes since last tick)
    fn should_flush(&self, config: &BatchConfig) -> bool {
        // Always flush if we hit max batch size
        if self.pending.len() >= config.max_batch_size {
            return true;
        }

        // Flush if batch timeout elapsed
        if let Some(first_at) = self.first_pending_at
            && first_at.elapsed() >= config.batch_timeout
        {
            return true;
        }

        // Eager commit: flush if we have pending writes and the queue hasn't grown
        // since the last tick (indicating the input queue has drained)
        if config.eager_commit && !self.pending.is_empty() {
            // If pending count hasn't changed since last tick, the queue is drained
            if self.pending.len() == self.pending_at_last_tick && self.pending_at_last_tick > 0 {
                return true;
            }
        }

        false
    }

    /// Update tracking for eager commit detection.
    /// Called at the start of each tick.
    fn update_tick_tracking(&mut self) {
        self.pending_at_last_tick = self.pending.len();
    }
}

/// A handle for submitting writes to the batch writer.
#[derive(Clone)]
pub struct BatchWriterHandle {
    state: Arc<Mutex<BatchState>>,
    config: BatchConfig,
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

    /// Get the current number of pending writes.
    pub fn pending_count(&self) -> usize {
        self.state.lock().len()
    }
}

/// Batch writer that coalesces writes.
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
    config: BatchConfig,
    /// Function to submit batched requests to Raft.
    submit_fn: F,
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
    /// Create a new batch writer.
    pub fn new(config: BatchConfig, submit_fn: F) -> Self {
        Self { state: Arc::new(Mutex::new(BatchState::new())), config, submit_fn }
    }

    /// Get a handle for submitting writes.
    pub fn handle(&self) -> BatchWriterHandle {
        BatchWriterHandle { state: self.state.clone(), config: self.config.clone() }
    }

    /// Run the batch writer loop.
    ///
    /// This should be spawned as a background task.
    #[instrument(skip(self))]
    pub async fn run(self) {
        let mut ticker = interval(self.config.tick_interval);

        info!(
            max_batch_size = self.config.max_batch_size,
            batch_timeout_ms = self.config.batch_timeout.as_millis(),
            eager_commit = self.config.eager_commit,
            "Starting batch writer"
        );

        loop {
            ticker.tick().await;

            // Check if we should flush
            let (batch, is_eager) = {
                let mut state = self.state.lock();

                // Track whether this is an eager commit (queue drained)
                let is_eager = self.config.eager_commit
                    && !state.pending.is_empty()
                    && state.pending.len() == state.pending_at_last_tick
                    && state.pending_at_last_tick > 0;

                let batch = if state.should_flush(&self.config) {
                    Some(state.take_batch())
                } else {
                    // Update tick tracking for next iteration
                    state.update_tick_tracking();
                    None
                };

                (batch, is_eager)
            };

            if let Some(batch) = batch {
                self.flush_batch(batch, is_eager).await;
            }
        }
    }

    /// Flush a batch of writes.
    async fn flush_batch(&self, batch: Vec<PendingWrite>, is_eager: bool) {
        let batch_size = batch.len();
        if batch_size == 0 {
            return;
        }

        let start = Instant::now();
        debug!(batch_size, is_eager, "Flushing batch");

        // Record batch metrics
        metrics::record_batch_coalesce(batch_size);
        if is_eager {
            metrics::record_eager_commit();
        }

        // Extract requests
        let requests: Vec<LedgerRequest> = batch.iter().map(|w| w.request.clone()).collect();

        // Submit to Raft
        let result = (self.submit_fn)(requests).await;

        let latency = start.elapsed().as_secs_f64();
        metrics::record_batch_flush(latency);

        // Distribute results to waiters
        match result {
            Ok(responses) => {
                if responses.len() != batch_size {
                    warn!(expected = batch_size, got = responses.len(), "Response count mismatch");
                }

                for (write, response) in batch.into_iter().zip(responses.into_iter()) {
                    let _ = write.response_tx.send(Ok(response));
                }

                info!(
                    batch_size,
                    latency_ms = latency * 1000.0,
                    is_eager,
                    "Batch flushed successfully"
                );
            },
            Err(e) => {
                warn!(error = %e, batch_size, "Batch flush failed");
                for write in batch {
                    let _ = write.response_tx.send(Err(BatchError::RaftError(e.clone())));
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn make_request(namespace_id: i64, vault_id: i64) -> LedgerRequest {
        LedgerRequest::Write {
            namespace_id: inferadb_ledger_types::NamespaceId::new(namespace_id),
            vault_id: inferadb_ledger_types::VaultId::new(vault_id),
            transactions: vec![],
        }
    }

    fn make_response(block_height: u64) -> LedgerResponse {
        LedgerResponse::Write { block_height, block_hash: [0u8; 32], assigned_sequence: 1 }
    }

    #[tokio::test]
    async fn test_batch_writer_handle() {
        let config = BatchConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_millis(100),
            tick_interval: Duration::from_millis(10),
            eager_commit: false, // Disable for this test to use timeout-based batching
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(config, move |requests| {
            let count = call_count_clone.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(requests.into_iter().enumerate().map(|(i, _)| make_response(i as u64)).collect())
            })
        });

        let handle = writer.handle();

        // Spawn the writer
        let writer_task = tokio::spawn(writer.run());

        // Submit some writes
        let rx1 = handle.submit(make_request(1, 1));
        let rx2 = handle.submit(make_request(1, 2));
        let rx3 = handle.submit(make_request(1, 3));

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
        let config = BatchConfig {
            max_batch_size: 3,
            batch_timeout: Duration::from_secs(10), // Long timeout
            tick_interval: Duration::from_millis(1),
            eager_commit: false, // Disable to test size-based flushing
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(config, move |requests| {
            let count = call_count_clone.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(requests.into_iter().enumerate().map(|(i, _)| make_response(i as u64)).collect())
            })
        });

        let handle = writer.handle();
        let writer_task = tokio::spawn(writer.run());

        // Submit exactly max_batch_size writes
        let rx1 = handle.submit(make_request(1, 1));
        let rx2 = handle.submit(make_request(1, 2));
        let rx3 = handle.submit(make_request(1, 3));

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
        let config = BatchConfig {
            max_batch_size: 2,
            batch_timeout: Duration::from_millis(10),
            tick_interval: Duration::from_millis(1),
            eager_commit: false, // Disable for basic state tests
        };

        let mut state = BatchState::new();
        assert!(state.is_empty());
        assert!(!state.should_flush(&config));

        // Add one write
        let (tx1, _rx1) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(1, 1),
            response_tx: tx1,
            queued_at: Instant::now(),
        });
        assert_eq!(state.len(), 1);
        assert!(!state.should_flush(&config)); // Not at max size yet

        // Add another to reach max size
        let (tx2, _rx2) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(1, 2),
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

    /// Test that eager_commit flushes when queue drains.
    ///
    /// With eager_commit enabled, if the pending count hasn't grown since the
    /// last tick, we should flush immediately rather than waiting for timeout.
    #[test]
    fn test_eager_commit_detection() {
        let config_eager = BatchConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_secs(10), // Long timeout
            tick_interval: Duration::from_millis(1),
            eager_commit: true,
        };

        let config_no_eager = BatchConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_secs(10), // Long timeout
            tick_interval: Duration::from_millis(1),
            eager_commit: false,
        };

        let mut state = BatchState::new();

        // Add one write
        let (tx1, _rx1) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(1, 1),
            response_tx: tx1,
            queued_at: Instant::now(),
        });

        // First tick: record pending count
        state.update_tick_tracking();
        assert_eq!(state.pending_at_last_tick, 1);

        // With eager=false, should NOT flush (no timeout, not at max size)
        assert!(!state.should_flush(&config_no_eager));

        // With eager=true, should flush because pending count hasn't changed
        // (queue has drained)
        assert!(state.should_flush(&config_eager));

        // Now add another write to simulate more incoming
        let (tx2, _rx2) = oneshot::channel();
        state.push(PendingWrite {
            request: make_request(1, 2),
            response_tx: tx2,
            queued_at: Instant::now(),
        });

        // Even with eager=true, shouldn't flush now because count grew
        // (state.pending.len() = 2, state.pending_at_last_tick = 1)
        assert!(!state.should_flush(&config_eager));

        // Update tick tracking again
        state.update_tick_tracking();
        assert_eq!(state.pending_at_last_tick, 2);

        // Now with eager=true, should flush because queue drained
        assert!(state.should_flush(&config_eager));
    }

    /// Test that eager commit metrics are recorded correctly.
    #[tokio::test]
    async fn test_eager_commit_flushes_quickly() {
        let config = BatchConfig {
            max_batch_size: 100,                    // High limit
            batch_timeout: Duration::from_secs(10), // Very long timeout
            tick_interval: Duration::from_millis(5),
            eager_commit: true, // Enable eager commit
        };

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let writer = BatchWriter::new(config, move |requests| {
            let count = call_count_clone.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(requests.into_iter().enumerate().map(|(i, _)| make_response(i as u64)).collect())
            })
        });

        let handle = writer.handle();
        let writer_task = tokio::spawn(writer.run());

        // Submit a single write
        let rx1 = handle.submit(make_request(1, 1));

        // With eager_commit, should flush quickly (within ~2 ticks) instead of
        // waiting for the 10 second timeout
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should have flushed already due to eager commit
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Eager commit should have flushed the batch"
        );

        // Verify we got the result
        let result = rx1.await;
        assert!(result.is_ok());

        writer_task.abort();
    }

    // =========================================================================
    // Builder API tests (TDD for bon::Builder adoption)
    // =========================================================================

    #[test]
    fn test_batch_config_builder_with_defaults() {
        // builder().build() should produce the same result as Default::default()
        let from_builder = BatchConfig::builder().build();
        let from_default = BatchConfig::default();

        assert_eq!(from_builder.max_batch_size, from_default.max_batch_size);
        assert_eq!(from_builder.batch_timeout, from_default.batch_timeout);
        assert_eq!(from_builder.tick_interval, from_default.tick_interval);
        assert_eq!(from_builder.eager_commit, from_default.eager_commit);
    }

    #[test]
    fn test_batch_config_builder_custom_values() {
        let config = BatchConfig::builder()
            .max_batch_size(500)
            .batch_timeout(Duration::from_millis(50))
            .tick_interval(Duration::from_millis(10))
            .eager_commit(false)
            .build();

        assert_eq!(config.max_batch_size, 500);
        assert_eq!(config.batch_timeout, Duration::from_millis(50));
        assert_eq!(config.tick_interval, Duration::from_millis(10));
        assert!(!config.eager_commit);
    }

    #[test]
    fn test_batch_config_builder_partial_override() {
        // Only override some fields, rest should use defaults
        let config = BatchConfig::builder().max_batch_size(200).eager_commit(false).build();

        assert_eq!(config.max_batch_size, 200);
        assert_eq!(config.batch_timeout, Duration::from_millis(2)); // default
        assert_eq!(config.tick_interval, Duration::from_micros(500)); // default
        assert!(!config.eager_commit);
    }
}
