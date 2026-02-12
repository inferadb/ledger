//! Reconnecting stream wrappers.
//!
//! Provides stream wrappers that automatically reconnect on
//! disconnect and resume from the last position.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream;
use inferadb_ledger_proto::proto;
use tonic::Status;
use tracing::{debug, warn};

use crate::{
    config::RetryPolicy,
    error::{Result, SdkError},
};

/// Position tracker for stream resumption.
///
/// This trait allows the stream to track the position of the last
/// successfully received item for seamless reconnection.
pub trait PositionTracker: Clone + Send + 'static {
    /// The item type being streamed.
    type Item;

    /// Updates position based on received item.
    fn update(&mut self, item: &Self::Item);

    /// Returns the current position for reconnection.
    fn position(&self) -> u64;
}

/// Simple height-based position tracker.
///
/// Tracks the last seen block height for stream resumption.
#[derive(Debug, Clone, Default)]
pub struct HeightTracker {
    last_height: u64,
}

impl HeightTracker {
    /// Creates a new height tracker starting at the given height.
    pub fn new(start_height: u64) -> Self {
        Self { last_height: start_height.saturating_sub(1) }
    }

    /// Returns the last seen height.
    pub fn last_height(&self) -> u64 {
        self.last_height
    }
}

impl PositionTracker for HeightTracker {
    type Item = proto::BlockAnnouncement;

    fn update(&mut self, item: &Self::Item) {
        self.last_height = item.height;
    }

    fn position(&self) -> u64 {
        // Return the next height to resume from
        self.last_height.saturating_add(1)
    }
}

/// A stream wrapper that automatically reconnects on disconnect.
///
/// `ReconnectingStream` wraps an underlying stream and handles:
/// - Automatic reconnection when the stream ends or errors
/// - Exponential backoff between reconnection attempts
/// - Position-based resumption via a callback
///
/// # State Machine
///
/// ```text
/// ┌─────────┐      error/end     ┌──────────────┐
/// │ Active  │ ─────────────────▶ │ Reconnecting │
/// └─────────┘                    └──────────────┘
///     ▲                                │
///     │         reconnect_fn()         │
///     └────────────────────────────────┘
/// ```
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_sdk::streaming::{ReconnectingStream, HeightTracker};
/// use inferadb_ledger_sdk::RetryPolicy;
///
/// let stream = ReconnectingStream::new(
///     initial_stream,
///     HeightTracker::new(start_height),
///     retry_policy,
///     |last_height| async move {
///         // Reconnect and resume from last_height + 1
///         create_stream(last_height + 1).await
///     },
/// );
/// ```
pub struct ReconnectingStream<T, P, F, Fut>
where
    T: Send + 'static,
    P: PositionTracker<Item = T>,
    F: FnMut(u64) -> Fut + Send + 'static,
    Fut: Future<Output = Result<tonic::Streaming<T>>> + Send + 'static,
{
    state: StreamState<T, Fut>,
    position: P,
    retry_policy: RetryPolicy,
    reconnect_fn: F,
    reconnect_attempt: u32,
}

enum StreamState<T, Fut>
where
    T: Send + 'static,
    Fut: Future<Output = Result<tonic::Streaming<T>>> + Send + 'static,
{
    /// Stream is active and producing items.
    Active(Box<tonic::Streaming<T>>),
    /// Waiting for reconnection with backoff.
    Backoff(Pin<Box<tokio::time::Sleep>>),
    /// Reconnecting - awaiting new stream.
    Connecting(Pin<Box<Fut>>),
    /// Terminal state - max reconnection attempts exceeded.
    Exhausted,
    /// Temporary state during transitions.
    Transitioning,
}

impl<T, P, F, Fut> ReconnectingStream<T, P, F, Fut>
where
    T: Send + 'static,
    P: PositionTracker<Item = T>,
    F: FnMut(u64) -> Fut + Send + 'static,
    Fut: Future<Output = Result<tonic::Streaming<T>>> + Send + 'static,
{
    /// Creates a new reconnecting stream.
    ///
    /// # Arguments
    ///
    /// * `stream` - The initial stream to wrap
    /// * `position` - Position tracker for resumption
    /// * `retry_policy` - Policy for reconnection backoff
    /// * `reconnect_fn` - Function to create new stream from position
    pub fn new(
        stream: tonic::Streaming<T>,
        position: P,
        retry_policy: RetryPolicy,
        reconnect_fn: F,
    ) -> Self {
        Self {
            state: StreamState::Active(Box::new(stream)),
            position,
            retry_policy,
            reconnect_fn,
            reconnect_attempt: 0,
        }
    }

    /// Calculates backoff duration for current attempt.
    fn backoff_duration(&self) -> Duration {
        let base = self.retry_policy.initial_backoff;
        let multiplier = self.retry_policy.multiplier;
        let max = self.retry_policy.max_backoff;
        let jitter = self.retry_policy.jitter;

        // Exponential backoff: initial * multiplier^(attempt-1)
        let attempt = self.reconnect_attempt.saturating_sub(1) as f64;
        let delay_secs = base.as_secs_f64() * multiplier.powf(attempt);
        let delay = Duration::from_secs_f64(delay_secs);

        // Cap at max
        let capped = delay.min(max);

        // Apply jitter: ±jitter factor
        apply_jitter(capped, jitter)
    }

    /// Checks if we should retry based on the error.
    fn should_retry(&self, status: &Status) -> bool {
        // Only retry on transient errors
        matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted
        )
    }
}

impl<T, P, F, Fut> Stream for ReconnectingStream<T, P, F, Fut>
where
    T: Send + Unpin + 'static,
    P: PositionTracker<Item = T> + Unpin,
    F: FnMut(u64) -> Fut + Send + Unpin + 'static,
    Fut: Future<Output = Result<tonic::Streaming<T>>> + Send + Unpin + 'static,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Take ownership of state temporarily
            let state = std::mem::replace(&mut self.state, StreamState::Transitioning);

            match state {
                StreamState::Active(mut stream) => {
                    match Pin::new(stream.as_mut()).poll_next(cx) {
                        Poll::Ready(Some(Ok(item))) => {
                            // Update position tracker with new item
                            self.position.update(&item);
                            // Reset reconnect attempts on success
                            self.reconnect_attempt = 0;
                            // Restore stream state
                            self.state = StreamState::Active(stream);
                            return Poll::Ready(Some(Ok(item)));
                        },
                        Poll::Ready(Some(Err(status))) => {
                            // Stream error - check if retryable
                            if self.should_retry(&status)
                                && self.reconnect_attempt < self.retry_policy.max_attempts
                            {
                                self.reconnect_attempt += 1;
                                let backoff = self.backoff_duration();
                                debug!(
                                    attempt = self.reconnect_attempt,
                                    backoff_ms = backoff.as_millis() as u64,
                                    error = %status,
                                    "stream disconnected, reconnecting after backoff"
                                );
                                self.state =
                                    StreamState::Backoff(Box::pin(tokio::time::sleep(backoff)));
                                // Continue loop to start backoff
                                continue;
                            }
                            // Non-retryable or exhausted
                            self.state = StreamState::Exhausted;
                            return Poll::Ready(Some(Err(SdkError::StreamDisconnected {
                                message: status.to_string(),
                            })));
                        },
                        Poll::Ready(None) => {
                            // Stream ended - attempt reconnection
                            if self.reconnect_attempt < self.retry_policy.max_attempts {
                                self.reconnect_attempt += 1;
                                let backoff = self.backoff_duration();
                                debug!(
                                    attempt = self.reconnect_attempt,
                                    backoff_ms = backoff.as_millis() as u64,
                                    "stream ended, reconnecting after backoff"
                                );
                                self.state =
                                    StreamState::Backoff(Box::pin(tokio::time::sleep(backoff)));
                                // Continue loop to start backoff
                                continue;
                            }
                            // Exhausted reconnection attempts
                            self.state = StreamState::Exhausted;
                            return Poll::Ready(None);
                        },
                        Poll::Pending => {
                            self.state = StreamState::Active(stream);
                            return Poll::Pending;
                        },
                    }
                },
                StreamState::Backoff(mut sleep) => {
                    match Pin::new(&mut sleep).poll(cx) {
                        Poll::Ready(()) => {
                            // Backoff complete, start reconnection
                            let position = self.position.position();
                            debug!(position, "backoff complete, initiating reconnection");
                            let fut = (self.reconnect_fn)(position);
                            self.state = StreamState::Connecting(Box::pin(fut));
                            // Continue loop to poll connecting
                            continue;
                        },
                        Poll::Pending => {
                            self.state = StreamState::Backoff(sleep);
                            return Poll::Pending;
                        },
                    }
                },
                StreamState::Connecting(mut fut) => {
                    match Pin::new(&mut fut).poll(cx) {
                        Poll::Ready(Ok(stream)) => {
                            debug!(
                                attempt = self.reconnect_attempt,
                                position = self.position.position(),
                                "reconnection successful"
                            );
                            self.state = StreamState::Active(Box::new(stream));
                            // Continue loop to poll new stream
                            continue;
                        },
                        Poll::Ready(Err(e)) => {
                            // Reconnection failed
                            warn!(
                                attempt = self.reconnect_attempt,
                                error = %e,
                                "reconnection failed"
                            );
                            if self.reconnect_attempt < self.retry_policy.max_attempts {
                                self.reconnect_attempt += 1;
                                let backoff = self.backoff_duration();
                                self.state =
                                    StreamState::Backoff(Box::pin(tokio::time::sleep(backoff)));
                                // Continue loop for another backoff
                                continue;
                            }
                            // Exhausted
                            self.state = StreamState::Exhausted;
                            return Poll::Ready(Some(Err(e)));
                        },
                        Poll::Pending => {
                            self.state = StreamState::Connecting(fut);
                            return Poll::Pending;
                        },
                    }
                },
                StreamState::Exhausted => {
                    self.state = StreamState::Exhausted;
                    return Poll::Ready(None);
                },
                StreamState::Transitioning => {
                    // This should never happen - indicates a bug
                    self.state = StreamState::Exhausted;
                    return Poll::Ready(Some(Err(SdkError::StreamDisconnected {
                        message: "internal state error".to_owned(),
                    })));
                },
            }
        }
    }
}

/// Applies jitter to a duration.
///
/// Jitter is applied as ±factor randomness, e.g., 100ms with 0.25 jitter
/// produces a value in the range [75ms, 125ms].
fn apply_jitter(duration: Duration, factor: f64) -> Duration {
    // Clamp factor to [0.0, 1.0]
    let factor = factor.clamp(0.0, 1.0);

    if factor == 0.0 {
        return duration;
    }

    let base = duration.as_secs_f64();
    let min = base * (1.0 - factor);
    let max = base * (1.0 + factor);

    let jittered = rand::RngExt::random_range(&mut rand::rng(), min..=max);
    Duration::from_secs_f64(jittered)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use super::*;

    /// Simple test tracker that just counts items.
    #[derive(Clone)]
    struct CountTracker {
        count: Arc<AtomicU32>,
    }

    impl CountTracker {
        fn new() -> Self {
            Self { count: Arc::new(AtomicU32::new(0)) }
        }

        fn count(&self) -> u32 {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl PositionTracker for CountTracker {
        type Item = u32;

        fn update(&mut self, _item: &Self::Item) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        fn position(&self) -> u64 {
            self.count.load(Ordering::SeqCst) as u64
        }
    }

    #[test]
    fn test_height_tracker_new() {
        let tracker = HeightTracker::new(10);
        // Last height is start - 1 because we haven't received anything yet
        assert_eq!(tracker.last_height(), 9);
    }

    #[test]
    fn test_height_tracker_new_zero() {
        let tracker = HeightTracker::new(0);
        // saturating_sub prevents underflow
        assert_eq!(tracker.last_height(), 0);
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: 0.0, // No jitter for deterministic test
        };

        // We can't easily test backoff_duration without a full stream setup
        // due to the type parameters, so we'll test apply_jitter separately
        // and verify the backoff logic via integration tests.

        // Test that apply_jitter with 0 factor returns unchanged duration
        let dur = Duration::from_millis(100);
        assert_eq!(apply_jitter(dur, 0.0), dur);

        // Verify policy values are correct
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(10));
    }

    #[test]
    fn test_apply_jitter_zero_factor() {
        let dur = Duration::from_millis(100);
        let result = apply_jitter(dur, 0.0);
        assert_eq!(result, dur);
    }

    #[test]
    fn test_apply_jitter_within_bounds() {
        let dur = Duration::from_millis(100);
        let factor = 0.25;

        // Run multiple times to verify bounds
        for _ in 0..100 {
            let result = apply_jitter(dur, factor);
            let millis = result.as_millis();
            // 100ms ± 25% = 75ms to 125ms
            assert!(millis >= 75, "jittered value {} is below minimum", millis);
            assert!(millis <= 125, "jittered value {} is above maximum", millis);
        }
    }

    #[test]
    fn test_apply_jitter_clamps_factor() {
        let dur = Duration::from_millis(100);

        // Factor > 1.0 should be clamped
        for _ in 0..100 {
            let result = apply_jitter(dur, 1.5);
            let millis = result.as_millis();
            // Clamped to 1.0: 0ms to 200ms
            assert!(millis <= 200, "jittered value {} exceeds clamped maximum", millis);
        }
    }

    #[tokio::test]
    async fn test_count_tracker_updates() {
        let mut tracker = CountTracker::new();
        assert_eq!(tracker.count(), 0);
        assert_eq!(tracker.position(), 0);

        tracker.update(&42);
        assert_eq!(tracker.count(), 1);
        assert_eq!(tracker.position(), 1);

        tracker.update(&43);
        assert_eq!(tracker.count(), 2);
        assert_eq!(tracker.position(), 2);
    }

    // =========================================================================
    // HeightTracker with BlockAnnouncement Tests
    // =========================================================================

    #[test]
    fn test_height_tracker_update_with_block_announcement() {
        let mut tracker = HeightTracker::new(1);

        // Initially, position should be 1 (next height to resume from)
        assert_eq!(tracker.position(), 1);

        // Create a block announcement at height 5
        let announcement = proto::BlockAnnouncement {
            namespace_id: Some(proto::NamespaceId { id: 1 }),
            vault_id: Some(proto::VaultId { id: 2 }),
            height: 5,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        tracker.update(&announcement);

        // After update, last_height should be 5
        assert_eq!(tracker.last_height(), 5);
        // Position should return 6 (next height to resume from)
        assert_eq!(tracker.position(), 6);
    }

    #[test]
    fn test_height_tracker_updates_incrementally() {
        let mut tracker = HeightTracker::new(1);

        for height in 1..=10 {
            let announcement = proto::BlockAnnouncement {
                namespace_id: Some(proto::NamespaceId { id: 1 }),
                vault_id: Some(proto::VaultId { id: 0 }),
                height,
                block_hash: None,
                state_root: None,
                timestamp: None,
            };

            tracker.update(&announcement);

            assert_eq!(tracker.last_height(), height);
            assert_eq!(tracker.position(), height + 1);
        }
    }

    #[test]
    fn test_height_tracker_position_for_resumption() {
        // When starting at height 100, last_height is 99 (haven't received 100 yet)
        let tracker = HeightTracker::new(100);
        assert_eq!(tracker.last_height(), 99);
        // position() returns the next height to resume from
        assert_eq!(tracker.position(), 100);
    }

    #[test]
    fn test_height_tracker_handles_height_zero_announcement() {
        let mut tracker = HeightTracker::new(1);

        // Height 0 is technically invalid but tracker should handle it gracefully
        let announcement = proto::BlockAnnouncement {
            namespace_id: Some(proto::NamespaceId { id: 1 }),
            vault_id: Some(proto::VaultId { id: 0 }),
            height: 0,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        tracker.update(&announcement);

        assert_eq!(tracker.last_height(), 0);
        // position uses saturating_add, so 0 + 1 = 1
        assert_eq!(tracker.position(), 1);
    }

    // Note: Integration tests for reconnection behavior will require
    // a mock tonic::Streaming, which is challenging to create directly.
    // These tests will be added in Task 18 (Mock Server) or Task 19 (Integration Tests).
}
