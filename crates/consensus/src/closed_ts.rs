//! Closed timestamp tracking for zero-hop follower reads.
//!
//! The leader computes a closed timestamp — a point in time before which no
//! future writes can occur — and piggybacks it on AppendEntries heartbeats.
//! Followers store this timestamp and use it to determine whether a stale read
//! can be served locally without contacting the leader.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Tracks the closed timestamp for a shard.
///
/// The closed timestamp represents a point in time before which the leader
/// guarantees no new writes will be assigned. Followers can serve reads at
/// timestamps at or before the closed timestamp without forwarding to the
/// leader.
#[derive(Debug)]
pub struct ClosedTimestampTracker {
    /// Closed timestamp in nanoseconds relative to [`epoch`](Self::epoch).
    closed_nanos: AtomicU64,
    /// Process-local epoch for elapsed-time computation.
    epoch: Instant,
    /// How far behind `now` the closed timestamp trails on the leader.
    default_lag: Duration,
}

impl ClosedTimestampTracker {
    /// Creates a new tracker with the given default lag.
    ///
    /// The lag determines how far behind real time the closed timestamp trails
    /// on the leader. A larger lag gives more headroom for in-flight writes but
    /// increases the staleness visible to follower reads.
    pub fn new(default_lag: Duration) -> Self {
        Self { closed_nanos: AtomicU64::new(0), epoch: Instant::now(), default_lag }
    }

    /// Stores a new closed timestamp received from the leader.
    ///
    /// Called on followers when processing an AppendEntries heartbeat.
    pub fn update(&self, closed_ts_nanos: u64) {
        self.closed_nanos.store(closed_ts_nanos, Ordering::Release);
    }

    /// Returns the current closed timestamp in nanoseconds.
    pub fn closed_ts_nanos(&self) -> u64 {
        self.closed_nanos.load(Ordering::Acquire)
    }

    /// Computes the closed timestamp on the leader: `now - default_lag`.
    pub fn compute_leader_closed_ts(&self) -> u64 {
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        now_nanos.saturating_sub(self.default_lag.as_nanos() as u64)
    }

    /// Returns whether a read with the given maximum staleness can be served
    /// from the local replica without contacting the leader.
    ///
    /// A read is servable when the closed timestamp is at least as recent as
    /// `now - max_staleness_nanos`.
    pub fn can_serve_read(&self, max_staleness_nanos: u64) -> bool {
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        let target = now_nanos.saturating_sub(max_staleness_nanos);
        self.closed_ts_nanos() >= target
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_closed_ts_is_zero() {
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(3));
        assert_eq!(tracker.closed_ts_nanos(), 0);
    }

    #[test]
    fn update_stores_and_reads_back() {
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(3));
        tracker.update(42_000_000);
        assert_eq!(tracker.closed_ts_nanos(), 42_000_000);

        tracker.update(99_000_000);
        assert_eq!(tracker.closed_ts_nanos(), 99_000_000);
    }

    #[test]
    fn update_is_not_monotonic_by_itself() {
        // The tracker stores whatever the leader sends; monotonicity is the
        // leader's responsibility, not the tracker's.
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(3));
        tracker.update(100);
        tracker.update(50);
        assert_eq!(tracker.closed_ts_nanos(), 50);
    }

    #[test]
    fn can_serve_read_with_sufficient_staleness() {
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(3));

        // Set closed_ts to a value close to `now` from the tracker's epoch.
        let approx_now = tracker.epoch.elapsed().as_nanos() as u64;
        tracker.update(approx_now);

        // With a max staleness of 5 seconds, the closed timestamp (approx now)
        // is well above the target (now - 5s), so the read should be servable.
        assert!(tracker.can_serve_read(5_000_000_000));
    }

    #[test]
    fn cannot_serve_read_when_closed_ts_far_behind() {
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(3));

        // Busy-spin until the epoch has advanced at least 1 microsecond.
        // This avoids thread::sleep while guaranteeing elapsed > 0.
        while tracker.epoch.elapsed() < Duration::from_micros(1) {}

        // closed_ts is 0 (never updated). With 0 max staleness,
        // target = now_nanos, so closed_ts(0) < target fails.
        assert!(!tracker.can_serve_read(0));
    }

    #[test]
    fn leader_closed_ts_is_at_most_now() {
        let lag = Duration::from_secs(3);
        let tracker = ClosedTimestampTracker::new(lag);

        let closed = tracker.compute_leader_closed_ts();
        let now_nanos = tracker.epoch.elapsed().as_nanos() as u64;

        assert!(closed <= now_nanos);
    }

    #[test]
    fn leader_closed_ts_saturates_at_zero_when_lag_exceeds_elapsed() {
        // With a large lag and very little elapsed time, the subtraction
        // should saturate at 0 rather than underflow.
        let tracker = ClosedTimestampTracker::new(Duration::from_secs(999));
        assert_eq!(tracker.compute_leader_closed_ts(), 0);
    }
}
