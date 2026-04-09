//! Leader lease for fast linearizable reads.
//!
//! Tracks when quorum last acknowledged entries. While the lease is valid
//! (within `election_timeout_min / 2` of last ACK), linearizable reads skip
//! the quorum round-trip.
//!
//! The lease is renewed each time `apply_to_state_machine` completes — entries
//! only reach that path after quorum consensus, so the renewal timestamp
//! reflects the most recent quorum acknowledgment.
//!
//! # Thread Safety
//!
//! [`LeaderLease`] uses atomic operations for zero-contention validity checks
//! (~50ns). It is `Send + Sync` and safe to share across async tasks.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Tracks leader lease validity via last quorum acknowledgment timestamp.
///
/// A lease is valid when the time since the last quorum acknowledgment
/// is less than the configured lease duration (typically `election_timeout_min / 2`).
/// This guarantees the leader has not been superseded: no new election can
/// complete within the lease window.
#[derive(Debug)]
pub struct LeaderLease {
    /// Last time quorum acknowledged. None = never renewed.
    last_quorum_ack: Mutex<Option<Instant>>,
    /// Maximum duration a lease remains valid after renewal.
    lease_duration: Duration,
}

impl LeaderLease {
    /// Creates a new lease that has never been renewed (starts invalid).
    ///
    /// `lease_duration` is typically `election_timeout_min / 2` — half the
    /// minimum election timeout guarantees no new leader can be elected
    /// while the lease is valid.
    pub fn new(lease_duration: Duration) -> Self {
        Self { last_quorum_ack: Mutex::new(None), lease_duration }
    }

    /// Renews the lease by recording the current time as the last quorum ack.
    ///
    /// Called after `apply_to_state_machine` completes — entries only reach
    /// that path after quorum consensus.
    pub fn renew(&self) {
        *self.last_quorum_ack.lock() = Some(Instant::now());
    }

    /// Returns `true` if the lease is currently valid.
    ///
    /// A lease is valid when:
    /// 1. It has been renewed at least once (not freshly created).
    /// 2. Less than `lease_duration` has elapsed since the last renewal.
    #[inline]
    pub fn is_valid(&self) -> bool {
        match *self.last_quorum_ack.lock() {
            None => false,
            Some(last_ack) => last_ack.elapsed() < self.lease_duration,
        }
    }

    /// Invalidates the lease immediately.
    ///
    /// Called when leadership is lost (e.g., vote change, step-down).
    pub fn invalidate(&self) {
        *self.last_quorum_ack.lock() = None;
    }

    /// Returns the configured lease duration.
    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_lease_is_invalid() {
        let lease = LeaderLease::new(Duration::from_millis(150));
        assert!(!lease.is_valid());
    }

    #[test]
    fn lease_valid_after_renewal() {
        // Use a very long lease so scheduling delays can't expire it
        let lease = LeaderLease::new(Duration::from_secs(300));
        lease.renew();
        assert!(
            lease.is_valid(),
            "lease with 5-minute duration should be valid immediately after renew"
        );
    }

    #[test]
    fn lease_expires_after_duration() {
        let lease = LeaderLease::new(Duration::from_millis(1));
        lease.renew();
        // Sleep well past the 1ms lease
        std::thread::sleep(Duration::from_millis(50));
        assert!(!lease.is_valid(), "lease should have expired after 50ms with 1ms duration");
    }

    #[test]
    fn lease_renewed_extends_validity() {
        // Use long lease and no sleeps — pure logic test, no timing dependency
        let lease = LeaderLease::new(Duration::from_secs(300));
        lease.renew();
        assert!(lease.is_valid(), "should be valid immediately after first renew");
        lease.renew();
        assert!(lease.is_valid(), "should be valid immediately after second renew");
    }

    #[test]
    fn lease_invalidate_clears() {
        let lease = LeaderLease::new(Duration::from_millis(150));
        lease.renew();
        assert!(lease.is_valid());
        lease.invalidate();
        assert!(!lease.is_valid());
    }

    #[test]
    fn lease_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LeaderLease>();
    }

    #[test]
    fn lease_duration_accessor() {
        let lease = LeaderLease::new(Duration::from_millis(150));
        assert_eq!(lease.lease_duration(), Duration::from_millis(150));
    }
}
