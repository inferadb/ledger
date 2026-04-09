//! Leader lease for fast linearizable reads.
//!
//! Parameterized by [`Clock`] trait for deterministic simulation testing.
//! A valid lease allows the leader to serve reads locally without a
//! round-trip to followers, reducing read latency.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::clock::Clock;

/// Tracks leader lease validity.
///
/// The lease becomes valid when a quorum acknowledges the leader
/// (via [`renew`](Self::renew)) and expires after the configured
/// duration. On step-down, call [`invalidate`](Self::invalidate)
/// to immediately revoke the lease.
#[derive(Debug)]
pub struct LeaderLease<C: Clock> {
    last_quorum_ack: Mutex<Option<Instant>>,
    lease_duration: Duration,
    clock: C,
}

impl<C: Clock> LeaderLease<C> {
    /// Creates a new lease that starts invalid.
    pub fn new(lease_duration: Duration, clock: C) -> Self {
        Self { last_quorum_ack: Mutex::new(None), lease_duration, clock }
    }

    /// Renews the lease (called when quorum acknowledges).
    pub fn renew(&self) {
        *self.last_quorum_ack.lock() = Some(self.clock.now());
    }

    /// Returns true if the lease is currently valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        match *self.last_quorum_ack.lock() {
            None => false,
            Some(last_ack) => {
                let now = self.clock.now();
                now.duration_since(last_ack) < self.lease_duration
            },
        }
    }

    /// Explicitly invalidates the lease (on step-down).
    pub fn invalidate(&self) {
        *self.last_quorum_ack.lock() = None;
    }

    /// Returns the configured lease duration.
    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::clock::SimulatedClock;

    fn make_lease(duration_ms: u64) -> (LeaderLease<Arc<SimulatedClock>>, Arc<SimulatedClock>) {
        let clock = Arc::new(SimulatedClock::new());
        let lease = LeaderLease::new(Duration::from_millis(duration_ms), clock.clone());
        (lease, clock)
    }

    #[test]
    fn new_lease_is_invalid() {
        let (lease, _clock) = make_lease(5000);
        assert!(!lease.is_valid());
    }

    #[test]
    fn valid_after_renew() {
        let (lease, _clock) = make_lease(5000);
        lease.renew();
        assert!(lease.is_valid());
    }

    #[test]
    fn expires_after_duration() {
        let (lease, clock) = make_lease(100);
        lease.renew();
        assert!(lease.is_valid());
        clock.advance(Duration::from_millis(200));
        assert!(!lease.is_valid());
    }

    #[test]
    fn invalidate_revokes_active_lease() {
        let (lease, _clock) = make_lease(5000);
        lease.renew();
        assert!(lease.is_valid());
        lease.invalidate();
        assert!(!lease.is_valid());
    }

    #[test]
    fn double_invalidate_is_safe() {
        let (lease, _clock) = make_lease(5000);
        lease.renew();
        lease.invalidate();
        lease.invalidate();
        assert!(!lease.is_valid());
    }

    #[test]
    fn renew_extends_validity_window() {
        let (lease, clock) = make_lease(100);
        lease.renew();
        clock.advance(Duration::from_millis(80));
        assert!(lease.is_valid());
        lease.renew();
        clock.advance(Duration::from_millis(80));
        // 80ms since last renew, within 100ms window.
        assert!(lease.is_valid());
    }

    #[test]
    fn renew_after_invalidate_restores_validity() {
        let (lease, _clock) = make_lease(5000);
        lease.renew();
        lease.invalidate();
        assert!(!lease.is_valid());
        lease.renew();
        assert!(lease.is_valid());
    }

    #[test]
    fn exact_boundary_is_not_valid() {
        // is_valid uses strict less-than: `elapsed < lease_duration`
        let (lease, clock) = make_lease(100);
        lease.renew();
        clock.advance(Duration::from_millis(100));
        assert!(!lease.is_valid());
    }

    #[test]
    fn lease_duration_accessor() {
        let (lease, _clock) = make_lease(250);
        assert_eq!(lease.lease_duration(), Duration::from_millis(250));
    }
}
