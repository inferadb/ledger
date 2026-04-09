//! Injectable clock for deterministic simulation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Abstraction over time for deterministic testing.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current instant.
    fn now(&self) -> Instant;
}

/// Production clock using the system monotonic clock.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Simulated clock for deterministic testing.
/// Time advances only when `advance()` is called.
#[derive(Debug)]
pub struct SimulatedClock {
    nanos: AtomicU64,
    epoch: Instant,
}

impl SimulatedClock {
    /// Creates a new simulated clock starting at zero elapsed time.
    pub fn new() -> Self {
        Self { nanos: AtomicU64::new(0), epoch: Instant::now() }
    }

    /// Advances the simulated clock by the given duration.
    pub fn advance(&self, duration: Duration) {
        self.nanos.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
}

impl Default for SimulatedClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        self.epoch + Duration::from_nanos(self.nanos.load(Ordering::Relaxed))
    }
}

/// Allow `Arc<C>` to be used as a `Clock` (for sharing between multiple shards).
impl<C: Clock> Clock for Arc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_monotonic_instant() {
        let clock = SystemClock;
        let t1 = clock.now();
        let t2 = clock.now();
        assert!(t2 >= t1);
    }

    #[test]
    fn simulated_clock_starts_at_epoch() {
        let clock = SimulatedClock::new();
        let t1 = clock.now();
        let t2 = clock.now();
        // Without advance, readings should be identical (no real time elapses
        // in the simulated clock).
        assert_eq!(t1, t2);
    }

    #[test]
    fn simulated_clock_advance_moves_time_exactly() {
        let clock = SimulatedClock::new();
        let t1 = clock.now();
        clock.advance(Duration::from_secs(5));
        let t2 = clock.now();
        assert_eq!(t2.duration_since(t1), Duration::from_secs(5));
    }

    #[test]
    fn simulated_clock_cumulative_advances() {
        let clock = SimulatedClock::new();
        let t0 = clock.now();
        clock.advance(Duration::from_millis(100));
        clock.advance(Duration::from_millis(200));
        let t1 = clock.now();
        assert_eq!(t1.duration_since(t0), Duration::from_millis(300));
    }

    #[test]
    fn arc_clock_shares_time_across_clones() {
        let clock = Arc::new(SimulatedClock::new());
        let clone = Arc::clone(&clock);

        let t1 = clock.now();
        clone.advance(Duration::from_millis(100));
        let t2 = clock.now();
        assert_eq!(t2.duration_since(t1), Duration::from_millis(100));
    }

    #[test]
    fn zero_advance_does_not_change_time() {
        let clock = SimulatedClock::new();
        let t1 = clock.now();
        clock.advance(Duration::ZERO);
        let t2 = clock.now();
        assert_eq!(t1, t2);
    }
}
