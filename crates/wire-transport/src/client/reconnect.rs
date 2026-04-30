//! Single-flight reconnect mutex + backoff state machine.
//!
//! [`ReconnectBackoff`] is the transport-level backoff used by
//! [`crate::client::WireClient`] when a QUIC connection is gone (peer down,
//! network partition, NAT timeout). It's intentionally distinct from any
//! RPC-level retry policy in the SDK — compounding the two would produce
//! excessive retry storms after a long peer outage.
//!
//! The state machine is exponential-with-jitter: each [`ReconnectBackoff::wait`]
//! call sleeps the current delay then advances it (capped at `max`); a
//! successful connect must call [`ReconnectBackoff::reset`] so the next
//! disconnect starts at `initial`.

use std::time::Duration;

use parking_lot::Mutex;

/// Exponential backoff with jitter for transport-level reconnect attempts.
///
/// Distinct from any RPC-level retry policy in the SDK — transport-level
/// reconnect handles the case where the QUIC connection itself is gone
/// (peer down, network partition, NAT timeout). Compounding the two would
/// produce excessive retry storms.
#[derive(Debug)]
pub struct ReconnectBackoff {
    initial: Duration,
    max: Duration,
    multiplier: f64,
    /// ±jitter as a fraction of the current delay (e.g., 0.2 = ±20%).
    jitter_ratio: f64,
    /// Mutating delay state. Held for nanoseconds at a time, so a sync
    /// `parking_lot::Mutex` is fine — no async work happens under this lock.
    current: Mutex<Duration>,
}

impl ReconnectBackoff {
    /// Construct a backoff with the supplied parameters.
    ///
    /// `initial` and `max` are the lower and upper bounds for the sleep
    /// duration. `multiplier` is the geometric growth factor applied after
    /// each `wait`. `jitter_ratio` is the symmetric jitter fraction —
    /// e.g., `0.2` randomizes each delay by `±20%`.
    #[must_use]
    pub fn new(initial: Duration, max: Duration, multiplier: f64, jitter_ratio: f64) -> Self {
        Self { initial, max, multiplier, jitter_ratio, current: Mutex::new(initial) }
    }

    /// Default policy: 100 ms initial, 2× multiplier, 30 s max, ±20% jitter.
    ///
    /// Picked to match transport-level reconnect characteristics rather than
    /// RPC retry: the first retry is fast (typical transient failures
    /// resolve within hundreds of milliseconds), the cap is short enough
    /// that a long-down peer is still polled often enough for liveness,
    /// and the jitter is wide enough to break ties between many clients
    /// reconnecting at once.
    #[must_use]
    pub fn standard() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(30), 2.0, 0.2)
    }

    /// Sleep the current delay (with jitter applied), then advance the
    /// state for the next call.
    ///
    /// `wait` cooperates with `tokio::time::pause()` — under a paused clock
    /// the future resolves once simulated time advances past the jittered
    /// delay, making time-based unit tests deterministic.
    pub async fn wait(&self) {
        let delay = {
            let mut guard = self.current.lock();
            let base = *guard;
            // Advance for next time. `secs_f64` round-trip is fine here —
            // we only care about millisecond-class precision, and we cap
            // before the next call observes the value.
            let next_secs = (base.as_secs_f64() * self.multiplier).min(self.max.as_secs_f64());
            *guard = Duration::from_secs_f64(next_secs);
            base
        };
        let jittered = apply_jitter(delay, self.jitter_ratio);
        tokio::time::sleep(jittered).await;
    }

    /// Reset the delay state to `initial`. Call on every successful
    /// connect so the next disconnect starts the backoff schedule fresh.
    pub fn reset(&self) {
        let mut guard = self.current.lock();
        *guard = self.initial;
    }

    /// Initial delay this backoff was constructed with.
    #[cfg(test)]
    #[must_use]
    pub fn initial(&self) -> Duration {
        self.initial
    }

    /// Maximum delay this backoff was constructed with.
    #[cfg(test)]
    #[must_use]
    pub fn max(&self) -> Duration {
        self.max
    }
}

/// Apply symmetric jitter to `delay`.
///
/// Returns a duration uniformly distributed in
/// `[delay * (1 - ratio), delay * (1 + ratio)]`, clamped at zero.
/// `ratio <= 0` short-circuits and returns `delay` unchanged.
///
/// Pulled out as a free function so tests can verify jitter bounds without
/// running the time-based [`ReconnectBackoff::wait`] path.
fn apply_jitter(delay: Duration, ratio: f64) -> Duration {
    if ratio <= 0.0 {
        return delay;
    }
    let nanos = delay.as_nanos() as f64;
    let max_offset = nanos * ratio;
    // `rand::random_range` is uniform; the bounds are inclusive on both
    // ends so the test asserting `[800ms, 1200ms]` for `(1s, 0.2)` holds.
    let offset: f64 = rand::random_range(-max_offset..=max_offset);
    let final_nanos = (nanos + offset).max(0.0);
    Duration::from_nanos(final_nanos as u64)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn standard_policy_constants() {
        let backoff = ReconnectBackoff::standard();
        assert_eq!(backoff.initial(), Duration::from_millis(100));
        assert_eq!(backoff.max(), Duration::from_secs(30));
        assert!((backoff.multiplier - 2.0).abs() < f64::EPSILON);
        assert!((backoff.jitter_ratio - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn jitter_within_bounds() {
        // 1s ± 20% must always land in [800ms, 1200ms] (inclusive).
        let base = Duration::from_secs(1);
        let lower = Duration::from_millis(800);
        let upper = Duration::from_millis(1200);
        for _ in 0..1000 {
            let jittered = apply_jitter(base, 0.2);
            assert!(
                jittered >= lower && jittered <= upper,
                "jittered duration {jittered:?} outside [{lower:?}, {upper:?}]",
            );
        }
    }

    #[test]
    fn jitter_zero_ratio_is_identity() {
        let base = Duration::from_millis(250);
        for _ in 0..100 {
            assert_eq!(apply_jitter(base, 0.0), base);
            assert_eq!(apply_jitter(base, -0.5), base);
        }
    }

    #[test]
    fn jitter_clamps_at_zero() {
        // A massive ratio could in principle produce a negative offset
        // larger than the base; the clamp must still yield a non-negative
        // duration. We can't pin it to zero exactly (the offset is random)
        // but we can confirm it's representable.
        let base = Duration::from_nanos(10);
        for _ in 0..1000 {
            let _ = apply_jitter(base, 5.0); // must not panic, must not underflow
        }
    }

    /// `wait` advances under simulated time. Uses `tokio::time::pause()` so
    /// the test runs in microseconds rather than seconds — deterministic
    /// across CI and dev hosts.
    ///
    /// Each call to `wait` must:
    ///   1. sleep `current` (jittered),
    ///   2. multiply `current` by `multiplier`,
    ///   3. clamp `current` at `max`.
    /// We don't assert exact durations because of jitter — we assert that
    /// after enough iterations the internal `current` saturates at `max`.
    #[tokio::test(start_paused = true)]
    async fn backoff_advances_and_saturates() {
        let backoff = ReconnectBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(80),
            2.0,
            0.0, // disable jitter for determinism
        );

        // 10 → 20 → 40 → 80 (capped) → 80 → 80 ...
        backoff.wait().await; // sleeps 10ms, advances current to 20ms
        backoff.wait().await; // sleeps 20ms, advances current to 40ms
        backoff.wait().await; // sleeps 40ms, advances current to 80ms
        backoff.wait().await; // sleeps 80ms, current already at max, stays 80ms
        backoff.wait().await; // sleeps 80ms

        // After 5 waits with cap 80ms, internal state must be saturated at max.
        assert_eq!(*backoff.current.lock(), Duration::from_millis(80));
    }

    /// Successful connect calls `reset`; the next backoff cycle starts at
    /// `initial`, not the previously-saturated value.
    #[tokio::test(start_paused = true)]
    async fn backoff_resets_to_initial() {
        let backoff =
            ReconnectBackoff::new(Duration::from_millis(10), Duration::from_secs(60), 2.0, 0.0);

        backoff.wait().await; // current 10 → 20
        backoff.wait().await; // current 20 → 40
        assert_eq!(*backoff.current.lock(), Duration::from_millis(40));

        backoff.reset();
        assert_eq!(*backoff.current.lock(), Duration::from_millis(10));
    }
}
