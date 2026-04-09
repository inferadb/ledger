//! Injectable randomness for deterministic simulation.

use std::time::Duration;

/// Abstraction over randomness for deterministic testing.
pub trait RngSource: Send + Sync + 'static {
    /// Generates a random election timeout within `[min_ms, max_ms]`.
    fn election_timeout(&self, min_ms: u64, max_ms: u64) -> Duration;
}

/// Production RNG using the system CSPRNG.
#[derive(Debug, Clone, Copy)]
pub struct SystemRng;

impl RngSource for SystemRng {
    fn election_timeout(&self, min_ms: u64, max_ms: u64) -> Duration {
        debug_assert!(min_ms <= max_ms, "election_timeout: min_ms ({min_ms}) > max_ms ({max_ms})");
        use rand::RngExt;
        let mut rng = rand::rng();
        Duration::from_millis(rng.random_range(min_ms..=max_ms))
    }
}

/// Deterministic RNG for simulation. Same seed produces the same sequence.
#[derive(Debug)]
pub struct SimulatedRng {
    state: parking_lot::Mutex<u64>,
}

impl SimulatedRng {
    /// Creates a new deterministic RNG with the given seed.
    pub fn new(seed: u64) -> Self {
        Self { state: parking_lot::Mutex::new(seed) }
    }

    fn next_u64(&self) -> u64 {
        let mut state = self.state.lock();
        *state =
            state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        *state
    }
}

impl RngSource for SimulatedRng {
    fn election_timeout(&self, min_ms: u64, max_ms: u64) -> Duration {
        debug_assert!(min_ms <= max_ms, "election_timeout: min_ms ({min_ms}) > max_ms ({max_ms})");
        let range = max_ms - min_ms + 1;
        Duration::from_millis(min_ms + (self.next_u64() % range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulated_rng_same_seed_produces_identical_sequence() {
        let rng1 = SimulatedRng::new(42);
        let rng2 = SimulatedRng::new(42);
        for _ in 0..100 {
            assert_eq!(rng1.election_timeout(150, 300), rng2.election_timeout(150, 300));
        }
    }

    #[test]
    fn simulated_rng_different_seeds_produce_different_sequences() {
        let rng1 = SimulatedRng::new(1);
        let rng2 = SimulatedRng::new(2);
        let mut any_differ = false;
        for _ in 0..100 {
            if rng1.election_timeout(150, 300) != rng2.election_timeout(150, 300) {
                any_differ = true;
                break;
            }
        }
        assert!(any_differ, "different seeds should produce different sequences");
    }

    #[test]
    fn simulated_rng_values_within_range() {
        let rng = SimulatedRng::new(123);
        for _ in 0..1000 {
            let t = rng.election_timeout(150, 300);
            assert!(t >= Duration::from_millis(150));
            assert!(t <= Duration::from_millis(300));
        }
    }

    #[test]
    fn simulated_rng_equal_min_max_returns_exact_value() {
        let rng = SimulatedRng::new(999);
        for _ in 0..10 {
            assert_eq!(rng.election_timeout(200, 200), Duration::from_millis(200));
        }
    }

    #[test]
    fn system_rng_values_within_range() {
        let rng = SystemRng;
        for _ in 0..100 {
            let t = rng.election_timeout(150, 300);
            assert!(t >= Duration::from_millis(150));
            assert!(t <= Duration::from_millis(300));
        }
    }
}
