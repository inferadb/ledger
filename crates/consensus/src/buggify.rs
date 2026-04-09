//! Deterministic fault injection for simulation testing.
//!
//! Inspired by FoundationDB's BUGGIFY mechanism. Instrumentation points are
//! compiled into production code but the global switch (`BUGGIFY_ENABLED`) is
//! off by default. During simulation, the switch is turned on with a
//! deterministic seed so failures are reproducible.
//!
//! In production, `buggify()` compiles to a single relaxed atomic load (~1ns)
//! that always returns `false`.

use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, Ordering},
};

/// Global switch: off in production, on during simulation.
static BUGGIFY_ENABLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// Per-thread deterministic RNG for fault injection.
    /// `None` until `enable_buggify()` is called on this thread.
    static BUGGIFY_RNG: RefCell<Option<u64>> = const { RefCell::new(None) };
}

/// Returns `true` with the given probability, but ONLY during simulation.
///
/// In production (`BUGGIFY_ENABLED` is `false`), always returns `false`.
/// The fast path is a single relaxed atomic load.
///
/// During simulation, uses a deterministic LCG RNG seeded via
/// `enable_buggify()`. Same seed → identical sequence of decisions.
#[inline]
pub fn buggify(probability: f64) -> bool {
    if !BUGGIFY_ENABLED.load(Ordering::Relaxed) {
        return false;
    }
    BUGGIFY_RNG.with(|rng| {
        let mut state = rng.borrow_mut();
        match *state {
            Some(ref mut s) => {
                // LCG step (same constants as SimulatedRng).
                *s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let threshold = (probability * u64::MAX as f64) as u64;
                *s < threshold
            },
            None => false,
        }
    })
}

/// Activates buggification with a deterministic seed.
///
/// Call this once per thread at the start of a simulation test.
/// All subsequent `buggify()` calls on this thread use the seeded RNG.
pub fn enable_buggify(seed: u64) {
    BUGGIFY_ENABLED.store(true, Ordering::Relaxed);
    BUGGIFY_RNG.with(|rng| {
        *rng.borrow_mut() = Some(seed);
    });
}

/// Deactivates buggification. All subsequent `buggify()` calls return `false`.
pub fn disable_buggify() {
    BUGGIFY_ENABLED.store(false, Ordering::Relaxed);
    BUGGIFY_RNG.with(|rng| {
        *rng.borrow_mut() = None;
    });
}

/// Returns whether buggification is currently enabled.
pub fn is_buggify_enabled() -> bool {
    BUGGIFY_ENABLED.load(Ordering::Relaxed)
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Tests in this module mutate the global `BUGGIFY_ENABLED` flag and
    /// per-thread RNG state. A shared mutex prevents interleaving when
    /// `cargo test` runs them in parallel on the same thread.
    static SERIAL: Mutex<()> = Mutex::new(());

    #[test]
    fn buggify_disabled_always_returns_false() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        disable_buggify();
        for _ in 0..1000 {
            assert!(!buggify(1.0)); // Even 100% probability returns false when disabled.
        }
    }

    #[test]
    fn buggify_enabled_is_deterministic() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        enable_buggify(42);
        let results_a: Vec<bool> = (0..100).map(|_| buggify(0.5)).collect();

        // Reset with same seed — should get identical results.
        enable_buggify(42);
        let results_b: Vec<bool> = (0..100).map(|_| buggify(0.5)).collect();

        assert_eq!(results_a, results_b, "Same seed must produce identical sequence");
        disable_buggify();
    }

    #[test]
    fn buggify_different_seeds_differ() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        enable_buggify(1);
        let results_a: Vec<bool> = (0..100).map(|_| buggify(0.5)).collect();

        enable_buggify(2);
        let results_b: Vec<bool> = (0..100).map(|_| buggify(0.5)).collect();

        // With different seeds, at least some results should differ.
        assert_ne!(results_a, results_b, "Different seeds should produce different sequences");
        disable_buggify();
    }

    #[test]
    fn buggify_zero_probability_always_false() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        enable_buggify(42);
        for _ in 0..1000 {
            assert!(!buggify(0.0));
        }
        disable_buggify();
    }

    #[test]
    fn buggify_one_probability_always_true() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        enable_buggify(42);
        for _ in 0..1000 {
            assert!(buggify(1.0));
        }
        disable_buggify();
    }
}
