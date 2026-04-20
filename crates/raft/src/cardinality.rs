//! Cardinality tracking for metric emissions.
//!
//! Prevents unbounded label cardinality by tracking distinct label-set
//! fingerprints per metric name. When the count exceeds configured thresholds,
//! emissions are either admitted with overflow counting ([`CardinalityMode::Observe`])
//! or dropped ([`CardinalityMode::Enforce`]).
//!
//! # Usage
//!
//! ```no_run
//! use inferadb_ledger_raft::cardinality::{
//!     init_tracker, tracker, fingerprint_labels, CardinalityMode,
//! };
//!
//! // During bootstrap:
//! init_tracker(500, 1000, CardinalityMode::Observe);
//!
//! // At emission sites:
//! let fp = fingerprint_labels(&[("method", "Check"), ("service", "write")]);
//! let admitted = tracker()
//!     .map(|t| t.admit("ledger_grpc_requests_total", fp))
//!     .unwrap_or(true);
//! if admitted {
//!     // proceed with metric emission
//! }
//! ```

use std::{
    hash::{Hash, Hasher},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::{DashMap, DashSet};

// ---------------------------------------------------------------------------
// Per-metric cardinality overrides for known high-cardinality metric families.
// (metric_name, warn_threshold, max_threshold)
// ---------------------------------------------------------------------------
const OVERRIDES: &[(&str, u32, u32)] = &[
    ("ledger_grpc_requests_total", 20_000, 50_000),
    ("ledger_grpc_request_latency_seconds", 2_000, 5_000),
];

/// Cardinality enforcement mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityMode {
    /// Admit everything; increment overflow counter when over threshold.
    Observe,
    /// Drop emissions when over max; increment overflow counter.
    Enforce,
}

/// Per-metric cardinality tracking entry.
struct MetricEntry {
    /// Distinct label-set fingerprints seen.
    ///
    /// [`DashSet`] gives lock-free reads on the already-seen fast path and
    /// shard-local locks only on first-insert. This replaces a prior
    /// `Mutex<HashSet<u64>>` that became the workspace's dominant CPU
    /// contender at 256-client concurrency (~43% of active CPU in
    /// `__psynch_mutexwait`).
    fingerprints: DashSet<u64>,
    /// Count of emissions dropped or overflowed.
    overflow_count: AtomicU64,
}

/// Gates metric emissions to prevent unbounded label cardinality.
///
/// Uses [`DashMap`] for concurrent per-metric access. Each metric name maps to
/// a set of observed label-fingerprints. When the set exceeds `default_max`,
/// behavior depends on the configured [`CardinalityMode`].
pub struct CardinalityTracker {
    entries: DashMap<&'static str, MetricEntry>,
    default_warn: u32,
    default_max: u32,
    mode: CardinalityMode,
}

impl CardinalityTracker {
    /// Create a new tracker with the given warn/max thresholds and mode.
    pub fn new(default_warn: u32, default_max: u32, mode: CardinalityMode) -> Self {
        Self { entries: DashMap::new(), default_warn, default_max, mode }
    }

    /// Returns `true` if the emission should proceed.
    ///
    /// `label_fingerprint` is a pre-computed hash of the label key-value pairs
    /// (see [`fingerprint_labels`]).
    ///
    /// # Concurrency
    ///
    /// Backed by a [`DashSet`] rather than `Mutex<HashSet>` so the hot
    /// already-seen path is lock-free. The cap check (`len() >= max`) and the
    /// subsequent `insert()` are NOT held under a single lock, so two
    /// concurrent admits can both observe `len() == max - 1`, both pass the
    /// cap check, and both insert — admitting at most `N` extra fingerprints
    /// beyond `max` where `N` is the number of threads concurrently admitting
    /// novel fingerprints at the cap boundary. This is an acceptable overshoot
    /// for a cardinality gauge (label-explosion detector) — it is not a
    /// correctness bug.
    pub fn admit(&self, metric_name: &'static str, label_fingerprint: u64) -> bool {
        let entry = self.entries.entry(metric_name).or_insert_with(|| MetricEntry {
            fingerprints: DashSet::new(),
            overflow_count: AtomicU64::new(0),
        });

        // Fast path: already-seen fingerprint. DashSet::contains is lock-free
        // on the read side (per-shard RwLock taken as reader).
        if entry.fingerprints.contains(&label_fingerprint) {
            return true;
        }

        // Slow path: possibly-novel label combination. Check cap, then insert.
        // See doc comment on race semantics — `len()` here is best-effort.
        let current = entry.fingerprints.len() as u32;
        let max = self.resolved_max(metric_name);

        if current >= max {
            entry.overflow_count.fetch_add(1, Ordering::Relaxed);
            crate::metrics::record_cardinality_overflow(metric_name);

            match self.mode {
                CardinalityMode::Observe => {
                    entry.fingerprints.insert(label_fingerprint);
                    true
                },
                CardinalityMode::Enforce => false,
            }
        } else {
            let warn = self.resolved_warn(metric_name);
            if current >= warn {
                tracing::warn!(
                    metric_name,
                    current,
                    warn,
                    max,
                    "metric cardinality approaching limit"
                );
            }
            entry.fingerprints.insert(label_fingerprint);
            true
        }
    }

    /// Returns the overflow count for a given metric.
    pub fn overflow_count(&self, metric_name: &str) -> u64 {
        self.entries.get(metric_name).map(|e| e.overflow_count.load(Ordering::Relaxed)).unwrap_or(0)
    }

    /// Returns the number of distinct fingerprints tracked for a metric.
    pub fn distinct_count(&self, metric_name: &str) -> usize {
        self.entries.get(metric_name).map(|e| e.fingerprints.len()).unwrap_or(0)
    }

    /// Returns the top `n` metrics by distinct label-combination count, sorted
    /// descending. Useful for surfacing label leaks in health probes.
    pub fn top_by_cardinality(&self, n: usize) -> Vec<(&'static str, usize)> {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| (*entry.key(), entry.value().fingerprints.len()))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }

    /// Resolve the max cardinality for a metric, checking const overrides first.
    fn resolved_max(&self, metric_name: &str) -> u32 {
        for &(name, _warn, max) in OVERRIDES {
            if name == metric_name {
                return max;
            }
        }
        self.default_max
    }

    /// Resolve the warn threshold for a metric, checking const overrides first.
    fn resolved_warn(&self, metric_name: &str) -> u32 {
        for &(name, warn, _max) in OVERRIDES {
            if name == metric_name {
                return warn;
            }
        }
        self.default_warn
    }
}

// ---------------------------------------------------------------------------
// Global instance
// ---------------------------------------------------------------------------

static TRACKER: OnceLock<CardinalityTracker> = OnceLock::new();

/// Initialize the global cardinality tracker. Should be called once during
/// server bootstrap. Subsequent calls are silently ignored.
pub fn init_tracker(warn: u32, max: u32, mode: CardinalityMode) {
    let _ = TRACKER.set(CardinalityTracker::new(warn, max, mode));
}

/// Returns a reference to the global cardinality tracker, or `None` if
/// [`init_tracker`] has not been called yet.
pub fn tracker() -> Option<&'static CardinalityTracker> {
    TRACKER.get()
}

// ---------------------------------------------------------------------------
// Fingerprinting helper
// ---------------------------------------------------------------------------

/// Compute a fingerprint from label key-value pairs.
///
/// The fingerprint is order-dependent: `[("a","1"), ("b","2")]` produces a
/// different hash than `[("b","2"), ("a","1")]`. Callers should pass labels
/// in a consistent order (alphabetical by key is conventional).
pub fn fingerprint_labels(labels: &[(&str, &str)]) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    for (k, v) in labels {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admit_below_max_returns_true() {
        let tracker = CardinalityTracker::new(5, 10, CardinalityMode::Enforce);
        let fp = fingerprint_labels(&[("method", "Check")]);
        assert!(tracker.admit("test_metric", fp));
        assert_eq!(tracker.distinct_count("test_metric"), 1);
        assert_eq!(tracker.overflow_count("test_metric"), 0);
    }

    #[test]
    fn admit_above_max_observe_mode_returns_true_and_counts_overflow() {
        let tracker = CardinalityTracker::new(1, 2, CardinalityMode::Observe);

        // Fill to max
        let fp1 = fingerprint_labels(&[("method", "A")]);
        let fp2 = fingerprint_labels(&[("method", "B")]);
        assert!(tracker.admit("test_metric", fp1));
        assert!(tracker.admit("test_metric", fp2));

        // One more — over max, but Observe mode still admits
        let fp3 = fingerprint_labels(&[("method", "C")]);
        assert!(tracker.admit("test_metric", fp3));
        assert_eq!(tracker.distinct_count("test_metric"), 3);
        assert_eq!(tracker.overflow_count("test_metric"), 1);
    }

    #[test]
    fn admit_above_max_enforce_mode_returns_false() {
        let tracker = CardinalityTracker::new(1, 2, CardinalityMode::Enforce);

        let fp1 = fingerprint_labels(&[("method", "A")]);
        let fp2 = fingerprint_labels(&[("method", "B")]);
        assert!(tracker.admit("test_metric", fp1));
        assert!(tracker.admit("test_metric", fp2));

        // Over max — Enforce mode drops
        let fp3 = fingerprint_labels(&[("method", "C")]);
        assert!(!tracker.admit("test_metric", fp3));
        // Fingerprint was NOT inserted
        assert_eq!(tracker.distinct_count("test_metric"), 2);
        assert_eq!(tracker.overflow_count("test_metric"), 1);
    }

    #[test]
    fn same_fingerprint_always_admitted() {
        let tracker = CardinalityTracker::new(1, 2, CardinalityMode::Enforce);

        let fp1 = fingerprint_labels(&[("method", "A")]);
        let fp2 = fingerprint_labels(&[("method", "B")]);
        assert!(tracker.admit("test_metric", fp1));
        assert!(tracker.admit("test_metric", fp2));

        // Re-admit an already-seen fingerprint — should pass even at max
        assert!(tracker.admit("test_metric", fp1));
        assert!(tracker.admit("test_metric", fp2));
        assert_eq!(tracker.overflow_count("test_metric"), 0);
    }

    #[test]
    fn const_override_takes_precedence() {
        // Default max is 2, but the override for grpc_requests is 50_000
        let tracker = CardinalityTracker::new(1, 2, CardinalityMode::Enforce);

        let fp1 = fingerprint_labels(&[("method", "A")]);
        let fp2 = fingerprint_labels(&[("method", "B")]);
        let fp3 = fingerprint_labels(&[("method", "C")]);

        assert!(tracker.admit("ledger_grpc_requests_total", fp1));
        assert!(tracker.admit("ledger_grpc_requests_total", fp2));
        // Would be rejected under default_max=2, but override allows 50_000
        assert!(tracker.admit("ledger_grpc_requests_total", fp3));
        assert_eq!(tracker.overflow_count("ledger_grpc_requests_total"), 0);
    }
}
