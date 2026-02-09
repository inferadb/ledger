//! Metric cardinality budgets using HyperLogLog estimation.
//!
//! Tracks distinct label combinations per Prometheus metric family using
//! HyperLogLog, a probabilistic data structure with fixed memory (~4KB per
//! metric family). Emits warnings at a configurable threshold and drops
//! metric observations when cardinality exceeds the maximum budget.
//!
//! # Architecture
//!
//! [`CardinalityBudget`] wraps a `DashMap<String, HyperLogLog>` mapping metric
//! names to their per-family cardinality estimators. On each metric observation,
//! callers invoke [`CardinalityBudget::check_cardinality`] with the metric name
//! and label set. The method returns `true` if the observation is allowed or
//! `false` if it should be dropped (cardinality exceeded).
//!
//! Thresholds are stored as `AtomicU32` for lock-free runtime reconfiguration
//! via `update_thresholds()`.

use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use tracing::warn;

use crate::metrics::record_cardinality_overflow;

// ─── HyperLogLog ──────────────────────────────────────────────

/// Precision parameter: 2^P registers.
const P: u32 = 12;

/// Number of registers (4096).
const M: usize = 1 << P;

/// Bits remaining after extracting the register index.
const HASH_BITS: u32 = 64 - P;

/// Bias correction constant alpha_m for m = 4096.
/// Formula: alpha_m = 0.7213 / (1 + 1.079 / m)
const ALPHA_M: f64 = 0.7213 / (1.0 + 1.079 / M as f64);

/// HyperLogLog cardinality estimator with p=12 (4096 registers, ~4KB).
///
/// Achieves ~1.6% standard error, well within the 5% accuracy requirement.
/// Uses seahash for hashing to avoid adding dependencies (already in workspace).
#[derive(Clone)]
struct HyperLogLog {
    registers: Box<[u8; M]>,
}

impl HyperLogLog {
    /// Create a new empty HyperLogLog estimator.
    fn new() -> Self {
        Self { registers: Box::new([0u8; M]) }
    }

    /// Insert an element (the hash of a label set key).
    fn insert(&mut self, hash: u64) {
        let index = (hash >> HASH_BITS) as usize;
        let remaining = hash << P | (1 << (P - 1)); // Ensure at least 1 bit set
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        if leading_zeros > self.registers[index] {
            self.registers[index] = leading_zeros;
        }
    }

    /// Estimate the number of distinct elements inserted.
    fn estimate(&self) -> u64 {
        // Standard HyperLogLog: harmonic mean of 2^(-register) values
        let mut sum = 0.0_f64;
        let mut zero_count = 0u32;

        for &reg in self.registers.iter() {
            // 2^(-reg) — safe for all u8 values (no integer overflow)
            sum += f64::exp2(-(f64::from(reg)));
            if reg == 0 {
                zero_count += 1;
            }
        }

        let raw_estimate = ALPHA_M * (M as f64) * (M as f64) / sum;

        // Small-range correction: use linear counting when many registers are zero
        if raw_estimate <= 2.5 * M as f64 && zero_count > 0 {
            // Linear counting: m * ln(m / V) where V = number of zero registers
            let linear = M as f64 * (M as f64 / f64::from(zero_count)).ln();
            return linear as u64;
        }

        raw_estimate as u64
    }
}

// ─── CardinalityBudget ────────────────────────────────────────

/// Per-metric cardinality tracker with configurable warning and drop thresholds.
///
/// Thread-safe: uses `DashMap` for concurrent per-metric access and `AtomicU32`
/// for lock-free threshold reads during config updates.
pub struct CardinalityBudget {
    /// Per-metric-family HyperLogLog estimators.
    estimators: DashMap<String, HyperLogLog>,
    /// Emit WARN log when estimated cardinality reaches this count.
    warn_threshold: AtomicU32,
    /// Drop observations when estimated cardinality exceeds this count.
    max_threshold: AtomicU32,
}

impl CardinalityBudget {
    /// Create a new cardinality budget with the given thresholds.
    pub fn new(warn_cardinality: u32, max_cardinality: u32) -> Self {
        Self {
            estimators: DashMap::new(),
            warn_threshold: AtomicU32::new(warn_cardinality),
            max_threshold: AtomicU32::new(max_cardinality),
        }
    }

    /// Check whether a metric observation with the given label set should be allowed.
    ///
    /// Returns `true` if the observation is within budget, `false` if it should
    /// be dropped (cardinality exceeded `max_cardinality`).
    ///
    /// Side effects:
    /// - Inserts the label combination into the per-metric HyperLogLog.
    /// - Emits a WARN log when the warn threshold is first crossed.
    /// - Increments `ledger_metrics_cardinality_overflow_total` on drops.
    pub fn check_cardinality(&self, metric_name: &str, label_set: &[(&str, &str)]) -> bool {
        let label_hash = hash_label_set(label_set);

        let mut entry =
            self.estimators.entry(metric_name.to_string()).or_insert_with(HyperLogLog::new);
        let hll = entry.value_mut();
        hll.insert(label_hash);

        let estimated = hll.estimate();
        let warn = u64::from(self.warn_threshold.load(Ordering::Relaxed));
        let max = u64::from(self.max_threshold.load(Ordering::Relaxed));

        if estimated > max {
            record_cardinality_overflow(metric_name);
            return false;
        }

        if estimated > warn {
            warn!(
                metric = metric_name,
                estimated_cardinality = estimated,
                warn_threshold = warn,
                "Metric cardinality approaching limit"
            );
        }

        true
    }

    /// Update thresholds at runtime (called on config reload).
    pub fn update_thresholds(&self, warn_cardinality: u32, max_cardinality: u32) {
        self.warn_threshold.store(warn_cardinality, Ordering::Relaxed);
        self.max_threshold.store(max_cardinality, Ordering::Relaxed);
    }

    /// Get the current estimated cardinality for a specific metric family.
    ///
    /// Returns `None` if the metric has never been observed.
    #[cfg(test)]
    fn estimated_cardinality(&self, metric_name: &str) -> Option<u64> {
        self.estimators.get(metric_name).map(|hll| hll.estimate())
    }
}

/// Hash a sorted label set into a single u64 for HyperLogLog insertion.
///
/// Concatenates `key=value` pairs with `,` separator and hashes with seahash.
fn hash_label_set(label_set: &[(&str, &str)]) -> u64 {
    let mut buf = String::with_capacity(label_set.len() * 32);
    for (i, (key, value)) in label_set.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push_str(key);
        buf.push('=');
        buf.push_str(value);
    }
    seahash::hash(buf.as_bytes())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_empty_estimate_is_zero() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.estimate(), 0);
    }

    #[test]
    fn test_hll_single_insert() {
        let mut hll = HyperLogLog::new();
        hll.insert(seahash::hash(b"hello"));
        assert!(hll.estimate() >= 1);
    }

    #[test]
    fn test_hll_duplicate_inserts_same_estimate() {
        let mut hll = HyperLogLog::new();
        let hash = seahash::hash(b"hello");
        hll.insert(hash);
        let est1 = hll.estimate();
        hll.insert(hash);
        let est2 = hll.estimate();
        assert_eq!(est1, est2, "duplicate inserts should not change estimate");
    }

    #[test]
    fn test_hll_accuracy_within_5_percent_for_10k() {
        let mut hll = HyperLogLog::new();
        let n = 10_000u64;

        for i in 0..n {
            let hash = seahash::hash(&i.to_le_bytes());
            hll.insert(hash);
        }

        let estimate = hll.estimate();
        let error = (estimate as f64 - n as f64).abs() / n as f64;

        assert!(
            error < 0.05,
            "HyperLogLog estimate {} for {} distinct elements has {:.1}% error (max 5%)",
            estimate,
            n,
            error * 100.0
        );
    }

    #[test]
    fn test_hll_accuracy_within_5_percent_for_1k() {
        let mut hll = HyperLogLog::new();
        let n = 1_000u64;

        for i in 0..n {
            let hash = seahash::hash(&i.to_le_bytes());
            hll.insert(hash);
        }

        let estimate = hll.estimate();
        let error = (estimate as f64 - n as f64).abs() / n as f64;

        assert!(
            error < 0.05,
            "HyperLogLog estimate {} for {} distinct elements has {:.1}% error (max 5%)",
            estimate,
            n,
            error * 100.0
        );
    }

    #[test]
    fn test_budget_allows_within_threshold() {
        let budget = CardinalityBudget::new(100, 200);
        let allowed = budget.check_cardinality("test_metric", &[("key", "value1")]);
        assert!(allowed);
    }

    #[test]
    fn test_budget_drops_above_max() {
        let budget = CardinalityBudget::new(5, 10);

        // Insert enough distinct label sets to exceed max
        for i in 0..100 {
            let val = format!("value_{i}");
            budget.check_cardinality("test_metric", &[("key", &val)]);
        }

        let estimated = budget.estimated_cardinality("test_metric").unwrap();
        assert!(estimated > 10, "estimated {estimated} should exceed max threshold 10");

        // Next observation should be dropped
        let allowed = budget.check_cardinality("test_metric", &[("key", "new_value_xyz")]);
        assert!(!allowed, "observation should be dropped when cardinality exceeds max");
    }

    #[test]
    fn test_budget_warns_at_threshold() {
        let budget = CardinalityBudget::new(5, 1000);

        // Insert enough to exceed warn but not max
        for i in 0..50 {
            let val = format!("value_{i}");
            budget.check_cardinality("test_metric", &[("key", &val)]);
        }

        let estimated = budget.estimated_cardinality("test_metric").unwrap();
        assert!(estimated > 5, "estimated {estimated} should exceed warn threshold 5");

        // Should still be allowed (under max)
        let allowed = budget.check_cardinality("test_metric", &[("key", "still_allowed")]);
        assert!(allowed, "observation should be allowed when under max threshold");
    }

    #[test]
    fn test_budget_update_thresholds() {
        let budget = CardinalityBudget::new(100, 200);

        // Insert some data
        for i in 0..50 {
            let val = format!("value_{i}");
            budget.check_cardinality("test_metric", &[("key", &val)]);
        }

        // Lower the max threshold below current cardinality
        budget.update_thresholds(5, 10);

        // Next observation should be dropped
        let allowed = budget.check_cardinality("test_metric", &[("key", "should_drop")]);
        assert!(!allowed, "lowered max threshold should cause drops");
    }

    #[test]
    fn test_budget_independent_metric_families() {
        let budget = CardinalityBudget::new(100, 200);

        for i in 0..50 {
            let val = format!("value_{i}");
            budget.check_cardinality("metric_a", &[("key", &val)]);
        }

        // metric_b should start fresh
        let est = budget.estimated_cardinality("metric_b");
        assert!(est.is_none(), "unobserved metric should have no estimate");

        budget.check_cardinality("metric_b", &[("key", "first")]);
        let est_b = budget.estimated_cardinality("metric_b").unwrap();
        let est_a = budget.estimated_cardinality("metric_a").unwrap();

        assert!(
            est_b < est_a,
            "metric_b ({est_b}) should have fewer distinct values than metric_a ({est_a})"
        );
    }

    #[test]
    fn test_hash_label_set_deterministic() {
        let labels = [("namespace_id", "123"), ("operation", "write")];
        let h1 = hash_label_set(&labels);
        let h2 = hash_label_set(&labels);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_label_set_different_values_produce_different_hashes() {
        let h1 = hash_label_set(&[("key", "value1")]);
        let h2 = hash_label_set(&[("key", "value2")]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_overflow_counter_increments() {
        let budget = CardinalityBudget::new(5, 10);

        // Fill up the budget
        for i in 0..100 {
            let val = format!("value_{i}");
            budget.check_cardinality("overflow_metric", &[("key", &val)]);
        }

        // The record_cardinality_overflow function was called internally;
        // we verify via the return value that observations are dropped
        let allowed = budget.check_cardinality("overflow_metric", &[("key", "dropped")]);
        assert!(!allowed);
    }
}
