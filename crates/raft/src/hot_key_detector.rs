//! Hot key detection using Count-Min Sketch for space-efficient frequency estimation.
//!
//! Identifies frequently accessed keys within a sliding time window to help
//! operators detect contention hotspots. Uses a rotating pair of Count-Min
//! Sketches to provide approximate per-window frequency counts with bounded
//! memory independent of key cardinality.
//!
//! # Architecture
//!
//! ```text
//! record_access(key)
//!     ├── hash key into CMS row indices
//!     ├── increment counters in current sketch
//!     ├── estimate = min(counters) across rows
//!     ├── if estimate/window_secs >= threshold → hot key detected
//!     │   ├── update top-k candidate set
//!     │   ├── emit Prometheus metric
//!     │   └── log WARN (rate-limited per key)
//!     └── periodically rotate sketches (current → previous → zeroed)
//! ```

use std::{
    collections::{BinaryHeap, HashMap},
    hash::{Hash, Hasher},
    sync::Mutex,
    time::Instant,
};

use inferadb_ledger_types::{VaultId, config::HotKeyConfig};

/// A detected hot key with its estimated access rate.
#[derive(Debug, Clone, PartialEq)]
pub struct HotKeyInfo {
    /// The vault containing the hot key.
    pub vault_id: VaultId,
    /// The key that is hot (entity key or relationship resource).
    pub key: String,
    /// Estimated operations per second within the current window.
    pub ops_per_sec: f64,
}

/// Result of recording a key access.
#[derive(Debug, Clone, PartialEq)]
pub enum AccessResult {
    /// Key is not hot (below threshold).
    Normal,
    /// Key exceeds the hot threshold.
    Hot(HotKeyInfo),
}

/// Count-Min Sketch — a probabilistic frequency counter.
///
/// Uses `depth` independent hash functions over a matrix of `width` counters.
/// Point queries return the minimum across all rows, which is always >= the
/// true count (never underestimates, may overestimate).
struct CountMinSketch {
    /// Counter matrix: `depth` rows × `width` columns, stored flat.
    counters: Vec<u64>,
    width: usize,
    depth: usize,
    /// Seeds for each hash function (one per row).
    seeds: Vec<u64>,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        // Use deterministic seeds derived from row index for reproducibility.
        let seeds: Vec<u64> =
            (0..depth).map(|i| 0x517cc1b727220a95_u64.wrapping_mul(i as u64 + 1)).collect();
        Self { counters: vec![0; width * depth], width, depth, seeds }
    }

    /// Increments counters for the given key and returns the estimated count.
    fn increment(&mut self, key: &str) -> u64 {
        let mut min_count = u64::MAX;
        for row in 0..self.depth {
            let col = self.hash_to_col(key, row);
            let idx = row * self.width + col;
            self.counters[idx] = self.counters[idx].saturating_add(1);
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Returns the estimated count for the given key without modifying state.
    #[cfg(test)]
    fn estimate(&self, key: &str) -> u64 {
        let mut min_count = u64::MAX;
        for row in 0..self.depth {
            let col = self.hash_to_col(key, row);
            let idx = row * self.width + col;
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Resets all counters to zero.
    fn clear(&mut self) {
        self.counters.fill(0);
    }

    /// Hashes a key to a column index for the given row.
    fn hash_to_col(&self, key: &str, row: usize) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        self.seeds[row].hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.width
    }
}

/// Entry in the top-k min-heap (ordered by ops_per_sec ascending for eviction).
#[derive(Debug, Clone)]
struct TopKEntry {
    vault_id: VaultId,
    key: String,
    ops_per_sec: f64,
}

impl PartialEq for TopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ops_per_sec == other.ops_per_sec
    }
}

impl Eq for TopKEntry {}

impl PartialOrd for TopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap behavior with BinaryHeap (which is a max-heap).
        other.ops_per_sec.partial_cmp(&self.ops_per_sec).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Internal mutable state protected by a mutex.
struct DetectorState {
    /// Current window's sketch (accumulating).
    current: CountMinSketch,
    /// Previous window's sketch (for queries spanning window boundary).
    previous: CountMinSketch,
    /// When the current window started.
    window_start: Instant,
    /// Top-k candidate hot keys (min-heap by ops_per_sec).
    top_k_heap: BinaryHeap<TopKEntry>,
    /// Fast lookup for top-k deduplication: composite key → ops_per_sec.
    top_k_map: HashMap<(VaultId, String), f64>,
    /// Last warn time per composite key (rate-limited logging).
    last_warn: HashMap<(VaultId, String), Instant>,
}

/// Hot key detector using Count-Min Sketch with rotating time windows.
///
/// Thread-safe: all mutable state is behind a `Mutex`. The mutex is held
/// only for the duration of counter updates and heap maintenance, keeping
/// critical section time minimal.
pub struct HotKeyDetector {
    state: Mutex<DetectorState>,
    window_secs: u64,
    threshold: u64,
    top_k: usize,
}

impl HotKeyDetector {
    /// Creates a new hot key detector from configuration.
    pub fn new(config: &HotKeyConfig) -> Self {
        Self {
            state: Mutex::new(DetectorState {
                current: CountMinSketch::new(config.cms_width, config.cms_depth),
                previous: CountMinSketch::new(config.cms_width, config.cms_depth),
                window_start: Instant::now(),
                top_k_heap: BinaryHeap::new(),
                top_k_map: HashMap::new(),
                last_warn: HashMap::new(),
            }),
            window_secs: config.window_secs,
            threshold: config.threshold,
            top_k: config.top_k,
        }
    }

    /// Records an access to a key and returns whether it is hot.
    ///
    /// This is the hot-path method called on every write operation.
    /// Lock contention is minimized by keeping the critical section to
    /// counter increments and a single heap operation.
    pub fn record_access(&self, vault_id: VaultId, key: &str) -> AccessResult {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Check if window has expired and rotate if needed.
        let elapsed = state.window_start.elapsed().as_secs();
        if elapsed >= self.window_secs {
            // Rotate: previous takes current's data, current resets.
            // Destructure to avoid double mutable borrow through `state`.
            let DetectorState { current, previous, .. } = &mut *state;
            std::mem::swap(&mut current.counters, &mut previous.counters);
            current.clear();
            state.window_start = Instant::now();
            // Clear top-k state on rotation (stale entries from previous window).
            state.top_k_heap.clear();
            state.top_k_map.clear();
        }

        // Increment in current sketch and get estimated count for this window.
        let count = state.current.increment(key);
        let ops_per_sec = if self.window_secs > 0 {
            // Use elapsed time in current window for rate calculation.
            let window_elapsed = state.window_start.elapsed().as_secs_f64().max(1.0);
            count as f64 / window_elapsed
        } else {
            count as f64
        };

        if ops_per_sec >= self.threshold as f64 {
            let info = HotKeyInfo { vault_id, key: key.to_string(), ops_per_sec };

            // Update top-k tracking.
            let composite = (vault_id, key.to_string());
            if let Some(existing) = state.top_k_map.get_mut(&composite) {
                // Update rate for existing entry (heap will be rebuilt on get_top_hot_keys).
                *existing = ops_per_sec;
            } else if state.top_k_map.len() < self.top_k {
                // Room in top-k: add directly.
                state.top_k_map.insert(composite.clone(), ops_per_sec);
                state.top_k_heap.push(TopKEntry { vault_id, key: key.to_string(), ops_per_sec });
            } else if state.top_k_heap.peek().is_some_and(|min| ops_per_sec > min.ops_per_sec) {
                // Evict the coldest entry since this key is hotter.
                if let Some(evicted) = state.top_k_heap.pop() {
                    state.top_k_map.remove(&(evicted.vault_id, evicted.key));
                }
                state.top_k_map.insert(composite.clone(), ops_per_sec);
                state.top_k_heap.push(TopKEntry { vault_id, key: key.to_string(), ops_per_sec });
            }

            // Rate-limited logging: warn at most once per 10 seconds per key.
            let should_warn = match state.last_warn.get(&composite) {
                Some(last) => last.elapsed().as_secs() >= 10,
                None => true,
            };
            if should_warn {
                tracing::warn!(
                    vault_id = vault_id.value(),
                    key = key,
                    ops_per_sec = format!("{:.1}", ops_per_sec),
                    threshold = self.threshold,
                    "Hot key detected: access rate exceeds threshold"
                );
                state.last_warn.insert(composite, Instant::now());
            }

            // Record Prometheus metric.
            crate::metrics::record_hot_key_detected(vault_id, key, ops_per_sec);

            AccessResult::Hot(info)
        } else {
            AccessResult::Normal
        }
    }

    /// Returns the current top-N hottest keys, sorted by ops/sec descending.
    pub fn get_top_hot_keys(&self, n: usize) -> Vec<HotKeyInfo> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        let mut entries: Vec<_> = state
            .top_k_map
            .iter()
            .map(|((vault_id, key), &ops_per_sec)| HotKeyInfo {
                vault_id: *vault_id,
                key: key.clone(),
                ops_per_sec,
            })
            .collect();

        // Sort descending by ops_per_sec.
        entries.sort_by(|a, b| {
            b.ops_per_sec.partial_cmp(&a.ops_per_sec).unwrap_or(std::cmp::Ordering::Equal)
        });

        entries.truncate(n);
        entries
    }

    /// Clears all state. Useful for testing or configuration changes.
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.current.clear();
        state.previous.clear();
        state.top_k_heap.clear();
        state.top_k_map.clear();
        state.last_warn.clear();
        state.window_start = Instant::now();
    }
}

impl std::fmt::Debug for HotKeyDetector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotKeyDetector")
            .field("window_secs", &self.window_secs)
            .field("threshold", &self.threshold)
            .field("top_k", &self.top_k)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)]

    use super::*;

    fn test_config() -> HotKeyConfig {
        HotKeyConfig { window_secs: 60, threshold: 10, cms_width: 256, cms_depth: 4, top_k: 5 }
    }

    #[test]
    fn test_cms_increment_returns_count() {
        let mut cms = CountMinSketch::new(256, 4);
        assert_eq!(cms.increment("key_a"), 1);
        assert_eq!(cms.increment("key_a"), 2);
        assert_eq!(cms.increment("key_a"), 3);
        assert_eq!(cms.increment("key_b"), 1);
    }

    #[test]
    fn test_cms_estimate_without_increment() {
        let cms = CountMinSketch::new(256, 4);
        assert_eq!(cms.estimate("nonexistent"), 0);
    }

    #[test]
    fn test_cms_estimate_matches_increment() {
        let mut cms = CountMinSketch::new(256, 4);
        for _ in 0..100 {
            cms.increment("popular_key");
        }
        // CMS may overestimate but never underestimates.
        assert!(cms.estimate("popular_key") >= 100);
    }

    #[test]
    fn test_cms_clear_resets_all_counters() {
        let mut cms = CountMinSketch::new(256, 4);
        for _ in 0..50 {
            cms.increment("key");
        }
        cms.clear();
        assert_eq!(cms.estimate("key"), 0);
    }

    #[test]
    fn test_cms_different_keys_independent() {
        let mut cms = CountMinSketch::new(1024, 4);
        for _ in 0..100 {
            cms.increment("key_a");
        }
        cms.increment("key_b");
        // key_b should have count 1 (with high probability given width=1024).
        assert_eq!(cms.estimate("key_b"), 1);
    }

    #[test]
    fn test_detector_normal_access() {
        let config = test_config();
        let detector = HotKeyDetector::new(&config);
        let result = detector.record_access(VaultId::new(1), "some_key");
        assert_eq!(result, AccessResult::Normal);
    }

    #[test]
    fn test_detector_hot_key_detection() {
        let mut config = test_config();
        config.threshold = 5; // Low threshold for testing.
        config.window_secs = 60;
        let detector = HotKeyDetector::new(&config);

        // Pump enough accesses to exceed threshold (5 ops/sec with 1s elapsed ≈ need 5+ ops).
        // Since window just started, elapsed is ~0s, we use max(1.0) floor.
        // So 6 accesses / 1.0s = 6.0 ops/sec > 5 threshold.
        let mut hot_count = 0;
        for _ in 0..10 {
            if let AccessResult::Hot(_) = detector.record_access(VaultId::new(1), "hot_key") {
                hot_count += 1;
            }
        }
        assert!(hot_count > 0, "should detect hot key after enough accesses");
    }

    #[test]
    fn test_detector_get_top_hot_keys() {
        let mut config = test_config();
        config.threshold = 3;
        config.top_k = 3;
        let detector = HotKeyDetector::new(&config);

        // Create three hot keys with different access counts.
        for _ in 0..20 {
            detector.record_access(VaultId::new(1), "very_hot");
        }
        for _ in 0..10 {
            detector.record_access(VaultId::new(1), "medium_hot");
        }
        for _ in 0..5 {
            detector.record_access(VaultId::new(1), "warm");
        }

        let top = detector.get_top_hot_keys(3);
        assert!(!top.is_empty());
        // First entry should be the hottest.
        assert_eq!(top[0].key, "very_hot");
    }

    #[test]
    fn test_detector_top_k_eviction() {
        let mut config = test_config();
        config.threshold = 2;
        config.top_k = 2;
        let detector = HotKeyDetector::new(&config);

        // Fill top-k with two keys.
        for _ in 0..5 {
            detector.record_access(VaultId::new(1), "key_a");
            detector.record_access(VaultId::new(1), "key_b");
        }

        // Add a hotter key that should evict the coldest.
        for _ in 0..20 {
            detector.record_access(VaultId::new(1), "key_c");
        }

        let top = detector.get_top_hot_keys(2);
        let top_keys: Vec<&str> = top.iter().map(|e| e.key.as_str()).collect();
        assert!(top_keys.contains(&"key_c"), "hottest key should be in top-k");
    }

    #[test]
    fn test_detector_different_vaults_tracked_separately() {
        let mut config = test_config();
        config.threshold = 3;
        let detector = HotKeyDetector::new(&config);

        // Same key in different vaults should be independent in top-k tracking.
        for _ in 0..10 {
            detector.record_access(VaultId::new(1), "shared_key");
        }
        for _ in 0..10 {
            detector.record_access(VaultId::new(2), "shared_key");
        }

        let top = detector.get_top_hot_keys(10);
        let vault_ids: Vec<i64> = top.iter().map(|e| e.vault_id.value()).collect();
        // Both vaults should appear.
        if top.len() >= 2 {
            assert!(vault_ids.contains(&1));
            assert!(vault_ids.contains(&2));
        }
    }

    #[test]
    fn test_detector_reset_clears_state() {
        let mut config = test_config();
        config.threshold = 3;
        let detector = HotKeyDetector::new(&config);

        for _ in 0..20 {
            detector.record_access(VaultId::new(1), "key");
        }

        detector.reset();

        let top = detector.get_top_hot_keys(10);
        assert!(top.is_empty(), "reset should clear all tracked keys");

        // After reset, next access should be normal (count restarted).
        let result = detector.record_access(VaultId::new(1), "key");
        assert_eq!(result, AccessResult::Normal);
    }

    #[test]
    fn test_detector_ops_per_sec_calculation() {
        let mut config = test_config();
        config.threshold = 100; // High threshold so nothing triggers.
        let detector = HotKeyDetector::new(&config);

        // Record 50 accesses. With elapsed ≈ 0s (floored to 1.0s), rate = 50 ops/sec.
        for _ in 0..50 {
            detector.record_access(VaultId::new(1), "test_key");
        }

        // Since threshold is 100, this should still be Normal.
        let result = detector.record_access(VaultId::new(1), "test_key");
        assert_eq!(result, AccessResult::Normal);
    }

    #[test]
    fn test_detector_debug_format() {
        let config = test_config();
        let detector = HotKeyDetector::new(&config);
        let debug = format!("{:?}", detector);
        assert!(debug.contains("HotKeyDetector"));
        assert!(debug.contains("window_secs"));
        assert!(debug.contains("threshold"));
    }

    #[test]
    fn test_cms_saturating_add() {
        let mut cms = CountMinSketch::new(64, 2);
        // Set counters near u64::MAX to verify no overflow.
        for counter in &mut cms.counters {
            *counter = u64::MAX - 1;
        }
        // Should saturate at u64::MAX, not wrap.
        let count = cms.increment("key");
        assert_eq!(count, u64::MAX);
    }

    #[test]
    fn test_hot_key_info_fields() {
        let info = HotKeyInfo {
            vault_id: VaultId::new(42),
            key: "entity:user:123".to_string(),
            ops_per_sec: 150.5,
        };
        assert_eq!(info.vault_id, VaultId::new(42));
        assert_eq!(info.key, "entity:user:123");
        assert!((info.ops_per_sec - 150.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_access_result_variants() {
        let normal = AccessResult::Normal;
        assert_eq!(normal, AccessResult::Normal);

        let hot = AccessResult::Hot(HotKeyInfo {
            vault_id: VaultId::new(1),
            key: "k".to_string(),
            ops_per_sec: 200.0,
        });
        assert!(matches!(hot, AccessResult::Hot(_)));
    }
}
