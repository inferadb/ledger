//! Runtime configuration with lock-free hot reload.
//!
//! Provides [`RuntimeConfigHandle`] — a thread-safe handle to the current
//! runtime configuration that supports atomic updates. Components read the
//! config on every RPC via `ArcSwap::load()` (lock-free, wait-free on x86)
//! and operators update it via the `UpdateConfig` RPC or SIGHUP.

use std::sync::Arc;

use arc_swap::ArcSwap;
use inferadb_ledger_types::config::{ConfigError, RuntimeConfig};
use tracing::{info, warn};

use crate::{hot_key_detector::HotKeyDetector, rate_limit::RateLimiter};

/// Thread-safe handle to the current runtime configuration.
///
/// Uses `ArcSwap` for lock-free reads and atomic writes. Safe to clone
/// and share across tasks — all clones point to the same underlying config.
///
/// # Performance
///
/// - **Reads**: Lock-free via `ArcSwap::load()`. No contention on the hot path.
/// - **Writes**: Atomic pointer swap. Writers do not block readers.
#[derive(Debug, Clone)]
pub struct RuntimeConfigHandle {
    inner: Arc<ArcSwap<RuntimeConfig>>,
}

impl RuntimeConfigHandle {
    /// Create a new handle with the given initial configuration.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        Self { inner: Arc::new(ArcSwap::from_pointee(config)) }
    }

    /// Load the current configuration.
    ///
    /// Returns a reference-counted snapshot. The returned `Arc` is valid
    /// even if the config is updated concurrently — callers see a consistent
    /// snapshot until they drop the `Arc` and call `load()` again.
    #[must_use]
    pub fn load(&self) -> arc_swap::Guard<Arc<RuntimeConfig>> {
        self.inner.load()
    }

    /// Atomically update the configuration.
    ///
    /// Validates `new_config` before swapping. Returns the list of changed
    /// field paths (for audit logging) or a validation error.
    ///
    /// # Side Effects
    ///
    /// After swapping the config, this method propagates changes to
    /// dependent components (rate limiter, hot key detector) if provided.
    pub fn update(
        &self,
        new_config: RuntimeConfig,
        rate_limiter: Option<&Arc<RateLimiter>>,
        hot_key_detector: Option<&Arc<HotKeyDetector>>,
    ) -> Result<Vec<String>, ConfigError> {
        new_config.validate()?;

        let old_config = self.inner.load();
        let changed = old_config.diff(&new_config);

        if changed.is_empty() {
            return Ok(changed);
        }

        // Propagate rate limit config changes to the live RateLimiter.
        if changed.contains(&"rate_limit".to_string()) {
            if let Some(limiter) = rate_limiter {
                if let Some(ref rl) = new_config.rate_limit {
                    limiter.update_config(
                        rl.client_burst,
                        rl.client_rate,
                        rl.namespace_burst,
                        rl.namespace_rate,
                        rl.backpressure_threshold,
                    );
                    info!(
                        client_burst = rl.client_burst,
                        client_rate = rl.client_rate,
                        namespace_burst = rl.namespace_burst,
                        namespace_rate = rl.namespace_rate,
                        backpressure_threshold = rl.backpressure_threshold,
                        "Rate limiter config updated"
                    );
                }
            } else {
                warn!("Rate limit config changed but no rate limiter is configured");
            }
        }

        // Propagate hot key config changes to the live HotKeyDetector.
        if changed.contains(&"hot_key".to_string()) {
            if let Some(detector) = hot_key_detector {
                if let Some(ref hk) = new_config.hot_key {
                    detector.update_thresholds(hk.threshold, hk.window_secs);
                    info!(
                        threshold = hk.threshold,
                        window_secs = hk.window_secs,
                        "Hot key detector thresholds updated"
                    );
                }
            } else {
                warn!("Hot key config changed but no hot key detector is configured");
            }
        }

        // Compaction config changes take effect on the next compaction cycle
        // (the compactor reads from RuntimeConfigHandle).
        if changed.contains(&"compaction".to_string()) {
            info!("Compaction config updated — takes effect next cycle");
        }

        // Validation config changes take effect immediately (services read from
        // RuntimeConfigHandle on each request).
        if changed.contains(&"validation".to_string()) {
            info!("Validation config updated — takes effect on next request");
        }

        // Atomic swap — all subsequent load() calls see the new config.
        self.inner.store(Arc::new(new_config));

        Ok(changed)
    }

    /// Replace the configuration without validation or side effects.
    ///
    /// Used by the SIGHUP handler after external validation.
    pub fn store(&self, config: RuntimeConfig) {
        self.inner.store(Arc::new(config));
    }
}

impl Default for RuntimeConfigHandle {
    fn default() -> Self {
        Self::new(RuntimeConfig::default())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::config::{HotKeyConfig, RateLimitConfig, ValidationConfig};

    use super::*;

    #[test]
    fn test_load_returns_initial_config() {
        let config = RuntimeConfig::builder().rate_limit(RateLimitConfig::default()).build();
        let handle = RuntimeConfigHandle::new(config.clone());

        let loaded = handle.load();
        assert_eq!(*loaded.as_ref(), config);
    }

    #[test]
    fn test_update_swaps_config() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let new_config = RuntimeConfig::builder().rate_limit(RateLimitConfig::default()).build();

        let changed = handle.update(new_config.clone(), None, None).unwrap();
        assert!(changed.contains(&"rate_limit".to_string()));

        let loaded = handle.load();
        assert_eq!(loaded.rate_limit, Some(RateLimitConfig::default()));
    }

    #[test]
    fn test_update_reports_no_changes_when_identical() {
        let config = RuntimeConfig::default();
        let handle = RuntimeConfigHandle::new(config.clone());

        let changed = handle.update(config, None, None).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_update_rejects_invalid_config() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let bad_config = RuntimeConfig::builder()
            .rate_limit(RateLimitConfig {
                client_burst: 0, // invalid: must be > 0
                ..RateLimitConfig::default()
            })
            .build();

        let result = handle.update(bad_config, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_detects_all_sections() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig::builder()
            .rate_limit(RateLimitConfig::default())
            .hot_key(HotKeyConfig::default())
            .validation(ValidationConfig::default())
            .build();

        let diff = a.diff(&b);
        assert!(diff.contains(&"rate_limit".to_string()));
        assert!(diff.contains(&"hot_key".to_string()));
        assert!(diff.contains(&"validation".to_string()));
    }

    #[test]
    fn test_store_replaces_config_without_validation() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let new_config = RuntimeConfig::builder().rate_limit(RateLimitConfig::default()).build();

        handle.store(new_config.clone());

        let loaded = handle.load();
        assert_eq!(loaded.rate_limit, Some(RateLimitConfig::default()));
    }

    #[test]
    fn test_clone_shares_same_config() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let clone = handle.clone();

        let new_config = RuntimeConfig::builder().rate_limit(RateLimitConfig::default()).build();
        handle.store(new_config);

        // Clone sees the update too
        let loaded = clone.load();
        assert!(loaded.rate_limit.is_some());
    }

    #[test]
    fn test_default_creates_empty_config() {
        let handle = RuntimeConfigHandle::default();
        let loaded = handle.load();
        assert!(loaded.rate_limit.is_none());
        assert!(loaded.hot_key.is_none());
        assert!(loaded.compaction.is_none());
        assert!(loaded.validation.is_none());
    }
}
