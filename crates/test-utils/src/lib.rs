//! Shared test utilities for InferaDB Ledger crates.
//!
//! This crate provides common test helpers to reduce boilerplate across test modules:
//!
//! - [`TestDir`] - Managed temporary directory with path helpers
//! - [`assert_eventually`] - Poll a condition until it's true or timeout
//! - [`test_batch_config`] - Default batch configuration for tests
//! - [`test_rate_limit_config`] - Default rate limit configuration for tests

#![deny(unsafe_code)]
// Test utilities are allowed to use unwrap for simplicity
#![cfg_attr(test, allow(clippy::disallowed_methods))]

mod test_dir;
pub use test_dir::TestDir;

mod assertions;
pub use assertions::assert_eventually;

mod config;
pub use config::{TestRateLimitConfig, test_batch_config, test_rate_limit_config};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    // ============================================
    // TestDir tests
    // ============================================

    #[test]
    fn test_dir_creates_temp_directory() {
        let dir = TestDir::new();
        assert!(dir.path().exists(), "temp directory should exist");
        assert!(dir.path().is_dir(), "should be a directory");
    }

    #[test]
    fn test_dir_path_returns_valid_path() {
        let dir = TestDir::new();
        let path = dir.path();
        std::fs::write(path.join("test.txt"), "hello").expect("write file");
        assert!(path.join("test.txt").exists());
    }

    #[test]
    fn test_dir_join_creates_subdirectory_path() {
        let dir = TestDir::new();
        let subpath = dir.join("subdir/nested");
        assert!(subpath.starts_with(dir.path()));
        assert!(subpath.ends_with("subdir/nested"));
    }

    #[test]
    fn test_dir_cleanup_on_drop() {
        let path = {
            let dir = TestDir::new();
            let p = dir.path().to_path_buf();
            std::fs::write(p.join("file.txt"), "data").expect("write file");
            assert!(p.exists());
            p
        };
        assert!(!path.exists(), "temp directory should be cleaned up on drop");
    }

    // ============================================
    // assert_eventually tests
    // ============================================

    #[tokio::test]
    async fn test_assert_eventually_immediate_success() {
        let result = assert_eventually(Duration::from_millis(100), || true).await;
        assert!(result, "immediately true condition should succeed");
    }

    #[tokio::test]
    async fn test_assert_eventually_delayed_success() {
        // Condition becomes true after a few iterations
        let counter = AtomicUsize::new(0);
        let result = assert_eventually(Duration::from_millis(500), || {
            let val = counter.fetch_add(1, Ordering::SeqCst);
            val >= 3 // Becomes true on 4th call
        })
        .await;
        assert!(result, "condition should eventually become true");
        assert!(counter.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn test_assert_eventually_timeout() {
        let result = assert_eventually(Duration::from_millis(50), || false).await;
        assert!(!result, "never-true condition should timeout");
    }

    #[tokio::test]
    async fn test_assert_eventually_with_state() {
        use std::sync::{Arc, Mutex};

        // Simulate async state change
        let state = Arc::new(Mutex::new(0));
        let state_clone = state.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            *state_clone.lock().unwrap() = 42;
        });

        let result =
            assert_eventually(Duration::from_millis(200), || *state.lock().unwrap() == 42).await;
        assert!(result, "should detect state change");
    }

    // ============================================
    // Config helper tests
    // ============================================

    #[test]
    fn test_batch_config_returns_valid_config() {
        let config = test_batch_config();
        assert!(config.max_batch_size > 0, "batch size should be positive");
        assert!(config.batch_timeout.as_millis() > 0, "timeout should be positive");
    }

    #[test]
    fn test_rate_limit_config_returns_valid_config() {
        let config = test_rate_limit_config();
        assert!(config.max_concurrent > 0, "max concurrent should be positive");
        assert!(config.timeout_secs > 0, "timeout should be positive");
    }

    #[test]
    fn test_rate_limit_config_builder_with_defaults() {
        // When: Building with all defaults
        let config = TestRateLimitConfig::builder().build();
        // Then: Should use sensible test defaults (permissive values)
        assert!(config.max_concurrent > 0, "max_concurrent should have a positive default");
        assert!(config.timeout_secs > 0, "timeout_secs should have a positive default");
    }

    #[test]
    fn test_rate_limit_config_builder_with_custom_values() {
        // When: Building with custom values
        let config = TestRateLimitConfig::builder().max_concurrent(50).timeout_secs(15).build();
        // Then: Should use the custom values
        assert_eq!(config.max_concurrent, 50);
        assert_eq!(config.timeout_secs, 15);
    }

    #[test]
    fn test_rate_limit_config_builder_matches_factory_function() {
        // When: Using builder with defaults vs factory function
        let from_builder = TestRateLimitConfig::builder().build();
        let from_factory = test_rate_limit_config();
        // Then: Both should produce the same configuration
        assert_eq!(from_builder.max_concurrent, from_factory.max_concurrent);
        assert_eq!(from_builder.timeout_secs, from_factory.timeout_secs);
    }
}
