//! Test configuration helpers.
//!
//! Provides sensible default configurations for tests, centralizing
//! magic values that would otherwise be scattered across test modules.

use std::time::Duration;

use inferadb_ledger_types::config::BatchConfig;

/// Returns a default batch configuration suitable for tests.
///
/// Uses small values for fast test execution:
/// - `max_batch_size`: 10 (small batches for quick iteration)
/// - `batch_timeout`: 10ms (fast timeout for tests)
/// - `coalesce_enabled`: false (predictable behavior)
#[must_use]
pub fn test_batch_config() -> BatchConfig {
    BatchConfig {
        max_batch_size: 10,
        batch_timeout: Duration::from_millis(10),
        coalesce_enabled: false,
    }
}

/// Rate limiting configuration for tests.
///
/// This is a simple struct for test utilities since the production
/// `RateLimitConfig` is in the server crate.
#[derive(Debug, Clone)]
pub struct TestRateLimitConfig {
    /// Maximum concurrent requests.
    pub max_concurrent: usize,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

/// Returns a default rate limit configuration suitable for tests.
///
/// Uses permissive values to avoid test flakiness:
/// - `max_concurrent`: 100 (high concurrency for parallel tests)
/// - `timeout_secs`: 30 (generous timeout)
#[must_use]
pub fn test_rate_limit_config() -> TestRateLimitConfig {
    TestRateLimitConfig { max_concurrent: 100, timeout_secs: 30 }
}
