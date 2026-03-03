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
