//! Test assertion helpers.
//!
//! Provides polling-based assertions for async test scenarios.

use std::time::Duration;

use tokio::time::{Instant, sleep};

/// Default polling interval for [`assert_eventually`].
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Polls a condition until it returns true or the timeout expires.
///
/// This is useful for testing async operations where the exact timing
/// is non-deterministic. It avoids flaky tests that use fixed sleeps.
///
/// # Arguments
///
/// * `timeout` - Maximum wait duration
/// * `condition` - Closure returning `true` when the expected state holds
///
/// # Returns
///
/// `true` if the condition became true before timeout, `false` otherwise.
///
/// # Example
///
/// ```no_run
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::sync::Arc;
/// use std::time::Duration;
/// use inferadb_ledger_test_utils::assert_eventually;
///
/// #[tokio::test]
/// async fn test_async_operation() {
///     let flag = Arc::new(AtomicBool::new(false));
///     let flag_clone = flag.clone();
///
///     // Spawn task that sets flag after some work
///     tokio::spawn(async move {
///         tokio::time::sleep(Duration::from_millis(50)).await;
///         flag_clone.store(true, Ordering::SeqCst);
///     });
///
///     // Wait for flag to be set
///     let result = assert_eventually(Duration::from_millis(200), || {
///         flag.load(Ordering::SeqCst)
///     }).await;
///
///     assert!(result, "flag should be set");
/// }
/// ```
pub async fn assert_eventually<F>(timeout: Duration, condition: F) -> bool
where
    F: Fn() -> bool,
{
    let start = Instant::now();

    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        sleep(DEFAULT_POLL_INTERVAL).await;
    }

    // Final check after timeout
    condition()
}
