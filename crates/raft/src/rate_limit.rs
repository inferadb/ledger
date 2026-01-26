//! Per-namespace rate limiting.
//!
//! Per DESIGN.md §3.7: Mitigates noisy neighbor problems in multi-tenant shards
//! by applying rate limits per namespace_id at the shard leader.
//!
//! Uses a simple sliding window counter for simplicity. For production use cases
//! requiring more sophisticated rate limiting (token bucket, leaky bucket),
//! this module can be extended.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use inferadb_ledger_types::NamespaceId;
use parking_lot::Mutex;

/// Default requests per second per namespace.
const DEFAULT_REQUESTS_PER_SECOND: u64 = 1000;

/// Default window size for sliding window counter.
const DEFAULT_WINDOW_SIZE: Duration = Duration::from_secs(1);

/// Per-namespace rate limiter.
///
/// Uses a sliding window counter to track requests per namespace.
/// When a namespace exceeds its limit, new requests are rejected.
#[derive(Debug)]
pub struct NamespaceRateLimiter {
    /// Rate limit per namespace (requests per second).
    limit_per_namespace: u64,
    /// Window size for counting requests.
    window_size: Duration,
    /// Per-namespace counters: (window_start, count).
    counters: Mutex<HashMap<NamespaceId, WindowCounter>>,
    /// Total requests rejected due to rate limiting (for metrics).
    rejected_count: AtomicU64,
}

/// A sliding window counter for a single namespace.
#[derive(Debug)]
struct WindowCounter {
    /// Start of the current window.
    window_start: Instant,
    /// Count of requests in the current window.
    count: u64,
}

impl NamespaceRateLimiter {
    /// Create a new rate limiter with default settings.
    pub fn new() -> Self {
        Self::with_limit(DEFAULT_REQUESTS_PER_SECOND)
    }

    /// Create a rate limiter with a specific requests-per-second limit.
    pub fn with_limit(requests_per_second: u64) -> Self {
        Self {
            limit_per_namespace: requests_per_second,
            window_size: DEFAULT_WINDOW_SIZE,
            counters: Mutex::new(HashMap::new()),
            rejected_count: AtomicU64::new(0),
        }
    }

    /// Check if a request should be allowed for the given namespace.
    ///
    /// Returns `Ok(())` if the request is allowed, `Err(RateLimitExceeded)` otherwise.
    pub fn check(&self, namespace_id: NamespaceId) -> Result<(), RateLimitExceeded> {
        let now = Instant::now();
        let mut counters = self.counters.lock();

        let counter = counters
            .entry(namespace_id)
            .or_insert_with(|| WindowCounter { window_start: now, count: 0 });

        // If we're in a new window, reset the counter
        if now.duration_since(counter.window_start) >= self.window_size {
            counter.window_start = now;
            counter.count = 0;
        }

        // Check if we're within the limit
        if counter.count >= self.limit_per_namespace {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(RateLimitExceeded {
                namespace_id,
                limit: self.limit_per_namespace,
                window_size: self.window_size,
            });
        }

        // Allow the request
        counter.count += 1;
        Ok(())
    }

    /// Get the number of rejected requests (for metrics).
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(Ordering::Relaxed)
    }

    /// Get the current request count for a namespace.
    ///
    /// Useful for metrics and debugging.
    #[allow(dead_code)] // public API: utility for rate limit inspection
    pub fn current_count(&self, namespace_id: NamespaceId) -> u64 {
        let counters = self.counters.lock();
        counters.get(&namespace_id).map(|c| c.count).unwrap_or(0)
    }

    /// Remove stale entries that haven't been accessed recently.
    ///
    /// Call periodically to prevent unbounded memory growth.
    pub fn cleanup_stale(&self, max_age: Duration) {
        let now = Instant::now();
        let mut counters = self.counters.lock();
        counters.retain(|_, counter| now.duration_since(counter.window_start) < max_age);
    }
}

impl Default for NamespaceRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned when a namespace exceeds its rate limit.
#[derive(Debug, Clone)]
pub struct RateLimitExceeded {
    /// The namespace that exceeded its limit.
    pub namespace_id: NamespaceId,
    /// The configured limit.
    pub limit: u64,
    /// The window size.
    pub window_size: Duration,
}

impl std::fmt::Display for RateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rate limit exceeded for namespace {}: {} requests per {:?}",
            self.namespace_id, self.limit, self.window_size
        )
    }
}

impl std::error::Error for RateLimitExceeded {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_allows_under_limit() {
        let limiter = NamespaceRateLimiter::with_limit(10);

        // Should allow 10 requests
        for _ in 0..10 {
            assert!(limiter.check(1).is_ok());
        }
    }

    #[test]
    fn test_rejects_over_limit() {
        let limiter = NamespaceRateLimiter::with_limit(10);

        // Should allow 10 requests
        for _ in 0..10 {
            limiter.check(1).unwrap();
        }

        // 11th should be rejected
        assert!(limiter.check(1).is_err());
        assert_eq!(limiter.rejected_count(), 1);
    }

    #[test]
    fn test_separate_namespaces() {
        let limiter = NamespaceRateLimiter::with_limit(5);

        // Namespace 1: use all 5
        for _ in 0..5 {
            limiter.check(1).unwrap();
        }

        // Namespace 2: should still have its own quota
        for _ in 0..5 {
            assert!(limiter.check(2).is_ok());
        }

        // Both should now be at limit
        assert!(limiter.check(1).is_err());
        assert!(limiter.check(2).is_err());
    }

    #[test]
    fn test_default_rate_limiter() {
        let limiter = NamespaceRateLimiter::new();
        // Should allow many requests with default limit of 1000
        for _ in 0..100 {
            assert!(limiter.check(1).is_ok());
        }
    }

    #[test]
    fn test_error_display() {
        let err =
            RateLimitExceeded { namespace_id: 42, limit: 100, window_size: Duration::from_secs(1) };
        let msg = format!("{}", err);
        assert!(msg.contains("42"));
        assert!(msg.contains("100"));
    }
}
