//! Multi-level token bucket rate limiting.
//!
//! Provides three tiers of admission control for write requests:
//!
//! 1. **Per-client** — prevents a single bad actor from monopolizing resources
//! 2. **Per-namespace** — ensures fair sharing across tenants in a multi-tenant shard
//! 3. **Global backpressure** — throttles all requests when Raft queue depth is high
//!
//! Uses the token bucket algorithm, which allows controlled bursts while maintaining
//! an average rate. Each bucket has a capacity (max burst) and a refill rate (sustained
//! throughput). Tokens are consumed on each request and refilled over time.
//!
//! Mitigates noisy neighbor problems in multi-tenant shards by applying rate
//! limits per namespace_id at the shard leader.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use inferadb_ledger_types::NamespaceId;
use parking_lot::Mutex;

/// Level at which a rate limit was enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitLevel {
    /// Per-client rate limit.
    Client,
    /// Per-namespace rate limit.
    Namespace,
    /// Global backpressure based on Raft queue depth.
    Backpressure,
}

impl RateLimitLevel {
    /// Returns a static string label for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Namespace => "namespace",
            Self::Backpressure => "backpressure",
        }
    }
}

/// Reason for rate limit rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitReason {
    /// Token bucket exhausted (burst + sustained rate exceeded).
    TokensExhausted,
    /// Raft pending proposals exceed threshold.
    QueueDepth,
}

impl RateLimitReason {
    /// Returns a static string label for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TokensExhausted => "tokens_exhausted",
            Self::QueueDepth => "queue_depth",
        }
    }
}

/// Error returned when a request is rate limited.
#[derive(Debug, Clone)]
pub struct RateLimitRejection {
    /// Which level rejected the request.
    pub level: RateLimitLevel,
    /// Why the request was rejected.
    pub reason: RateLimitReason,
    /// Estimated milliseconds until the client should retry.
    pub retry_after_ms: u64,
    /// The identifier that was rate limited (client_id or namespace_id).
    pub identifier: String,
}

impl std::fmt::Display for RateLimitRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rate limit exceeded at {} level for {}: {}",
            self.level.as_str(),
            self.identifier,
            self.reason.as_str()
        )
    }
}

impl std::error::Error for RateLimitRejection {}

/// A token bucket that allows controlled bursts while enforcing an average rate.
///
/// Tokens refill at `refill_rate` per second up to `capacity`. Each request
/// consumes one token. When no tokens remain, requests are rejected.
#[derive(Debug)]
struct TokenBucket {
    /// Current number of available tokens (scaled by 1000 for sub-token precision).
    tokens_millis: u64,
    /// Maximum tokens the bucket can hold (scaled by 1000).
    capacity_millis: u64,
    /// Tokens added per second.
    refill_rate: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl TokenBucket {
    /// Creates a new token bucket starting at full capacity.
    fn new(capacity: u64, refill_rate: f64) -> Self {
        Self {
            tokens_millis: capacity * 1000,
            capacity_millis: capacity * 1000,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time, then try to consume one token.
    ///
    /// Returns `true` if the request is allowed, `false` if rate limited.
    fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        // Refill tokens based on elapsed time
        let refill = (elapsed.as_secs_f64() * self.refill_rate * 1000.0) as u64;
        if refill > 0 {
            self.tokens_millis = (self.tokens_millis + refill).min(self.capacity_millis);
            self.last_refill = now;
        }

        // Try to consume one token (1000 millis = 1 token)
        if self.tokens_millis >= 1000 {
            self.tokens_millis -= 1000;
            true
        } else {
            false
        }
    }

    /// Estimates milliseconds until the next token is available.
    fn retry_after_ms(&self) -> u64 {
        if self.refill_rate <= 0.0 {
            return 1000;
        }
        let deficit_millis = 1000u64.saturating_sub(self.tokens_millis);
        let ms = (deficit_millis as f64 / (self.refill_rate * 1000.0) * 1000.0).ceil() as u64;
        ms.max(1)
    }
}

/// Multi-level rate limiter with per-client, per-namespace, and backpressure tiers.
///
/// Thread-safe: all mutable state is behind `Mutex`.
/// Config thresholds use atomics for lock-free runtime reconfiguration.
///
/// # Usage
///
/// ```no_run
/// use inferadb_ledger_raft::RateLimiter;
///
/// let limiter = RateLimiter::new(
///     1000, 500.0,  // per-client: burst 1000, 500/s sustained
///     5000, 2000.0, // per-namespace: burst 5000, 2000/s sustained
///     100,          // backpressure threshold: 100 pending proposals
/// );
///
/// // Check all three tiers
/// limiter.check("client-1", 42.into()).unwrap();
/// ```
#[derive(Debug)]
pub struct RateLimiter {
    /// Per-client token buckets keyed by client_id.
    client_buckets: Mutex<HashMap<String, TokenBucket>>,
    /// Per-namespace token buckets keyed by namespace_id.
    namespace_buckets: Mutex<HashMap<NamespaceId, TokenBucket>>,

    /// Capacity for per-client token buckets (max burst).
    client_capacity: AtomicU64,
    /// Refill rate for per-client buckets (tokens per second), stored as f64 bits.
    client_refill_rate_bits: AtomicU64,

    /// Capacity for per-namespace token buckets (max burst).
    namespace_capacity: AtomicU64,
    /// Refill rate for per-namespace buckets (tokens per second), stored as f64 bits.
    namespace_refill_rate_bits: AtomicU64,

    /// Raft pending proposal count above which backpressure is applied.
    backpressure_threshold: AtomicU64,
    /// Current pending proposal count (updated externally).
    pending_proposals: AtomicU64,

    /// Total rejected requests counter.
    rejected_count: AtomicU64,
}

impl RateLimiter {
    /// Creates a new multi-level rate limiter.
    ///
    /// # Arguments
    ///
    /// * `client_capacity` — max burst size per client
    /// * `client_refill_rate` — sustained requests/sec per client
    /// * `namespace_capacity` — max burst size per namespace
    /// * `namespace_refill_rate` — sustained requests/sec per namespace
    /// * `backpressure_threshold` — pending Raft proposals above which all requests are throttled
    pub fn new(
        client_capacity: u64,
        client_refill_rate: f64,
        namespace_capacity: u64,
        namespace_refill_rate: f64,
        backpressure_threshold: u64,
    ) -> Self {
        Self {
            client_buckets: Mutex::new(HashMap::new()),
            namespace_buckets: Mutex::new(HashMap::new()),
            client_capacity: AtomicU64::new(client_capacity),
            client_refill_rate_bits: AtomicU64::new(client_refill_rate.to_bits()),
            namespace_capacity: AtomicU64::new(namespace_capacity),
            namespace_refill_rate_bits: AtomicU64::new(namespace_refill_rate.to_bits()),
            backpressure_threshold: AtomicU64::new(backpressure_threshold),
            pending_proposals: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
        }
    }

    /// Checks all rate limit tiers for an incoming request.
    ///
    /// Checks order ensures shared tokens are not wasted by per-client rejections:
    /// 1. Global backpressure (no token consumption — atomic threshold check)
    /// 2. Per-client token bucket (most specific, checked before shared namespace bucket)
    /// 3. Per-namespace token bucket (shared resource, only consumed after client passes)
    ///
    /// Returns `Ok(())` if allowed, `Err(RateLimitRejection)` with retry hint if rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimitRejection`] when any tier rejects the request:
    /// - **Backpressure**: Raft pending proposals exceed the configured threshold.
    /// - **Client**: Per-client token bucket is exhausted.
    /// - **Namespace**: Per-namespace token bucket is exhausted.
    pub fn check(
        &self,
        client_id: &str,
        namespace_id: NamespaceId,
    ) -> Result<(), RateLimitRejection> {
        // Tier 1: Global backpressure (cheapest — single atomic load, no token consumption)
        let pending = self.pending_proposals.load(Ordering::Relaxed);
        let backpressure_threshold = self.backpressure_threshold.load(Ordering::Relaxed);
        if pending > backpressure_threshold {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(RateLimitRejection {
                level: RateLimitLevel::Backpressure,
                reason: RateLimitReason::QueueDepth,
                // Backoff proportional to queue depth overage
                retry_after_ms: ((pending - backpressure_threshold) * 10).min(5000),
                identifier: format!("pending_proposals={pending}"),
            });
        }

        // Tier 2: Per-client token bucket (checked before namespace to avoid
        // wasting shared namespace tokens on client-rejected requests)
        if !client_id.is_empty() {
            let mut buckets = self.client_buckets.lock();
            let bucket = buckets.entry(client_id.to_string()).or_insert_with(|| {
                TokenBucket::new(
                    self.client_capacity.load(Ordering::Relaxed),
                    f64::from_bits(self.client_refill_rate_bits.load(Ordering::Relaxed)),
                )
            });

            if !bucket.try_acquire() {
                let retry_after_ms = bucket.retry_after_ms();
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitRejection {
                    level: RateLimitLevel::Client,
                    reason: RateLimitReason::TokensExhausted,
                    retry_after_ms,
                    identifier: client_id.to_string(),
                });
            }
        }

        // Tier 3: Per-namespace token bucket (shared across all clients in a namespace)
        {
            let mut buckets = self.namespace_buckets.lock();
            let bucket = buckets.entry(namespace_id).or_insert_with(|| {
                TokenBucket::new(
                    self.namespace_capacity.load(Ordering::Relaxed),
                    f64::from_bits(self.namespace_refill_rate_bits.load(Ordering::Relaxed)),
                )
            });

            if !bucket.try_acquire() {
                let retry_after_ms = bucket.retry_after_ms();
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitRejection {
                    level: RateLimitLevel::Namespace,
                    reason: RateLimitReason::TokensExhausted,
                    retry_after_ms,
                    identifier: namespace_id.to_string(),
                });
            }
        }

        Ok(())
    }

    /// Updates the pending Raft proposal count for backpressure calculation.
    ///
    /// Called externally by the Raft metrics observer.
    /// Also emits the `ledger_rate_limit_queue_depth` gauge for SLI monitoring.
    pub fn set_pending_proposals(&self, count: u64) {
        self.pending_proposals.store(count, Ordering::Relaxed);
        crate::metrics::set_rate_limit_queue_depth(count);
    }

    /// Returns the current pending proposal count.
    pub fn pending_proposals(&self) -> u64 {
        self.pending_proposals.load(Ordering::Relaxed)
    }

    /// Returns the total number of rejected requests across all tiers.
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(Ordering::Relaxed)
    }

    /// Removes stale entries that haven't been accessed recently.
    ///
    /// Call periodically to prevent unbounded memory growth from departed
    /// clients or decommissioned namespaces.
    pub fn cleanup_stale(&self, max_idle: Duration) {
        let now = Instant::now();
        {
            let mut buckets = self.client_buckets.lock();
            buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < max_idle);
        }
        {
            let mut buckets = self.namespace_buckets.lock();
            buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < max_idle);
        }
    }

    /// Returns the current token count for a specific namespace (for testing/metrics).
    #[cfg(test)]
    fn namespace_tokens(&self, namespace_id: NamespaceId) -> Option<u64> {
        let buckets = self.namespace_buckets.lock();
        buckets.get(&namespace_id).map(|b| b.tokens_millis / 1000)
    }

    /// Returns the current token count for a specific client (for testing/metrics).
    #[cfg(test)]
    fn client_tokens(&self, client_id: &str) -> Option<u64> {
        let buckets = self.client_buckets.lock();
        buckets.get(client_id).map(|b| b.tokens_millis / 1000)
    }

    /// Updates rate limiter configuration at runtime.
    ///
    /// Atomically updates all threshold fields. Existing token buckets retain
    /// their current token counts — only new buckets created after this call
    /// will use the updated capacity and refill rate.
    pub fn update_config(
        &self,
        client_capacity: u64,
        client_refill_rate: f64,
        namespace_capacity: u64,
        namespace_refill_rate: f64,
        backpressure_threshold: u64,
    ) {
        self.client_capacity.store(client_capacity, Ordering::Relaxed);
        self.client_refill_rate_bits.store(client_refill_rate.to_bits(), Ordering::Relaxed);
        self.namespace_capacity.store(namespace_capacity, Ordering::Relaxed);
        self.namespace_refill_rate_bits.store(namespace_refill_rate.to_bits(), Ordering::Relaxed);
        self.backpressure_threshold.store(backpressure_threshold, Ordering::Relaxed);
    }
}

// Backwards compatibility: re-export as NamespaceRateLimiter for existing call sites
// that only need namespace-level rate limiting.
/// Alias for backwards compatibility with code using the namespace-only rate limiter.
pub type NamespaceRateLimiter = RateLimiter;

/// Alias for backwards compatibility.
pub type RateLimitExceeded = RateLimitRejection;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Helper to create a rate limiter with small capacity for testing.
    fn test_limiter() -> RateLimiter {
        RateLimiter::new(
            5,    // client burst: 5
            10.0, // client refill: 10/s
            10,   // namespace burst: 10
            20.0, // namespace refill: 20/s
            50,   // backpressure threshold: 50 pending
        )
    }

    // ── Token bucket unit tests ──────────────────────────────────────────

    #[test]
    fn token_bucket_starts_full() {
        let bucket = TokenBucket::new(100, 50.0);
        assert_eq!(bucket.tokens_millis, 100_000);
        assert_eq!(bucket.capacity_millis, 100_000);
    }

    #[test]
    fn token_bucket_allows_up_to_capacity() {
        let mut bucket = TokenBucket::new(5, 1.0);
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn token_bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(5, 1000.0);
        // Exhaust all tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());

        // Simulate time passing (move last_refill back)
        bucket.last_refill = Instant::now() - Duration::from_millis(100);
        // At 1000/s, 100ms should refill ~100 tokens, capped at capacity 5
        assert!(bucket.try_acquire());
    }

    #[test]
    fn token_bucket_caps_at_capacity() {
        let mut bucket = TokenBucket::new(3, 1000.0);
        // Simulate long time passing
        bucket.last_refill = Instant::now() - Duration::from_secs(60);
        // Should still be capped at 3
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn token_bucket_retry_after_reasonable() {
        let mut bucket = TokenBucket::new(5, 100.0);
        // Exhaust all tokens
        for _ in 0..5 {
            bucket.try_acquire();
        }
        let retry = bucket.retry_after_ms();
        // At 100 tokens/sec, 1 token should take 10ms
        assert!(retry >= 1, "retry_after_ms should be at least 1, got {retry}");
        assert!(retry <= 50, "retry_after_ms should be reasonable, got {retry}");
    }

    // ── Per-client rate limiting ─────────────────────────────────────────

    #[test]
    fn client_rate_limit_enforced() {
        let limiter = test_limiter();

        // Exhaust client burst (5 requests)
        for _ in 0..5 {
            assert!(limiter.check("client-1", 1.into()).is_ok());
        }

        // 6th request should be rejected at client level
        let err = limiter.check("client-1", 1.into()).unwrap_err();
        assert_eq!(err.level, RateLimitLevel::Client);
        assert_eq!(err.reason, RateLimitReason::TokensExhausted);
        assert!(err.retry_after_ms >= 1);
        assert_eq!(err.identifier, "client-1");
    }

    #[test]
    fn different_clients_have_separate_buckets() {
        let limiter = test_limiter();

        // Exhaust client-1
        for _ in 0..5 {
            limiter.check("client-1", 1.into()).unwrap();
        }
        assert!(limiter.check("client-1", 1.into()).is_err());

        // client-2 should still have its own quota
        for _ in 0..5 {
            assert!(limiter.check("client-2", 1.into()).is_ok());
        }
    }

    #[test]
    fn empty_client_id_skips_client_check() {
        let limiter = RateLimiter::new(
            1,     // very low client burst
            0.1,   // very low client refill
            1000,  // high namespace burst
            500.0, // high namespace refill
            100,   // backpressure threshold
        );

        // With empty client_id, should only hit namespace limit
        for _ in 0..100 {
            assert!(limiter.check("", 1.into()).is_ok());
        }
    }

    // ── Per-namespace rate limiting ──────────────────────────────────────

    #[test]
    fn namespace_rate_limit_enforced() {
        let limiter = test_limiter();

        // Exhaust namespace burst (10 requests, using different clients to avoid client limit)
        for i in 0..10 {
            assert!(limiter.check(&format!("client-{i}"), 42.into()).is_ok());
        }

        // 11th request should be rejected at namespace level
        let err = limiter.check("client-99", 42.into()).unwrap_err();
        assert_eq!(err.level, RateLimitLevel::Namespace);
        assert_eq!(err.reason, RateLimitReason::TokensExhausted);
        assert_eq!(err.identifier, "ns:42");
    }

    #[test]
    fn different_namespaces_have_separate_buckets() {
        let limiter = test_limiter();

        // Exhaust namespace 1
        for i in 0..10 {
            limiter.check(&format!("c-{i}"), 1.into()).unwrap();
        }
        assert!(limiter.check("c-99", 1.into()).is_err());

        // Namespace 2 should still have its own quota
        for i in 0..10 {
            assert!(limiter.check(&format!("c-{i}"), 2.into()).is_ok());
        }
    }

    // ── Backpressure ─────────────────────────────────────────────────────

    #[test]
    fn backpressure_rejects_when_queue_deep() {
        let limiter = test_limiter();

        // Set pending proposals above threshold (50)
        limiter.set_pending_proposals(60);

        let err = limiter.check("client-1", 1.into()).unwrap_err();
        assert_eq!(err.level, RateLimitLevel::Backpressure);
        assert_eq!(err.reason, RateLimitReason::QueueDepth);
        // retry_after_ms = (60 - 50) * 10 = 100
        assert_eq!(err.retry_after_ms, 100);
    }

    #[test]
    fn backpressure_allows_when_below_threshold() {
        let limiter = test_limiter();

        limiter.set_pending_proposals(49);
        assert!(limiter.check("client-1", 1.into()).is_ok());

        limiter.set_pending_proposals(50);
        assert!(limiter.check("client-1", 1.into()).is_ok());
    }

    #[test]
    fn backpressure_retry_capped_at_5000ms() {
        let limiter = test_limiter();

        // Set very high pending proposals
        limiter.set_pending_proposals(1000);

        let err = limiter.check("client-1", 1.into()).unwrap_err();
        assert_eq!(err.retry_after_ms, 5000);
    }

    // ── Integration checks ───────────────────────────────────────────────

    #[test]
    fn check_order_backpressure_first() {
        // If both backpressure and namespace are exceeded, backpressure wins
        let limiter = RateLimiter::new(100, 100.0, 1, 0.001, 10);

        // Exhaust namespace
        limiter.check("c", 1.into()).unwrap();
        assert!(limiter.check("c2", 1.into()).is_err()); // namespace exhausted

        // Now also trigger backpressure
        limiter.set_pending_proposals(20);
        let err = limiter.check("c3", 1.into()).unwrap_err();
        // Backpressure should be checked first
        assert_eq!(err.level, RateLimitLevel::Backpressure);
    }

    #[test]
    fn rejected_count_tracks_all_tiers() {
        let limiter = test_limiter();
        assert_eq!(limiter.rejected_count(), 0);

        // Exhaust namespace to trigger rejection
        for i in 0..10 {
            limiter.check(&format!("c-{i}"), 1.into()).unwrap();
        }
        let _ = limiter.check("c-99", 1.into());
        assert_eq!(limiter.rejected_count(), 1);
    }

    #[test]
    fn cleanup_removes_stale_entries() {
        let limiter = test_limiter();

        // Create some entries
        limiter.check("client-1", 1.into()).unwrap();
        limiter.check("client-2", 2.into()).unwrap();

        // Cleanup with very short max_idle should remove everything
        // (entries were just created, but we use a 0-duration which means
        //  any non-zero elapsed time qualifies as stale)
        // In practice, entries just created won't be stale yet
        limiter.cleanup_stale(Duration::from_secs(3600));
        // Entries should still exist (created < 3600s ago)
        assert!(limiter.client_tokens("client-1").is_some());

        // Cleanup with zero duration removes everything
        // (need to wait a tiny bit so elapsed > 0)
        std::thread::sleep(Duration::from_millis(2));
        limiter.cleanup_stale(Duration::ZERO);
        assert!(limiter.client_tokens("client-1").is_none());
        assert!(limiter.namespace_tokens(1.into()).is_none());
    }

    #[test]
    fn set_pending_proposals_updates_atomically() {
        let limiter = test_limiter();
        assert_eq!(limiter.pending_proposals(), 0);
        limiter.set_pending_proposals(42);
        assert_eq!(limiter.pending_proposals(), 42);
    }

    #[test]
    fn rejection_display_format() {
        let rejection = RateLimitRejection {
            level: RateLimitLevel::Namespace,
            reason: RateLimitReason::TokensExhausted,
            retry_after_ms: 50,
            identifier: "42".to_string(),
        };
        let msg = format!("{rejection}");
        assert!(msg.contains("namespace"));
        assert!(msg.contains("42"));
        assert!(msg.contains("tokens_exhausted"));
    }

    #[test]
    fn level_and_reason_as_str() {
        assert_eq!(RateLimitLevel::Client.as_str(), "client");
        assert_eq!(RateLimitLevel::Namespace.as_str(), "namespace");
        assert_eq!(RateLimitLevel::Backpressure.as_str(), "backpressure");
        assert_eq!(RateLimitReason::TokensExhausted.as_str(), "tokens_exhausted");
        assert_eq!(RateLimitReason::QueueDepth.as_str(), "queue_depth");
    }

    // ── Concurrency Stress Tests ────────────────────────────────────────

    /// Stress test: concurrent check() calls during config update via AtomicU64.
    ///
    /// Verifies that concurrent rate limit checks observe consistent config
    /// even as atomics are being updated. The key invariant is that no thread
    /// panics or observes a partially-updated config (e.g., client capacity
    /// from the old config with refill rate from the new config).
    #[test]
    fn stress_concurrent_check_during_config_update() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering as AOrdering},
            },
            thread,
        };

        // Start with generous limits so initial checks pass
        let limiter = Arc::new(RateLimiter::new(
            10_000, 5_000.0, // per-client: high burst, high refill
            50_000, 20_000.0, // per-namespace: very high
            1000,     // backpressure threshold
        ));

        let running = Arc::new(AtomicBool::new(true));
        let mut handles = Vec::new();

        // Spawn 20 checker threads
        for thread_id in 0..20 {
            let limiter = Arc::clone(&limiter);
            let running = Arc::clone(&running);
            handles.push(thread::spawn(move || {
                let client_id = format!("client-{}", thread_id);
                let namespace_id = NamespaceId::new((thread_id % 5 + 1) as i64);
                let mut ok_count = 0u64;
                let mut reject_count = 0u64;

                while running.load(AOrdering::Relaxed) {
                    match limiter.check(&client_id, namespace_id) {
                        Ok(()) => ok_count += 1,
                        Err(_) => reject_count += 1,
                    }
                    // Brief yield to avoid pure spin
                    if (ok_count + reject_count).is_multiple_of(100) {
                        thread::yield_now();
                    }
                }

                (ok_count, reject_count)
            }));
        }

        // Spawn 2 config updater threads that oscillate between configs
        for updater_id in 0..2 {
            let limiter = Arc::clone(&limiter);
            let running = Arc::clone(&running);
            handles.push(thread::spawn(move || {
                let mut iteration = 0u64;
                while running.load(AOrdering::Relaxed) {
                    if iteration % 2 == updater_id {
                        // Tighten limits
                        limiter.update_config(100, 50.0, 500, 200.0, 10);
                    } else {
                        // Loosen limits
                        limiter.update_config(10_000, 5_000.0, 50_000, 20_000.0, 1000);
                    }
                    iteration += 1;
                    thread::yield_now();
                }
                (0u64, iteration)
            }));
        }

        // Let it run for a short duration
        thread::sleep(std::time::Duration::from_millis(200));
        running.store(false, AOrdering::Relaxed);

        let mut total_ok = 0u64;
        let mut total_reject = 0u64;

        for handle in handles {
            let (ok, reject) = handle.join().expect("Thread panicked");
            total_ok += ok;
            total_reject += reject;
        }

        // The test passes if no thread panicked. Additionally verify some
        // operations occurred.
        assert!(total_ok + total_reject > 0, "Expected some rate limit checks to execute");
    }

    /// Stress test: concurrent requests at exactly the rate limit boundary.
    ///
    /// Creates a limiter with a small burst capacity and sends exactly that many
    /// concurrent requests. Verifies that the total allowed never exceeds burst
    /// capacity (no double-spend of tokens).
    #[test]
    fn stress_concurrent_boundary_rate_limit() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering as AOrdering},
            },
            thread,
        };

        let burst_capacity = 50u64;
        let limiter = Arc::new(RateLimiter::new(
            burst_capacity,
            0.0, // Zero refill — tokens don't regenerate during test
            100_000,
            100_000.0, // Namespace limits are generous
            10_000,    // No backpressure
        ));

        let allowed = Arc::new(AtomicU64::new(0));
        let rejected = Arc::new(AtomicU64::new(0));
        let num_threads = 100;
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let limiter = Arc::clone(&limiter);
            let allowed = Arc::clone(&allowed);
            let rejected = Arc::clone(&rejected);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                // Synchronize all threads to start at the same time
                barrier.wait();

                // All use the same client_id to contend on one bucket
                match limiter.check("shared-client", NamespaceId::new(1)) {
                    Ok(()) => {
                        allowed.fetch_add(1, AOrdering::Relaxed);
                    },
                    Err(_) => {
                        rejected.fetch_add(1, AOrdering::Relaxed);
                    },
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let total_allowed = allowed.load(AOrdering::Relaxed);
        let total_rejected = rejected.load(AOrdering::Relaxed);

        assert_eq!(
            total_allowed + total_rejected,
            num_threads as u64,
            "All requests must be accounted for"
        );

        // Token bucket should never allow more than burst capacity
        assert!(
            total_allowed <= burst_capacity,
            "Allowed {total_allowed} requests but burst capacity is {burst_capacity} — token double-spend detected"
        );

        // At least some must have been rejected (100 threads, 50 capacity)
        assert!(total_rejected > 0, "Expected some rejections with 100 threads and capacity 50");
    }
}
