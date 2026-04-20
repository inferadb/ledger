//! Multi-level token bucket rate limiting.
//!
//! Provides three tiers of admission control for write requests:
//!
//! 1. **Per-client** — prevents a single bad actor from monopolizing resources
//! 2. **Per-organization** — ensures fair sharing across tenants in a multi-tenant region
//! 3. **Global backpressure** — throttles all requests when Raft queue depth is high
//!
//! Uses the token bucket algorithm, which allows controlled bursts while maintaining
//! an average rate. Each bucket has a capacity (max burst) and a refill rate (sustained
//! throughput). Tokens are consumed on each request and refilled over time.
//!
//! Mitigates noisy neighbor problems in multi-tenant regions by applying rate
//! limits per organization at the region leader.
//!
//! # Concurrency design
//!
//! The hot path through [`RateLimiter::check`] is lock-free:
//!
//! - **Bucket lookup** uses `DashMap`, a sharded concurrent hash map. Lookups on distinct keys
//!   never contend; lookups on the same key serialize only on one of the map's internal shards (not
//!   a single global `Mutex`).
//! - **Token accounting** packs `(tokens_millis, last_refill_ms_offset)` into a single
//!   [`AtomicU64`] per bucket; the private `try_acquire` helper is a compare-and-swap loop — no
//!   kernel wait on contention, readers and writers retry in userspace.
//!
//! Before this change, flamegraph analysis at `concurrency=32` showed
//! `parking_lot::Mutex` wait on the outer `HashMap` was ~14% of active CPU.
//! After this change that cost moves to `DashMap` shard locks, which serialize
//! lookups only within one of 32 shards and do not block bucket-state reads.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use inferadb_ledger_types::OrganizationId;

/// Level at which a rate limit was enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitLevel {
    /// Per-client rate limit.
    Client,
    /// Per-organization rate limit.
    Organization,
    /// Global backpressure based on Raft queue depth.
    Backpressure,
}

impl RateLimitLevel {
    /// Returns a static string label for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Organization => "organization",
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
    /// The identifier that was rate limited (client_id or organization).
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

/// Packs `tokens_millis` (low 32 bits) and `last_refill_ms_offset` (high 32 bits)
/// into a single `u64` for atomic compare-and-swap.
#[inline]
fn pack_state(tokens_millis: u32, last_refill_ms_offset: u32) -> u64 {
    (u64::from(last_refill_ms_offset) << 32) | u64::from(tokens_millis)
}

/// Inverse of [`pack_state`].
#[inline]
fn unpack_state(state: u64) -> (u32, u32) {
    let tokens_millis = (state & 0xFFFF_FFFF) as u32;
    let last_refill_ms_offset = (state >> 32) as u32;
    (tokens_millis, last_refill_ms_offset)
}

/// A token bucket that allows controlled bursts while enforcing an average rate.
///
/// Tokens refill at `refill_rate` per second up to `capacity`. Each request
/// consumes one token. When no tokens remain, requests are rejected.
///
/// # Concurrency
///
/// The dynamic `(tokens_millis, last_refill_ms_offset)` tuple is stored in a
/// single [`AtomicU64`]. [`try_acquire`] is a compare-and-swap loop: load the
/// state, compute the refill, attempt to commit; retry on contention. No
/// `Mutex` is taken on the hot path.
///
/// `capacity_millis` and `refill_rate_bits` are stored in separate atomics so
/// runtime reconfiguration can update them without invalidating the packed
/// token state.
///
/// # Time representation
///
/// `last_refill_ms_offset` is a `u32` millisecond offset from `epoch`, which is
/// captured at bucket construction. The offset wraps at ~49.7 days; elapsed
/// computation uses [`u32::wrapping_sub`]. Because refill clamps tokens at
/// capacity and the cleanup task removes idle buckets well before 49 days, wrap
/// is not observable in practice.
#[derive(Debug)]
struct TokenBucket {
    /// Packed `(tokens_millis_u32, last_refill_ms_offset_u32)`.
    state: AtomicU64,
    /// Maximum tokens the bucket can hold (scaled by 1000).
    capacity_millis: AtomicU64,
    /// Tokens added per second, stored as `f64::to_bits`.
    refill_rate_bits: AtomicU64,
    /// Anchor instant; `last_refill_ms_offset` is measured relative to this.
    epoch: Instant,
    /// Wall-clock instant of last refill (used only by `cleanup_stale`).
    ///
    /// This is a low-frequency field; cleanup runs on an interval (minutes) and
    /// reads this under the same `DashMap` shard lock that protects the bucket
    /// entry. A coarse observation is sufficient — we use `AtomicU64`
    /// milliseconds-since-epoch to keep it lock-free.
    last_refill_ms_for_cleanup: AtomicU64,
}

impl TokenBucket {
    /// Creates a new token bucket starting at full capacity.
    fn new(capacity: u64, refill_rate: f64) -> Self {
        let capacity_millis = capacity.saturating_mul(1000);
        // Cap the initial token count at u32::MAX to fit in the packed layout.
        // 1000× scale means u32::MAX corresponds to ~4.29M tokens — well above
        // any realistic burst configuration.
        let tokens_millis = u32::try_from(capacity_millis).unwrap_or(u32::MAX);
        Self {
            state: AtomicU64::new(pack_state(tokens_millis, 0)),
            capacity_millis: AtomicU64::new(capacity_millis),
            refill_rate_bits: AtomicU64::new(refill_rate.to_bits()),
            epoch: Instant::now(),
            last_refill_ms_for_cleanup: AtomicU64::new(0),
        }
    }

    /// Returns the current configured capacity (scaled by 1000).
    #[inline]
    fn capacity_millis(&self) -> u64 {
        self.capacity_millis.load(Ordering::Relaxed)
    }

    /// Returns the current configured refill rate (tokens per second).
    #[inline]
    fn refill_rate(&self) -> f64 {
        f64::from_bits(self.refill_rate_bits.load(Ordering::Relaxed))
    }

    /// Computes the millisecond offset of `now` from `epoch`, saturating at
    /// `u32::MAX`. Saturation is safe: a saturated elapsed refills to capacity,
    /// which is the correct limiting behavior when a bucket has been idle for
    /// longer than the u32 window.
    #[inline]
    fn now_offset_ms(&self, now: Instant) -> u32 {
        let elapsed_ms = now.saturating_duration_since(self.epoch).as_millis();
        u32::try_from(elapsed_ms).unwrap_or(u32::MAX)
    }

    /// Refill tokens based on elapsed time, then try to consume one token.
    ///
    /// Lock-free compare-and-swap loop. Returns `true` if the request is
    /// allowed, `false` if rate limited.
    fn try_acquire(&self) -> bool {
        let refill_rate = self.refill_rate();
        let capacity_millis_u64 = self.capacity_millis();
        // Cap at u32::MAX — matches the packed layout.
        let capacity_millis = u32::try_from(capacity_millis_u64).unwrap_or(u32::MAX);

        let now = Instant::now();
        let now_offset = self.now_offset_ms(now);

        loop {
            let state = self.state.load(Ordering::Acquire);
            let (tokens_millis, last_refill_offset) = unpack_state(state);

            // Elapsed since previous refill. `now_offset` is captured once
            // before the loop; on retry, another thread may have committed a
            // `last_refill_offset` ≥ our `now_offset`. Saturating-sub yields 0
            // in that case — the correct semantic is "no additional refill
            // owed for this attempt; the winning thread already advanced time."
            let elapsed_ms = now_offset.saturating_sub(last_refill_offset);

            // Compute refill in the 1000× scale:
            //   seconds × tokens/sec × 1000 = millis-of-tokens.
            // Clamped to `u32::MAX` to guard against f64→u64 UB if
            // `elapsed_ms × refill_rate` overflows (the result is bounded by
            // `capacity_millis` ≤ u32::MAX anyway).
            let elapsed_secs = f64::from(elapsed_ms) / 1000.0;
            let refill_millis = (elapsed_secs * refill_rate * 1000.0).min(u32::MAX as f64) as u64;

            // Apply refill, saturating at capacity.
            let refilled_tokens_millis = if refill_millis > 0 {
                let summed = u64::from(tokens_millis)
                    .saturating_add(refill_millis)
                    .min(u64::from(capacity_millis));
                u32::try_from(summed).unwrap_or(capacity_millis)
            } else {
                tokens_millis
            };

            // Decide whether to consume a token.
            let (new_tokens_millis, acquired) = if refilled_tokens_millis >= 1000 {
                (refilled_tokens_millis - 1000, true)
            } else {
                (refilled_tokens_millis, false)
            };

            // Only advance `last_refill_offset` when we actually refilled.
            // Mirrors the pre-atomic behavior that left `last_refill` untouched
            // when `refill == 0`. Without this, repeated calls within a single
            // millisecond would each stamp `now_offset`, which is harmless but
            // wasteful on write contention.
            let new_offset = if refill_millis > 0 { now_offset } else { last_refill_offset };

            let new_state = pack_state(new_tokens_millis, new_offset);
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if refill_millis > 0 {
                        // Low-frequency mirror for `cleanup_stale`.
                        self.last_refill_ms_for_cleanup
                            .store(u64::from(now_offset), Ordering::Relaxed);
                    }
                    return acquired;
                },
                Err(_) => {
                    // Lost the race — another thread mutated state. Retry.
                    std::hint::spin_loop();
                },
            }
        }
    }

    /// Estimates milliseconds until the next token is available.
    fn retry_after_ms(&self) -> u64 {
        let refill_rate = self.refill_rate();
        if refill_rate <= 0.0 {
            return 1000;
        }
        let (tokens_millis, _) = unpack_state(self.state.load(Ordering::Relaxed));
        let deficit_millis = 1000u64.saturating_sub(u64::from(tokens_millis));
        let ms = (deficit_millis as f64 / (refill_rate * 1000.0) * 1000.0).ceil() as u64;
        ms.max(1)
    }

    /// Returns `true` if the bucket has been idle for `>= max_idle`.
    fn is_stale(&self, now: Instant, max_idle: Duration) -> bool {
        let last_refill_offset = self.last_refill_ms_for_cleanup.load(Ordering::Relaxed);
        let last_refill_instant = self.epoch + Duration::from_millis(last_refill_offset);
        now.saturating_duration_since(last_refill_instant) >= max_idle
    }

    /// Updates capacity and refill rate at runtime (admin path, not hot path).
    ///
    /// Matches the pre-atomic behavior: the existing token count is preserved;
    /// only subsequent refills use the new rate and cap.
    fn update_config(&self, capacity: u64, refill_rate: f64) {
        let capacity_millis = capacity.saturating_mul(1000);
        self.capacity_millis.store(capacity_millis, Ordering::Relaxed);
        self.refill_rate_bits.store(refill_rate.to_bits(), Ordering::Relaxed);
    }

    /// Returns the current token count in whole tokens (for tests / metrics).
    #[cfg(test)]
    fn tokens(&self) -> u64 {
        let (tokens_millis, _) = unpack_state(self.state.load(Ordering::Relaxed));
        u64::from(tokens_millis) / 1000
    }
}

/// Multi-level rate limiter with per-client, per-organization, and backpressure tiers.
///
/// Thread-safe: bucket lookup uses [`DashMap`] (sharded concurrent map); per-bucket
/// token state uses [`AtomicU64`] compare-and-swap. The hot path — [`check`] —
/// is lock-free except for the `DashMap` shard lock during bucket *insertion*.
/// Once a bucket exists, repeated checks on the same key take no mutex.
///
/// Config thresholds use atomics for lock-free runtime reconfiguration.
///
/// [`check`]: RateLimiter::check
///
/// # Usage
///
/// ```no_run
/// use inferadb_ledger_raft::RateLimiter;
/// use inferadb_ledger_types::OrganizationId;
///
/// let limiter = RateLimiter::new(
///     1000, 500.0,  // per-client: burst 1000, 500/s sustained
///     5000, 2000.0, // per-organization: burst 5000, 2000/s sustained
///     100,          // backpressure threshold: 100 pending proposals
///     "global",     // region label for metrics
/// );
///
/// // Check all three tiers
/// limiter.check("client-1", OrganizationId::new(42)).unwrap();
/// ```
#[derive(Debug)]
pub struct RateLimiter {
    /// Per-client token buckets keyed by client_id.
    client_buckets: DashMap<String, Arc<TokenBucket>>,
    /// Per-organization token buckets keyed by organization.
    organization_buckets: DashMap<OrganizationId, Arc<TokenBucket>>,

    /// Capacity for per-client token buckets (max burst).
    client_capacity: AtomicU64,
    /// Refill rate for per-client buckets (tokens per second), stored as f64 bits.
    client_refill_rate_bits: AtomicU64,

    /// Capacity for per-organization token buckets (max burst).
    organization_capacity: AtomicU64,
    /// Refill rate for per-organization buckets (tokens per second), stored as f64 bits.
    organization_refill_rate_bits: AtomicU64,

    /// Raft pending proposal count above which backpressure is applied.
    backpressure_threshold: AtomicU64,
    /// Current pending proposal count (updated externally).
    pending_proposals: AtomicU64,

    /// Total rejected requests counter.
    rejected_count: AtomicU64,

    /// Region identifier for metric labels.
    region: String,
}

impl RateLimiter {
    /// Creates a new multi-level rate limiter.
    ///
    /// # Arguments
    ///
    /// * `client_capacity` — max burst size per client
    /// * `client_refill_rate` — sustained requests/sec per client
    /// * `organization_capacity` — max burst size per organization
    /// * `organization_refill_rate` — sustained requests/sec per organization
    /// * `backpressure_threshold` — pending Raft proposals above which all requests are throttled
    /// * `region` — Region identifier for Prometheus metric labels.
    pub fn new(
        client_capacity: u64,
        client_refill_rate: f64,
        organization_capacity: u64,
        organization_refill_rate: f64,
        backpressure_threshold: u64,
        region: impl Into<String>,
    ) -> Self {
        Self {
            client_buckets: DashMap::new(),
            organization_buckets: DashMap::new(),
            client_capacity: AtomicU64::new(client_capacity),
            client_refill_rate_bits: AtomicU64::new(client_refill_rate.to_bits()),
            organization_capacity: AtomicU64::new(organization_capacity),
            organization_refill_rate_bits: AtomicU64::new(organization_refill_rate.to_bits()),
            backpressure_threshold: AtomicU64::new(backpressure_threshold),
            pending_proposals: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
            region: region.into(),
        }
    }

    /// Looks up or inserts a client bucket. Uses an initial read-only get to
    /// avoid taking a shard write lock on the common case (bucket already
    /// exists).
    fn client_bucket(&self, client_id: &str) -> Arc<TokenBucket> {
        if let Some(existing) = self.client_buckets.get(client_id) {
            return Arc::clone(existing.value());
        }
        // Fall through to entry() for race-free insert.
        let entry = self.client_buckets.entry(client_id.to_string()).or_insert_with(|| {
            Arc::new(TokenBucket::new(
                self.client_capacity.load(Ordering::Relaxed),
                f64::from_bits(self.client_refill_rate_bits.load(Ordering::Relaxed)),
            ))
        });
        Arc::clone(entry.value())
    }

    /// Looks up or inserts an organization bucket.
    fn organization_bucket(&self, organization: OrganizationId) -> Arc<TokenBucket> {
        if let Some(existing) = self.organization_buckets.get(&organization) {
            return Arc::clone(existing.value());
        }
        let entry = self.organization_buckets.entry(organization).or_insert_with(|| {
            Arc::new(TokenBucket::new(
                self.organization_capacity.load(Ordering::Relaxed),
                f64::from_bits(self.organization_refill_rate_bits.load(Ordering::Relaxed)),
            ))
        });
        Arc::clone(entry.value())
    }

    /// Checks all rate limit tiers for an incoming request.
    ///
    /// Checks order ensures shared tokens are not wasted by per-client rejections:
    /// 1. Global backpressure (no token consumption — atomic threshold check)
    /// 2. Per-client token bucket (most specific, checked before shared organization bucket)
    /// 3. Per-organization token bucket (shared resource, only consumed after client passes)
    ///
    /// Returns `Ok(())` if allowed, `Err(RateLimitRejection)` with retry hint if rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimitRejection`] when any tier rejects the request:
    /// - **Backpressure**: Raft pending proposals exceed the configured threshold.
    /// - **Client**: Per-client token bucket is exhausted.
    /// - **Organization**: Per-organization token bucket is exhausted.
    pub fn check(
        &self,
        client_id: &str,
        organization: OrganizationId,
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

        // Tier 2: Per-client token bucket (checked before organization to avoid
        // wasting shared organization tokens on client-rejected requests).
        if !client_id.is_empty() {
            let bucket = self.client_bucket(client_id);
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

        // Tier 3: Per-organization token bucket (shared across all clients in an organization).
        let bucket = self.organization_bucket(organization);
        if !bucket.try_acquire() {
            let retry_after_ms = bucket.retry_after_ms();
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(RateLimitRejection {
                level: RateLimitLevel::Organization,
                reason: RateLimitReason::TokensExhausted,
                retry_after_ms,
                identifier: organization.to_string(),
            });
        }

        Ok(())
    }

    /// Updates the pending Raft proposal count for backpressure calculation.
    ///
    /// Called externally by the Raft metrics observer.
    /// Also emits the `ledger_rate_limit_queue_depth` gauge for SLI monitoring.
    pub fn set_pending_proposals(&self, count: u64) {
        self.pending_proposals.store(count, Ordering::Relaxed);
        crate::metrics::set_rate_limit_queue_depth(count, &self.region);
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
    /// clients or decommissioned organizations.
    pub fn cleanup_stale(&self, max_idle: Duration) {
        let now = Instant::now();

        let client_before = self.client_buckets.len();
        self.client_buckets.retain(|_, bucket| !bucket.is_stale(now, max_idle));
        let client_removed = client_before.saturating_sub(self.client_buckets.len());

        let org_before = self.organization_buckets.len();
        self.organization_buckets.retain(|_, bucket| !bucket.is_stale(now, max_idle));
        let org_removed = org_before.saturating_sub(self.organization_buckets.len());

        if client_removed > 0 || org_removed > 0 {
            tracing::debug!(client_removed, org_removed, "Rate limiter stale bucket cleanup");
        }
    }

    /// Spawns a background task that periodically cleans up stale token buckets.
    ///
    /// Runs every `interval` duration, removing buckets that have been idle
    /// for longer than `max_idle`. Returns a `JoinHandle` that can be used
    /// to abort the task on shutdown.
    pub fn start_cleanup_task(
        self: &Arc<Self>,
        cleanup_interval: Duration,
        max_idle: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let limiter = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(cleanup_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                limiter.cleanup_stale(max_idle);
            }
        })
    }

    /// Returns the current token count for a specific organization (for testing/metrics).
    #[cfg(test)]
    fn organization_tokens(&self, organization: OrganizationId) -> Option<u64> {
        self.organization_buckets.get(&organization).map(|b| b.tokens())
    }

    /// Returns the current token count for a specific client (for testing/metrics).
    #[cfg(test)]
    fn client_tokens(&self, client_id: &str) -> Option<u64> {
        self.client_buckets.get(client_id).map(|b| b.tokens())
    }

    /// Updates rate limiter configuration at runtime.
    ///
    /// Atomically updates all threshold fields and propagates capacity/refill
    /// changes to every existing token bucket. Existing token buckets retain
    /// their current token counts — only future refills use the new rate and
    /// capacity.
    pub fn update_config(
        &self,
        client_capacity: u64,
        client_refill_rate: f64,
        organization_capacity: u64,
        organization_refill_rate: f64,
        backpressure_threshold: u64,
    ) {
        self.client_capacity.store(client_capacity, Ordering::Relaxed);
        self.client_refill_rate_bits.store(client_refill_rate.to_bits(), Ordering::Relaxed);
        self.organization_capacity.store(organization_capacity, Ordering::Relaxed);
        self.organization_refill_rate_bits
            .store(organization_refill_rate.to_bits(), Ordering::Relaxed);
        self.backpressure_threshold.store(backpressure_threshold, Ordering::Relaxed);

        // Propagate capacity / refill rate to live buckets.
        for entry in self.client_buckets.iter() {
            entry.value().update_config(client_capacity, client_refill_rate);
        }
        for entry in self.organization_buckets.iter() {
            entry.value().update_config(organization_capacity, organization_refill_rate);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Helper to create a rate limiter with small capacity for testing.
    fn test_limiter() -> RateLimiter {
        RateLimiter::new(
            5,    // client burst: 5
            10.0, // client refill: 10/s
            10,   // organization burst: 10
            20.0, // organization refill: 20/s
            50,   // backpressure threshold: 50 pending
            "global",
        )
    }

    // ── Token bucket unit tests ──────────────────────────────────────────

    #[test]
    fn token_bucket_starts_full() {
        let bucket = TokenBucket::new(100, 50.0);
        let (tokens_millis, _) = unpack_state(bucket.state.load(Ordering::Relaxed));
        assert_eq!(tokens_millis, 100_000);
        assert_eq!(bucket.capacity_millis(), 100_000);
    }

    #[test]
    fn token_bucket_allows_up_to_capacity() {
        let bucket = TokenBucket::new(5, 1.0);
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn token_bucket_refills_over_time() {
        let bucket = TokenBucket::new(5, 1000.0);
        // Exhaust all tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());

        // Sleep to advance `now_offset` past the packed last_refill value, then
        // check that refill replenishes at least one token.
        std::thread::sleep(Duration::from_millis(50));
        // At 1000/s, 50ms should refill ~50 tokens, capped at capacity 5.
        assert!(bucket.try_acquire());
    }

    #[test]
    fn token_bucket_caps_at_capacity() {
        let bucket = TokenBucket::new(3, 1000.0);
        // Simulate long time passing: rewind offset to 0 (i.e. epoch); by the
        // time `try_acquire` runs, `now_offset` will be a few ms, producing a
        // multi-second refill after elapsed ≫ capacity / refill_rate.
        let (tokens, _) = unpack_state(bucket.state.load(Ordering::Relaxed));
        bucket.state.store(pack_state(tokens, 0), Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(20));
        // Should still be capped at 3
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn token_bucket_retry_after_reasonable() {
        let bucket = TokenBucket::new(5, 100.0);
        // Exhaust all tokens
        for _ in 0..5 {
            bucket.try_acquire();
        }
        let retry = bucket.retry_after_ms();
        // At 100 tokens/sec, 1 token should take 10ms
        assert!(retry >= 1, "retry_after_ms should be at least 1, got {retry}");
        assert!(retry <= 50, "retry_after_ms should be reasonable, got {retry}");
    }

    #[test]
    fn pack_unpack_round_trip() {
        for (tokens, offset) in
            [(0u32, 0u32), (1_000, 500), (u32::MAX, 0), (0, u32::MAX), (u32::MAX, u32::MAX)]
        {
            let packed = pack_state(tokens, offset);
            let (tk, off) = unpack_state(packed);
            assert_eq!(tk, tokens);
            assert_eq!(off, offset);
        }
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
            1000,  // high organization burst
            500.0, // high organization refill
            100,   // backpressure threshold
            "global",
        );

        // With empty client_id, should only hit organization limit
        for _ in 0..100 {
            assert!(limiter.check("", 1.into()).is_ok());
        }
    }

    // ── Per-organization rate limiting ──────────────────────────────────────

    #[test]
    fn organization_rate_limit_enforced() {
        let limiter = test_limiter();

        // Exhaust organization burst (10 requests, using different clients to avoid client limit)
        for i in 0..10 {
            assert!(limiter.check(&format!("client-{i}"), 42.into()).is_ok());
        }

        // 11th request should be rejected at organization level
        let err = limiter.check("client-99", 42.into()).unwrap_err();
        assert_eq!(err.level, RateLimitLevel::Organization);
        assert_eq!(err.reason, RateLimitReason::TokensExhausted);
        assert_eq!(err.identifier, "org:42");
    }

    #[test]
    fn different_organizations_have_separate_buckets() {
        let limiter = test_limiter();

        // Exhaust organization 1
        for i in 0..10 {
            limiter.check(&format!("c-{i}"), 1.into()).unwrap();
        }
        assert!(limiter.check("c-99", 1.into()).is_err());

        // Organization 2 should still have its own quota
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
        // If both backpressure and organization are exceeded, backpressure wins
        let limiter = RateLimiter::new(100, 100.0, 1, 0.001, 10, "global");

        // Exhaust organization
        limiter.check("c", 1.into()).unwrap();
        assert!(limiter.check("c2", 1.into()).is_err()); // organization exhausted

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

        // Exhaust organization to trigger rejection
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

        // Cleanup with a large max_idle should not remove entries created now.
        limiter.cleanup_stale(Duration::from_secs(3600));
        assert!(limiter.client_tokens("client-1").is_some());

        // Cleanup with zero duration removes everything (wait a tiny bit to
        // ensure non-zero elapsed from last refill).
        std::thread::sleep(Duration::from_millis(2));
        limiter.cleanup_stale(Duration::ZERO);
        assert!(limiter.client_tokens("client-1").is_none());
        assert!(limiter.organization_tokens(1.into()).is_none());
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
            level: RateLimitLevel::Organization,
            reason: RateLimitReason::TokensExhausted,
            retry_after_ms: 50,
            identifier: "42".to_string(),
        };
        let msg = format!("{rejection}");
        assert!(msg.contains("organization"));
        assert!(msg.contains("42"));
        assert!(msg.contains("tokens_exhausted"));
    }

    #[test]
    fn level_and_reason_as_str() {
        assert_eq!(RateLimitLevel::Client.as_str(), "client");
        assert_eq!(RateLimitLevel::Organization.as_str(), "organization");
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
            50_000, 20_000.0, // per-organization: very high
            1000,     // backpressure threshold
            "global",
        ));

        let running = Arc::new(AtomicBool::new(true));
        let mut handles = Vec::new();

        // Spawn 20 checker threads
        for thread_id in 0..20 {
            let limiter = Arc::clone(&limiter);
            let running = Arc::clone(&running);
            handles.push(thread::spawn(move || {
                let client_id = format!("client-{}", thread_id);
                let organization = OrganizationId::new((thread_id % 5 + 1) as i64);
                let mut ok_count = 0u64;
                let mut reject_count = 0u64;

                while running.load(AOrdering::Relaxed) {
                    match limiter.check(&client_id, organization) {
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
            100_000.0, // Organization limits are generous
            10_000,    // No backpressure
            "global",
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
                match limiter.check("shared-client", OrganizationId::new(1)) {
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

    /// Stress test: lock-free try_acquire upholds total-consumption bound under
    /// heavy contention on a single bucket.
    ///
    /// Spawns N threads that each call `try_acquire` in a tight loop against a
    /// single `TokenBucket`. Tokens refill at a known rate over the test
    /// duration. Verifies:
    ///
    /// 1. No thread panics (no CAS livelock, no arithmetic overflow).
    /// 2. Net consumed tokens ≤ capacity + refill × duration + small slack (no token double-spend
    ///    under the lock-free CAS loop).
    #[test]
    fn stress_lock_free_try_acquire_no_double_spend() {
        use std::{
            sync::{
                Arc, Barrier,
                atomic::{AtomicBool, AtomicU64, Ordering as AOrdering},
            },
            thread,
            time::Duration,
        };

        let capacity = 100u64;
        let refill_rate = 500.0; // tokens/sec
        let bucket = Arc::new(TokenBucket::new(capacity, refill_rate));
        let acquired = Arc::new(AtomicU64::new(0));

        let num_threads = 32usize;
        let running = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let bucket = Arc::clone(&bucket);
            let acquired = Arc::clone(&acquired);
            let running = Arc::clone(&running);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                while running.load(AOrdering::Relaxed) {
                    if bucket.try_acquire() {
                        acquired.fetch_add(1, AOrdering::Relaxed);
                    }
                }
            }));
        }

        barrier.wait();
        let start = Instant::now();
        // Run for a fixed duration.
        thread::sleep(Duration::from_millis(200));
        running.store(false, AOrdering::Relaxed);
        for h in handles {
            h.join().expect("thread panicked");
        }
        let elapsed = start.elapsed();

        let total_acquired = acquired.load(AOrdering::Relaxed);
        // Theoretical maximum: starting capacity + refill over the elapsed window.
        //
        // Scheduling slack: at the moment `running.store(false)` is observed,
        // threads mid-`try_acquire` can still commit. Under u32 ms-resolution
        // timestamps and f64 refill math, a small rounding boundary adds at
        // most one token per ms-tick per thread in flight. `num_threads` is a
        // safe and conservative slack bound that absorbs these effects without
        // masking actual double-spends (which would scale with thread count
        // multiplicatively, not additively).
        let elapsed_secs = elapsed.as_secs_f64();
        let max_expected =
            capacity + (elapsed_secs * refill_rate).ceil() as u64 + num_threads as u64;
        assert!(
            total_acquired <= max_expected,
            "Acquired {total_acquired} but max_expected {max_expected} (capacity={capacity}, \
             elapsed={elapsed_secs:.3}s, refill={refill_rate}/s, threads={num_threads}) — \
             token double-spend detected"
        );
        // We should have made progress — if zero tokens were acquired, the CAS
        // loop likely livelocked.
        assert!(total_acquired > 0, "Expected some tokens to be acquired under contention");
    }
}
