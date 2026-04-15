//! Region leader resolution and caching for data residency routing.
//!
//! When the SDK needs to reach the Raft leader for a specific data residency
//! region, it resolves the leader endpoint via the gateway's
//! `ResolveRegionLeader` RPC and caches the result with two thresholds:
//!
//! - `soft_ttl` (default 30s): past this, cached entries are returned immediately AND a background
//!   refresh is triggered (stale-while-revalidate).
//! - `hard_ttl` (default 120s): past this, the entry is considered expired and the next caller
//!   blocks on a fresh resolve.
//!
//! This avoids a discovery round-trip on every request while still reacting
//! to leader changes within seconds, and smooths out tail latency during
//! refresh by never blocking callers on stale-but-usable entries.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::{BoxFuture, FutureExt, Shared};
use inferadb_ledger_proto::proto;
use inferadb_ledger_types::Region;
use tonic::transport::Channel;

use crate::{
    error::{Result, SdkError},
    proto_util::region_to_proto_i32,
};

/// Internal cached leader entry.
#[derive(Debug, Clone)]
struct CachedLeader {
    endpoint: Arc<str>,
    resolved_at: Instant,
    soft_ttl: Duration,
    hard_ttl: Duration,
}

impl CachedLeader {
    /// Returns `true` if the entry is younger than `soft_ttl` — no refresh needed.
    fn is_fresh(&self) -> bool {
        self.resolved_at.elapsed() < self.soft_ttl
    }

    /// Returns `true` if the entry is between `soft_ttl` and `hard_ttl`:
    /// usable, but a background refresh should be triggered.
    fn is_stale_but_usable(&self) -> bool {
        let age = self.resolved_at.elapsed();
        age >= self.soft_ttl && age < self.hard_ttl
    }
}

/// Default soft TTL applied when the server returns `ttl_seconds = 0`.
const DEFAULT_SOFT_TTL_SECS: u64 = 30;

/// Default hard TTL applied when no config override is provided.
const DEFAULT_HARD_TTL_SECS: u64 = 120;

/// Upper bound on server-returned `ttl_seconds`. Anything larger is clamped
/// to this value before being used as `soft_ttl` — defends against absurd
/// server values that would leave stale entries around for hours or longer.
const MAX_SERVER_TTL_SECS: u64 = 600;

/// Classification of a cached leader entry's freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFreshness {
    /// Entry is within `soft_ttl` — no refresh needed.
    Fresh,
    /// Entry is past `soft_ttl` but within `hard_ttl` — return it but
    /// spawn a background refresh.
    StaleButUsable,
}

/// Cache for the Raft leader endpoint of a specific data residency region.
///
/// Thread-safe: uses a `parking_lot::RwLock` so concurrent readers do not
/// block each other. Only `resolve` and `invalidate` take a write lock.
#[derive(Debug)]
pub struct RegionLeaderCache {
    region: Region,
    default_soft_ttl: Duration,
    default_hard_ttl: Duration,
    cached: parking_lot::RwLock<Option<CachedLeader>>,
    in_flight: parking_lot::Mutex<Option<Shared<BoxFuture<'static, Arc<Result<String>>>>>>,
    metrics: parking_lot::RwLock<Arc<dyn crate::metrics::SdkMetrics>>,
}

impl RegionLeaderCache {
    /// Creates a new empty cache for the given region with default TTLs.
    #[must_use]
    pub fn new(region: Region) -> Self {
        Self::with_ttls(
            region,
            Duration::from_secs(DEFAULT_SOFT_TTL_SECS),
            Duration::from_secs(DEFAULT_HARD_TTL_SECS),
        )
    }

    /// Creates a new empty cache for the given region with explicit TTLs.
    #[must_use]
    pub fn with_ttls(region: Region, soft_ttl: Duration, hard_ttl: Duration) -> Self {
        Self {
            region,
            default_soft_ttl: soft_ttl,
            default_hard_ttl: hard_ttl,
            cached: parking_lot::RwLock::new(None),
            in_flight: parking_lot::Mutex::new(None),
            metrics: parking_lot::RwLock::new(Arc::new(crate::metrics::NoopSdkMetrics)),
        }
    }

    /// Installs the metrics sink for this cache.
    ///
    /// Intended to be called once during pool construction. Subsequent reads
    /// from hot paths (`get_or_resolve`, resolve, etc.) observe the installed
    /// sink without blocking.
    pub fn set_metrics(&self, metrics: Arc<dyn crate::metrics::SdkMetrics>) {
        *self.metrics.write() = metrics;
    }

    /// Returns a clone of the current metrics sink.
    fn metrics(&self) -> Arc<dyn crate::metrics::SdkMetrics> {
        Arc::clone(&self.metrics.read())
    }

    /// Returns the region label as a string for metric tagging.
    fn region_label(&self) -> String {
        self.region.to_string()
    }

    /// Returns the cached leader endpoint if usable (fresh OR stale-but-usable).
    ///
    /// Returns `None` when the cache is empty or the entry is past `hard_ttl`.
    /// Callers that need to distinguish fresh from stale-but-usable should use
    /// [`cached_endpoint_with_freshness`](Self::cached_endpoint_with_freshness).
    #[must_use]
    pub fn cached_endpoint(&self) -> Option<Arc<str>> {
        self.cached_endpoint_with_freshness().map(|(ep, _)| ep)
    }

    /// Returns the cached endpoint along with its freshness classification.
    ///
    /// Returns `None` when the cache is empty or the entry is past `hard_ttl`.
    #[must_use]
    pub fn cached_endpoint_with_freshness(&self) -> Option<(Arc<str>, CacheFreshness)> {
        let guard = self.cached.read();
        guard.as_ref().and_then(|c| {
            if c.is_fresh() {
                Some((Arc::clone(&c.endpoint), CacheFreshness::Fresh))
            } else if c.is_stale_but_usable() {
                Some((Arc::clone(&c.endpoint), CacheFreshness::StaleButUsable))
            } else {
                None
            }
        })
    }

    /// Returns the region this cache is associated with.
    #[must_use]
    pub fn region(&self) -> Region {
        self.region
    }

    /// Clears the cached leader entry, forcing the next access to re-resolve.
    pub fn invalidate(&self) {
        *self.cached.write() = None;
    }

    /// Applies a leader hint (parsed from a `NotLeader` error) to the cache.
    ///
    /// If the hint contains an endpoint, the cache is updated in place with
    /// the default TTLs. If the endpoint is absent, this is a no-op.
    ///
    /// Call this on receiving a `NotLeader` error with leader hints (see
    /// [`ServerErrorDetails::leader_hint`](crate::error::ServerErrorDetails::leader_hint))
    /// so the next request targets the newly-known leader without a round-trip
    /// through `ResolveRegionLeader`.
    pub fn apply_hint(&self, hint: &crate::error::LeaderHint) {
        let Some(endpoint) = hint.leader_endpoint.as_deref() else {
            return;
        };
        *self.cached.write() = Some(CachedLeader {
            endpoint: Arc::from(endpoint),
            resolved_at: Instant::now(),
            soft_ttl: self.default_soft_ttl,
            hard_ttl: self.default_hard_ttl,
        });
    }

    /// Fetches the current leader endpoint, implementing stale-while-revalidate.
    ///
    /// - Fresh (age < `soft_ttl`): return immediately.
    /// - Stale-but-usable (`soft_ttl` <= age < `hard_ttl`): return immediately AND trigger a
    ///   background refresh if none is in flight.
    /// - Expired or absent (age >= `hard_ttl`): block on a coalesced resolve.
    ///
    /// Prefer this over calling [`resolve`](Self::resolve) directly when the
    /// caller has the gateway channel available.
    ///
    /// # Errors
    ///
    /// Returns an [`SdkError`] if the underlying `resolve` call fails on the
    /// expired-or-absent path.
    pub async fn get_or_resolve(self: &Arc<Self>, gateway_channel: &Channel) -> Result<String> {
        let snapshot = { self.cached.read().clone() };
        let label = self.region_label();
        let metrics = self.metrics();
        match snapshot {
            Some(ref c) if c.is_fresh() => {
                metrics.leader_cache_hit(&label);
                Ok(c.endpoint.as_ref().to_owned())
            },
            Some(ref c) if c.is_stale_but_usable() => {
                metrics.leader_cache_hit(&label);
                metrics.region_resolve_stale_served(&label);
                self.spawn_background_refresh(gateway_channel.clone());
                Ok(c.endpoint.as_ref().to_owned())
            },
            _ => {
                metrics.leader_cache_miss(&label);
                self.resolve(gateway_channel).await
            },
        }
    }

    /// Spawns a background refresh if none is already in flight.
    ///
    /// No-op when the single-flight slot is already occupied (including by
    /// a caller currently awaiting `resolve`).
    pub fn spawn_background_refresh(self: &Arc<Self>, gateway_channel: Channel) {
        if self.in_flight.lock().is_some() {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _ = this.resolve(&gateway_channel).await;
        });
    }

    /// Resolves the region leader via the gateway's `ResolveRegionLeader` RPC,
    /// caches the result, and returns the leader endpoint.
    ///
    /// # Errors
    ///
    /// Returns an [`SdkError`] if the gRPC call fails (network error, server
    /// unavailable, unknown region, etc.).
    pub async fn resolve(self: &Arc<Self>, gateway_channel: &Channel) -> Result<String> {
        let gateway = gateway_channel.clone();
        let this = Arc::clone(self);
        self.run_single_flight(move || async move { this.resolve_via_gateway(&gateway).await })
            .await
    }

    /// Performs the actual `ResolveRegionLeader` RPC and writes the result into
    /// the cache. This is the inner path driven through the single-flight slot.
    async fn resolve_via_gateway(&self, gateway_channel: &Channel) -> Result<String> {
        let previous_endpoint = self.cached.read().as_ref().map(|c| Arc::clone(&c.endpoint));

        let mut client = proto::system_discovery_service_client::SystemDiscoveryServiceClient::new(
            gateway_channel.clone(),
        );

        let response = client
            .resolve_region_leader(proto::ResolveRegionLeaderRequest {
                region: region_to_proto_i32(self.region),
            })
            .await
            .map_err(SdkError::from)?;

        let resp = response.into_inner();
        let soft_ttl = if resp.ttl_seconds > 0 {
            // Clamp to defend against pathological server values.
            Duration::from_secs(u64::from(resp.ttl_seconds).min(MAX_SERVER_TTL_SECS))
        } else {
            self.default_soft_ttl
        };
        // Ensure hard_ttl >= soft_ttl. If the server's soft_ttl exceeds our
        // configured default_hard_ttl, scale hard to at least 4x soft.
        let hard_ttl = std::cmp::max(self.default_hard_ttl, soft_ttl.saturating_mul(4));

        let endpoint = resp.endpoint;

        *self.cached.write() = Some(CachedLeader {
            endpoint: Arc::from(endpoint.as_str()),
            resolved_at: Instant::now(),
            soft_ttl,
            hard_ttl,
        });

        if let Some(prev) = previous_endpoint
            && prev.as_ref() != endpoint.as_str()
        {
            self.metrics().leader_cache_flap(&self.region_label());
        }

        Ok(endpoint)
    }

    /// Drives an async operation through the single-flight slot.
    ///
    /// If no operation is currently in flight, builds a new `Shared` future
    /// and stores it in the slot. Concurrent callers observing the occupied
    /// slot clone and await the same future. The slot is cleared as the
    /// future resolves, so the next cache miss starts a fresh resolution.
    async fn run_single_flight<F, Fut>(self: &Arc<Self>, op: F) -> Result<String>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        let shared = {
            let mut slot = self.in_flight.lock();
            if let Some(existing) = slot.as_ref() {
                self.metrics().region_resolve_coalesced(&self.region_label());
                existing.clone()
            } else {
                let this = Arc::clone(self);
                let fut: BoxFuture<'static, Arc<Result<String>>> = Box::pin(async move {
                    let result = Arc::new(op().await);
                    // Clear the slot so the next miss triggers a fresh call.
                    this.in_flight.lock().take();
                    result
                });
                let shared = fut.shared();
                *slot = Some(shared.clone());
                shared
            }
        };
        let result = shared.await;
        // `Result<String, SdkError>` is not `Clone` (SdkError contains
        // non-Clone variants like `tonic::transport::Error`). The shared future
        // therefore yields `Arc<Result<_>>`; each awaiter reconstructs an owned
        // `Result`. Ok clones the endpoint string. Err is downgraded to
        // `SdkError::Connection` with the message rendered — typed error
        // information (gRPC code, request_id, trace_id, error_details) is lost
        // on the failure path for ALL awaiters, including the uncontended first
        // caller. Acceptable trade-off: the current caller of `resolve` in
        // `ConnectionPool::get_region_channel` only distinguishes Ok/Err and
        // logs on Err. If richer error classification is needed in the future,
        // replace this with a `Clone`-compatible error type or switch to a
        // broadcast-channel coalescing primitive.
        match &*result {
            Ok(endpoint) => Ok(endpoint.clone()),
            Err(err) => Err(SdkError::Connection { message: err.to_string() }),
        }
    }

    /// Test-only hook that drives an arbitrary closure through the same
    /// single-flight coalescing used by [`resolve`]. Lets tests exercise
    /// the coalescing primitive without spinning up a gRPC server.
    #[cfg(test)]
    pub(crate) async fn resolve_via_closure<F, Fut>(self: &Arc<Self>, op: F) -> Result<String>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.run_single_flight(op).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Test metrics sink that counts calls by (method, region).
    #[derive(Debug, Default)]
    struct CountingTestMetrics {
        counters: parking_lot::Mutex<std::collections::HashMap<(String, String), u64>>,
    }

    impl CountingTestMetrics {
        fn bump(&self, method: &str, region: &str) {
            let mut m = self.counters.lock();
            *m.entry((method.to_owned(), region.to_owned())).or_insert(0) += 1;
        }

        fn get(&self, method: &str, region: &str) -> u64 {
            *self.counters.lock().get(&(method.to_owned(), region.to_owned())).unwrap_or(&0)
        }
    }

    impl crate::metrics::SdkMetrics for CountingTestMetrics {
        fn leader_cache_hit(&self, region: &str) {
            self.bump("hit", region);
        }
        fn leader_cache_miss(&self, region: &str) {
            self.bump("miss", region);
        }
        fn leader_cache_flap(&self, region: &str) {
            self.bump("flap", region);
        }
        fn region_resolve_coalesced(&self, region: &str) {
            self.bump("coalesce", region);
        }
        fn region_resolve_stale_served(&self, region: &str) {
            self.bump("stale_served", region);
        }
    }

    #[test]
    fn fresh_entry_is_fresh_not_stale() {
        let entry = CachedLeader {
            endpoint: Arc::from("http://x:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        };
        assert!(entry.is_fresh());
        assert!(!entry.is_stale_but_usable());
    }

    #[test]
    fn stale_but_usable_detected() {
        let entry = CachedLeader {
            endpoint: Arc::from("http://x:5000"),
            resolved_at: Instant::now() - Duration::from_secs(60),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        };
        assert!(!entry.is_fresh());
        assert!(entry.is_stale_but_usable());
    }

    #[test]
    fn past_hard_ttl_not_usable() {
        let entry = CachedLeader {
            endpoint: Arc::from("http://x:5000"),
            resolved_at: Instant::now() - Duration::from_secs(200),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        };
        assert!(!entry.is_fresh());
        assert!(!entry.is_stale_but_usable());
    }

    #[test]
    fn boundary_at_soft_ttl_is_stale_not_fresh() {
        // age == soft_ttl should classify as stale-but-usable (strict `<` on is_fresh).
        let entry = CachedLeader {
            endpoint: Arc::from("http://x:5000"),
            resolved_at: Instant::now() - Duration::from_secs(30),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        };
        assert!(!entry.is_fresh(), "age == soft_ttl must not be fresh");
        assert!(entry.is_stale_but_usable(), "age == soft_ttl must be stale-but-usable");
    }

    #[test]
    fn boundary_at_hard_ttl_is_expired() {
        // age == hard_ttl should classify as expired (strict `<` on is_stale_but_usable).
        let entry = CachedLeader {
            endpoint: Arc::from("http://x:5000"),
            resolved_at: Instant::now() - Duration::from_secs(120),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        };
        assert!(!entry.is_fresh());
        assert!(!entry.is_stale_but_usable(), "age == hard_ttl must be expired");
    }

    #[test]
    fn cached_endpoint_returns_stale_but_usable() {
        let cache = RegionLeaderCache::with_ttls(
            Region::US_EAST_VA,
            Duration::from_millis(1),
            Duration::from_secs(10),
        );
        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://stale:5000"),
            resolved_at: Instant::now() - Duration::from_millis(500),
            soft_ttl: Duration::from_millis(1),
            hard_ttl: Duration::from_secs(10),
        });
        assert!(cache.cached_endpoint().is_some());
    }

    #[test]
    fn cached_endpoint_returns_none_past_hard_ttl() {
        let cache = RegionLeaderCache::with_ttls(
            Region::US_EAST_VA,
            Duration::from_millis(1),
            Duration::from_millis(10),
        );
        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://expired:5000"),
            resolved_at: Instant::now() - Duration::from_secs(60),
            soft_ttl: Duration::from_millis(1),
            hard_ttl: Duration::from_millis(10),
        });
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn cached_endpoint_with_freshness_classifies_correctly() {
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://fresh:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });
        let (_ep, freshness) = cache.cached_endpoint_with_freshness().expect("cache populated");
        assert_eq!(freshness, CacheFreshness::Fresh);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://stale:5000"),
            resolved_at: Instant::now() - Duration::from_secs(60),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });
        let (_ep, freshness) = cache.cached_endpoint_with_freshness().expect("cache populated");
        assert_eq!(freshness, CacheFreshness::StaleButUsable);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://expired:5000"),
            resolved_at: Instant::now() - Duration::from_secs(300),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });
        assert!(cache.cached_endpoint_with_freshness().is_none());
    }

    #[test]
    fn invalidate_clears_cache() {
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://10.0.1.5:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });
        assert!(cache.cached_endpoint().is_some());
        cache.invalidate();
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn new_cache_has_no_endpoint() {
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn region_accessor() {
        let cache = RegionLeaderCache::new(Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(cache.region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn apply_hint_with_endpoint_populates_cache() {
        use crate::error::LeaderHint;
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        let hint = LeaderHint {
            leader_id: Some(42),
            leader_endpoint: Some("http://10.0.2.5:5000".to_owned()),
            term: Some(7),
        };
        cache.apply_hint(&hint);
        assert_eq!(cache.cached_endpoint().as_deref(), Some("http://10.0.2.5:5000"));
    }

    #[test]
    fn apply_hint_without_endpoint_does_nothing() {
        use crate::error::LeaderHint;
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        let hint = LeaderHint { leader_id: Some(42), leader_endpoint: None, term: Some(7) };
        cache.apply_hint(&hint);
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn apply_hint_overwrites_existing_cache() {
        use crate::error::LeaderHint;
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://old:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });

        let hint = LeaderHint {
            leader_id: Some(1),
            leader_endpoint: Some("http://new:5000".to_owned()),
            term: None,
        };
        cache.apply_hint(&hint);

        assert_eq!(cache.cached_endpoint().as_deref(), Some("http://new:5000"));
    }

    #[tokio::test]
    async fn concurrent_resolves_coalesce_into_one_call() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let call_count = Arc::new(AtomicU32::new(0));
        let cache = Arc::new(RegionLeaderCache::new(Region::US_EAST_VA));

        let mut handles = Vec::new();
        for _ in 0..50 {
            let cache = Arc::clone(&cache);
            let count = Arc::clone(&call_count);
            handles.push(tokio::spawn(async move {
                cache
                    .resolve_via_closure(move || {
                        let count = Arc::clone(&count);
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            Ok::<String, SdkError>("http://leader:5000".to_owned())
                        }
                    })
                    .await
            }));
        }

        for h in handles {
            h.await.expect("join").expect("resolve");
        }

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "concurrent resolves must coalesce into one call"
        );
    }

    #[tokio::test]
    async fn sequential_resolves_do_not_coalesce() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let call_count = Arc::new(AtomicU32::new(0));
        let cache = Arc::new(RegionLeaderCache::new(Region::US_EAST_VA));

        for _ in 0..3 {
            let count = Arc::clone(&call_count);
            cache
                .resolve_via_closure(move || {
                    let count = Arc::clone(&count);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok::<String, SdkError>("http://leader:5000".to_owned())
                    }
                })
                .await
                .expect("resolve");
        }

        assert_eq!(call_count.load(Ordering::SeqCst), 3, "sequential resolves must each run");
    }

    #[test]
    fn set_metrics_replaces_default_noop() {
        let metrics = Arc::new(CountingTestMetrics::default());
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);
        // Emission via metrics() should reach the counter.
        cache.metrics().leader_cache_hit(&cache.region_label());
        assert_eq!(metrics.get("hit", "us-east-va"), 1);
    }

    #[tokio::test]
    async fn metrics_emitted_on_singleflight_coalesce() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let metrics = Arc::new(CountingTestMetrics::default());
        let cache = Arc::new(RegionLeaderCache::new(Region::US_EAST_VA));
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);

        let call_count = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let cache = Arc::clone(&cache);
            let count = Arc::clone(&call_count);
            handles.push(tokio::spawn(async move {
                cache
                    .resolve_via_closure(move || {
                        let count = Arc::clone(&count);
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(30)).await;
                            Ok::<String, SdkError>("http://leader:5000".to_owned())
                        }
                    })
                    .await
            }));
        }
        for h in handles {
            h.await.expect("join").expect("resolve");
        }

        // One caller ran the op; the remaining 19 must have coalesced.
        // Timing-sensitive; assert at least several to allow scheduler variance.
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert!(
            metrics.get("coalesce", "us-east-va") >= 5,
            "expected several coalesce events, got {}",
            metrics.get("coalesce", "us-east-va")
        );
    }

    #[tokio::test]
    async fn get_or_resolve_emits_hit_on_fresh_cache() {
        let metrics = Arc::new(CountingTestMetrics::default());
        let cache = Arc::new(RegionLeaderCache::new(Region::US_EAST_VA));
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://fresh:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });

        // Fresh snapshot — no gateway call needed. Dummy channel is fine
        // because the fresh arm returns before touching it.
        let dummy = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let _ = cache.get_or_resolve(&dummy).await.expect("fresh");

        assert_eq!(metrics.get("hit", "us-east-va"), 1);
        assert_eq!(metrics.get("miss", "us-east-va"), 0);
        assert_eq!(metrics.get("stale_served", "us-east-va"), 0);
    }

    #[tokio::test]
    async fn get_or_resolve_emits_stale_served_on_stale_cache() {
        let metrics = Arc::new(CountingTestMetrics::default());
        let cache = Arc::new(RegionLeaderCache::new(Region::US_EAST_VA));
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://stale:5000"),
            resolved_at: Instant::now() - Duration::from_secs(60),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });

        let dummy = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let _ = cache.get_or_resolve(&dummy).await.expect("stale-but-usable");

        assert_eq!(metrics.get("hit", "us-east-va"), 1);
        assert_eq!(metrics.get("stale_served", "us-east-va"), 1);
        assert_eq!(metrics.get("miss", "us-east-va"), 0);
    }

    #[test]
    fn apply_hint_does_not_fire_flap_metric() {
        // Flap is reserved for resolve-path endpoint changes; hints are a
        // different signal and must not be conflated.
        let metrics = Arc::new(CountingTestMetrics::default());
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);

        *cache.cached.write() = Some(CachedLeader {
            endpoint: Arc::from("http://A:5000"),
            resolved_at: Instant::now(),
            soft_ttl: Duration::from_secs(30),
            hard_ttl: Duration::from_secs(120),
        });

        cache.apply_hint(&crate::error::LeaderHint {
            leader_id: Some(1),
            leader_endpoint: Some("http://B:5000".to_owned()),
            term: None,
        });

        assert_eq!(metrics.get("flap", "us-east-va"), 0);
    }
}
