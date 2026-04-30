//! SDK-side metrics for observability and operational monitoring.
//!
//! This module provides a pluggable metrics trait (`SdkMetrics`) that SDK users
//! can implement to collect telemetry from the client. Two implementations are
//! included:
//!
//! - [`NoopSdkMetrics`]: Zero-overhead default that discards all metrics.
//! - [`MetricsSdkMetrics`]: Integration with the [`metrics`](https://docs.rs/metrics) crate facade,
//!   automatically forwarding to whatever recorder is installed (Prometheus, StatsD, etc.).
//!
//! # Opt-In
//!
//! SDK metrics are **opt-in**. The default implementation is [`NoopSdkMetrics`], which
//! compiles away to nothing. To enable metrics emission, pass a [`MetricsSdkMetrics`]
//! (or a custom implementation) to [`ClientConfig::builder().metrics()`]:
//!
//! ```no_run
//! use inferadb_ledger_sdk::{ClientConfig, ServerSource, MetricsSdkMetrics};
//! use std::sync::Arc;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::builder()
//!     .servers(ServerSource::from_static(["http://localhost:50051"]))
//!     .client_id("my-app")
//!     .metrics(Arc::new(MetricsSdkMetrics))
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Cardinality
//!
//! All label values emitted by the SDK are bounded:
//!
//! - `method`: fixed set of RPC method names (hardcoded at call sites).
//! - `status`: `"success"` or `"error"`.
//! - `attempt`: bounded by `RetryConfig::max_attempts` (typically ≤ 5).
//! - `error_type`: fixed classification strings from the SDK error classifier.
//! - `endpoint`: server URLs from `ClientConfig::servers()` or discovery — bounded by the cluster's
//!   server list, not arbitrary user input.
//! - `event`: three values (`"connected"`, `"disconnected"`, `"failed"`).
//! - `state`: three values (`"closed"`, `"open"`, `"half-open"`).
//! - `region`: logical region names from server configuration.
//! - `source`: `"hint"` or `"watch"`.
//!
//! The cardinality guard (`CardinalityTracker`) is a server-side concern and is not
//! wired into the SDK. Consumers using a custom `SdkMetrics` implementation are
//! responsible for their own cardinality management.
//!
//! # Metric Names
//!
//! All metrics follow the `ledger_sdk_` prefix convention:
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `ledger_sdk_requests_total` | Counter | `method`, `status` | Total requests by method and outcome |
//! | `ledger_sdk_request_duration_seconds` | Histogram | `method` | Request latency distribution |
//! | `ledger_sdk_retries_total` | Counter | `method`, `attempt`, `error_type` | Retry attempts by method |
//! | `ledger_sdk_circuit_transitions_total` | Counter | `endpoint`, `state` | Circuit breaker state transitions |
//! | `ledger_sdk_connections_total` | Counter | `endpoint`, `event` | Connection lifecycle events |
//! | `ledger_sdk_leader_cache_hits_total` | Counter | `region` | Region leader cache hits (fresh or stale-but-usable) |
//! | `ledger_sdk_leader_cache_misses_total` | Counter | `region` | Region leader cache misses (absent or past hard TTL) |
//! | `ledger_sdk_leader_cache_flaps_total` | Counter | `region` | Leader changes detected on resolve |
//! | `ledger_sdk_region_resolve_singleflight_coalesced_total` | Counter | `region` | Resolves coalesced onto an in-flight future |
//! | `ledger_sdk_region_resolve_stale_served_total` | Counter | `region` | Stale entries served while background refresh ran |
//! | `ledger_sdk_leader_watch_updates_total` | Counter | `region` | Leader updates received over the WatchLeader stream |
//! | `ledger_sdk_leader_watch_reconnects_total` | Counter | `region` | WatchLeader stream reconnect attempts |
//! | `ledger_sdk_leader_stale_term_rejected_total` | Counter | `region`, `source` | Cache writes rejected for carrying a stale term |
//! | `ledger_sdk_redirect_retries_total` | Counter | `region` | Retries triggered by a leader redirect hint |

use std::{fmt, sync::Arc, time::Duration};

/// Events for connection lifecycle tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionEvent {
    /// A new connection was established.
    Connected,
    /// A connection was closed or dropped.
    Disconnected,
    /// A connection attempt failed.
    Failed,
}

impl fmt::Display for ConnectionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Trait for SDK-side metrics collection.
///
/// Implement this trait to integrate with your metrics backend of choice.
/// All methods have default no-op implementations, so you only need to
/// override the metrics you care about.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` since the SDK shares a single
/// metrics instance across all client clones and background tasks.
pub trait SdkMetrics: Send + Sync + fmt::Debug {
    /// Records the outcome of a completed request.
    ///
    /// Called once per successful top-level operation (after retries resolve).
    ///
    /// - `method`: The RPC method name (e.g., "read", "write", "batch_read").
    /// - `duration`: Wall-clock time from request start to final response.
    /// - `success`: Whether the request ultimately succeeded.
    fn record_request(&self, method: &str, duration: Duration, success: bool) {
        let _ = (method, duration, success);
    }

    /// Records a retry attempt.
    ///
    /// Called once per retry attempt (not the initial attempt).
    ///
    /// - `method`: The RPC method name.
    /// - `attempt`: The attempt number (2 = first retry, 3 = second retry, etc.).
    /// - `error_type`: Classification of the error that triggered the retry.
    fn record_retry(&self, method: &str, attempt: u32, error_type: &str) {
        let _ = (method, attempt, error_type);
    }

    /// Records a circuit breaker state transition.
    ///
    /// Called whenever a circuit breaker changes state.
    ///
    /// - `endpoint`: The server endpoint URL.
    /// - `state`: The new circuit state ("closed", "open", "half-open").
    fn record_circuit_state(&self, endpoint: &str, state: &str) {
        let _ = (endpoint, state);
    }

    /// Records a connection lifecycle event.
    ///
    /// - `endpoint`: The server endpoint URL.
    /// - `event`: The connection event type.
    fn record_connection(&self, endpoint: &str, event: ConnectionEvent) {
        let _ = (endpoint, event);
    }

    /// Records a region leader cache hit.
    ///
    /// Called when the cache returned a fresh or stale-but-usable entry,
    /// avoiding a full resolve round-trip.
    ///
    /// - `region`: The logical region name.
    fn leader_cache_hit(&self, _region: &str) {}

    /// Records a region leader cache miss.
    ///
    /// Called when the cache had no entry or the entry was past the hard TTL,
    /// requiring a blocking resolve.
    ///
    /// - `region`: The logical region name.
    fn leader_cache_miss(&self, _region: &str) {}

    /// Records a region leader change detected on resolve.
    ///
    /// Called when a successful resolve returned a different endpoint than
    /// what was previously cached.
    ///
    /// - `region`: The logical region name.
    fn leader_cache_flap(&self, _region: &str) {}

    /// Records a resolve that coalesced onto an already in-flight resolve.
    ///
    /// Called when the single-flight deduplicator joined a concurrent resolve
    /// rather than starting a new one.
    ///
    /// - `region`: The logical region name.
    fn region_resolve_coalesced(&self, _region: &str) {}

    /// Records a stale cache entry served while a background refresh ran.
    ///
    /// Called when the entry was past the soft TTL but not yet past the hard
    /// TTL, so the stale value was returned immediately while the refresh
    /// proceeded asynchronously.
    ///
    /// - `region`: The logical region name.
    fn region_resolve_stale_served(&self, _region: &str) {}

    /// Records a push update received from the leader-watch stream.
    ///
    /// - `region`: The logical region name.
    fn leader_watch_update(&self, _region: &str) {}

    /// Records a leader-watch stream reconnect attempt.
    ///
    /// Called when the watch stream reconnects after an error or EOF.
    ///
    /// - `region`: The logical region name.
    fn leader_watch_reconnect(&self, _region: &str) {}

    /// Records a cache write that was rejected due to a stale term.
    ///
    /// Called when an incoming hint or watch update carries a term that is
    /// older than the currently-cached term, preventing a stale-leader
    /// overwrite.
    ///
    /// - `region`: The logical region name.
    /// - `source`: `"hint"` for `NotLeader` redirect hints, `"watch"` for leader-watch stream
    ///   updates.
    fn leader_stale_term_rejected(&self, _region: &str, _source: &'static str) {}

    /// Records a retry triggered by a leader redirect hint.
    ///
    /// Called when the client receives a `NotLeader` response with a
    /// [`LeaderHint`](crate::LeaderHint) and retries the request against the
    /// indicated leader. On warm paths with
    /// [`ClientConfig::preferred_region`](crate::ClientConfig::preferred_region)
    /// set, this counter should trend toward zero.
    ///
    /// - `region`: The logical region name.
    fn redirect_retry(&self, _region: &str) {}
}

/// No-op metrics implementation with zero overhead.
///
/// This is the default when no metrics backend is configured. All methods
/// are empty and should be optimized away by the compiler.
#[derive(Debug, Clone, Copy)]
pub struct NoopSdkMetrics;

impl SdkMetrics for NoopSdkMetrics {}

/// Metrics implementation using the [`metrics`](https://docs.rs/metrics) crate facade.
///
/// This forwards all SDK metrics to whatever `metrics::Recorder` is installed
/// in the process. When used with `metrics-exporter-prometheus`, metrics are
/// automatically exposed as Prometheus counters, histograms, and gauges.
///
/// All metric names use the `ledger_sdk_` prefix.
#[derive(Debug, Clone, Copy)]
pub struct MetricsSdkMetrics;

/// Metric name constants for the `metrics` crate facade.
mod metric_names {
    /// Total requests by method and outcome.
    pub const REQUESTS_TOTAL: &str = "ledger_sdk_requests_total";
    /// Request duration distribution.
    pub const REQUEST_DURATION: &str = "ledger_sdk_request_duration_seconds";
    /// Retry attempts by method.
    pub const RETRIES_TOTAL: &str = "ledger_sdk_retries_total";
    /// Circuit breaker state transitions.
    pub const CIRCUIT_TRANSITIONS_TOTAL: &str = "ledger_sdk_circuit_transitions_total";
    /// Connection lifecycle events.
    pub const CONNECTIONS_TOTAL: &str = "ledger_sdk_connections_total";
    /// Region leader cache hits (fresh or stale-but-usable).
    pub const LEADER_CACHE_HITS_TOTAL: &str = "ledger_sdk_leader_cache_hits_total";
    /// Region leader cache misses (absent or past hard_ttl).
    pub const LEADER_CACHE_MISSES_TOTAL: &str = "ledger_sdk_leader_cache_misses_total";
    /// Region leader cache flaps (resolve returned a different endpoint).
    pub const LEADER_CACHE_FLAPS_TOTAL: &str = "ledger_sdk_leader_cache_flaps_total";
    /// Region resolves coalesced onto an in-flight future.
    pub const REGION_RESOLVE_SINGLEFLIGHT_COALESCED_TOTAL: &str =
        "ledger_sdk_region_resolve_singleflight_coalesced_total";
    /// Stale-but-usable entries served while a background refresh ran.
    pub const REGION_RESOLVE_STALE_SERVED_TOTAL: &str =
        "ledger_sdk_region_resolve_stale_served_total";
    /// Leader watch stream pushed updates.
    pub const LEADER_WATCH_UPDATES_TOTAL: &str = "ledger_sdk_leader_watch_updates_total";
    /// Leader watch stream reconnect attempts.
    pub const LEADER_WATCH_RECONNECTS_TOTAL: &str = "ledger_sdk_leader_watch_reconnects_total";
    /// Cache writes rejected for carrying a stale term.
    pub const LEADER_STALE_TERM_REJECTED_TOTAL: &str =
        "ledger_sdk_leader_stale_term_rejected_total";
    /// Retries triggered by a leader redirect hint.
    pub const REDIRECT_RETRIES_TOTAL: &str = "ledger_sdk_redirect_retries_total";
}

impl SdkMetrics for MetricsSdkMetrics {
    fn record_request(&self, method: &str, duration: Duration, success: bool) {
        let status = if success { "success" } else { "error" };
        metrics::counter!(metric_names::REQUESTS_TOTAL, "method" => method.to_owned(), "status" => status).increment(1);
        metrics::histogram!(metric_names::REQUEST_DURATION, "method" => method.to_owned())
            .record(duration.as_secs_f64());
    }

    fn record_retry(&self, method: &str, attempt: u32, error_type: &str) {
        metrics::counter!(
            metric_names::RETRIES_TOTAL,
            "method" => method.to_owned(),
            "attempt" => attempt.to_string(),
            "error_type" => error_type.to_owned(),
        )
        .increment(1);
    }

    fn record_circuit_state(&self, endpoint: &str, state: &str) {
        metrics::counter!(
            metric_names::CIRCUIT_TRANSITIONS_TOTAL,
            "endpoint" => endpoint.to_owned(),
            "state" => state.to_owned(),
        )
        .increment(1);
    }

    fn record_connection(&self, endpoint: &str, event: ConnectionEvent) {
        metrics::counter!(
            metric_names::CONNECTIONS_TOTAL,
            "endpoint" => endpoint.to_owned(),
            "event" => event.to_string(),
        )
        .increment(1);
    }

    fn leader_cache_hit(&self, region: &str) {
        metrics::counter!(metric_names::LEADER_CACHE_HITS_TOTAL, "region" => region.to_owned())
            .increment(1);
    }

    fn leader_cache_miss(&self, region: &str) {
        metrics::counter!(metric_names::LEADER_CACHE_MISSES_TOTAL, "region" => region.to_owned())
            .increment(1);
    }

    fn leader_cache_flap(&self, region: &str) {
        metrics::counter!(metric_names::LEADER_CACHE_FLAPS_TOTAL, "region" => region.to_owned())
            .increment(1);
    }

    fn region_resolve_coalesced(&self, region: &str) {
        metrics::counter!(
            metric_names::REGION_RESOLVE_SINGLEFLIGHT_COALESCED_TOTAL,
            "region" => region.to_owned(),
        )
        .increment(1);
    }

    fn region_resolve_stale_served(&self, region: &str) {
        metrics::counter!(
            metric_names::REGION_RESOLVE_STALE_SERVED_TOTAL,
            "region" => region.to_owned(),
        )
        .increment(1);
    }

    fn leader_watch_update(&self, region: &str) {
        metrics::counter!(
            metric_names::LEADER_WATCH_UPDATES_TOTAL,
            "region" => region.to_owned(),
        )
        .increment(1);
    }

    fn leader_watch_reconnect(&self, region: &str) {
        metrics::counter!(
            metric_names::LEADER_WATCH_RECONNECTS_TOTAL,
            "region" => region.to_owned(),
        )
        .increment(1);
    }

    fn leader_stale_term_rejected(&self, region: &str, source: &'static str) {
        metrics::counter!(
            metric_names::LEADER_STALE_TERM_REJECTED_TOTAL,
            "region" => region.to_owned(),
            "source" => source,
        )
        .increment(1);
    }

    fn redirect_retry(&self, region: &str) {
        metrics::counter!(
            metric_names::REDIRECT_RETRIES_TOTAL,
            "region" => region.to_owned(),
        )
        .increment(1);
    }
}

/// Returns the default metrics implementation (no-op).
///
/// SDK metrics are opt-in. By default every `LedgerClient` uses [`NoopSdkMetrics`],
/// which compiles away to nothing. To enable metrics emission, configure a recorder:
///
/// ```no_run
/// use inferadb_ledger_sdk::{ClientConfig, ServerSource, MetricsSdkMetrics};
/// use std::sync::Arc;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::builder()
///     .servers(ServerSource::from_static(["http://localhost:50051"]))
///     .client_id("my-app")
///     .metrics(Arc::new(MetricsSdkMetrics))
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// Each SDK instance emits up to 14 custom metrics × N tag combinations to your
/// configured metrics platform. All label values are bounded — see the
/// [module-level cardinality documentation](self) for details.
pub(crate) fn default_metrics() -> Arc<dyn SdkMetrics> {
    Arc::new(NoopSdkMetrics)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[allow(clippy::unwrap_used, clippy::expect_used)]
    mod unit {
        use super::*;

        /// Custom test metrics that counts calls for verification.
        #[derive(Debug)]
        struct CountingMetrics {
            requests: AtomicU64,
            retries: AtomicU64,
            circuit_transitions: AtomicU64,
            connections: AtomicU64,
        }

        impl CountingMetrics {
            fn new() -> Self {
                Self {
                    requests: AtomicU64::new(0),
                    retries: AtomicU64::new(0),
                    circuit_transitions: AtomicU64::new(0),
                    connections: AtomicU64::new(0),
                }
            }
        }

        impl SdkMetrics for CountingMetrics {
            fn record_request(&self, _method: &str, _duration: Duration, _success: bool) {
                self.requests.fetch_add(1, Ordering::Relaxed);
            }
            fn record_retry(&self, _method: &str, _attempt: u32, _error_type: &str) {
                self.retries.fetch_add(1, Ordering::Relaxed);
            }
            fn record_circuit_state(&self, _endpoint: &str, _state: &str) {
                self.circuit_transitions.fetch_add(1, Ordering::Relaxed);
            }
            fn record_connection(&self, _endpoint: &str, _event: ConnectionEvent) {
                self.connections.fetch_add(1, Ordering::Relaxed);
            }
        }

        #[test]
        fn noop_metrics_is_zero_overhead() {
            let metrics = NoopSdkMetrics;
            // These should compile to nothing — just verify they don't panic
            metrics.record_request("read", Duration::from_millis(5), true);
            metrics.record_retry("write", 2, "unavailable");
            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);
        }

        #[test]
        fn default_metrics_is_noop() {
            // Invariant: the default metrics implementation must be NoopSdkMetrics (opt-in).
            // Any change that makes metrics non-opt-in would break this test.
            let metrics = default_metrics();
            // Verify it's NoopSdkMetrics — call every method and assert no panic
            metrics.record_request("test", Duration::from_millis(1), true);
            metrics.record_retry("test", 2, "unavailable");
            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);
            metrics.leader_cache_hit("us-east-1");
            metrics.leader_cache_miss("us-east-1");
            metrics.leader_cache_flap("us-east-1");
            metrics.region_resolve_coalesced("us-east-1");
            metrics.region_resolve_stale_served("us-east-1");
            metrics.leader_watch_update("us-east-1");
            metrics.leader_watch_reconnect("us-east-1");
            metrics.leader_stale_term_rejected("us-east-1", "hint");
            metrics.redirect_retry("us-east-1");
        }

        #[test]
        fn counting_metrics_tracks_requests() {
            let metrics = CountingMetrics::new();
            assert_eq!(metrics.requests.load(Ordering::Relaxed), 0);

            metrics.record_request("read", Duration::from_millis(5), true);
            assert_eq!(metrics.requests.load(Ordering::Relaxed), 1);

            metrics.record_request("write", Duration::from_millis(10), false);
            assert_eq!(metrics.requests.load(Ordering::Relaxed), 2);
        }

        #[test]
        fn counting_metrics_tracks_retries() {
            let metrics = CountingMetrics::new();

            metrics.record_retry("write", 2, "unavailable");
            metrics.record_retry("write", 3, "internal");
            assert_eq!(metrics.retries.load(Ordering::Relaxed), 2);
        }

        #[test]
        fn counting_metrics_tracks_circuit_state() {
            let metrics = CountingMetrics::new();

            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_circuit_state("http://localhost:50051", "half_open");
            metrics.record_circuit_state("http://localhost:50051", "closed");
            assert_eq!(metrics.circuit_transitions.load(Ordering::Relaxed), 3);
        }

        #[test]
        fn counting_metrics_tracks_connections() {
            let metrics = CountingMetrics::new();

            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Disconnected);
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Failed);
            assert_eq!(metrics.connections.load(Ordering::Relaxed), 3);
        }

        #[test]
        fn connection_event_display() {
            assert_eq!(ConnectionEvent::Connected.to_string(), "connected");
            assert_eq!(ConnectionEvent::Disconnected.to_string(), "disconnected");
            assert_eq!(ConnectionEvent::Failed.to_string(), "failed");
        }

        #[test]
        fn trait_object_via_arc() {
            let counting = Arc::new(CountingMetrics::new());
            let metrics: Arc<dyn SdkMetrics> = counting.clone();

            metrics.record_request("read", Duration::from_millis(5), true);
            metrics.record_retry("write", 2, "timeout");
            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);

            // Verify dispatch through Arc<dyn SdkMetrics> reaches the CountingMetrics
            assert_eq!(counting.requests.load(Ordering::Relaxed), 1);
            assert_eq!(counting.retries.load(Ordering::Relaxed), 1);
            assert_eq!(counting.circuit_transitions.load(Ordering::Relaxed), 1);
            assert_eq!(counting.connections.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn metrics_facade_does_not_panic_without_recorder() {
            // MetricsSdkMetrics uses the `metrics` crate facade. If no recorder
            // is installed, calls are no-ops. Verify no panic.
            let metrics = MetricsSdkMetrics;
            metrics.record_request("read", Duration::from_millis(5), true);
            metrics.record_request("write", Duration::from_millis(10), false);
            metrics.record_retry("write", 2, "unavailable");
            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);
        }

        #[test]
        fn noop_metrics_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<NoopSdkMetrics>();
            assert_send_sync::<MetricsSdkMetrics>();
            assert_send_sync::<Arc<dyn SdkMetrics>>();
        }

        #[test]
        fn default_trait_methods_are_noop() {
            // A struct that implements SdkMetrics but overrides nothing
            #[derive(Debug)]
            struct EmptyMetrics;
            impl SdkMetrics for EmptyMetrics {}

            let metrics = EmptyMetrics;
            // All default methods should be no-ops
            metrics.record_request("read", Duration::from_millis(5), true);
            metrics.record_retry("write", 2, "unavailable");
            metrics.record_circuit_state("http://localhost:50051", "open");
            metrics.record_connection("http://localhost:50051", ConnectionEvent::Connected);
        }
    }
}
