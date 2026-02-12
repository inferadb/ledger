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
//!
//! # Example
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
    /// - `method`: The RPC method name (e.g., "read", "write", "batch_write").
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
    /// - `state`: The new circuit state ("closed", "open", "half_open").
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
}

/// Creates the default metrics instance (no-op).
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
        fn noop_is_default() {
            let metrics = default_metrics();
            // Should not panic
            metrics.record_request("read", Duration::from_millis(1), true);
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
