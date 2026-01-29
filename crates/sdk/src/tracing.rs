//! Distributed tracing support via W3C Trace Context propagation.
//!
//! This module provides automatic injection of trace context into outgoing gRPC requests,
//! enabling end-to-end distributed tracing when OpenTelemetry is configured.
//!
//! # How It Works
//!
//! When tracing is enabled, the SDK extracts trace context from the current tracing span
//! (if one exists with OpenTelemetry context) and injects it as W3C `traceparent` headers
//! into all outgoing requests.
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_sdk::{ClientConfig, LedgerClient, TraceConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::builder()
//!     .endpoints(vec!["http://localhost:50051".into()])
//!     .client_id("my-service")
//!     .trace(TraceConfig::enabled())
//!     .build()?;
//!
//! let client = LedgerClient::new(config).await?;
//!
//! // When called within an instrumented span, trace context is automatically propagated
//! tracing::info_span!("my_operation").in_scope(|| async {
//!     client.read(1, None, "key").await
//! }).await?;
//! # Ok(())
//! # }
//! ```

use inferadb_ledger_raft::trace_context::{TraceContext, inject_into_metadata};
use opentelemetry::trace::TraceContextExt;
use tonic::service::Interceptor;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Configuration for distributed tracing in the SDK.
///
/// By default, tracing is disabled. When enabled, the SDK automatically propagates
/// W3C Trace Context headers (`traceparent`, `tracestate`) to the server.
#[derive(Debug, Clone, Default)]
pub struct TraceConfig {
    /// Whether trace context propagation is enabled.
    enabled: bool,
}

impl TraceConfig {
    /// Create a new `TraceConfig` with tracing enabled.
    ///
    /// When enabled, the SDK will:
    /// 1. Extract trace context from the current OpenTelemetry span (if present)
    /// 2. Generate a new trace context if none exists
    /// 3. Inject the `traceparent` header into all outgoing gRPC requests
    #[must_use]
    pub fn enabled() -> Self {
        Self { enabled: true }
    }

    /// Check if trace context propagation is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Tonic interceptor that injects W3C Trace Context into outgoing requests.
///
/// This interceptor is applied to all gRPC clients when tracing is enabled.
/// It extracts trace context from the current span (using `tracing-opentelemetry`)
/// and injects it as metadata headers.
#[derive(Debug, Clone)]
pub struct TraceContextInterceptor {
    enabled: bool,
}

impl TraceContextInterceptor {
    /// Create a new interceptor from trace configuration.
    pub fn new(config: &TraceConfig) -> Self {
        Self { enabled: config.enabled }
    }
}

impl Interceptor for TraceContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if !self.enabled {
            return Ok(request);
        }

        let trace_ctx = extract_from_current_span().unwrap_or_default();
        inject_into_metadata(request.metadata_mut(), &trace_ctx);

        Ok(request)
    }
}

/// Extract trace context from the current tracing span, if OpenTelemetry context is present.
///
/// Returns `None` if:
/// - No current span exists
/// - The span doesn't have OpenTelemetry context attached
/// - The trace ID is invalid (all zeros)
fn extract_from_current_span() -> Option<TraceContext> {
    let current_span = tracing::Span::current();
    let otel_context = current_span.context();
    let span_ref = otel_context.span();
    let span_context = span_ref.span_context();

    // Check if we have a valid trace context
    if !span_context.is_valid() {
        return None;
    }

    Some(TraceContext {
        trace_id: span_context.trace_id().to_string(),
        span_id: span_context.span_id().to_string(),
        parent_span_id: None, // The current span IS the parent for the outgoing request
        trace_flags: span_context.trace_flags().to_u8(),
        trace_state: {
            let header = span_context.trace_state().header();
            if header.is_empty() { None } else { Some(header) }
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_config_default_disabled() {
        let config = TraceConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_trace_config_enabled() {
        let config = TraceConfig::enabled();
        assert!(config.is_enabled());
    }

    #[test]
    fn test_interceptor_noop_when_disabled() {
        let config = TraceConfig::default();
        let mut interceptor = TraceContextInterceptor::new(&config);

        let request = tonic::Request::new(());
        let result = interceptor.call(request).expect("should succeed");

        // No traceparent header should be added when disabled
        assert!(result.metadata().get("traceparent").is_none());
    }

    #[test]
    fn test_interceptor_injects_traceparent_when_enabled() {
        let config = TraceConfig::enabled();
        let mut interceptor = TraceContextInterceptor::new(&config);

        let request = tonic::Request::new(());
        let result = interceptor.call(request).expect("should succeed");

        // Should inject traceparent header (with a new trace since no span context)
        let traceparent =
            result.metadata().get("traceparent").expect("traceparent should be present");
        let value = traceparent.to_str().expect("should be valid string");

        // W3C traceparent format: version-traceid-spanid-flags
        let parts: Vec<&str> = value.split('-').collect();
        assert_eq!(parts.len(), 4, "traceparent should have 4 parts");
        assert_eq!(parts[0], "00", "version should be 00");
        assert_eq!(parts[1].len(), 32, "trace_id should be 32 hex chars");
        assert_eq!(parts[2].len(), 16, "span_id should be 16 hex chars");
        assert_eq!(parts[3].len(), 2, "flags should be 2 hex chars");
    }

    #[test]
    fn test_new_trace_without_parent() {
        // Without any OpenTelemetry context, extract_from_current_span returns None
        let result = extract_from_current_span();
        assert!(result.is_none());
    }

    #[test]
    fn test_trace_propagation_format() {
        // Verify TraceContext::new() generates valid W3C format
        let ctx = TraceContext::new();
        let traceparent = ctx.to_traceparent();

        // Should match: 00-{trace_id}-{span_id}-{flags}
        let parts: Vec<&str> = traceparent.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00"); // version
        assert_eq!(parts[1].len(), 32); // trace_id (16 bytes = 32 hex)
        assert_eq!(parts[2].len(), 16); // span_id (8 bytes = 16 hex)
        assert!(parts[3] == "01" || parts[3] == "00", "flags should be 00 or 01");
    }
}
