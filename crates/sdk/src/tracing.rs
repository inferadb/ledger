//! Distributed tracing configuration for the SDK.
//!
//! [`TraceConfig`] is a public configuration toggle that enables W3C Trace
//! Context propagation when constructing a [`ClientConfig`](crate::ClientConfig).
//! The wire transport handles trace-context injection internally; this type
//! is the consumer-facing knob.

/// Configuration for distributed tracing in the SDK.
///
/// By default, tracing is disabled. When enabled, the SDK propagates W3C
/// Trace Context headers (`traceparent`, `tracestate`) to the server via the
/// underlying transport so distributed traces span the SDK→server boundary.
#[derive(Debug, Clone, Default)]
pub struct TraceConfig {
    /// Whether trace context propagation is enabled.
    enabled: bool,
}

impl TraceConfig {
    /// Creates a new `TraceConfig` with tracing enabled.
    ///
    /// When enabled, the SDK will:
    /// 1. Extract trace context from the current OpenTelemetry span (if present)
    /// 2. Generate a new trace context if none exists
    /// 3. Inject the `traceparent` header into all outgoing requests
    #[must_use]
    pub fn enabled() -> Self {
        Self { enabled: true }
    }

    /// Checks if trace context propagation is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
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
}
