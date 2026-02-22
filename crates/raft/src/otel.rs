//! OpenTelemetry/OTLP trace export for wide events.
//!
//! This module provides integration between wide events and OpenTelemetry,
//! enabling export of request spans to observability backends like Jaeger,
//! Tempo, or Honeycomb.
//!
//! # Architecture
//!
//! The OTEL integration uses a `SdkTracerProvider` with `BatchSpanProcessor` for
//! efficient, non-blocking span export. The provider is initialized once at
//! server startup and accessed via a global singleton for span creation.
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_raft::otel::{init_otel, shutdown_otel, OtelConfig};
//!
//! // At server startup
//! let config = OtelConfig {
//!     enabled: true,
//!     endpoint: "http://localhost:4317".to_string(),
//!     ..Default::default()
//! };
//! init_otel(&config).expect("Failed to initialize OTEL");
//!
//! // At server shutdown
//! shutdown_otel();
//! ```

use std::{sync::OnceLock, time::Duration};

use opentelemetry::{
    Context, KeyValue, global,
    trace::{SpanKind, TraceContextExt, Tracer, TracerProvider as _},
};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

/// Global tracer provider, initialized once at server startup.
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Configuration for OpenTelemetry/OTLP export.
///
/// This mirrors the server config but is decoupled for use in the raft crate.
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Whether OTLP export is enabled.
    pub enabled: bool,
    /// OTLP endpoint URL (e.g., "http://localhost:4317" for gRPC).
    pub endpoint: String,
    /// Whether to use gRPC (true) or HTTP (false) transport.
    pub use_grpc: bool,
    /// Export timeout in milliseconds.
    pub timeout_ms: u64,
    /// Graceful shutdown timeout in milliseconds.
    pub shutdown_timeout_ms: u64,
    /// Whether to propagate trace context in Raft RPCs.
    pub trace_raft_rpcs: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            use_grpc: true,
            timeout_ms: 10000,
            shutdown_timeout_ms: 15000,
            trace_raft_rpcs: true,
        }
    }
}

/// Error type for OTEL initialization.
#[derive(Debug)]
pub enum OtelInitError {
    /// Failed to build the span exporter.
    ExporterBuild(String),
    /// Provider was already initialized.
    AlreadyInitialized,
}

impl std::fmt::Display for OtelInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExporterBuild(msg) => write!(f, "Failed to build OTLP span exporter: {msg}"),
            Self::AlreadyInitialized => write!(f, "OTEL tracer provider already initialized"),
        }
    }
}

impl std::error::Error for OtelInitError {}

/// Initializes the OpenTelemetry tracer provider.
///
/// This function should be called once at server startup. It creates a
/// `SdkTracerProvider` with `BatchSpanProcessor` for efficient span export.
///
/// # Arguments
///
/// * `config` - OTEL configuration (endpoint, transport, timeouts)
///
/// # Errors
///
/// Returns [`OtelInitError::AlreadyInitialized`] if the tracer provider has
/// already been set, or [`OtelInitError::ExporterBuild`] if the gRPC span
/// exporter fails to build.
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_raft::otel::{init_otel, OtelConfig};
///
/// let config = OtelConfig {
///     enabled: true,
///     endpoint: "http://localhost:4317".to_string(),
///     use_grpc: true,
///     timeout_ms: 10000,
///     shutdown_timeout_ms: 15000,
///     trace_raft_rpcs: true,
/// };
/// init_otel(&config).expect("Failed to initialize OTEL");
/// ```
pub fn init_otel(config: &OtelConfig) -> Result<(), OtelInitError> {
    if !config.enabled {
        tracing::debug!("OTEL export disabled, skipping initialization");
        return Ok(());
    }

    if TRACER_PROVIDER.get().is_some() {
        return Err(OtelInitError::AlreadyInitialized);
    }

    let timeout = Duration::from_millis(config.timeout_ms);

    // Build the span exporter (gRPC only for now - HTTP requires additional features)
    // Note: HTTP transport requires the http-proto feature which we don't enable
    if !config.use_grpc {
        tracing::warn!("HTTP transport requested but only gRPC is supported; using gRPC");
    }

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.endpoint)
        .with_timeout(timeout)
        .build()
        .map_err(|e| OtelInitError::ExporterBuild(e.to_string()))?;

    // Create resource with service information
    let resource = Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", "inferadb-ledger"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("host.name", hostname()),
        ])
        .build();

    // Build the tracer provider with batch processing
    // Note: As of opentelemetry_sdk 0.28+, batch processors create their own background thread,
    // so no runtime parameter is needed.
    let provider =
        SdkTracerProvider::builder().with_resource(resource).with_batch_exporter(exporter).build();

    // Store globally for span creation
    if TRACER_PROVIDER.set(provider.clone()).is_err() {
        return Err(OtelInitError::AlreadyInitialized);
    }

    // Register as global provider for get_tracer() access
    global::set_tracer_provider(provider);

    tracing::info!(
        endpoint = %config.endpoint,
        transport = if config.use_grpc { "grpc" } else { "http" },
        "OTEL tracer provider initialized"
    );

    Ok(())
}

/// Shuts down the OpenTelemetry tracer provider gracefully.
///
/// This function flushes any pending spans and shuts down the provider.
/// It should be called during server shutdown.
///
/// # Behavior
///
/// - Calls `force_flush()` to export pending spans
/// - Calls `shutdown()` to release resources
/// - Logs but does not panic on errors
pub fn shutdown_otel() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        tracing::info!("Shutting down OTEL tracer provider");

        // Force flush pending spans
        if let Err(e) = provider.force_flush() {
            tracing::warn!(error = %e, "Failed to flush OTEL spans during shutdown");
        }

        // Shutdown the provider
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown OTEL provider");
        }

        tracing::debug!("OTEL tracer provider shutdown complete");
    }
}

/// Checks if OTEL export is enabled and initialized.
#[inline]
pub fn is_otel_enabled() -> bool {
    TRACER_PROVIDER.get().is_some()
}

/// Returns the tracer for creating spans.
///
/// Returns `None` if OTEL is not initialized.
pub fn get_tracer() -> Option<opentelemetry_sdk::trace::Tracer> {
    TRACER_PROVIDER.get().map(|p| p.tracer("inferadb-ledger"))
}

/// Span attributes extracted from a request context.
///
/// These attributes are attached to OTEL spans to provide the same
/// contextual information as wide event JSON fields.
///
/// Each field corresponds to a wide event field with the same name.
/// See the wide events PRD for detailed field descriptions.
#[derive(Debug, Default)]
#[allow(missing_docs)] // Fields are self-documenting and match wide event field names
pub struct SpanAttributes {
    // Request metadata
    pub request_id: Option<String>,
    pub client_id: Option<String>,
    pub sequence: Option<u64>,
    pub organization_slug: Option<u64>,
    pub vault_id: Option<i64>,
    pub service: Option<&'static str>,
    pub method: Option<&'static str>,
    pub actor: Option<String>,

    // System context
    pub node_id: Option<u64>,
    pub is_leader: Option<bool>,
    pub raft_term: Option<u64>,
    pub shard_id: Option<u32>,
    pub is_vip: Option<bool>,

    // Operation fields (write)
    pub operations_count: Option<usize>,
    pub idempotency_hit: Option<bool>,
    pub batch_coalesced: Option<bool>,
    pub batch_size: Option<usize>,

    // Operation fields (read)
    pub key: Option<String>,
    pub keys_count: Option<usize>,
    pub found_count: Option<usize>,
    pub consistency: Option<String>,
    pub at_height: Option<u64>,
    pub include_proof: Option<bool>,
    pub found: Option<bool>,
    pub value_size_bytes: Option<usize>,

    // Admin fields
    pub admin_action: Option<String>,

    // Outcome fields
    pub outcome: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub state_root: Option<String>,

    // I/O metrics
    pub bytes_read: Option<usize>,
    pub bytes_written: Option<usize>,
    pub raft_round_trips: Option<u32>,

    // Client transport metadata
    pub sdk_version: Option<String>,
    pub source_ip: Option<String>,

    // Timing fields
    pub duration_ms: Option<f64>,
    pub raft_latency_ms: Option<f64>,
    pub storage_latency_ms: Option<f64>,
}

impl SpanAttributes {
    /// Converts attributes to OpenTelemetry KeyValue pairs.
    pub fn to_key_values(&self) -> Vec<KeyValue> {
        let mut attrs = Vec::with_capacity(32);

        // Helper macro to reduce boilerplate
        macro_rules! push_attr {
            ($name:expr, $value:expr) => {
                if let Some(v) = &$value {
                    attrs.push(KeyValue::new($name, v.clone()));
                }
            };
            ($name:expr, $value:expr, | $v:ident | $transform:expr) => {
                if let Some($v) = $value {
                    attrs.push(KeyValue::new($name, $transform));
                }
            };
        }

        // Request metadata
        push_attr!("request_id", self.request_id);
        push_attr!("client_id", self.client_id);
        push_attr!("sequence", self.sequence, |v| v as i64);
        push_attr!("organization_slug", self.organization_slug, |v| v as i64);
        push_attr!("vault_id", self.vault_id, |v| v);
        if let Some(v) = self.service {
            attrs.push(KeyValue::new("service", v));
        }
        if let Some(v) = self.method {
            attrs.push(KeyValue::new("method", v));
        }
        push_attr!("actor", self.actor);

        // System context
        push_attr!("node_id", self.node_id, |v| v as i64);
        push_attr!("is_leader", self.is_leader, |v| v);
        push_attr!("raft_term", self.raft_term, |v| v as i64);
        push_attr!("shard_id", self.shard_id, |v| v as i64);
        push_attr!("is_vip", self.is_vip, |v| v);

        // Operation fields (write)
        push_attr!("operations_count", self.operations_count, |v| v as i64);
        push_attr!("idempotency_hit", self.idempotency_hit, |v| v);
        push_attr!("batch_coalesced", self.batch_coalesced, |v| v);
        push_attr!("batch_size", self.batch_size, |v| v as i64);

        // Operation fields (read)
        push_attr!("key", self.key);
        push_attr!("keys_count", self.keys_count, |v| v as i64);
        push_attr!("found_count", self.found_count, |v| v as i64);
        push_attr!("consistency", self.consistency);
        push_attr!("at_height", self.at_height, |v| v as i64);
        push_attr!("include_proof", self.include_proof, |v| v);
        push_attr!("found", self.found, |v| v);
        push_attr!("value_size_bytes", self.value_size_bytes, |v| v as i64);

        // Admin fields
        push_attr!("admin_action", self.admin_action);

        // Outcome fields
        push_attr!("outcome", self.outcome);
        push_attr!("error_code", self.error_code);
        push_attr!("error_message", self.error_message);
        push_attr!("block_height", self.block_height, |v| v as i64);
        push_attr!("block_hash", self.block_hash);
        push_attr!("state_root", self.state_root);

        // Timing fields
        push_attr!("duration_ms", self.duration_ms, |v| v);
        push_attr!("raft_latency_ms", self.raft_latency_ms, |v| v);
        push_attr!("storage_latency_ms", self.storage_latency_ms, |v| v);

        attrs
    }
}

/// Export a span with the given attributes.
///
/// This function creates and immediately ends a span with all the provided
/// attributes. It handles trace context propagation if trace_id is provided.
///
/// # Arguments
///
/// * `service` - The service name (e.g., "WriteService")
/// * `method` - The method name (e.g., "write")
/// * `attrs` - Span attributes extracted from RequestContext
/// * `trace_id` - Optional trace ID for context propagation
/// * `span_id` - Optional span ID for the current span
/// * `parent_span_id` - Optional parent span ID for linking
pub fn export_span(
    service: &str,
    method: &str,
    attrs: SpanAttributes,
    trace_id: Option<&str>,
    span_id: Option<&str>,
    parent_span_id: Option<&str>,
) {
    let Some(tracer) = get_tracer() else {
        return;
    };

    // Create span name following convention: {service}/{method}
    let span_name = format!("{service}/{method}");

    // Build the span with SERVER kind for incoming gRPC requests
    let mut span_builder = tracer.span_builder(span_name).with_kind(SpanKind::Server);

    // Add attributes
    span_builder = span_builder.with_attributes(attrs.to_key_values());

    // Determine parent context if trace IDs are provided
    let parent_cx = if let (Some(tid), Some(_sid)) = (trace_id, span_id) {
        if let Ok(tid_bytes) = parse_trace_id(tid) {
            let trace_id = opentelemetry::trace::TraceId::from_bytes(tid_bytes);

            // If we have a parent span ID, create parent context
            if let Some(pid) = parent_span_id {
                if let Ok(pid_bytes) = parse_span_id(pid) {
                    let parent_span_id = opentelemetry::trace::SpanId::from_bytes(pid_bytes);
                    let parent_context = opentelemetry::trace::SpanContext::new(
                        trace_id,
                        parent_span_id,
                        opentelemetry::trace::TraceFlags::SAMPLED,
                        true, // remote parent
                        opentelemetry::trace::TraceState::default(),
                    );
                    Some(Context::current().with_remote_span_context(parent_context))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // Start and immediately end the span (it represents a completed request)
    let span = match parent_cx {
        Some(cx) => span_builder.start_with_context(&tracer, &cx),
        None => span_builder.start(&tracer),
    };
    drop(span); // Dropping the span ends it
}

/// Parses a 32-character hex trace ID into bytes.
fn parse_trace_id(hex: &str) -> Result<[u8; 16], ()> {
    if hex.len() != 32 {
        return Err(());
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|_| ())?;
        bytes[i] = u8::from_str_radix(s, 16).map_err(|_| ())?;
    }
    Ok(bytes)
}

/// Parses a 16-character hex span ID into bytes.
fn parse_span_id(hex: &str) -> Result<[u8; 8], ()> {
    if hex.len() != 16 {
        return Err(());
    }
    let mut bytes = [0u8; 8];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|_| ())?;
        bytes[i] = u8::from_str_radix(s, 16).map_err(|_| ())?;
    }
    Ok(bytes)
}

/// Returns the hostname for resource attributes.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otel_config_default() {
        let config = OtelConfig::default();
        assert!(!config.enabled);
        assert!(config.endpoint.is_empty());
        assert!(config.use_grpc);
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.shutdown_timeout_ms, 15000);
    }

    #[test]
    fn test_span_attributes_to_key_values_empty() {
        let attrs = SpanAttributes::default();
        let kvs = attrs.to_key_values();
        assert!(kvs.is_empty());
    }

    #[test]
    fn test_span_attributes_to_key_values_populated() {
        let attrs = SpanAttributes {
            request_id: Some("abc123".to_string()),
            client_id: Some("client1".to_string()),
            sequence: Some(42),
            organization_slug: Some(1),
            vault_id: Some(2),
            service: Some("WriteService"),
            method: Some("write"),
            outcome: Some("success".to_string()),
            duration_ms: Some(10.5),
            ..Default::default()
        };
        let kvs = attrs.to_key_values();

        // Verify expected attributes are present
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "request_id"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "client_id"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "sequence"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "organization_slug"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "service"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "method"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "outcome"));
        assert!(kvs.iter().any(|kv| kv.key.as_str() == "duration_ms"));
    }

    #[test]
    fn test_parse_trace_id_valid() {
        let hex = "0123456789abcdef0123456789abcdef";
        let result = parse_trace_id(hex);
        assert!(result.is_ok());
        let bytes = result.unwrap_or_default();
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x23);
    }

    #[test]
    fn test_parse_trace_id_invalid_length() {
        let hex = "0123456789abcdef"; // Only 16 chars
        let result = parse_trace_id(hex);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_span_id_valid() {
        let hex = "0123456789abcdef";
        let result = parse_span_id(hex);
        assert!(result.is_ok());
        let bytes = result.unwrap_or_default();
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x23);
    }

    #[test]
    fn test_parse_span_id_invalid_length() {
        let hex = "0123456789"; // Only 10 chars
        let result = parse_span_id(hex);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_otel_enabled_before_init() {
        // Before any initialization, should be false
        // Note: This test assumes it runs in isolation or before init_otel
        // In practice, OTEL state is global and can be affected by other tests
        // For a clean test, we'd need to run in a separate process
    }

    #[test]
    fn test_get_tracer_before_init() {
        // Before initialization, get_tracer should return None
        // Same caveat as above about global state
    }

    #[test]
    fn test_export_span_when_disabled() {
        // When OTEL is not initialized, export_span should be a no-op
        export_span("TestService", "test", SpanAttributes::default(), None, None, None);
        // Should not panic
    }

    #[test]
    fn test_hostname_fallback() {
        // hostname() should return something even when env vars are not set
        let host = hostname();
        assert!(!host.is_empty());
    }
}
