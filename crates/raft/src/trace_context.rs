//! W3C Trace Context propagation for distributed tracing.
//!
//! This module implements W3C Trace Context extraction from incoming gRPC requests
//! and injection into outgoing Raft RPCs. It follows the W3C Trace Context
//! specification: <https://www.w3.org/TR/trace-context/>
//!
//! ## Header Format
//!
//! The `traceparent` header format is:
//! `{version}-{trace_id}-{parent_id}-{trace_flags}`
//!
//! Example: `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`
//!
//! - version: 2 hex chars (always "00" for current spec)
//! - trace_id: 32 hex chars (16 bytes)
//! - parent_id: 16 hex chars (8 bytes, also called span_id)
//! - trace_flags: 2 hex chars (1 byte, bit 0 = sampled)

use std::fmt;

use tonic::metadata::MetadataMap;
use uuid::Uuid;

/// W3C Trace Context header name for trace propagation.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// W3C Trace Context header name for vendor-specific trace state.
pub const TRACESTATE_HEADER: &str = "tracestate";

/// Trace flags bit indicating the trace is sampled.
pub const TRACE_FLAG_SAMPLED: u8 = 0x01;

/// Extracted trace context from an incoming request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C trace ID (32 hex chars, 16 bytes).
    pub trace_id: String,
    /// Span ID for this request (16 hex chars, 8 bytes).
    pub span_id: String,
    /// Parent span ID from incoming context, if present (16 hex chars, 8 bytes).
    pub parent_span_id: Option<String>,
    /// W3C trace flags (bit 0 = sampled).
    pub trace_flags: u8,
    /// Optional vendor-specific trace state.
    pub trace_state: Option<String>,
}

impl TraceContext {
    /// Generate a new trace context with a random trace ID and span ID.
    ///
    /// Used when no incoming trace context exists.
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            trace_flags: TRACE_FLAG_SAMPLED,
            trace_state: None,
        }
    }

    /// Create a child span context, inheriting the trace ID and using the
    /// current span ID as the parent.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
        }
    }

    /// Check if the sampled flag is set.
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & TRACE_FLAG_SAMPLED != 0
    }

    /// Format as W3C traceparent header value.
    pub fn to_traceparent(&self) -> String {
        format!("00-{}-{}-{:02x}", self.trace_id, self.span_id, self.trace_flags)
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_traceparent())
    }
}

/// Generate a random 32-character hex trace ID (16 bytes).
fn generate_trace_id() -> String {
    // Use two UUIDs concatenated for 32 hex chars
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    hex_encode(bytes)
}

/// Generate a random 16-character hex span ID (8 bytes).
fn generate_span_id() -> String {
    let uuid = Uuid::new_v4();
    let bytes = &uuid.as_bytes()[..8];
    hex_encode(bytes)
}

/// Encode bytes as lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse error for trace context extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Header value is not valid ASCII.
    InvalidAscii,
    /// traceparent format is invalid.
    InvalidFormat,
    /// Version byte is unsupported.
    UnsupportedVersion,
    /// Trace ID is all zeros (invalid per spec).
    InvalidTraceId,
    /// Parent ID is all zeros (invalid per spec).
    InvalidParentId,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidAscii => write!(f, "traceparent header contains non-ASCII"),
            ParseError::InvalidFormat => write!(f, "traceparent header has invalid format"),
            ParseError::UnsupportedVersion => write!(f, "traceparent version is unsupported"),
            ParseError::InvalidTraceId => write!(f, "trace_id is all zeros"),
            ParseError::InvalidParentId => write!(f, "parent_id is all zeros"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Extract trace context from gRPC metadata.
///
/// If the `traceparent` header is present and valid, extracts the trace context.
/// Returns `None` if the header is missing.
/// Returns `Err` if the header is present but malformed.
pub fn extract_from_metadata(metadata: &MetadataMap) -> Result<Option<TraceContext>, ParseError> {
    let traceparent = match metadata.get(TRACEPARENT_HEADER) {
        Some(value) => value.to_str().map_err(|_| ParseError::InvalidAscii)?,
        None => return Ok(None),
    };

    let trace_state =
        metadata.get(TRACESTATE_HEADER).and_then(|v| v.to_str().ok()).map(String::from);

    parse_traceparent(traceparent, trace_state).map(Some)
}

/// Parse a W3C traceparent header value.
///
/// Format: `{version}-{trace_id}-{parent_id}-{trace_flags}`
fn parse_traceparent(
    traceparent: &str,
    trace_state: Option<String>,
) -> Result<TraceContext, ParseError> {
    // Split by dash - expecting exactly 4 parts for version 00
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() < 4 {
        return Err(ParseError::InvalidFormat);
    }

    // Version check (must be "00" for current spec, but we accept any version
    // and just use the first 4 fields)
    let version = parts[0];
    if version.len() != 2 {
        return Err(ParseError::InvalidFormat);
    }

    // Validate version is a valid hex byte
    u8::from_str_radix(version, 16).map_err(|_| ParseError::InvalidFormat)?;

    // Version 00 should have exactly 4 parts
    // Future versions might have more, which we ignore per spec
    if version == "00" && parts.len() != 4 {
        return Err(ParseError::InvalidFormat);
    }

    // Extract trace_id (32 hex chars)
    let trace_id = parts[1];
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ParseError::InvalidFormat);
    }
    // All zeros is invalid
    if trace_id.chars().all(|c| c == '0') {
        return Err(ParseError::InvalidTraceId);
    }

    // Extract parent_id (16 hex chars)
    let parent_id = parts[2];
    if parent_id.len() != 16 || !parent_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ParseError::InvalidFormat);
    }
    // All zeros is invalid
    if parent_id.chars().all(|c| c == '0') {
        return Err(ParseError::InvalidParentId);
    }

    // Extract trace_flags (2 hex chars)
    let flags_str = parts[3];
    if flags_str.len() != 2 {
        return Err(ParseError::InvalidFormat);
    }
    let trace_flags = u8::from_str_radix(flags_str, 16).map_err(|_| ParseError::InvalidFormat)?;

    Ok(TraceContext {
        trace_id: trace_id.to_lowercase(),
        span_id: generate_span_id(), // New span for this request
        parent_span_id: Some(parent_id.to_lowercase()),
        trace_flags,
        trace_state,
    })
}

/// Inject trace context into gRPC metadata.
///
/// Adds the `traceparent` header (and `tracestate` if present) to the metadata.
pub fn inject_into_metadata(metadata: &mut MetadataMap, context: &TraceContext) {
    // traceparent header
    let traceparent = context.to_traceparent();
    if let Ok(value) = traceparent.parse() {
        metadata.insert(TRACEPARENT_HEADER, value);
    }

    // tracestate header (optional)
    if let Some(ref state) = context.trace_state
        && let Ok(value) = state.parse()
    {
        metadata.insert(TRACESTATE_HEADER, value);
    }
}

/// Extract or generate trace context from gRPC metadata.
///
/// If valid trace context exists, returns it. If the header is missing or
/// malformed, generates a new trace context. Logs a warning for malformed
/// headers.
pub fn extract_or_generate(metadata: &MetadataMap) -> TraceContext {
    match extract_from_metadata(metadata) {
        Ok(Some(ctx)) => ctx,
        Ok(None) => TraceContext::new(),
        Err(e) => {
            tracing::warn!("Malformed traceparent header: {}, generating new trace context", e);
            TraceContext::new()
        },
    }
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataValue;

    use super::*;

    #[test]
    fn test_generate_trace_id_length() {
        let trace_id = generate_trace_id();
        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_span_id_length() {
        let span_id = generate_span_id();
        assert_eq!(span_id.len(), 16);
        assert!(span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_trace_context_new() {
        let ctx = TraceContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.parent_span_id.is_none());
        assert_eq!(ctx.trace_flags, TRACE_FLAG_SAMPLED);
        assert!(ctx.trace_state.is_none());
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.child();

        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
        assert_eq!(child.trace_flags, parent.trace_flags);
    }

    #[test]
    fn test_trace_context_is_sampled() {
        let mut ctx = TraceContext::new();
        assert!(ctx.is_sampled());

        ctx.trace_flags = 0x00;
        assert!(!ctx.is_sampled());

        ctx.trace_flags = 0x01;
        assert!(ctx.is_sampled());

        // Other bits don't affect sampled
        ctx.trace_flags = 0x03;
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_trace_context_to_traceparent() {
        let ctx = TraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            parent_span_id: None,
            trace_flags: 0x01,
            trace_state: None,
        };

        let traceparent = ctx.to_traceparent();
        assert_eq!(traceparent, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    #[test]
    fn test_parse_traceparent_valid() {
        let result =
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", None);

        assert!(result.is_ok());
        let ctx = result.unwrap_or_else(|_| TraceContext::new());
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_span_id, Some("b7ad6b7169203331".to_string()));
        assert_eq!(ctx.trace_flags, 0x01);
        // span_id is newly generated
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[test]
    fn test_parse_traceparent_with_tracestate() {
        let result = parse_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            Some("congo=t61rcWkgMzE".to_string()),
        );

        assert!(result.is_ok());
        let ctx = result.unwrap_or_else(|_| TraceContext::new());
        assert_eq!(ctx.trace_state, Some("congo=t61rcWkgMzE".to_string()));
    }

    #[test]
    fn test_parse_traceparent_invalid_format() {
        // Too few parts
        assert!(matches!(
            parse_traceparent("00-trace-parent", None),
            Err(ParseError::InvalidFormat)
        ));

        // Invalid trace_id length
        assert!(matches!(
            parse_traceparent("00-0af7651916cd43dd-b7ad6b7169203331-01", None),
            Err(ParseError::InvalidFormat)
        ));

        // Invalid parent_id length
        assert!(matches!(
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b71-01", None),
            Err(ParseError::InvalidFormat)
        ));

        // Invalid trace_flags length
        assert!(matches!(
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1", None),
            Err(ParseError::InvalidFormat)
        ));
    }

    #[test]
    fn test_parse_traceparent_all_zeros_trace_id() {
        assert!(matches!(
            parse_traceparent("00-00000000000000000000000000000000-b7ad6b7169203331-01", None),
            Err(ParseError::InvalidTraceId)
        ));
    }

    #[test]
    fn test_parse_traceparent_all_zeros_parent_id() {
        assert!(matches!(
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", None),
            Err(ParseError::InvalidParentId)
        ));
    }

    #[test]
    fn test_extract_from_metadata_missing() {
        let metadata = MetadataMap::new();
        let result = extract_from_metadata(&metadata);
        assert!(result.is_ok());
        assert!(result.unwrap_or_default().is_none());
    }

    #[test]
    fn test_extract_from_metadata_valid() {
        let mut metadata = MetadataMap::new();
        let header_value =
            MetadataValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        metadata.insert(TRACEPARENT_HEADER, header_value);

        let result = extract_from_metadata(&metadata);
        assert!(result.is_ok());
        let opt = result.unwrap_or_default();
        assert!(opt.is_some());
        let ctx = opt.unwrap_or_default();
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
    }

    #[test]
    fn test_inject_into_metadata() {
        let ctx = TraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            parent_span_id: None,
            trace_flags: 0x01,
            trace_state: Some("congo=t61rcWkgMzE".to_string()),
        };

        let mut metadata = MetadataMap::new();
        inject_into_metadata(&mut metadata, &ctx);

        assert_eq!(
            metadata.get(TRACEPARENT_HEADER).and_then(|v| v.to_str().ok()),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(
            metadata.get(TRACESTATE_HEADER).and_then(|v| v.to_str().ok()),
            Some("congo=t61rcWkgMzE")
        );
    }

    #[test]
    fn test_extract_or_generate_with_valid_header() {
        let mut metadata = MetadataMap::new();
        let header_value =
            MetadataValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        metadata.insert(TRACEPARENT_HEADER, header_value);

        let ctx = extract_or_generate(&metadata);
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
    }

    #[test]
    fn test_extract_or_generate_without_header() {
        let metadata = MetadataMap::new();
        let ctx = extract_or_generate(&metadata);

        // Should generate new context
        assert_eq!(ctx.trace_id.len(), 32);
        assert!(ctx.parent_span_id.is_none());
    }

    #[test]
    fn test_extract_or_generate_with_malformed_header() {
        let mut metadata = MetadataMap::new();
        let header_value = MetadataValue::from_static("invalid");
        metadata.insert(TRACEPARENT_HEADER, header_value);

        let ctx = extract_or_generate(&metadata);

        // Should generate new context despite invalid header
        assert_eq!(ctx.trace_id.len(), 32);
        assert!(ctx.parent_span_id.is_none());
    }

    #[test]
    fn test_trace_context_display() {
        let ctx = TraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            parent_span_id: None,
            trace_flags: 0x01,
            trace_state: None,
        };

        assert_eq!(ctx.to_string(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    #[test]
    fn test_uppercase_hex_normalized() {
        // Uppercase hex should be normalized to lowercase
        let result =
            parse_traceparent("00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01", None);

        assert!(result.is_ok());
        let ctx = result.unwrap_or_else(|_| TraceContext::new());
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_span_id, Some("b7ad6b7169203331".to_string()));
    }

    #[test]
    fn test_non_sampled_flag() {
        let result =
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00", None);

        assert!(result.is_ok());
        let ctx = result.unwrap_or_else(|_| TraceContext::new());
        assert_eq!(ctx.trace_flags, 0x00);
        assert!(!ctx.is_sampled());
    }
}
