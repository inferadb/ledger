//! SDK-specific error types with recovery context.
//!
//! Provides a two-tier error model:
//! - **Transport errors**: Connection failures, timeouts, gRPC status codes
//! - **Domain errors**: Idempotency errors, CAS failures
//!
//! Errors include retryability classification and recovery context.

use snafu::{Location, Snafu};
use tonic::Code;

/// Result type alias for SDK operations.
pub type Result<T> = std::result::Result<T, SdkError>;

/// Format an RPC error message with optional correlation IDs.
fn format_rpc_error(
    code: &Code,
    message: &str,
    request_id: &Option<String>,
    trace_id: &Option<String>,
) -> String {
    let mut s = format!("RPC error (code={code:?}): {message}");
    if let Some(rid) = request_id {
        s.push_str(&format!(" [request_id={rid}]"));
    }
    if let Some(tid) = trace_id {
        s.push_str(&format!(" [trace_id={tid}]"));
    }
    s
}

/// Format a rate-limited error message with retry-after and optional correlation IDs.
fn format_rate_limited(
    message: &str,
    retry_after: &std::time::Duration,
    request_id: &Option<String>,
    trace_id: &Option<String>,
) -> String {
    let mut s = format!("Rate limited: {message} (retry after {}ms)", retry_after.as_millis());
    if let Some(rid) = request_id {
        s.push_str(&format!(" [request_id={rid}]"));
    }
    if let Some(tid) = trace_id {
        s.push_str(&format!(" [trace_id={tid}]"));
    }
    s
}

/// Format an idempotency error message with optional conflict details.
fn format_idempotency(
    message: &str,
    conflict_key: &Option<String>,
    original_tx_id: &Option<String>,
) -> String {
    let mut s = format!("Idempotency error: {message}");
    if let Some(key) = conflict_key {
        s.push_str(&format!(" [conflict_key={key}]"));
    }
    if let Some(tx_id) = original_tx_id {
        s.push_str(&format!(" [original_tx_id={tx_id}]"));
    }
    s
}

/// Extract a string metadata value from a gRPC status metadata map.
fn extract_metadata(metadata: &tonic::metadata::MetadataMap, key: &str) -> Option<String> {
    metadata.get(key).and_then(|v| v.to_str().ok()).map(String::from)
}

/// Converts a gRPC status code to a short snake_case label for metrics.
fn code_to_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// SDK error types with context-rich error messages.
///
/// Each variant carries diagnostic context (correlation IDs, retry history)
/// to help operators correlate SDK errors with server-side logs.
///
/// # Recovery Guide
///
/// | Variant              | Retryable | Recovery Action                                             |
/// | -------------------- | --------- | ----------------------------------------------------------- |
/// | `Connection`         | Yes       | Check network connectivity; server may be starting up       |
/// | `Transport`          | Yes       | TLS or HTTP/2 failure; verify certificates and connectivity |
/// | `Rpc`                | Depends   | Check `is_retryable()`; see gRPC code for details           |
/// | `RateLimited`        | Yes       | Wait for `retry_after` duration before retrying             |
/// | `RetryExhausted`     | No        | All retries failed; check `attempt_history` for root cause  |
/// | `Config`             | No        | Fix configuration and recreate the client                   |
/// | `Idempotency`        | No        | Generate a new idempotency key for the write                |
/// | `AlreadyCommitted`   | No        | Idempotent success; original write was applied              |
/// | `StreamDisconnected` | Yes       | Reconnect the stream; `ReconnectingStream` handles this     |
/// | `Timeout`            | Yes       | Increase timeout or reduce request complexity               |
/// | `Shutdown`           | No        | Client is shutting down; create a new client instance       |
/// | `Cancelled`          | No        | Caller cancelled via `CancellationToken`                    |
/// | `InvalidUrl`         | No        | Fix the server URL in configuration                         |
/// | `Unavailable`        | Yes       | Server health check failed; retry after short delay         |
/// | `ProofVerification`  | No        | Merkle proof is invalid; data may be tampered               |
/// | `Validation`         | No        | Fix request parameters to conform to field limits           |
/// | `CircuitOpen`        | No        | Wait for `retry_after`; circuit breaker will probe          |
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SdkError {
    /// Failed to establish connection to the server.
    ///
    /// **Recovery**: Retryable. Check that the server is running and reachable.
    /// The SDK's built-in retry logic handles transient connection failures.
    #[snafu(display("Connection error at {location}: {message}"))]
    Connection {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Transport-level error (HTTP/2 framing, TLS handshake).
    ///
    /// **Recovery**: Retryable. Verify TLS certificates are valid and the
    /// server supports HTTP/2. Network issues typically resolve on retry.
    #[snafu(display("Transport error at {location}: {source}"))]
    Transport {
        /// Underlying transport error.
        source: tonic::transport::Error,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// gRPC RPC error with status code and optional correlation IDs.
    ///
    /// When the server populates `x-request-id` and `x-trace-id` in response
    /// metadata, these fields are extracted automatically by `From<tonic::Status>`.
    /// Operators can use them to correlate SDK errors with server-side wide event logs.
    #[snafu(display("{}", format_rpc_error(code, message, request_id, trace_id)))]
    Rpc {
        /// gRPC status code.
        code: Code,
        /// Error message from server.
        message: String,
        /// Server-assigned request ID for log correlation.
        request_id: Option<String>,
        /// Distributed trace ID for cross-service correlation.
        trace_id: Option<String>,
    },

    /// Rate limit exceeded.
    ///
    /// Returned when the server responds with `RESOURCE_EXHAUSTED` and includes
    /// a `retry-after-ms` trailing metadata value. The `retry_after` duration
    /// tells the caller how long to wait before retrying.
    #[snafu(display("{}", format_rate_limited(message, retry_after, request_id, trace_id)))]
    RateLimited {
        /// Human-readable rate limit message from server.
        message: String,
        /// Suggested wait duration before retrying.
        retry_after: std::time::Duration,
        /// Server-assigned request ID for log correlation.
        request_id: Option<String>,
        /// Distributed trace ID for cross-service correlation.
        trace_id: Option<String>,
    },

    /// Retry attempts exhausted.
    ///
    /// Contains per-attempt error history for diagnosing intermittent failures.
    /// Each entry is `(attempt_number, error_description)`.
    #[snafu(display("Retry exhausted after {attempts} attempts: {last_error}"))]
    RetryExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// Last error message before giving up.
        last_error: String,
        /// Per-attempt error history: `(attempt_number, error_description)`.
        attempt_history: Vec<(u32, String)>,
    },

    /// Configuration validation error (invalid URL, missing field, constraint violation).
    ///
    /// **Recovery**: Not retryable. Fix the `ClientConfig` parameters and
    /// recreate the client. See [`ClientConfig`](crate::ClientConfig) for valid ranges.
    #[snafu(display("Configuration error: {message}"))]
    Config {
        /// Error description.
        message: String,
    },

    /// Client-side idempotency error.
    ///
    /// Returned when an idempotency key is reused with a different payload.
    /// This is not retryable - the client must generate a new idempotency key.
    #[snafu(display("{}", format_idempotency(message, conflict_key, original_tx_id)))]
    Idempotency {
        /// Error description.
        message: String,
        /// The idempotency key that caused the conflict.
        conflict_key: Option<String>,
        /// Transaction ID from the original (conflicting) write.
        original_tx_id: Option<String>,
    },

    /// Write was already committed (idempotent retry detected).
    ///
    /// This is not an error - the original write succeeded. The SDK returns
    /// success with the original transaction details when this is detected.
    #[snafu(display("Already committed: tx_id={tx_id} at block_height={block_height}"))]
    AlreadyCommitted {
        /// Transaction ID from the original commit.
        tx_id: String,
        /// Block height where the transaction was committed.
        block_height: u64,
    },

    /// Streaming connection lost (server restart, network partition).
    ///
    /// **Recovery**: Retryable. Use [`ReconnectingStream`](crate::ReconnectingStream)
    /// for automatic reconnection with position tracking.
    #[snafu(display("Stream disconnected: {message}"))]
    StreamDisconnected {
        /// Disconnect reason.
        message: String,
    },

    /// Operation timed out (exceeded configured `ClientConfig::timeout`).
    ///
    /// **Recovery**: Retryable. Consider increasing the timeout for large
    /// batch operations or during cluster leadership transitions.
    #[snafu(display("Operation timed out after {duration_ms}ms"))]
    Timeout {
        /// Timeout duration in milliseconds.
        duration_ms: u64,
    },

    /// Client is shutting down (global cancellation triggered).
    ///
    /// **Recovery**: Not retryable on this client instance. Create a new
    /// `LedgerClient` if the application needs to continue operations.
    #[snafu(display("Client shutting down"))]
    Shutdown,

    /// Request was cancelled via cancellation token.
    ///
    /// Returned when an in-flight request is cancelled by a
    /// [`CancellationToken`](tokio_util::sync::CancellationToken) provided
    /// by the caller. Unlike `Shutdown`, which cancels all requests globally,
    /// `Cancelled` applies to a single request or a group of requests sharing
    /// a token.
    #[snafu(display("Request cancelled"))]
    Cancelled,

    /// URL parsing error (malformed server address).
    ///
    /// **Recovery**: Not retryable. Fix the URL format in the client
    /// configuration. URLs must include the scheme (e.g., `http://` or `https://`).
    #[snafu(display("Invalid URL '{url}': {message}"))]
    InvalidUrl {
        /// The invalid URL.
        url: String,
        /// Parse error description.
        message: String,
    },

    /// Service is unavailable.
    ///
    /// Returned when a health check indicates the service is not available.
    #[snafu(display("Service unavailable: {message}"))]
    Unavailable {
        /// Unavailable reason.
        message: String,
    },

    /// Proof verification failed.
    ///
    /// Returned when a Merkle proof or chain proof fails verification.
    #[snafu(display("Proof verification failed: {reason}"))]
    ProofVerification {
        /// Reason for verification failure.
        reason: &'static str,
    },

    /// Client-side input validation failed.
    ///
    /// Returned when an operation violates configured field limits (key size,
    /// value size, character whitelist, batch size). This is a client-side
    /// check that prevents invalid requests from reaching the server.
    #[snafu(display("Validation error: {message}"))]
    Validation {
        /// Description of the validation failure.
        message: String,
    },

    /// Circuit breaker is open for the target endpoint.
    ///
    /// Returned when the circuit breaker has tripped due to consecutive
    /// failures against an endpoint. The request is rejected immediately
    /// without network I/O to prevent cascading failures.
    #[snafu(display("Circuit open for {endpoint}, retry after {retry_after:?}"))]
    CircuitOpen {
        /// The endpoint whose circuit is open.
        endpoint: String,
        /// Suggested wait duration before retrying.
        retry_after: std::time::Duration,
    },
}

impl SdkError {
    /// Returns true if the error is transient and the operation should be retried.
    ///
    /// Retryable errors:
    /// - `UNAVAILABLE`: Server temporarily unreachable
    /// - `DEADLINE_EXCEEDED`: Request timed out
    /// - `RESOURCE_EXHAUSTED`: Rate limited (but prefer `RateLimited` variant's `retry_after`)
    /// - `ABORTED`: Transaction conflict (retry may succeed)
    /// - `RateLimited`: Explicitly rate-limited with retry-after hint
    /// - Transport errors (network issues)
    ///
    /// Non-retryable errors:
    /// - `INVALID_ARGUMENT`: Request is malformed
    /// - `PERMISSION_DENIED`: Authentication/authorization failure
    /// - `UNAUTHENTICATED`: Missing credentials
    /// - `Idempotency`: Idempotency key reused with different payload
    /// - `AlreadyCommitted`: Operation already succeeded
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            Self::Connection { .. } => true,
            Self::Timeout { .. } => true,
            Self::StreamDisconnected { .. } => true,
            Self::RateLimited { .. } => true, // Retry after suggested delay
            Self::Rpc { code, .. } => matches!(
                code,
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
            ),
            // Non-retryable
            Self::Config { .. } => false,
            Self::AlreadyCommitted { .. } => false,
            Self::Idempotency { .. } => false,
            Self::RetryExhausted { .. } => false,
            Self::Shutdown => false,
            Self::Cancelled => false, // Intentional cancellation
            Self::InvalidUrl { .. } => false,
            Self::Unavailable { .. } => true, // May become available
            Self::ProofVerification { .. } => false, // Data integrity error
            Self::Validation { .. } => false, // Request is malformed
            Self::CircuitOpen { .. } => false, // Fast-fail, don't retry against open circuit
        }
    }

    /// Returns a short classification string for this error, suitable for
    /// use as a metrics label.
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Self::Connection { .. } => "connection".to_owned(),
            Self::Transport { .. } => "transport".to_owned(),
            Self::Rpc { code, .. } => format!("rpc_{}", code_to_label(*code)),
            Self::RateLimited { .. } => "rate_limited".to_owned(),
            Self::RetryExhausted { .. } => "retry_exhausted".to_owned(),
            Self::Config { .. } => "config".to_owned(),
            Self::Idempotency { .. } => "idempotency".to_owned(),
            Self::AlreadyCommitted { .. } => "already_committed".to_owned(),
            Self::StreamDisconnected { .. } => "stream_disconnected".to_owned(),
            Self::Timeout { .. } => "timeout".to_owned(),
            Self::Shutdown => "shutdown".to_owned(),
            Self::Cancelled => "cancelled".to_owned(),
            Self::InvalidUrl { .. } => "invalid_url".to_owned(),
            Self::Unavailable { .. } => "unavailable".to_owned(),
            Self::ProofVerification { .. } => "proof_verification".to_owned(),
            Self::Validation { .. } => "validation".to_owned(),
            Self::CircuitOpen { .. } => "circuit_open".to_owned(),
        }
    }

    /// Returns the gRPC status code if this is an RPC or rate-limited error.
    #[must_use]
    pub fn code(&self) -> Option<Code> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
            Self::RateLimited { .. } => Some(Code::ResourceExhausted),
            _ => None,
        }
    }

    /// Returns the server-assigned request ID if present.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Rpc { request_id, .. } | Self::RateLimited { request_id, .. } => {
                request_id.as_deref()
            },
            _ => None,
        }
    }

    /// Returns the distributed trace ID if present.
    #[must_use]
    pub fn trace_id(&self) -> Option<&str> {
        match self {
            Self::Rpc { trace_id, .. } | Self::RateLimited { trace_id, .. } => trace_id.as_deref(),
            _ => None,
        }
    }
}

impl From<tonic::transport::Error> for SdkError {
    fn from(source: tonic::transport::Error) -> Self {
        Self::Transport { source, location: Location::default() }
    }
}

impl From<tonic::Status> for SdkError {
    fn from(status: tonic::Status) -> Self {
        let metadata = status.metadata();
        let request_id = extract_metadata(metadata, "x-request-id");
        let trace_id = extract_metadata(metadata, "x-trace-id");

        // Check for rate limiting: RESOURCE_EXHAUSTED + retry-after-ms metadata
        if status.code() == Code::ResourceExhausted
            && let Some(retry_after_str) = extract_metadata(metadata, "retry-after-ms")
            && let Ok(ms) = retry_after_str.parse::<u64>()
        {
            return Self::RateLimited {
                message: status.message().to_owned(),
                retry_after: std::time::Duration::from_millis(ms),
                request_id,
                trace_id,
            };
        }

        Self::Rpc {
            code: status.code(),
            message: status.message().to_owned(),
            request_id,
            trace_id,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_error_retryable_unavailable() {
        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "server unavailable".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_deadline_exceeded() {
        let err = SdkError::Rpc {
            code: Code::DeadlineExceeded,
            message: "timeout".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_resource_exhausted() {
        let err = SdkError::Rpc {
            code: Code::ResourceExhausted,
            message: "rate limited".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_aborted() {
        let err = SdkError::Rpc {
            code: Code::Aborted,
            message: "transaction conflict".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_invalid_argument() {
        let err = SdkError::Rpc {
            code: Code::InvalidArgument,
            message: "bad request".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_permission_denied() {
        let err = SdkError::Rpc {
            code: Code::PermissionDenied,
            message: "access denied".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_unauthenticated() {
        let err = SdkError::Rpc {
            code: Code::Unauthenticated,
            message: "not authenticated".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_transport_error_is_retryable() {
        let err = SdkError::Connection {
            message: "connection refused".to_owned(),
            location: Location::default(),
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_idempotency_key_reused_not_retryable() {
        let err = SdkError::Idempotency {
            message: "key reused".to_owned(),
            conflict_key: None,
            original_tx_id: None,
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_config_error_not_retryable() {
        let err = SdkError::Config { message: "invalid config".to_owned() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status() {
        let status = tonic::Status::unavailable("server down");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Unavailable, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_code_accessor() {
        let err = SdkError::Rpc {
            code: Code::NotFound,
            message: "not found".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert_eq!(err.code(), Some(Code::NotFound));

        let err2 = SdkError::Timeout { duration_ms: 1000 };
        assert_eq!(err2.code(), None);
    }

    #[test]
    fn test_already_committed_not_retryable() {
        let err = SdkError::AlreadyCommitted { tx_id: "tx-123".to_owned(), block_height: 42 };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_already_committed_display() {
        let err = SdkError::AlreadyCommitted { tx_id: "tx-abc".to_owned(), block_height: 100 };
        let msg = format!("{err}");
        assert!(msg.contains("tx-abc"));
        assert!(msg.contains("100"));
    }

    // Tests for From<tonic::Status> covering all gRPC status codes
    #[test]
    fn test_from_tonic_status_ok() {
        let status = tonic::Status::ok("success");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Ok, .. }));
    }

    #[test]
    fn test_from_tonic_status_cancelled() {
        let status = tonic::Status::cancelled("operation cancelled");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Cancelled, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_unknown() {
        let status = tonic::Status::unknown("unknown error");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Unknown, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_invalid_argument() {
        let status = tonic::Status::invalid_argument("bad input");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::InvalidArgument, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_deadline_exceeded() {
        let status = tonic::Status::deadline_exceeded("timed out");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::DeadlineExceeded, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_not_found() {
        let status = tonic::Status::not_found("resource missing");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::NotFound, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_already_exists() {
        let status = tonic::Status::already_exists("duplicate");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::AlreadyExists, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_permission_denied() {
        let status = tonic::Status::permission_denied("forbidden");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::PermissionDenied, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_resource_exhausted() {
        let status = tonic::Status::resource_exhausted("quota exceeded");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::ResourceExhausted, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_failed_precondition() {
        let status = tonic::Status::failed_precondition("precondition failed");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::FailedPrecondition, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_aborted() {
        let status = tonic::Status::aborted("transaction aborted");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Aborted, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_out_of_range() {
        let status = tonic::Status::out_of_range("value out of range");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::OutOfRange, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_unimplemented() {
        let status = tonic::Status::unimplemented("not implemented");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Unimplemented, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_internal() {
        let status = tonic::Status::internal("server error");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Internal, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_unavailable() {
        let status = tonic::Status::unavailable("service down");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Unavailable, .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_data_loss() {
        let status = tonic::Status::data_loss("data corrupted");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::DataLoss, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_unauthenticated() {
        let status = tonic::Status::unauthenticated("no credentials");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::Unauthenticated, .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_tonic_status_preserves_message() {
        let status = tonic::Status::internal("detailed error message");
        let err: SdkError = status.into();
        match err {
            SdkError::Rpc { message, .. } => {
                assert_eq!(message, "detailed error message");
            },
            _ => panic!("Expected Rpc variant"),
        }
    }

    #[test]
    fn test_unavailable_error_is_retryable() {
        let err = SdkError::Unavailable { message: "service down".to_string() };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_unavailable_error_display() {
        let err = SdkError::Unavailable { message: "service down".to_string() };
        let display = format!("{}", err);
        assert!(display.contains("Service unavailable"));
        assert!(display.contains("service down"));
    }

    #[test]
    fn test_cancelled_not_retryable() {
        let err = SdkError::Cancelled;
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_cancelled_display() {
        let err = SdkError::Cancelled;
        assert_eq!(format!("{err}"), "Request cancelled");
    }

    #[test]
    fn test_cancelled_has_no_code() {
        let err = SdkError::Cancelled;
        assert_eq!(err.code(), None);
    }

    // --- Enhanced Error Context Tests ---

    #[test]
    fn test_rpc_error_with_correlation_ids() {
        let err = SdkError::Rpc {
            code: Code::Internal,
            message: "server error".to_owned(),
            request_id: Some("req-123".to_owned()),
            trace_id: Some("trace-abc".to_owned()),
        };
        assert_eq!(err.request_id(), Some("req-123"));
        assert_eq!(err.trace_id(), Some("trace-abc"));
        let display = format!("{err}");
        assert!(display.contains("[request_id=req-123]"));
        assert!(display.contains("[trace_id=trace-abc]"));
    }

    #[test]
    fn test_rpc_error_without_correlation_ids() {
        let err = SdkError::Rpc {
            code: Code::Internal,
            message: "server error".to_owned(),
            request_id: None,
            trace_id: None,
        };
        assert_eq!(err.request_id(), None);
        assert_eq!(err.trace_id(), None);
        let display = format!("{err}");
        assert!(!display.contains("[request_id="));
        assert!(!display.contains("[trace_id="));
    }

    #[test]
    fn test_from_tonic_status_extracts_metadata() {
        let mut status = tonic::Status::internal("server error");
        status.metadata_mut().insert("x-request-id", "req-456".parse().unwrap());
        status.metadata_mut().insert("x-trace-id", "trace-def".parse().unwrap());
        let err: SdkError = status.into();
        match &err {
            SdkError::Rpc { request_id, trace_id, .. } => {
                assert_eq!(request_id.as_deref(), Some("req-456"));
                assert_eq!(trace_id.as_deref(), Some("trace-def"));
            },
            _ => panic!("Expected Rpc variant"),
        }
    }

    #[test]
    fn test_from_tonic_status_no_metadata() {
        let status = tonic::Status::internal("no metadata");
        let err: SdkError = status.into();
        assert_eq!(err.request_id(), None);
        assert_eq!(err.trace_id(), None);
    }

    #[test]
    fn test_rate_limited_from_resource_exhausted_with_retry_after() {
        let mut status = tonic::Status::resource_exhausted("rate limit exceeded");
        status.metadata_mut().insert("retry-after-ms", "5000".parse().unwrap());
        let err: SdkError = status.into();
        match &err {
            SdkError::RateLimited { message, retry_after, .. } => {
                assert_eq!(message, "rate limit exceeded");
                assert_eq!(*retry_after, std::time::Duration::from_millis(5000));
            },
            _ => panic!("Expected RateLimited variant, got {:?}", err),
        }
        assert!(err.is_retryable());
        assert_eq!(err.code(), Some(Code::ResourceExhausted));
    }

    #[test]
    fn test_rate_limited_with_correlation_ids() {
        let mut status = tonic::Status::resource_exhausted("throttled");
        status.metadata_mut().insert("retry-after-ms", "1000".parse().unwrap());
        status.metadata_mut().insert("x-request-id", "req-789".parse().unwrap());
        status.metadata_mut().insert("x-trace-id", "trace-ghi".parse().unwrap());
        let err: SdkError = status.into();
        assert_eq!(err.request_id(), Some("req-789"));
        assert_eq!(err.trace_id(), Some("trace-ghi"));
        let display = format!("{err}");
        assert!(display.contains("retry after 1000ms"));
        assert!(display.contains("[request_id=req-789]"));
    }

    #[test]
    fn test_resource_exhausted_without_retry_after_is_rpc() {
        let status = tonic::Status::resource_exhausted("no retry-after");
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::ResourceExhausted, .. }));
    }

    #[test]
    fn test_resource_exhausted_with_invalid_retry_after_is_rpc() {
        let mut status = tonic::Status::resource_exhausted("bad value");
        status.metadata_mut().insert("retry-after-ms", "not-a-number".parse().unwrap());
        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::Rpc { code: Code::ResourceExhausted, .. }));
    }

    #[test]
    fn test_retry_exhausted_with_attempt_history() {
        let err = SdkError::RetryExhausted {
            attempts: 3,
            last_error: "unavailable".to_owned(),
            attempt_history: vec![
                (1, "connection refused".to_owned()),
                (2, "timeout".to_owned()),
                (3, "unavailable".to_owned()),
            ],
        };
        let display = format!("{err}");
        assert!(display.contains("3 attempts"));
        assert!(!err.is_retryable());
        if let SdkError::RetryExhausted { attempt_history, .. } = &err {
            assert_eq!(attempt_history.len(), 3);
            assert_eq!(attempt_history[0], (1, "connection refused".to_owned()));
        }
    }

    #[test]
    fn test_idempotency_with_conflict_details() {
        let err = SdkError::Idempotency {
            message: "key reused".to_owned(),
            conflict_key: Some("idem-key-abc".to_owned()),
            original_tx_id: Some("tx-original".to_owned()),
        };
        let display = format!("{err}");
        assert!(display.contains("[conflict_key=idem-key-abc]"));
        assert!(display.contains("[original_tx_id=tx-original]"));
    }

    #[test]
    fn test_idempotency_without_conflict_details() {
        let err = SdkError::Idempotency {
            message: "key reused".to_owned(),
            conflict_key: None,
            original_tx_id: None,
        };
        let display = format!("{err}");
        assert!(!display.contains("[conflict_key="));
        assert!(!display.contains("[original_tx_id="));
    }

    #[test]
    fn test_rate_limited_display() {
        let err = SdkError::RateLimited {
            message: "namespace quota".to_owned(),
            retry_after: std::time::Duration::from_millis(2500),
            request_id: None,
            trace_id: None,
        };
        let display = format!("{err}");
        assert!(display.contains("Rate limited"));
        assert!(display.contains("retry after 2500ms"));
    }

    #[test]
    fn test_rate_limited_code_is_resource_exhausted() {
        let err = SdkError::RateLimited {
            message: "throttled".to_owned(),
            retry_after: std::time::Duration::from_secs(1),
            request_id: None,
            trace_id: None,
        };
        assert_eq!(err.code(), Some(Code::ResourceExhausted));
    }

    #[test]
    fn test_non_rpc_errors_have_no_request_id() {
        let err = SdkError::Timeout { duration_ms: 1000 };
        assert_eq!(err.request_id(), None);
        assert_eq!(err.trace_id(), None);
    }
}
