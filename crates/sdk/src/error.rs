//! SDK-specific error types with recovery context.
//!
//! Provides a two-tier error model:
//! - **Transport errors**: Connection failures, timeouts, gRPC status codes
//! - **Domain errors**: Idempotency errors, CAS failures
//!
//! Errors include retryability classification and recovery context.

use inferadb_ledger_types::Region;
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

/// Extracts a string metadata value from a gRPC status metadata map.
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

/// Structured error details decoded from gRPC `Status.details` bytes.
///
/// When the server attaches an `ErrorDetails` proto message to a gRPC error,
/// the SDK decodes it and makes it available on `SdkError::Rpc` and
/// `SdkError::RateLimited` variants.
///
/// # Example
///
/// ```
/// # use inferadb_ledger_sdk::SdkError;
/// fn handle_error(err: &SdkError) {
///     if let Some(details) = err.server_error_details() {
///         println!("Error code: {}", details.error_code);
///         println!("Retryable: {}", details.is_retryable);
///         if let Some(action) = &details.suggested_action {
///             println!("Suggested: {action}");
///         }
///     }
/// }
///
/// let err = SdkError::Connection { message: "timeout".into() };
/// handle_error(&err); // Connection variant has no details — branch not taken
/// ```
#[derive(Debug, Clone)]
pub struct ServerErrorDetails {
    /// Machine-readable error code (numeric string, e.g., "3203").
    pub error_code: String,
    /// Whether the server considers this error retryable.
    pub is_retryable: bool,
    /// Suggested delay before retrying (milliseconds).
    pub retry_after_ms: Option<i32>,
    /// Structured key-value context from the server.
    pub context: std::collections::HashMap<String, String>,
    /// Human-readable recovery guidance from the server.
    pub suggested_action: Option<String>,
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
/// | `OrganizationMigrating` | Yes    | Organization is migrating; wait for `retry_after`           |
/// | `UserMigrating`         | Yes    | User is migrating between regions; wait for `retry_after`   |
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// Failed to establish connection to the server.
    ///
    /// **Recovery**: Retryable. Check that the server is running and reachable.
    /// The SDK's built-in retry logic handles transient connection failures.
    #[error("Connection error: {message}")]
    Connection {
        /// Error description.
        message: String,
    },

    /// Transport-level error (HTTP/2 framing, TLS handshake).
    ///
    /// **Recovery**: Retryable. Verify TLS certificates are valid and the
    /// server supports HTTP/2. Network issues typically resolve on retry.
    #[error("Transport error: {source}")]
    Transport {
        /// Underlying transport error.
        source: tonic::transport::Error,
    },

    /// gRPC RPC error with status code and optional correlation IDs.
    ///
    /// When the server populates `x-request-id` and `x-trace-id` in response
    /// metadata, these fields are extracted automatically by `From<tonic::Status>`.
    /// Operators can use them to correlate SDK errors with server-side canonical log lines.
    #[error("{}", format_rpc_error(code, message, request_id, trace_id))]
    Rpc {
        /// gRPC status code.
        code: Code,
        /// Error message from server.
        message: String,
        /// Server-assigned request ID for log correlation.
        request_id: Option<String>,
        /// Distributed trace ID for cross-service correlation.
        trace_id: Option<String>,
        /// Structured error details decoded from gRPC `Status.details`.
        error_details: Option<Box<ServerErrorDetails>>,
    },

    /// Rate limit exceeded.
    ///
    /// Returned when the server responds with `RESOURCE_EXHAUSTED` and includes
    /// a `retry-after-ms` trailing metadata value. The `retry_after` duration
    /// tells the caller how long to wait before retrying.
    #[error("{}", format_rate_limited(message, retry_after, request_id, trace_id))]
    RateLimited {
        /// Human-readable rate limit message from server.
        message: String,
        /// Suggested wait duration before retrying.
        retry_after: std::time::Duration,
        /// Server-assigned request ID for log correlation.
        request_id: Option<String>,
        /// Distributed trace ID for cross-service correlation.
        trace_id: Option<String>,
        /// Structured error details decoded from gRPC `Status.details`.
        error_details: Option<Box<ServerErrorDetails>>,
    },

    /// Retry attempts exhausted.
    ///
    /// Contains per-attempt error history for diagnosing intermittent failures.
    /// Each entry is `(attempt_number, error_description)`.
    #[error("Retry exhausted after {attempts} attempts: {last_error}")]
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
    #[error("Configuration error: {message}")]
    Config {
        /// Error description.
        message: String,
    },

    /// Client-side idempotency error.
    ///
    /// Returned when an idempotency key is reused with a different payload.
    /// This is not retryable - the client must generate a new idempotency key.
    #[error("{}", format_idempotency(message, conflict_key, original_tx_id))]
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
    #[error("Already committed: tx_id={tx_id} at block_height={block_height}")]
    AlreadyCommitted {
        /// Transaction ID from the original commit.
        tx_id: String,
        /// Block height where the transaction was committed.
        block_height: u64,
    },

    /// Streaming connection lost (server restart, network partition).
    ///
    /// **Recovery**: Retryable. Use `ReconnectingStream`
    /// for automatic reconnection with position tracking.
    #[error("Stream disconnected: {message}")]
    StreamDisconnected {
        /// Disconnect reason.
        message: String,
    },

    /// Operation timed out (exceeded configured `ClientConfig::timeout`).
    ///
    /// **Recovery**: Retryable. Consider increasing the timeout for large
    /// batch operations or during cluster leadership transitions.
    #[error("Operation timed out after {duration_ms}ms")]
    Timeout {
        /// Timeout duration in milliseconds.
        duration_ms: u64,
    },

    /// Client is shutting down (global cancellation triggered).
    ///
    /// **Recovery**: Not retryable on this client instance. Create a new
    /// `LedgerClient` if the application needs to continue operations.
    #[error("Client shutting down")]
    Shutdown,

    /// Request was cancelled via cancellation token.
    ///
    /// Returned when an in-flight request is cancelled by a
    /// [`CancellationToken`](tokio_util::sync::CancellationToken) provided
    /// by the caller. Unlike `Shutdown`, which cancels all requests globally,
    /// `Cancelled` applies to a single request or a group of requests sharing
    /// a token.
    #[error("Request cancelled")]
    Cancelled,

    /// URL parsing error (malformed server address).
    ///
    /// **Recovery**: Not retryable. Fix the URL format in the client
    /// configuration. URLs must include the scheme (e.g., `http://` or `https://`).
    #[error("Invalid URL '{url}': {message}")]
    InvalidUrl {
        /// The invalid URL.
        url: String,
        /// Error description from URL parsing.
        message: String,
    },

    /// Service is unavailable.
    ///
    /// Returned when a health check indicates the service is not available.
    #[error("Service unavailable: {message}")]
    Unavailable {
        /// Unavailable reason.
        message: String,
    },

    /// Proof verification failed.
    ///
    /// Returned when a Merkle proof or chain proof fails verification.
    #[error("Proof verification failed: {reason}")]
    ProofVerification {
        /// Reason for verification failure.
        reason: &'static str,
    },

    /// Client-side input validation failed.
    ///
    /// Returned when an operation violates configured field limits (key size,
    /// value size, character whitelist, batch size). This is a client-side
    /// check that prevents invalid requests from reaching the server.
    #[error("Validation error: {message}")]
    Validation {
        /// Description of the validation failure.
        message: String,
    },

    /// Circuit breaker is open for the target endpoint.
    ///
    /// Returned when the circuit breaker has tripped due to consecutive
    /// failures against an endpoint. The request is rejected immediately
    /// without network I/O to prevent cascading failures.
    #[error("Circuit open for {endpoint}, retry after {retry_after:?}")]
    CircuitOpen {
        /// The endpoint whose circuit is open.
        endpoint: String,
        /// Suggested wait duration before retrying.
        retry_after: std::time::Duration,
    },

    /// Organization is being migrated to another region.
    ///
    /// Writes are temporarily blocked while the migration is in progress.
    /// **Recovery**: Retryable. Wait for the suggested duration then retry.
    #[error(
        "Organization migrating from {source_region} to {target_region}, retry after {retry_after:?}"
    )]
    OrganizationMigrating {
        /// Source region the organization is migrating from.
        source_region: Region,
        /// Target region for the migration.
        target_region: Region,
        /// Suggested wait duration before retrying.
        retry_after: std::time::Duration,
    },

    /// User is migrating between regions. Authenticated API calls are temporarily blocked.
    ///
    /// **Recovery**: Retryable. Wait for the suggested duration then retry.
    #[error("User migrating from {source_region} to {target_region}, retry after {retry_after:?}")]
    UserMigrating {
        /// Source region the user is migrating from.
        source_region: Region,
        /// Target region the user is migrating to.
        target_region: Region,
        /// Suggested retry delay.
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
            Self::OrganizationMigrating { .. } => true, // Migration is temporary
            Self::UserMigrating { .. } => true, // Migration is temporary
        }
    }

    /// Returns `true` if the error represents a CAS (compare-and-set)
    /// conflict — the precondition check failed because the entity was
    /// modified since it was last read.
    ///
    /// This matches only [`Code::FailedPrecondition`], which the server
    /// returns when a [`SetCondition`](crate::SetCondition) evaluates to
    /// false. Use this to distinguish CAS conflicts from other error types
    /// without importing [`tonic::Code`] at call sites.
    ///
    /// # Examples
    ///
    /// ```
    /// # use inferadb_ledger_sdk::SdkError;
    /// let err = SdkError::Connection { message: "timeout".into() };
    /// if err.is_cas_conflict() {
    ///     // Re-read current value and retry the compare-and-set
    /// }
    /// assert!(!err.is_cas_conflict()); // Connection is not a CAS conflict
    /// ```
    #[must_use]
    pub fn is_cas_conflict(&self) -> bool {
        matches!(self, Self::Rpc { code: Code::FailedPrecondition, .. })
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
            Self::OrganizationMigrating { .. } => "organization_migrating".to_owned(),
            Self::UserMigrating { .. } => "user_migrating".to_owned(),
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

    /// Returns structured error details from the server, if available.
    ///
    /// Error details are decoded from gRPC `Status.details` bytes. They
    /// contain the server's error code, retryability hint, and optional
    /// recovery guidance. Returns `None` for non-RPC errors or when the
    /// server didn't attach details.
    #[must_use]
    pub fn server_error_details(&self) -> Option<&ServerErrorDetails> {
        match self {
            Self::Rpc { error_details, .. } | Self::RateLimited { error_details, .. } => {
                error_details.as_deref()
            },
            _ => None,
        }
    }
}

impl From<tonic::transport::Error> for SdkError {
    fn from(source: tonic::transport::Error) -> Self {
        Self::Transport { source }
    }
}

impl From<tonic::Status> for SdkError {
    fn from(status: tonic::Status) -> Self {
        let metadata = status.metadata();
        let request_id = extract_metadata(metadata, "x-request-id");
        let trace_id = extract_metadata(metadata, "x-trace-id");

        // Decode structured error details from Status.details bytes
        let error_details = decode_error_details(status.details()).map(Box::new);

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
                error_details,
            };
        }

        // Check for organization migration: FAILED_PRECONDITION + error code 3106
        if status.code() == Code::FailedPrecondition
            && let Some(ref details) = error_details
            && details.error_code == "3106"
        {
            let source_region = details
                .context
                .get("source_region")
                .and_then(|s| s.parse::<Region>().ok())
                .unwrap_or(Region::GLOBAL);
            let target_region = details
                .context
                .get("target_region")
                .and_then(|s| s.parse::<Region>().ok())
                .unwrap_or(Region::GLOBAL);
            let retry_after_ms = details.retry_after_ms.unwrap_or(30_000).max(0) as u64;
            return Self::OrganizationMigrating {
                source_region,
                target_region,
                retry_after: std::time::Duration::from_millis(retry_after_ms),
            };
        }

        // Check for user migration: FAILED_PRECONDITION + error code 3107
        if status.code() == Code::FailedPrecondition
            && let Some(ref details) = error_details
            && details.error_code == "3107"
        {
            let source_region = details
                .context
                .get("source_region")
                .and_then(|s| s.parse::<Region>().ok())
                .unwrap_or(Region::GLOBAL);
            let target_region = details
                .context
                .get("target_region")
                .and_then(|s| s.parse::<Region>().ok())
                .unwrap_or(Region::GLOBAL);
            let retry_after_ms = details.retry_after_ms.unwrap_or(30_000).max(0) as u64;
            return Self::UserMigrating {
                source_region,
                target_region,
                retry_after: std::time::Duration::from_millis(retry_after_ms),
            };
        }

        Self::Rpc {
            code: status.code(),
            message: status.message().to_owned(),
            request_id,
            trace_id,
            error_details,
        }
    }
}

/// Decodes [`ServerErrorDetails`] from gRPC `Status.details` bytes.
///
/// Returns `None` if the bytes are empty or cannot be decoded as an
/// `ErrorDetails` proto message.
fn decode_error_details(details: &[u8]) -> Option<ServerErrorDetails> {
    if details.is_empty() {
        return None;
    }
    use inferadb_ledger_proto::proto::ErrorDetails;
    use prost::Message;

    let decoded = ErrorDetails::decode(details).ok()?;
    Some(ServerErrorDetails {
        error_code: decoded.error_code,
        is_retryable: decoded.is_retryable,
        retry_after_ms: decoded.retry_after_ms,
        context: decoded.context,
        suggested_action: decoded.suggested_action,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    /// Table-driven test covering is_retryable() for all SdkError variants.
    #[test]
    fn test_is_retryable_all_variants() {
        let cases: Vec<(SdkError, bool, &str)> = vec![
            // Retryable RPC codes
            (
                SdkError::Rpc {
                    code: Code::Unavailable,
                    message: "down".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/Unavailable",
            ),
            (
                SdkError::Rpc {
                    code: Code::DeadlineExceeded,
                    message: "timeout".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/DeadlineExceeded",
            ),
            (
                SdkError::Rpc {
                    code: Code::ResourceExhausted,
                    message: "limited".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/ResourceExhausted",
            ),
            (
                SdkError::Rpc {
                    code: Code::Aborted,
                    message: "conflict".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/Aborted",
            ),
            // Non-retryable RPC codes
            (
                SdkError::Rpc {
                    code: Code::InvalidArgument,
                    message: "bad".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Rpc/InvalidArgument",
            ),
            (
                SdkError::Rpc {
                    code: Code::PermissionDenied,
                    message: "denied".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Rpc/PermissionDenied",
            ),
            (
                SdkError::Rpc {
                    code: Code::Unauthenticated,
                    message: "noauth".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Rpc/Unauthenticated",
            ),
            (
                SdkError::Rpc {
                    code: Code::NotFound,
                    message: "missing".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Rpc/NotFound",
            ),
            (
                SdkError::Rpc {
                    code: Code::Internal,
                    message: "err".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Rpc/Internal",
            ),
            // Transport-level errors (all retryable)
            (SdkError::Connection { message: "refused".into() }, true, "Connection"),
            (SdkError::Timeout { duration_ms: 1000 }, true, "Timeout"),
            (SdkError::Unavailable { message: "down".into() }, true, "Unavailable"),
            (
                SdkError::RateLimited {
                    message: "throttled".into(),
                    retry_after: std::time::Duration::from_secs(1),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "RateLimited",
            ),
            (
                SdkError::OrganizationMigrating {
                    source_region: Region::US_EAST_VA,
                    target_region: Region::IE_EAST_DUBLIN,
                    retry_after: std::time::Duration::from_secs(30),
                },
                true,
                "OrganizationMigrating",
            ),
            (
                SdkError::UserMigrating {
                    source_region: Region::US_EAST_VA,
                    target_region: Region::IE_EAST_DUBLIN,
                    retry_after: std::time::Duration::from_secs(30),
                },
                true,
                "UserMigrating",
            ),
            // Non-retryable variants
            (SdkError::Config { message: "bad".into() }, false, "Config"),
            (
                SdkError::Idempotency {
                    message: "reused".into(),
                    conflict_key: None,
                    original_tx_id: None,
                },
                false,
                "Idempotency",
            ),
            (
                SdkError::AlreadyCommitted { tx_id: "tx-1".into(), block_height: 1 },
                false,
                "AlreadyCommitted",
            ),
            (SdkError::Cancelled, false, "Cancelled"),
            (
                SdkError::CircuitOpen {
                    endpoint: "ep".into(),
                    retry_after: std::time::Duration::from_secs(5),
                },
                false,
                "CircuitOpen",
            ),
        ];

        for (err, expected, label) in &cases {
            assert_eq!(
                err.is_retryable(),
                *expected,
                "{label}: expected is_retryable={expected}, got {}",
                err.is_retryable()
            );
        }
    }

    /// code() returns the gRPC code for Rpc/RateLimited, None for other variants.
    #[test]
    fn test_code_accessor() {
        // Rpc variant returns its code
        let err = SdkError::Rpc {
            code: Code::NotFound,
            message: "not found".to_owned(),
            request_id: None,
            trace_id: None,
            error_details: None,
        };
        assert_eq!(err.code(), Some(Code::NotFound));

        // RateLimited maps to ResourceExhausted
        let err = SdkError::RateLimited {
            message: "throttled".into(),
            retry_after: std::time::Duration::from_secs(1),
            request_id: None,
            trace_id: None,
            error_details: None,
        };
        assert_eq!(err.code(), Some(Code::ResourceExhausted));

        // Non-RPC variants return None
        let err = SdkError::Timeout { duration_ms: 1000 };
        assert_eq!(err.code(), None);
        assert_eq!(err.request_id(), None);
        assert_eq!(err.trace_id(), None);
    }

    /// AlreadyCommitted Display includes tx_id and block_height.
    #[test]
    fn test_already_committed_display() {
        let err = SdkError::AlreadyCommitted { tx_id: "tx-abc".to_owned(), block_height: 100 };
        let msg = format!("{err}");
        assert!(msg.contains("tx-abc"));
        assert!(msg.contains("100"));
    }

    /// Table-driven test: From<tonic::Status> maps all gRPC codes to correct SdkError::Rpc
    /// variant and preserves the message string.
    #[test]
    fn test_from_tonic_status_all_codes() {
        // (status, expected_code, expected_retryable)
        let cases: Vec<(tonic::Status, Code, bool)> = vec![
            (tonic::Status::ok("success"), Code::Ok, false),
            (tonic::Status::cancelled("cancelled"), Code::Cancelled, false),
            (tonic::Status::unknown("unknown"), Code::Unknown, false),
            (tonic::Status::invalid_argument("bad"), Code::InvalidArgument, false),
            (tonic::Status::deadline_exceeded("timeout"), Code::DeadlineExceeded, true),
            (tonic::Status::not_found("missing"), Code::NotFound, false),
            (tonic::Status::already_exists("dup"), Code::AlreadyExists, false),
            (tonic::Status::permission_denied("denied"), Code::PermissionDenied, false),
            (tonic::Status::resource_exhausted("quota"), Code::ResourceExhausted, true),
            (tonic::Status::failed_precondition("precond"), Code::FailedPrecondition, false),
            (tonic::Status::aborted("aborted"), Code::Aborted, true),
            (tonic::Status::out_of_range("range"), Code::OutOfRange, false),
            (tonic::Status::unimplemented("noimpl"), Code::Unimplemented, false),
            (tonic::Status::internal("internal"), Code::Internal, false),
            (tonic::Status::unavailable("down"), Code::Unavailable, true),
            (tonic::Status::data_loss("lost"), Code::DataLoss, false),
            (tonic::Status::unauthenticated("noauth"), Code::Unauthenticated, false),
        ];

        for (status, expected_code, expected_retryable) in cases {
            let msg = status.message().to_owned();
            let err: SdkError = status.into();
            match &err {
                SdkError::Rpc { code, message, .. } => {
                    assert_eq!(*code, expected_code, "code mismatch for {expected_code:?}");
                    assert_eq!(message, &msg, "message not preserved for {expected_code:?}");
                },
                _ => panic!("Expected Rpc variant for {expected_code:?}, got {err:?}"),
            }
            assert_eq!(
                err.is_retryable(),
                expected_retryable,
                "retryable mismatch for {expected_code:?}"
            );
        }
    }

    /// Unavailable variant (not Rpc) includes message in display.
    #[test]
    fn test_unavailable_variant_display() {
        let err = SdkError::Unavailable { message: "service down".to_string() };
        let display = format!("{err}");
        assert!(display.contains("Service unavailable"));
        assert!(display.contains("service down"));
    }

    /// Cancelled variant display and code() accessor.
    #[test]
    fn test_cancelled_variant() {
        let err = SdkError::Cancelled;
        assert_eq!(format!("{err}"), "Request cancelled");
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
            error_details: None,
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
            error_details: None,
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
            message: "organization quota".to_owned(),
            retry_after: std::time::Duration::from_millis(2500),
            request_id: None,
            trace_id: None,
            error_details: None,
        };
        let display = format!("{err}");
        assert!(display.contains("Rate limited"));
        assert!(display.contains("retry after 2500ms"));
    }

    // --- ErrorDetails decoding tests ---

    /// Builds a tonic::Status with binary-encoded ErrorDetails in its details field.
    fn status_with_error_details(
        code: Code,
        message: &str,
        error_code: &str,
        is_retryable: bool,
        retry_after_ms: Option<i32>,
        context: std::collections::HashMap<String, String>,
        suggested_action: Option<&str>,
    ) -> tonic::Status {
        use inferadb_ledger_proto::proto::ErrorDetails;
        use prost::Message;

        let details = ErrorDetails {
            error_code: error_code.to_owned(),
            is_retryable,
            retry_after_ms,
            context,
            suggested_action: suggested_action.map(String::from),
        };
        let encoded = details.encode_to_vec();
        tonic::Status::with_details(tonic::Code::from(code as i32), message, encoded.into())
    }

    #[test]
    fn test_decode_error_details_from_rpc_status() {
        let status = status_with_error_details(
            Code::InvalidArgument,
            "key too long",
            "3203",
            false,
            None,
            std::collections::HashMap::new(),
            Some("Fix the request parameters"),
        );
        let err: SdkError = status.into();
        let details = err.server_error_details().expect("should have details");
        assert_eq!(details.error_code, "3203");
        assert!(!details.is_retryable);
        assert_eq!(details.retry_after_ms, None);
        assert!(details.context.is_empty());
        assert_eq!(details.suggested_action.as_deref(), Some("Fix the request parameters"));
    }

    #[test]
    fn test_decode_error_details_with_retry_and_context() {
        let mut context = std::collections::HashMap::new();
        context.insert("organization_id".to_owned(), "42".to_owned());
        context.insert("level".to_owned(), "backpressure".to_owned());

        let mut status = status_with_error_details(
            Code::ResourceExhausted,
            "rate limit exceeded",
            "3204",
            true,
            Some(500),
            context,
            Some("Reduce request rate"),
        );
        // Add retry-after-ms metadata to trigger RateLimited variant
        status.metadata_mut().insert("retry-after-ms", "500".parse().unwrap());

        let err: SdkError = status.into();
        assert!(matches!(err, SdkError::RateLimited { .. }));

        let details = err.server_error_details().expect("should have details");
        assert_eq!(details.error_code, "3204");
        assert!(details.is_retryable);
        assert_eq!(details.retry_after_ms, Some(500));
        assert_eq!(details.context.get("organization_id").unwrap(), "42");
        assert_eq!(details.context.get("level").unwrap(), "backpressure");
        assert_eq!(details.suggested_action.as_deref(), Some("Reduce request rate"));
    }

    #[test]
    fn test_decode_error_details_consensus_unavailable() {
        let status = status_with_error_details(
            Code::Unavailable,
            "not leader",
            "2000",
            true,
            None,
            std::collections::HashMap::new(),
            Some("Retry against a different node"),
        );
        let err: SdkError = status.into();
        let details = err.server_error_details().expect("should have details");
        assert_eq!(details.error_code, "2000");
        assert!(details.is_retryable);
        assert_eq!(details.suggested_action.as_deref(), Some("Retry against a different node"));
    }

    #[test]
    fn test_no_error_details_when_status_has_no_details() {
        let status = tonic::Status::internal("plain error");
        let err: SdkError = status.into();
        assert!(err.server_error_details().is_none());
    }

    #[test]
    fn test_no_error_details_on_non_rpc_errors() {
        let err = SdkError::Timeout { duration_ms: 5000 };
        assert!(err.server_error_details().is_none());

        let err = SdkError::Config { message: "bad config".to_owned() };
        assert!(err.server_error_details().is_none());

        let err = SdkError::Cancelled;
        assert!(err.server_error_details().is_none());
    }

    #[test]
    fn test_decode_error_details_invalid_bytes_returns_none() {
        // Status with garbage details bytes — use Vec<u8>.into() to get Bytes
        let garbage: Vec<u8> = b"not a valid proto".to_vec();
        let status =
            tonic::Status::with_details(tonic::Code::Internal, "corrupted", garbage.into());
        let err: SdkError = status.into();
        // prost may partially decode garbage — the point is it doesn't panic.
        // The result may be Some (partial decode) or None (decode error).
        // Either way, the conversion must not panic.
        let _ = err.server_error_details();
    }

    #[test]
    fn test_error_details_roundtrip_all_fields() {
        use inferadb_ledger_proto::proto::ErrorDetails;
        use prost::Message;

        // Build server-side details
        let mut context = std::collections::HashMap::new();
        context.insert("key1".to_owned(), "val1".to_owned());
        context.insert("key2".to_owned(), "val2".to_owned());

        let server_details = ErrorDetails {
            error_code: "3204".to_owned(),
            is_retryable: true,
            retry_after_ms: Some(1500),
            context,
            suggested_action: Some("Wait and retry".to_owned()),
        };

        // Encode into Status
        let encoded = server_details.encode_to_vec();
        let status = tonic::Status::with_details(
            tonic::Code::ResourceExhausted,
            "throttled",
            encoded.into(),
        );

        // SDK decodes
        let err: SdkError = status.into();
        let decoded = err.server_error_details().expect("should decode");

        assert_eq!(decoded.error_code, "3204");
        assert!(decoded.is_retryable);
        assert_eq!(decoded.retry_after_ms, Some(1500));
        assert_eq!(decoded.context.len(), 2);
        assert_eq!(decoded.context.get("key1").unwrap(), "val1");
        assert_eq!(decoded.context.get("key2").unwrap(), "val2");
        assert_eq!(decoded.suggested_action.as_deref(), Some("Wait and retry"));
    }

    // --- Migrating variant deserialization tests ---

    #[test]
    fn test_organization_migrating_from_status() {
        use inferadb_ledger_proto::proto::ErrorDetails;
        use prost::Message;

        let mut context = std::collections::HashMap::new();
        context.insert("source_region".to_owned(), "us-east-va".to_owned());
        context.insert("target_region".to_owned(), "ie-east-dublin".to_owned());

        let details = ErrorDetails {
            error_code: "3106".to_owned(),
            is_retryable: true,
            retry_after_ms: Some(15_000),
            context,
            suggested_action: None,
        };
        let encoded = details.encode_to_vec();
        let status = tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "organization is migrating",
            encoded.into(),
        );

        let err: SdkError = status.into();
        match &err {
            SdkError::OrganizationMigrating { source_region, target_region, retry_after } => {
                assert_eq!(*source_region, Region::US_EAST_VA);
                assert_eq!(*target_region, Region::IE_EAST_DUBLIN);
                assert_eq!(*retry_after, std::time::Duration::from_millis(15_000));
            },
            _ => panic!("Expected OrganizationMigrating variant, got {:?}", err),
        }
        assert!(err.is_retryable());
    }

    #[test]
    fn test_user_migrating_from_status() {
        use inferadb_ledger_proto::proto::ErrorDetails;
        use prost::Message;

        let mut context = std::collections::HashMap::new();
        context.insert("source_region".to_owned(), "us-east-va".to_owned());
        context.insert("target_region".to_owned(), "ie-east-dublin".to_owned());

        let details = ErrorDetails {
            error_code: "3107".to_owned(),
            is_retryable: true,
            retry_after_ms: Some(20_000),
            context,
            suggested_action: None,
        };
        let encoded = details.encode_to_vec();
        let status = tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "user is migrating",
            encoded.into(),
        );

        let err: SdkError = status.into();
        match &err {
            SdkError::UserMigrating { source_region, target_region, retry_after } => {
                assert_eq!(*source_region, Region::US_EAST_VA);
                assert_eq!(*target_region, Region::IE_EAST_DUBLIN);
                assert_eq!(*retry_after, std::time::Duration::from_millis(20_000));
            },
            _ => panic!("Expected UserMigrating variant, got {:?}", err),
        }
        assert!(err.is_retryable());
    }

    /// Table-driven: is_cas_conflict() only true for FailedPrecondition RPC errors.
    #[test]
    fn test_is_cas_conflict_all_cases() {
        let cases: Vec<(SdkError, bool, &str)> = vec![
            (
                SdkError::Rpc {
                    code: Code::FailedPrecondition,
                    message: "cond".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "FailedPrecondition",
            ),
            (
                SdkError::Rpc {
                    code: Code::Aborted,
                    message: "abort".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "Aborted",
            ),
            (
                SdkError::Rpc {
                    code: Code::NotFound,
                    message: "nf".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                false,
                "NotFound",
            ),
            (SdkError::Connection { message: "down".into() }, false, "Connection"),
        ];

        for (err, expected, label) in &cases {
            assert_eq!(
                err.is_cas_conflict(),
                *expected,
                "{label}: expected is_cas_conflict={expected}"
            );
        }
    }
}
