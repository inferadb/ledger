//! Error types for SDK operations.
//!
//! [`SdkError`] is the single error type returned by all SDK operations.
//! Use [`SdkError::is_retryable`] to decide whether to retry an operation,
//! and [`SdkError::server_error_details`] to access structured error context
//! decoded from the wire protocol's error frame.
//!
//! See the [`SdkError`] recovery table for a per-variant breakdown of
//! retryability and recommended handling.

use inferadb_ledger_types::Region;
use inferadb_ledger_wire::ErrorCode;
use inferadb_ledger_wire_transport::TransportError;

/// Result type alias for SDK operations.
pub type Result<T> = std::result::Result<T, SdkError>;

/// Format an RPC error message with optional correlation IDs.
fn format_rpc_error(
    code: &ErrorCode,
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

/// Converts a wire-protocol error code to a short snake_case label for metrics.
fn code_to_label(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::NotFound => "not_found",
        ErrorCode::AlreadyExists => "already_exists",
        ErrorCode::FailedPrecondition => "failed_precondition",
        ErrorCode::PermissionDenied => "permission_denied",
        ErrorCode::InvalidArgument => "invalid_argument",
        ErrorCode::Internal => "internal",
        ErrorCode::Unauthenticated => "unauthenticated",
        ErrorCode::RateLimited => "rate_limited",
        ErrorCode::Expired => "expired",
        ErrorCode::TooManyAttempts => "too_many_attempts",
        ErrorCode::InvitationRateLimited => "invitation_rate_limited",
        ErrorCode::InvitationAlreadyResolved => "invitation_already_resolved",
        ErrorCode::InvitationEmailMismatch => "invitation_email_mismatch",
        ErrorCode::InvitationAlreadyMember => "invitation_already_member",
        ErrorCode::InvitationDuplicatePending => "invitation_duplicate_pending",
        ErrorCode::StaleRouting => "stale_routing",
        ErrorCode::Deprecated => "deprecated",
    }
}

/// Structured error details decoded from a wire-protocol error frame.
///
/// When the server attaches structured context to a wire error, the SDK
/// surfaces it on `SdkError::Rpc` and `SdkError::RateLimited` variants.
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

/// A hint about the current Raft leader, extracted from server `ErrorDetails`
/// on a `NotLeader` response.
///
/// All fields are optional. The server omits fields it does not know; any
/// `None` field should be treated as "unknown". Returned by
/// [`ServerErrorDetails::leader_hint`].
///
/// The SDK uses this hint to update its [`RegionLeaderCache`](crate::RegionLeaderCache)
/// and [`VaultLeaderCache`](crate::VaultLeaderCache) before the next retry,
/// avoiding a full resolve round-trip.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LeaderHint, SdkError};
/// fn redirect_from_error(err: &SdkError) -> Option<LeaderHint> {
///     err.server_error_details().and_then(|d| d.leader_hint())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderHint {
    /// Leader node ID, if known.
    pub leader_id: Option<u64>,
    /// Leader endpoint URI, if known (e.g. `"http://10.0.2.5:5000"`).
    pub leader_endpoint: Option<String>,
    /// Raft term the leader was last observed in, if known.
    ///
    /// Cache writes are term-gated: a hint with a lower term than what is
    /// currently cached is ignored to prevent stale redirects.
    pub term: Option<u64>,
    /// Internal organization ID the leader is for, if known.
    ///
    /// Populated by the server when the rejection is scoped to a known
    /// organization. The SDK exposes this for consumer logging and
    /// correlation; the leader caches themselves are keyed by slug
    /// (see [`organization_slug`](Self::organization_slug)), so the
    /// internal ID is not used for routing decisions.
    pub organization_id: Option<u64>,
    /// Internal vault ID the leader is for, if known.
    ///
    /// `None` for region- and org-scoped rejections, and for legacy servers
    /// that pre-date this field. When present, the SDK uses it together with
    /// [`organization_id`](Self::organization_id) to update the per-vault
    /// leader cache entry.
    pub vault_id: Option<u64>,
    /// Organization Snowflake slug the leader is for, if known.
    ///
    /// Populated from the server-side hint when available. Used to key the
    /// [`VaultLeaderCache`](crate::VaultLeaderCache) without a slug-to-ID
    /// round-trip. `None` for legacy servers that pre-date this field.
    pub organization_slug: Option<u64>,
    /// Vault Snowflake slug the leader is for, if known.
    ///
    /// Paired with [`organization_slug`](Self::organization_slug) to update
    /// the [`VaultLeaderCache`](crate::VaultLeaderCache). `None` for region-
    /// and org-scoped rejections, and for legacy servers that pre-date this
    /// field.
    pub vault_slug: Option<u64>,
}

impl ServerErrorDetails {
    /// Extracts a leader hint from the structured context map.
    ///
    /// Returns `None` when no recognized hint fields are present or parseable.
    /// Empty-string endpoint values are treated as absent to prevent dialing
    /// an empty URI. Any field that is missing or not parseable is left as
    /// `None` in the returned [`LeaderHint`].
    #[must_use]
    pub fn leader_hint(&self) -> Option<LeaderHint> {
        let leader_id = self.context.get("leader_id").and_then(|s| s.parse().ok());
        let leader_endpoint =
            self.context.get("leader_endpoint").filter(|s| !s.is_empty()).cloned();
        let term = self.context.get("leader_term").and_then(|s| s.parse().ok());
        let organization_id = self.context.get("leader_shard").and_then(|s| s.parse().ok());
        let vault_id = self.context.get("leader_vault").and_then(|s| s.parse().ok());
        let organization_slug =
            self.context.get("leader_organization_slug").and_then(|s| s.parse().ok());
        let vault_slug = self.context.get("leader_vault_slug").and_then(|s| s.parse().ok());

        if leader_id.is_none()
            && leader_endpoint.is_none()
            && term.is_none()
            && organization_id.is_none()
            && vault_id.is_none()
            && organization_slug.is_none()
            && vault_slug.is_none()
        {
            return None;
        }
        Some(LeaderHint {
            leader_id,
            leader_endpoint,
            term,
            organization_id,
            vault_id,
            organization_slug,
            vault_slug,
        })
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
/// | `Transport`          | Yes       | TLS or QUIC failure; verify certificates and connectivity   |
/// | `Rpc`                | Depends   | Check `is_retryable()`; see error code for details          |
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

    /// Transport-level error (QUIC connection, TLS handshake, codec).
    ///
    /// **Recovery**: Retryable. Verify TLS certificates are valid and the
    /// network path supports QUIC (UDP). Network issues typically resolve on retry.
    #[error("Transport error: {source}")]
    Transport {
        /// Underlying wire-transport error.
        source: TransportError,
    },

    /// RPC error with wire error code and optional correlation IDs.
    ///
    /// When the server populates `request_id` / `trace_id` in the response
    /// frame header, these fields are extracted into the error so operators
    /// can correlate SDK errors with server-side canonical log lines.
    #[error("{}", format_rpc_error(code, message, request_id, trace_id))]
    Rpc {
        /// Wire-protocol error code.
        code: ErrorCode,
        /// Error message from server.
        message: String,
        /// Server-assigned request ID for log correlation.
        request_id: Option<String>,
        /// Distributed trace ID for cross-service correlation.
        trace_id: Option<String>,
        /// Structured error details decoded from the wire error frame.
        error_details: Option<Box<ServerErrorDetails>>,
    },

    /// Rate limit exceeded.
    ///
    /// Returned when the server responds with [`ErrorCode::RateLimited`] and
    /// includes a non-zero `retry_after_ms`. The `retry_after` duration tells
    /// the caller how long to wait before retrying.
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
        /// Structured error details decoded from the wire error frame.
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
    /// - [`ErrorCode::StaleRouting`]: Leader changed, redirect and retry
    /// - [`ErrorCode::RateLimited`] / [`ErrorCode::TooManyAttempts`] /
    ///   [`ErrorCode::InvitationRateLimited`]: Rate-limited (but prefer `RateLimited` variant's
    ///   `retry_after`)
    /// - `RateLimited`: Explicitly rate-limited with retry-after hint
    /// - Transport / Connection errors (network issues)
    /// - Timeouts and stream disconnects
    ///
    /// Non-retryable errors:
    /// - [`ErrorCode::InvalidArgument`]: Request is malformed
    /// - [`ErrorCode::PermissionDenied`]: Authorization failure
    /// - [`ErrorCode::Unauthenticated`]: Missing/invalid credentials
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
                ErrorCode::StaleRouting
                    | ErrorCode::RateLimited
                    | ErrorCode::TooManyAttempts
                    | ErrorCode::InvitationRateLimited
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
    /// This matches only [`ErrorCode::FailedPrecondition`], which the server
    /// returns when a [`SetCondition`](crate::SetCondition) evaluates to
    /// false. Use this to distinguish CAS conflicts from other error types
    /// without importing [`ErrorCode`] at call sites.
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
        matches!(self, Self::Rpc { code: ErrorCode::FailedPrecondition, .. })
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

    /// Returns the wire error code if this is an RPC or rate-limited error.
    #[must_use]
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
            Self::RateLimited { .. } => Some(ErrorCode::RateLimited),
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
    /// Error details are decoded from the wire-protocol error frame. They
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
                    code: ErrorCode::StaleRouting,
                    message: "leader moved".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/StaleRouting",
            ),
            (
                SdkError::Rpc {
                    code: ErrorCode::RateLimited,
                    message: "limited".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/RateLimited",
            ),
            (
                SdkError::Rpc {
                    code: ErrorCode::TooManyAttempts,
                    message: "too many".into(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                },
                true,
                "Rpc/TooManyAttempts",
            ),
            // Non-retryable RPC codes
            (
                SdkError::Rpc {
                    code: ErrorCode::InvalidArgument,
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
                    code: ErrorCode::PermissionDenied,
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
                    code: ErrorCode::Unauthenticated,
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
                    code: ErrorCode::NotFound,
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
                    code: ErrorCode::Internal,
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

    /// code() returns the wire ErrorCode for Rpc/RateLimited, None for other variants.
    #[test]
    fn test_code_accessor() {
        // Rpc variant returns its code
        let err = SdkError::Rpc {
            code: ErrorCode::NotFound,
            message: "not found".to_owned(),
            request_id: None,
            trace_id: None,
            error_details: None,
        };
        assert_eq!(err.code(), Some(ErrorCode::NotFound));

        // RateLimited maps to ErrorCode::RateLimited
        let err = SdkError::RateLimited {
            message: "throttled".into(),
            retry_after: std::time::Duration::from_secs(1),
            request_id: None,
            trace_id: None,
            error_details: None,
        };
        assert_eq!(err.code(), Some(ErrorCode::RateLimited));

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
            code: ErrorCode::Internal,
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
            code: ErrorCode::Internal,
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

    /// Table-driven: is_cas_conflict() only true for FailedPrecondition RPC errors.
    #[test]
    fn test_is_cas_conflict_all_cases() {
        let cases: Vec<(SdkError, bool, &str)> = vec![
            (
                SdkError::Rpc {
                    code: ErrorCode::FailedPrecondition,
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
                    code: ErrorCode::NotFound,
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

    #[test]
    fn server_error_details_leader_hint_all_fields() {
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "42".to_owned()),
                ("leader_endpoint".to_owned(), "http://10.0.2.5:5000".to_owned()),
                ("leader_term".to_owned(), "7".to_owned()),
                ("leader_shard".to_owned(), "5".to_owned()),
                ("leader_vault".to_owned(), "99".to_owned()),
            ]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.leader_id, Some(42));
        assert_eq!(hint.leader_endpoint.as_deref(), Some("http://10.0.2.5:5000"));
        assert_eq!(hint.term, Some(7));
        assert_eq!(hint.organization_id, Some(5));
        assert_eq!(hint.vault_id, Some(99));
    }

    #[test]
    fn server_error_details_leader_hint_vault_absent_means_none() {
        // Legacy servers (pre-vault hint) and region/org-scoped rejections
        // omit the leader_vault key. The hint still parses, with vault_id = None.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "42".to_owned()),
                ("leader_endpoint".to_owned(), "http://10.0.2.5:5000".to_owned()),
                ("leader_term".to_owned(), "7".to_owned()),
                ("leader_shard".to_owned(), "5".to_owned()),
            ]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.leader_id, Some(42));
        assert_eq!(hint.organization_id, Some(5));
        assert_eq!(hint.vault_id, None);
    }

    #[test]
    fn server_error_details_leader_hint_only_vault() {
        // A hint with only the vault key still triggers a Some result —
        // even minimal vault-only updates are propagated.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([(
                "leader_vault".to_owned(),
                "77".to_owned(),
            )]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.vault_id, Some(77));
        assert_eq!(hint.leader_id, None);
        assert_eq!(hint.leader_endpoint, None);
        assert_eq!(hint.term, None);
        assert_eq!(hint.organization_id, None);
    }

    #[test]
    fn server_error_details_leader_hint_malformed_vault_ignored() {
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "1".to_owned()),
                ("leader_vault".to_owned(), "not-a-number".to_owned()),
            ]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.leader_id, Some(1));
        assert_eq!(hint.vault_id, None);
    }

    #[test]
    fn server_error_details_leader_hint_partial() {
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([("leader_id".to_owned(), "42".to_owned())]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.leader_id, Some(42));
        assert_eq!(hint.leader_endpoint, None);
        assert_eq!(hint.term, None);
        assert_eq!(hint.vault_id, None);
    }

    #[test]
    fn server_error_details_leader_hint_absent() {
        let details = ServerErrorDetails {
            error_code: "3204".into(),
            is_retryable: false,
            retry_after_ms: None,
            context: std::collections::HashMap::new(),
            suggested_action: None,
        };
        assert!(details.leader_hint().is_none());
    }

    #[test]
    fn server_error_details_leader_hint_malformed_ids_ignored() {
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([(
                "leader_id".to_owned(),
                "not-a-number".to_owned(),
            )]),
            suggested_action: None,
        };
        // Returns None overall if all three fields are unparseable/absent.
        assert!(details.leader_hint().is_none());
    }

    #[test]
    fn server_error_details_leader_hint_empty_endpoint_treated_as_absent() {
        // Server guards against emitting empty endpoints (per error_details.rs),
        // but an adversarial/malformed response could still send one. Treat it
        // as absent to avoid dialing an empty URI.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_endpoint".to_owned(), String::new()),
                ("leader_id".to_owned(), "42".to_owned()),
            ]),
            suggested_action: None,
        };
        let hint = details.leader_hint().unwrap();
        assert_eq!(hint.leader_id, Some(42));
        assert_eq!(hint.leader_endpoint, None); // empty string filtered to None
    }
}
