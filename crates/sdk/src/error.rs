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

/// SDK error types with context-rich error messages.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SdkError {
    /// Failed to establish connection.
    #[snafu(display("Connection error at {location}: {message}"))]
    Connection {
        /// Error description.
        message: String,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Transport-level error (HTTP/2, TLS).
    #[snafu(display("Transport error at {location}: {source}"))]
    Transport {
        /// Underlying transport error.
        source: tonic::transport::Error,
        /// Source location.
        #[snafu(implicit)]
        location: Location,
    },

    /// gRPC RPC error with status code.
    #[snafu(display("RPC error (code={code:?}): {message}"))]
    Rpc {
        /// gRPC status code.
        code: Code,
        /// Error message from server.
        message: String,
    },

    /// Retry attempts exhausted.
    #[snafu(display("Retry exhausted after {attempts} attempts: {last_error}"))]
    RetryExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// Last error message before giving up.
        last_error: String,
    },

    /// Configuration validation error.
    #[snafu(display("Configuration error: {message}"))]
    Config {
        /// Error description.
        message: String,
    },

    /// Client-side idempotency error.
    #[snafu(display("Idempotency error: {message}"))]
    Idempotency {
        /// Error description.
        message: String,
    },

    /// Sequence gap detected - requires recovery.
    #[snafu(display("Sequence gap: server has {server_has}, client expected to send {expected}"))]
    SequenceGap {
        /// Sequence number server expected.
        expected: u64,
        /// Last committed sequence on server.
        server_has: u64,
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

    /// Streaming connection lost.
    #[snafu(display("Stream disconnected: {message}"))]
    StreamDisconnected {
        /// Disconnect reason.
        message: String,
    },

    /// Operation timed out.
    #[snafu(display("Operation timed out after {duration_ms}ms"))]
    Timeout {
        /// Timeout duration in milliseconds.
        duration_ms: u64,
    },

    /// Client is shutting down.
    #[snafu(display("Client shutting down"))]
    Shutdown,

    /// URL parsing error.
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
}

impl SdkError {
    /// Returns true if the error is transient and the operation should be retried.
    ///
    /// Retryable errors:
    /// - `UNAVAILABLE`: Server temporarily unreachable
    /// - `DEADLINE_EXCEEDED`: Request timed out
    /// - `RESOURCE_EXHAUSTED`: Rate limited
    /// - `ABORTED`: Transaction conflict (retry may succeed)
    /// - Transport errors (network issues)
    ///
    /// Non-retryable errors:
    /// - `INVALID_ARGUMENT`: Request is malformed
    /// - `PERMISSION_DENIED`: Authentication/authorization failure
    /// - `UNAUTHENTICATED`: Missing credentials
    /// - `SequenceGap`: Requires recovery flow, not automatic retry
    /// - `AlreadyCommitted`: Operation already succeeded
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            Self::Connection { .. } => true,
            Self::Timeout { .. } => true,
            Self::StreamDisconnected { .. } => true,
            Self::Rpc { code, .. } => matches!(
                code,
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
            ),
            // Non-retryable
            Self::Config { .. } => false,
            Self::SequenceGap { .. } => false,
            Self::AlreadyCommitted { .. } => false,
            Self::Idempotency { .. } => false,
            Self::RetryExhausted { .. } => false,
            Self::Shutdown => false,
            Self::InvalidUrl { .. } => false,
            Self::Unavailable { .. } => true, // May become available
            Self::ProofVerification { .. } => false, // Data integrity error
        }
    }

    /// Returns the gRPC status code if this is an RPC error.
    #[must_use]
    pub fn code(&self) -> Option<Code> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
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
        Self::Rpc { code: status.code(), message: status.message().to_owned() }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_error_retryable_unavailable() {
        let err =
            SdkError::Rpc { code: Code::Unavailable, message: "server unavailable".to_owned() };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_deadline_exceeded() {
        let err = SdkError::Rpc { code: Code::DeadlineExceeded, message: "timeout".to_owned() };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_resource_exhausted() {
        let err =
            SdkError::Rpc { code: Code::ResourceExhausted, message: "rate limited".to_owned() };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_retryable_aborted() {
        let err = SdkError::Rpc { code: Code::Aborted, message: "transaction conflict".to_owned() };
        assert!(err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_invalid_argument() {
        let err = SdkError::Rpc { code: Code::InvalidArgument, message: "bad request".to_owned() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_permission_denied() {
        let err =
            SdkError::Rpc { code: Code::PermissionDenied, message: "access denied".to_owned() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_rpc_error_non_retryable_unauthenticated() {
        let err =
            SdkError::Rpc { code: Code::Unauthenticated, message: "not authenticated".to_owned() };
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
    fn test_sequence_gap_not_retryable() {
        let err = SdkError::SequenceGap { expected: 5, server_has: 4 };
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
        let err = SdkError::Rpc { code: Code::NotFound, message: "not found".to_owned() };
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
}
