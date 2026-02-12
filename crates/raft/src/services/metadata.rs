//! Correlation metadata helpers for gRPC responses.
//!
//! Injects `x-request-id` and `x-trace-id` into response metadata, ensuring
//! that both successful responses and error statuses carry correlation IDs for
//! debugging and SDK error enrichment. Also attaches structured
//! [`ErrorDetails`](inferadb_ledger_proto::proto::ErrorDetails) to error statuses for
//! machine-readable error handling.

use std::collections::HashMap;

use inferadb_ledger_proto::proto;
use prost::Message;
use tonic::{Response, Status};

/// Injects `x-request-id` and `x-trace-id` correlation metadata into a gRPC response.
///
/// Called on every successful response to propagate server-generated correlation IDs
/// back to the SDK, where they are extracted and attached to `SdkError` variants.
pub(crate) fn response_with_correlation<T>(
    body: T,
    request_id: &uuid::Uuid,
    trace_id: &str,
) -> Response<T> {
    let mut response = Response::new(body);
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(&request_id.to_string()) {
        response.metadata_mut().insert("x-request-id", val);
    }
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(trace_id) {
        response.metadata_mut().insert("x-trace-id", val);
    }
    response
}

/// Injects correlation metadata and structured error details into a gRPC error `Status`.
///
/// Ensures that even error responses carry:
/// 1. Correlation IDs (`x-request-id`, `x-trace-id`) for SDK error enrichment.
/// 2. Binary-encoded [`ErrorDetails`] in `Status.details` for machine-readable error handling
///    (error code, retryability, recovery guidance).
///
/// If the status already has non-empty details (set by specialized error builders
/// in [`super::error_details`]), those are preserved. Otherwise, a generic
/// `ErrorDetails` is synthesized from the gRPC status code.
pub(crate) fn status_with_correlation(
    status: Status,
    request_id: &uuid::Uuid,
    trace_id: &str,
) -> Status {
    // If details are already populated (from a specialized error builder), preserve them.
    // Otherwise, synthesize generic ErrorDetails from the gRPC code.
    let status = if status.details().is_empty() {
        let details = error_details_from_code(status.code());
        let encoded = details.encode_to_vec();
        Status::with_details(status.code(), status.message(), encoded.into())
    } else {
        status
    };

    let mut status = status;
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(&request_id.to_string()) {
        status.metadata_mut().insert("x-request-id", val);
    }
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(trace_id) {
        status.metadata_mut().insert("x-trace-id", val);
    }
    status
}

/// Synthesizes a generic `ErrorDetails` from a gRPC status code.
///
/// Maps each gRPC code to the most appropriate `ErrorCode`, retryability flag,
/// and recovery guidance. Used as a fallback when specialized error builders
/// weren't used at the call site.
pub(crate) fn error_details_from_code(code: tonic::Code) -> proto::ErrorDetails {
    use inferadb_ledger_types::ErrorCode;

    let (error_code, is_retryable, suggested_action) = match code {
        tonic::Code::InvalidArgument => (
            ErrorCode::AppInvalidArgument,
            false,
            "Fix the request parameters to conform to field limits",
        ),
        tonic::Code::NotFound => {
            (ErrorCode::AppEntityNotFound, false, "Verify the resource exists before retrying")
        },
        tonic::Code::AlreadyExists => {
            (ErrorCode::AppAlreadyCommitted, false, "Operation already succeeded; no retry needed")
        },
        tonic::Code::ResourceExhausted => {
            (ErrorCode::AppInternal, true, "Reduce request rate or wait before retrying")
        },
        tonic::Code::FailedPrecondition => (
            ErrorCode::AppPreconditionFailed,
            false,
            "Check preconditions and retry with updated values",
        ),
        tonic::Code::Unavailable => (
            ErrorCode::ConsensusNotLeader,
            true,
            "Retry against a different node or wait for leader election",
        ),
        tonic::Code::DeadlineExceeded => {
            (ErrorCode::AppInternal, true, "Increase timeout or reduce request complexity")
        },
        tonic::Code::Internal => (ErrorCode::AppInternal, false, "Check server logs for details"),
        tonic::Code::Aborted => {
            (ErrorCode::AppInternal, true, "Retry the operation; conflict may have resolved")
        },
        _ => (ErrorCode::AppInternal, false, "Check server logs for details"),
    };

    proto::ErrorDetails {
        error_code: error_code.as_u16().to_string(),
        is_retryable,
        retry_after_ms: None,
        context: HashMap::new(),
        suggested_action: Some(suggested_action.to_owned()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_response_with_correlation_injects_metadata() {
        let request_id = uuid::Uuid::new_v4();
        let response = response_with_correlation("body", &request_id, "trace-abc");

        let md = response.metadata();
        assert_eq!(md.get("x-request-id").unwrap().to_str().unwrap(), request_id.to_string());
        assert_eq!(md.get("x-trace-id").unwrap().to_str().unwrap(), "trace-abc");
    }

    #[test]
    fn test_status_with_correlation_injects_metadata_and_details() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::invalid_argument("bad field");
        let enriched = status_with_correlation(status, &request_id, "trace-123");

        // Verify correlation metadata
        let md = enriched.metadata();
        assert_eq!(md.get("x-request-id").unwrap().to_str().unwrap(), request_id.to_string());
        assert_eq!(md.get("x-trace-id").unwrap().to_str().unwrap(), "trace-123");

        // Verify binary error details
        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert_eq!(details.error_code, "3203"); // AppInvalidArgument
        assert!(!details.is_retryable);
        assert!(details.suggested_action.is_some());
    }

    #[test]
    fn test_status_with_correlation_preserves_existing_details() {
        let request_id = uuid::Uuid::new_v4();

        // Pre-build a status with custom details
        let custom_details = proto::ErrorDetails {
            error_code: "2000".to_owned(),
            is_retryable: true,
            retry_after_ms: Some(500),
            context: HashMap::new(),
            suggested_action: Some("custom action".to_owned()),
        };
        let encoded = custom_details.encode_to_vec();
        let status = Status::with_details(tonic::Code::Unavailable, "not leader", encoded.into());

        let enriched = status_with_correlation(status, &request_id, "trace-456");

        // Should preserve the original custom details, not overwrite
        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert_eq!(details.error_code, "2000");
        assert!(details.is_retryable);
        assert_eq!(details.retry_after_ms, Some(500));
        assert_eq!(details.suggested_action.as_deref(), Some("custom action"));
    }

    #[test]
    fn test_status_with_correlation_unavailable_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::unavailable("leader unknown");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn test_status_with_correlation_resource_exhausted_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::resource_exhausted("rate limited");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn test_status_with_correlation_deadline_exceeded_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::deadline_exceeded("timed out");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn test_status_with_correlation_internal_not_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::internal("unexpected");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(!details.is_retryable);
        assert_eq!(details.error_code, "3204"); // AppInternal
    }
}
