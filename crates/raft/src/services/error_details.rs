//! Structured error details for gRPC responses.
//!
//! Encodes [`proto::ErrorDetails`] into `Status::details` bytes so SDK clients
//! can parse error codes, retryability, and recovery guidance without string
//! matching. Backward-compatible: clients that ignore `details` still see the
//! human-readable `Status.message`.

use std::collections::HashMap;

use inferadb_ledger_proto::proto;

/// Build an [`ErrorDetails`] proto message from error attributes.
///
/// Used by service helpers ([`super::helpers::check_rate_limit`],
/// [`super::helpers::validate_operations`]) to construct structured error
/// details that are encoded into `Status::with_details()`. The generic
/// fallback path in [`super::metadata::status_with_correlation`] covers
/// error paths that don't build explicit details.
pub(crate) fn build_error_details(
    error_code: u16,
    is_retryable: bool,
    retry_after_ms: Option<i32>,
    context: HashMap<String, String>,
    suggested_action: Option<&str>,
) -> proto::ErrorDetails {
    proto::ErrorDetails {
        error_code: error_code.to_string(),
        is_retryable,
        retry_after_ms,
        context,
        suggested_action: suggested_action.map(String::from),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_build_error_details_basic() {
        let details =
            build_error_details(3203, false, None, HashMap::new(), Some("Fix parameters"));
        assert_eq!(details.error_code, "3203");
        assert!(!details.is_retryable);
        assert_eq!(details.retry_after_ms, None);
        assert!(details.context.is_empty());
        assert_eq!(details.suggested_action.as_deref(), Some("Fix parameters"));
    }

    #[test]
    fn test_build_error_details_with_retry_and_context() {
        let mut context = HashMap::new();
        context.insert("namespace_id".to_owned(), "42".to_owned());

        let details = build_error_details(3204, true, Some(1000), context, None);
        assert_eq!(details.error_code, "3204");
        assert!(details.is_retryable);
        assert_eq!(details.retry_after_ms, Some(1000));
        assert_eq!(details.context.get("namespace_id").unwrap(), "42");
        assert_eq!(details.suggested_action, None);
    }

    #[test]
    fn test_error_details_encode_decode_roundtrip() {
        let mut context = HashMap::new();
        context.insert("key".to_owned(), "value".to_owned());

        let original = build_error_details(1101, false, None, context, Some("Run integrity check"));

        let encoded = original.encode_to_vec();
        let decoded = proto::ErrorDetails::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.error_code, "1101");
        assert!(!decoded.is_retryable);
        assert_eq!(decoded.retry_after_ms, None);
        assert_eq!(decoded.context.get("key").unwrap(), "value");
        assert_eq!(decoded.suggested_action.as_deref(), Some("Run integrity check"));
    }

    #[test]
    fn test_build_error_details_all_fields_populated() {
        let mut context = HashMap::new();
        context.insert("namespace_id".to_owned(), "42".to_owned());
        context.insert("level".to_owned(), "backpressure".to_owned());

        let details =
            build_error_details(3204, true, Some(500), context, Some("Reduce request rate"));

        // Encode and decode to verify wire format fidelity
        let encoded = details.encode_to_vec();
        let decoded = proto::ErrorDetails::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.error_code, "3204");
        assert!(decoded.is_retryable);
        assert_eq!(decoded.retry_after_ms, Some(500));
        assert_eq!(decoded.context.len(), 2);
        assert_eq!(decoded.context.get("namespace_id").unwrap(), "42");
        assert_eq!(decoded.context.get("level").unwrap(), "backpressure");
        assert_eq!(decoded.suggested_action.as_deref(), Some("Reduce request rate"));
    }

    #[test]
    fn test_build_error_details_empty_details() {
        let details = build_error_details(0, false, None, HashMap::new(), None);

        let encoded = details.encode_to_vec();
        let decoded = proto::ErrorDetails::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.error_code, "0");
        assert!(!decoded.is_retryable);
        assert_eq!(decoded.retry_after_ms, None);
        assert!(decoded.context.is_empty());
        assert_eq!(decoded.suggested_action, None);
    }
}
