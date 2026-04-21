//! Structured error details for gRPC responses.
//!
//! Encodes [`proto::ErrorDetails`] into `Status::details` bytes so SDK clients
//! can parse error codes, retryability, and recovery guidance without string
//! matching. Backward-compatible: clients that ignore `details` still see the
//! human-readable `Status.message`.

use std::collections::HashMap;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::DiagnosticCode;

/// `ErrorDetails.context` key for the current Raft leader's node ID (numeric string).
pub(crate) const LEADER_ID_KEY: &str = "leader_id";
/// `ErrorDetails.context` key for the current Raft leader's endpoint URI.
pub(crate) const LEADER_ENDPOINT_KEY: &str = "leader_endpoint";
/// `ErrorDetails.context` key for the Raft term the leader was observed in (numeric string).
pub(crate) const LEADER_TERM_KEY: &str = "leader_term";
/// `ErrorDetails.context` key for the organization id the leader is for
/// (numeric string; `"0"` for the data-region group).
///
/// Distinct from the consensus-layer `ShardId` (an opaque seahash) — the
/// organization id is what the SDK uses for routing and what its
/// `RegionLeaderCache` keys on. Data-region groups emit `"0"`; per-
/// organization groups emit the organization's id.
pub(crate) const LEADER_SHARD_KEY: &str = "leader_shard";

/// Builds an [`ErrorDetails`] proto message from error attributes.
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

/// Builds an [`ErrorDetails`] proto for a `NotLeader` rejection, carrying the
/// current known leader identity so the client can redirect without issuing
/// a separate `ResolveRegionLeader` RPC.
///
/// Hint keys written into `context`:
/// - `leader_id` — numeric string (u64)
/// - `leader_endpoint` — full URI (e.g. `"http://10.0.2.5:5000"`)
/// - `leader_term` — numeric string (u64)
/// - `leader_shard` — numeric string (organization id; `"0"` for the data-region group)
///
/// Any argument may be `None` when the server has no information for that
/// field; the key is simply omitted.
pub(crate) fn build_not_leader_details(
    leader_id: Option<u64>,
    leader_endpoint: Option<&str>,
    leader_term: Option<u64>,
    leader_shard: Option<u64>,
) -> proto::ErrorDetails {
    let mut context = HashMap::new();
    if let Some(id) = leader_id {
        context.insert(LEADER_ID_KEY.to_owned(), id.to_string());
    }
    if let Some(ep) = leader_endpoint.filter(|s| !s.is_empty()) {
        context.insert(LEADER_ENDPOINT_KEY.to_owned(), ep.to_owned());
    }
    if let Some(term) = leader_term {
        context.insert(LEADER_TERM_KEY.to_owned(), term.to_string());
    }
    if let Some(shard) = leader_shard {
        context.insert(LEADER_SHARD_KEY.to_owned(), shard.to_string());
    }

    proto::ErrorDetails {
        error_code: DiagnosticCode::ConsensusNotLeader.as_u16().to_string(),
        is_retryable: true,
        retry_after_ms: None,
        context,
        suggested_action: Some(
            "Retry against the indicated leader; update your region leader cache".to_owned(),
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn build_error_details_basic() {
        let details =
            build_error_details(3203, false, None, HashMap::new(), Some("Fix parameters"));
        assert_eq!(details.error_code, "3203");
        assert!(!details.is_retryable);
        assert_eq!(details.retry_after_ms, None);
        assert!(details.context.is_empty());
        assert_eq!(details.suggested_action.as_deref(), Some("Fix parameters"));
    }

    #[test]
    fn build_error_details_with_retry_and_context() {
        let mut context = HashMap::new();
        context.insert("organization".to_owned(), "42".to_owned());

        let details = build_error_details(3204, true, Some(1000), context, None);
        assert_eq!(details.error_code, "3204");
        assert!(details.is_retryable);
        assert_eq!(details.retry_after_ms, Some(1000));
        assert_eq!(details.context.get("organization").unwrap(), "42");
        assert_eq!(details.suggested_action, None);
    }

    #[test]
    fn error_details_encode_decode_roundtrip() {
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
    fn build_error_details_all_fields_populated() {
        let mut context = HashMap::new();
        context.insert("organization".to_owned(), "42".to_owned());
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
        assert_eq!(decoded.context.get("organization").unwrap(), "42");
        assert_eq!(decoded.context.get("level").unwrap(), "backpressure");
        assert_eq!(decoded.suggested_action.as_deref(), Some("Reduce request rate"));
    }

    #[test]
    fn build_error_details_empty_details() {
        let details = build_error_details(0, false, None, HashMap::new(), None);

        let encoded = details.encode_to_vec();
        let decoded = proto::ErrorDetails::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.error_code, "0");
        assert!(!decoded.is_retryable);
        assert_eq!(decoded.retry_after_ms, None);
        assert!(decoded.context.is_empty());
        assert_eq!(decoded.suggested_action, None);
    }

    #[test]
    fn build_not_leader_details_all_fields() {
        let details =
            build_not_leader_details(Some(42), Some("http://10.0.2.5:5000"), Some(7), Some(5));
        assert_eq!(details.error_code, "2000"); // ConsensusNotLeader
        assert!(details.is_retryable);
        assert_eq!(details.context.get("leader_id").unwrap(), "42");
        assert_eq!(details.context.get("leader_endpoint").unwrap(), "http://10.0.2.5:5000");
        assert_eq!(details.context.get("leader_term").unwrap(), "7");
        assert_eq!(details.context.get("leader_shard").unwrap(), "5");
        assert!(details.suggested_action.as_deref().unwrap().contains("leader"));
    }

    #[test]
    fn build_not_leader_details_partial_fields() {
        let details = build_not_leader_details(Some(42), None, None, None);
        assert!(details.context.contains_key("leader_id"));
        assert!(!details.context.contains_key("leader_endpoint"));
        assert!(!details.context.contains_key("leader_term"));
        assert!(!details.context.contains_key("leader_shard"));
    }

    #[test]
    fn build_not_leader_details_no_hints() {
        let details = build_not_leader_details(None, None, None, None);
        assert!(details.context.is_empty());
        assert!(details.is_retryable);
    }

    #[test]
    fn build_not_leader_details_empty_endpoint_omitted() {
        let details = build_not_leader_details(Some(42), Some(""), Some(7), Some(0));
        assert!(!details.context.contains_key("leader_endpoint"));
        assert!(details.context.contains_key("leader_id"));
        assert!(details.context.contains_key("leader_term"));
        assert_eq!(details.context.get("leader_shard").unwrap(), "0");
    }

    #[test]
    fn build_not_leader_details_encode_decode() {
        let details = build_not_leader_details(Some(1), Some("http://x:5000"), Some(3), Some(12));
        let encoded = details.encode_to_vec();
        let decoded = proto::ErrorDetails::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.context.get("leader_id").unwrap(), "1");
        assert_eq!(decoded.context.get("leader_endpoint").unwrap(), "http://x:5000");
        assert_eq!(decoded.context.get("leader_term").unwrap(), "3");
        assert_eq!(decoded.context.get("leader_shard").unwrap(), "12");
    }
}
