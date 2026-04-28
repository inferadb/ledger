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

/// Builds a `NotLeader` `Status` with leader hints attached as `ErrorDetails`.
///
/// Prefer this over `Status::unavailable(message)` for any not-leader rejection
/// so the client can update its region leader cache directly from the error
/// path, without issuing a separate `ResolveRegionLeader` RPC.
///
/// `leader_vault` is `Some(_)` only for vault-scoped rejections (per-vault
/// Raft groups). Region- and org-scoped rejections pass `None`; the SDK
/// uses the absence of the `leader_vault` key as the signal to fall back
/// to the org-level cache entry.
pub(crate) fn status_with_not_leader_hint(
    message: impl Into<String>,
    leader_id: Option<u64>,
    leader_endpoint: Option<&str>,
    leader_term: Option<u64>,
    leader_shard: Option<u64>,
    leader_vault: Option<u64>,
) -> Status {
    let details = super::error_details::build_not_leader_details(
        leader_id,
        leader_endpoint,
        leader_term,
        leader_shard,
        leader_vault,
    );
    let encoded = details.encode_to_vec();
    Status::with_details(tonic::Code::Unavailable, message, encoded.into())
}

/// Builds a `NotLeader` `Status` by extracting `(leader_id, leader_endpoint, term)`
/// from a consensus handle and a peer-address map.
///
/// Prefer this over calling [`status_with_not_leader_hint`] directly when the
/// call site has a [`inferadb_ledger_raft::ConsensusHandle`] and peer map in
/// scope — consolidates the leader-state extraction boilerplate so all
/// not-leader rejections populate the same hint shape.
///
/// Region- and org-scoped call sites pass `None` for `leader_vault`. Vault-
/// scoped call sites (per-vault Raft groups) pass `Some(vault_id_as_u64)`
/// so the SDK can key its `VaultLeaderCache` on `(region, organization_id,
/// vault_id)` rather than just `(region, organization_id)`.
pub(crate) fn not_leader_status_from_handle(
    handle: &inferadb_ledger_raft::ConsensusHandle,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
    message: impl Into<String>,
    leader_vault: Option<u64>,
) -> Status {
    let shard_state = handle.shard_state();
    let term = handle.current_term();
    let leader_id = shard_state.leader.map(|n| n.0);
    let leader_endpoint =
        leader_id.and_then(|id| peer_addresses.and_then(|m| m.get(id))).map(ensure_endpoint_url);
    // OrganizationId is i64 (Snowflake); the leader-hint wire field is u64.
    // Snowflake IDs are positive so this cast preserves the value.
    let leader_organization = handle.organization_id().value() as u64;
    status_with_not_leader_hint(
        message,
        leader_id,
        leader_endpoint.as_deref(),
        Some(term),
        Some(leader_organization),
        leader_vault,
    )
}

/// Builds a `NotLeader` `Status` for a cross-region redirect, carrying the
/// remote region's leader hint (if known) so the SDK can reconnect directly
/// to the target region's leader.
///
/// Note: `RoutingInfo.leader_hint` is currently always `None` for cross-region
/// redirects — see the doc on [`super::region_resolver::RoutingInfo::leader_hint`]
/// for why. The helper remains correct: passing `None` makes the SDK fall back
/// to `ResolveRegionLeader` / `WatchLeader` on an in-region node.
pub(crate) fn not_leader_remote_region(
    redirect: &super::region_resolver::RedirectInfo,
    message: impl Into<String>,
) -> Status {
    // Cross-region redirects don't include a per-shard hint: the local
    // node has no view into the remote region's per-shard leadership map
    // (each `(region, shard)` is its own Raft group). The SDK's
    // `RegionLeaderCache` falls back to `ResolveRegionLeader` /
    // `WatchLeader` against an in-region node to learn shard leaders.
    // The vault hint is also absent for the same reason.
    status_with_not_leader_hint(
        message,
        None,
        redirect.routing.leader_hint.as_deref(),
        None,
        None,
        None,
    )
}

/// Prepends `http://` if the address has no URI scheme.
///
/// Normalizes a peer address into a valid endpoint URL for client connections.
///
/// Peer addresses are stored as bare `host:port` strings or Unix socket paths.
/// Client-facing leader hints must be valid URIs so the SDK can pass them to
/// `tonic::transport::Endpoint`. UDS paths (starting with `/`) and addresses
/// with an explicit scheme are returned as-is; bare `host:port` gets `http://`
/// prepended.
///
/// `pub(super)` so sibling modules (`discovery`, etc.) can share this helper.
pub(super) fn ensure_endpoint_url(addr: String) -> String {
    if addr.starts_with('/') || addr.contains("://") { addr } else { format!("http://{addr}") }
}

/// Synthesizes a generic `ErrorDetails` from a gRPC status code.
///
/// Maps each gRPC code to the most appropriate `DiagnosticCode`, retryability flag,
/// and recovery guidance. Used as a fallback when specialized error builders
/// weren't used at the call site.
pub(crate) fn error_details_from_code(code: tonic::Code) -> proto::ErrorDetails {
    use inferadb_ledger_types::DiagnosticCode;

    let (error_code, is_retryable, suggested_action) = match code {
        tonic::Code::InvalidArgument => (
            DiagnosticCode::AppInvalidArgument,
            false,
            "Fix the request parameters to conform to field limits",
        ),
        tonic::Code::NotFound => {
            (DiagnosticCode::AppEntityNotFound, false, "Verify the resource exists before retrying")
        },
        tonic::Code::AlreadyExists => (
            DiagnosticCode::AppAlreadyCommitted,
            false,
            "Operation already succeeded; no retry needed",
        ),
        tonic::Code::ResourceExhausted => {
            (DiagnosticCode::AppQuotaExceeded, true, "Reduce request rate or wait before retrying")
        },
        tonic::Code::FailedPrecondition => (
            DiagnosticCode::AppPreconditionFailed,
            false,
            "Check preconditions and retry with updated values",
        ),
        tonic::Code::Unavailable => (
            DiagnosticCode::ConsensusNotLeader,
            true,
            "Retry against a different node or wait for leader election",
        ),
        tonic::Code::DeadlineExceeded => {
            (DiagnosticCode::AppInternal, true, "Increase timeout or reduce request complexity")
        },
        tonic::Code::Internal => {
            (DiagnosticCode::AppInternal, false, "Check server logs for details")
        },
        tonic::Code::Aborted => {
            (DiagnosticCode::AppInternal, true, "Retry the operation; conflict may have resolved")
        },
        _ => (DiagnosticCode::AppInternal, false, "Check server logs for details"),
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
    fn response_with_correlation_injects_metadata() {
        let request_id = uuid::Uuid::new_v4();
        let response = response_with_correlation("body", &request_id, "trace-abc");

        let md = response.metadata();
        assert_eq!(md.get("x-request-id").unwrap().to_str().unwrap(), request_id.to_string());
        assert_eq!(md.get("x-trace-id").unwrap().to_str().unwrap(), "trace-abc");
    }

    #[test]
    fn status_with_correlation_injects_metadata_and_details() {
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
    fn status_with_correlation_preserves_existing_details() {
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
    fn status_with_correlation_unavailable_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::unavailable("leader unknown");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn status_with_correlation_resource_exhausted_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::resource_exhausted("rate limited");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn status_with_correlation_deadline_exceeded_is_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::deadline_exceeded("timed out");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(details.is_retryable);
    }

    #[test]
    fn status_with_not_leader_hint_populates_details() {
        let status = status_with_not_leader_hint(
            "not leader for region us-east-va shard 5 vault 99",
            Some(42),
            Some("http://10.0.2.5:5000"),
            Some(7),
            Some(5),
            Some(99),
        );
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert!(details.is_retryable);
        assert_eq!(details.context.get("leader_id").unwrap(), "42");
        assert_eq!(details.context.get("leader_endpoint").unwrap(), "http://10.0.2.5:5000");
        assert_eq!(details.context.get("leader_term").unwrap(), "7");
        assert_eq!(details.context.get("leader_shard").unwrap(), "5");
        assert_eq!(details.context.get("leader_vault").unwrap(), "99");
    }

    #[test]
    fn status_with_not_leader_hint_survives_correlation() {
        let status = status_with_not_leader_hint("not leader", Some(1), None, None, Some(0), None);
        let request_id = uuid::Uuid::new_v4();
        let enriched = status_with_correlation(status, &request_id, "trace");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert_eq!(details.context.get("leader_id").unwrap(), "1");
        assert_eq!(details.context.get("leader_shard").unwrap(), "0");
        assert!(!details.context.contains_key("leader_vault"));
    }

    #[test]
    fn status_with_not_leader_hint_omits_vault_when_none() {
        // Region- and org-scoped rejections pass leader_vault = None;
        // the resulting ErrorDetails MUST omit the leader_vault key so the
        // SDK falls back to its org-level cache.
        let status =
            status_with_not_leader_hint("not leader", Some(1), None, Some(2), Some(3), None);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert!(!details.context.contains_key("leader_vault"));
        assert_eq!(details.context.get("leader_shard").unwrap(), "3");
    }

    #[test]
    fn not_leader_remote_region_with_endpoint_hint_populates_details() {
        use inferadb_ledger_types::{OrganizationId, Region};

        use crate::services::region_resolver::{RedirectInfo, RoutingInfo};

        let routing = RoutingInfo {
            region: Region::US_EAST_VA,
            leader_hint: Some("node-1:50051".to_string()),
        };
        let remote = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let status = not_leader_remote_region(&remote, "remote region");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert_eq!(
            details.context.get("leader_endpoint").map(String::as_str),
            Some("node-1:50051")
        );
        assert!(details.is_retryable);
    }

    #[test]
    fn not_leader_remote_region_without_hint_omits_endpoint() {
        use inferadb_ledger_types::{OrganizationId, Region};

        use crate::services::region_resolver::{RedirectInfo, RoutingInfo};

        let routing = RoutingInfo { region: Region::US_EAST_VA, leader_hint: None };
        let remote = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let status = not_leader_remote_region(&remote, "remote region");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert!(!details.context.contains_key("leader_endpoint"));
    }

    #[test]
    fn status_with_correlation_internal_not_retryable() {
        let request_id = uuid::Uuid::new_v4();
        let status = Status::internal("unexpected");
        let enriched = status_with_correlation(status, &request_id, "t");

        let details = proto::ErrorDetails::decode(enriched.details()).unwrap();
        assert!(!details.is_retryable);
        assert_eq!(details.error_code, "3204"); // AppInternal
    }
}
