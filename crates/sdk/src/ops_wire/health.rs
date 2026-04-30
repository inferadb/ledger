//! Wire-based health-check op.
//!
//! Mirrors [`super::super::ops::health`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`HealthServiceClient`](inferadb_ledger_wire_services::HealthServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! [`RpcError`] → [`SdkError`] conversion and the wire-response → domain-type
//! mapping in isolation. The `check_health` / `check_health_vault` entry
//! points themselves are not exercised yet — [`LedgerClient`](crate::LedgerClient)
//! keeps dispatching through the tonic `ops::health` path until F.1 flips
//! the connection pool.

use std::sync::Arc;

use inferadb_ledger_wire::services::{health as w, shared as ws};
use inferadb_ledger_wire_services::HealthServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::Result,
    ops_wire::rpc_error_to_sdk_error,
    types::admin::{HealthCheckResult, HealthStatus},
};

/// Issue a node-level health check via the wire transport.
///
/// Mirrors [`LedgerClient::health_check_detailed`](crate::LedgerClient::health_check_detailed)
/// at the dispatch layer. The call goes out as
/// [`HealthService::check`](inferadb_ledger_wire_services::HealthService) with
/// no organization / vault scoping; the server returns the node's overall
/// health.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's retry
/// loop is responsible for rotating it across attempts (the wire transport
/// does not generate one for the caller).
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (rate-limit family, migration codes, all others land in `Rpc`).
pub(crate) async fn check_health(
    wire_client: Arc<WireClient>,
    request_id: u128,
) -> Result<HealthCheckResult> {
    let client = HealthServiceClient::new(wire_client);
    let request = w::HealthCheckRequest { organization: None, vault: None };
    let response = client.check(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_health_check_result_from_wire(response))
}

/// Issue a vault-scoped health check via the wire transport.
///
/// Mirrors [`LedgerClient::health_check_vault`](crate::LedgerClient::health_check_vault)
/// at the dispatch layer. Both the organization and vault slugs are
/// required — the server uses them to locate the per-vault Raft group and
/// report block height, divergence, and recovery state in
/// [`HealthCheckResult::details`].
///
/// # Errors
///
/// Same shape as [`check_health`] — transport failures become
/// [`SdkError::Connection`]; server-returned [`WireError`]s flow through
/// [`from_wire_error_with_correlation`].
pub(crate) async fn check_health_vault(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: ws::OrganizationSlug,
    vault: ws::VaultSlug,
) -> Result<HealthCheckResult> {
    let client = HealthServiceClient::new(wire_client);
    let request = w::HealthCheckRequest { organization: Some(organization), vault: Some(vault) };
    let response = client.check(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_health_check_result_from_wire(response))
}

/// Maps a wire `HealthCheckResponse` into the SDK's domain
/// [`HealthCheckResult`].
///
/// `status` translates by integer tag (the wire enum is `#[repr(i32)]` with
/// the same values as the proto enum); unknown tags collapse to
/// [`HealthStatus::Unspecified`]. `details` widens from the wire's
/// `BTreeMap<String, String>` to the domain type's `HashMap<String, String>`.
fn domain_health_check_result_from_wire(resp: w::HealthCheckResponse) -> HealthCheckResult {
    HealthCheckResult {
        status: wire_health_status_to_domain(resp.status),
        message: resp.message,
        details: resp.details.into_iter().collect(),
    }
}

/// Numeric-tag mapping from the wire `HealthStatus` to the domain enum.
///
/// Mirrors the proto-side `HealthStatus::from_proto(i32)` in
/// [`crate::types::admin`]. An unknown tag (the `#[non_exhaustive]` reach
/// in the wire enum is on the response shape, not the discriminant — but
/// future additions land here) collapses to `Unspecified` rather than
/// panicking.
fn wire_health_status_to_domain(status: ws::HealthStatus) -> HealthStatus {
    match status {
        ws::HealthStatus::Healthy => HealthStatus::Healthy,
        ws::HealthStatus::Degraded => HealthStatus::Degraded,
        ws::HealthStatus::Unavailable => HealthStatus::Unavailable,
        ws::HealthStatus::Unspecified => HealthStatus::Unspecified,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the `RpcError` → `SdkError` shim and the
    //! wire-response → domain-type mapping. End-to-end tests against a real
    //! `WireClient` / `WireServer` are deferred to E.7 (TestCluster
    //! migration) — at that point [`check_health`] / [`check_health_vault`]
    //! get exercised in full.

    use std::collections::BTreeMap;

    use inferadb_ledger_wire::{ErrorCode as WireErrorCode, WireError};
    use inferadb_ledger_wire_transport::{RpcError, TransportError};

    use super::*;
    use crate::error::SdkError;

    #[test]
    fn rpc_error_wire_error_maps_to_sdk_error() {
        // RpcError::WireError(_) must round-trip through the shared
        // `from_wire_error_with_correlation` helper — same path as the
        // tonic shim — so the resulting SdkError carries the right code.
        let wire_err = WireError {
            code: WireErrorCode::FailedPrecondition,
            message: "bad state".to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let sdk_err = rpc_error_to_sdk_error(RpcError::WireError(wire_err));
        match sdk_err {
            SdkError::Rpc { code, message, .. } => {
                assert_eq!(code, WireErrorCode::FailedPrecondition);
                assert_eq!(message, "bad state");
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_transport_maps_to_connection() {
        // Transport-level failure surfaces as SdkError::Connection so
        // the SDK retry loop classifies it as retryable rather than a
        // server-returned semantic error.
        let transport_err = TransportError::AuthHandshakeFailed { reason: "tls verify".to_owned() };
        let sdk_err = rpc_error_to_sdk_error(RpcError::Transport(transport_err));
        match sdk_err {
            SdkError::Connection { message } => {
                assert!(message.contains("wire transport error"), "unexpected message: {message}");
                assert!(message.contains("tls verify"), "unexpected message: {message}");
            },
            other => panic!("expected Connection, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_opcode_mismatch_maps_to_connection() {
        // Opcode mismatch is a protocol-level dispatch failure — the SDK
        // can't reach a real handler, so it lands in Connection.
        let sdk_err =
            rpc_error_to_sdk_error(RpcError::OpcodeMismatch { expected: 0x0B00, actual: 0x0B01 });
        match sdk_err {
            SdkError::Connection { message } => {
                assert!(
                    message.contains("0x0b00") && message.contains("0x0b01"),
                    "unexpected message: {message}"
                );
            },
            other => panic!("expected Connection, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_protocol_violation_maps_to_connection() {
        let sdk_err = rpc_error_to_sdk_error(RpcError::ProtocolViolation(
            "missing is_response flag".to_owned(),
        ));
        match sdk_err {
            SdkError::Connection { message } => {
                assert!(
                    message.contains("wire protocol violation"),
                    "unexpected message: {message}"
                );
                assert!(message.contains("missing is_response flag"));
            },
            other => panic!("expected Connection, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_decode_wire_error_maps_to_connection() {
        let sdk_err = rpc_error_to_sdk_error(RpcError::DecodeWireError("bad postcard".to_owned()));
        match sdk_err {
            SdkError::Connection { message } => {
                assert!(
                    message.contains("failed to decode wire error"),
                    "unexpected message: {message}"
                );
                assert!(message.contains("bad postcard"));
            },
            other => panic!("expected Connection, got {other:?}"),
        }
    }

    #[test]
    fn domain_mapping_round_trip() {
        // Build a wire response with each known status + a few details
        // entries, project through the domain helper, assert field-by-field
        // equality on the resulting HealthCheckResult.
        let mut details = BTreeMap::new();
        details.insert("current_term".to_owned(), "42".to_owned());
        details.insert("disk_writable".to_owned(), "true".to_owned());

        let wire_resp = w::HealthCheckResponse {
            status: ws::HealthStatus::Degraded,
            message: "raft lag elevated".to_owned(),
            details: details.clone(),
        };
        let domain = domain_health_check_result_from_wire(wire_resp);
        assert_eq!(domain.status, HealthStatus::Degraded);
        assert_eq!(domain.message, "raft lag elevated");
        assert_eq!(domain.details.len(), 2);
        assert_eq!(domain.details.get("current_term").map(String::as_str), Some("42"));
        assert_eq!(domain.details.get("disk_writable").map(String::as_str), Some("true"));
    }

    #[test]
    fn wire_health_status_each_variant_maps_correctly() {
        // Pin the per-variant mapping so a future addition to the wire
        // enum doesn't silently fall through to Unspecified without a
        // matching domain variant.
        assert_eq!(
            wire_health_status_to_domain(ws::HealthStatus::Unspecified),
            HealthStatus::Unspecified
        );
        assert_eq!(wire_health_status_to_domain(ws::HealthStatus::Healthy), HealthStatus::Healthy);
        assert_eq!(
            wire_health_status_to_domain(ws::HealthStatus::Degraded),
            HealthStatus::Degraded
        );
        assert_eq!(
            wire_health_status_to_domain(ws::HealthStatus::Unavailable),
            HealthStatus::Unavailable,
        );
    }
}
