//! Wire-based relationship op (check_relationship).
//!
//! Mirrors [`super::super::ops::relationship`]: same returned domain types,
//! same `Result<_, SdkError>` shape on the consumer side. Internal dispatch
//! goes through the wire-services-generated
//! [`ReadServiceClient`](inferadb_ledger_wire_services::ReadServiceClient)
//! instead of the proto-generated tonic client.
//!
//! Scope: only `check_relationship` migrates here. `list_relationships` is a
//! sibling task (it ships with the rest of the list-style read ops in
//! `ops::list`), so splitting keeps each file's surface area small and
//! mirrors the tonic-side file partition.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! [`ReadConsistency`] mapping, the
//! [`RpcError`](inferadb_ledger_wire_transport::RpcError) →
//! [`SdkError`](crate::SdkError) conversion, the wire-response →
//! [`CheckRelationshipOutcome`] projection, and the
//! [`CheckRelationshipRequest`] / [`CheckRelationshipResponse`] postcard
//! round-trip. The entry point itself is not exercised yet —
//! [`LedgerClient`](crate::LedgerClient) keeps dispatching through the tonic
//! `ops::relationship` path until F.1 flips the connection pool.

use std::sync::Arc;

use inferadb_ledger_wire::services::{read as wr, shared as ws};
use inferadb_ledger_wire_services::ReadServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::Result,
    ops_wire::rpc_error_to_sdk_error,
    types::{query::CheckRelationshipOutcome, read::ReadConsistency},
};

// ---------------------------------------------------------------------------
// CheckRelationship
// ---------------------------------------------------------------------------

/// Issue a `CheckRelationship` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::check_relationship`](crate::LedgerClient::check_relationship)
/// at the dispatch layer. Returns the storage-primitive existence outcome —
/// the SDK does not evaluate authorization policy or schema-driven inheritance
/// here; the server only answers "is this exact `(resource, relation,
/// subject)` tuple stored?". `vault` is required (relationships are always
/// vault-scoped); `caller` identifies the user on whose behalf the call is
/// made.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's
/// retry loop is responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (rate-limit family, migration codes, all others land in `Rpc`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_relationship(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: ws::VaultSlug,
    resource: String,
    relation: String,
    subject: String,
    consistency: ReadConsistency,
) -> Result<CheckRelationshipOutcome> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::CheckRelationshipRequest {
        organization: Some(organization),
        vault: Some(vault),
        resource,
        relation,
        subject,
        consistency: domain_consistency_to_wire(consistency),
        caller: Some(caller),
    };
    let response =
        client.check_relationship(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(CheckRelationshipOutcome {
        exists: response.exists,
        checked_at_height: response.checked_at_height,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map the SDK domain `ReadConsistency` to the wire enum.
///
/// The wire enum is `#[repr(i32)]` with the same values as the proto enum;
/// the SDK domain enum has no `Unspecified` variant (Rust `Default` is
/// `Eventual`), so this is a total mapping with no fallback arm. Mirrors the
/// helper in [`super::data`] — kept private here rather than promoted to
/// `ops_wire/mod.rs` so each ops_wire file remains self-contained, matching
/// the existing pattern across health / events / invitation / etc.
fn domain_consistency_to_wire(c: ReadConsistency) -> ws::ReadConsistency {
    match c {
        ReadConsistency::Eventual => ws::ReadConsistency::Eventual,
        ReadConsistency::Linearizable => ws::ReadConsistency::Linearizable,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the `ReadConsistency` mapping, the
    //! [`RpcError`] → [`SdkError`] shim, the wire-response →
    //! [`CheckRelationshipOutcome`] projection, and the
    //! [`CheckRelationshipRequest`] / [`CheckRelationshipResponse`] postcard
    //! round-trip. End-to-end tests against a real `WireClient` /
    //! `WireServer` are deferred to E.7 (TestCluster migration) — at that
    //! point [`check_relationship`] gets exercised in full.

    use std::collections::BTreeMap;

    use inferadb_ledger_wire::{ErrorCode as WireErrorCode, WireError};
    use inferadb_ledger_wire_transport::{RpcError, TransportError};

    use super::*;
    use crate::error::SdkError;

    // -------------------------------------------------------------------
    // ReadConsistency mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_consistency_each_variant_maps_correctly() {
        // The SDK domain enum has only Eventual / Linearizable; the wire
        // enum's Unspecified variant is unreachable from this path. Pin
        // both arms explicitly so a future Default change doesn't silently
        // flip semantics.
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Eventual),
            ws::ReadConsistency::Eventual,
        );
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Linearizable),
            ws::ReadConsistency::Linearizable,
        );
    }

    // -------------------------------------------------------------------
    // CheckRelationshipRequest / CheckRelationshipResponse postcard
    // round-trips.
    // -------------------------------------------------------------------

    #[test]
    fn check_relationship_request_postcard_round_trips() {
        // Constructed through the same field shape the production fn
        // produces; confirms the wire encoding is self-inverse.
        let req = wr::CheckRelationshipRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            vault: Some(ws::VaultSlug::new(11)),
            resource: "doc:123".to_owned(),
            relation: "viewer".to_owned(),
            subject: "user:7".to_owned(),
            consistency: ws::ReadConsistency::Linearizable,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::CheckRelationshipRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn check_relationship_request_with_subject_relation_roundtrips() {
        // Subjects may include the `#relation` suffix (userset-style); the
        // wire shape is just `String`, so this is a "no special handling"
        // pin.
        let req = wr::CheckRelationshipRequest {
            organization: Some(ws::OrganizationSlug::new(1)),
            vault: Some(ws::VaultSlug::new(1)),
            resource: "folder:42".to_owned(),
            relation: "editor".to_owned(),
            subject: "group:engineers#member".to_owned(),
            consistency: ws::ReadConsistency::Eventual,
            caller: Some(ws::UserSlug::new(3)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::CheckRelationshipRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn check_relationship_response_round_trips_via_postcard() {
        let resp = wr::CheckRelationshipResponse { exists: true, checked_at_height: 1234 };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::CheckRelationshipResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn check_relationship_response_false_round_trips() {
        // Negative outcome must survive the round-trip too — the bool
        // encoding has no sentinel collision risk, but pin it anyway so a
        // future codec swap doesn't silently flip the default.
        let resp = wr::CheckRelationshipResponse { exists: false, checked_at_height: 0 };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::CheckRelationshipResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    // -------------------------------------------------------------------
    // Wire response → CheckRelationshipOutcome projection.
    // -------------------------------------------------------------------

    #[test]
    fn check_relationship_response_projects_to_outcome_true() {
        // Mirrors the production projection in `check_relationship`.
        let resp = wr::CheckRelationshipResponse { exists: true, checked_at_height: 99 };
        let outcome = CheckRelationshipOutcome {
            exists: resp.exists,
            checked_at_height: resp.checked_at_height,
        };
        assert!(outcome.exists);
        assert_eq!(outcome.checked_at_height, 99);
    }

    #[test]
    fn check_relationship_response_projects_to_outcome_false() {
        let resp = wr::CheckRelationshipResponse { exists: false, checked_at_height: 7 };
        let outcome = CheckRelationshipOutcome {
            exists: resp.exists,
            checked_at_height: resp.checked_at_height,
        };
        assert!(!outcome.exists);
        assert_eq!(outcome.checked_at_height, 7);
    }

    // -------------------------------------------------------------------
    // RpcError → SdkError shim coverage.
    // -------------------------------------------------------------------

    #[test]
    fn rpc_error_wire_error_maps_to_sdk_rpc() {
        // Server-returned WireError must round-trip through the shared
        // `from_wire_error_with_correlation` helper (the same path the
        // tonic shim uses) so the resulting SdkError carries the right
        // code + message.
        let wire_err = WireError {
            code: WireErrorCode::FailedPrecondition,
            message: "vault not found".to_owned(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        };
        let sdk_err = rpc_error_to_sdk_error(RpcError::WireError(wire_err));
        match sdk_err {
            SdkError::Rpc { code, message, .. } => {
                assert_eq!(code, WireErrorCode::FailedPrecondition);
                assert_eq!(message, "vault not found");
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_transport_maps_to_connection() {
        // Transport-level failure surfaces as SdkError::Connection so the
        // SDK retry loop classifies it as retryable rather than as a
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
}
