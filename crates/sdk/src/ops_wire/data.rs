//! Wire-based data ops (read / batch_read / write).
//!
//! Mirrors [`super::super::ops::data`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`ReadServiceClient`](inferadb_ledger_wire_services::ReadServiceClient)
//! and
//! [`WriteServiceClient`](inferadb_ledger_wire_services::WriteServiceClient)
//! instead of the proto-generated tonic clients.
//!
//! Scope: only the three core data ops migrate here. The other entries in
//! `ops::data` (historical_read, get_block, get_block_range, get_tip,
//! watch_blocks) ship in later E.5.x tasks — they each have their own
//! response-shape conversions that don't share code with read/batch_read/
//! write, so splitting keeps each file's surface area small.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! oneof variant mappings (OperationKind, SetConditionKind,
//! WriteResponseResult, WriteErrorCode), the
//! [`RpcError`](inferadb_ledger_wire_transport::RpcError) →
//! [`SdkError`](crate::SdkError) conversion, and the wire-response →
//! domain-type mapping in isolation. The three entry points themselves are
//! not exercised yet — [`LedgerClient`](crate::LedgerClient) keeps
//! dispatching through the tonic `ops::data` path until F.1 flips the
//! connection pool.

use std::sync::Arc;

use bytes::Bytes;
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};
use inferadb_ledger_wire_services::{ReadServiceClient, WriteServiceClient};
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    types::{
        query::{Operation, SetCondition},
        read::{ReadConsistency, WriteSuccess},
    },
};

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

/// Issue a `Read` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::read`](crate::LedgerClient::read) at the
/// dispatch layer. Returns `Ok(Some(value))` when the key exists, `Ok(None)`
/// when it does not. `vault` is `None` for organization-level keys.
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
pub(crate) async fn read(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: Option<ws::VaultSlug>,
    key: String,
    consistency: ReadConsistency,
) -> Result<Option<Vec<u8>>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::ReadRequest {
        organization: Some(organization),
        vault,
        key,
        consistency: domain_consistency_to_wire(consistency),
        caller: Some(caller),
    };
    let response = client.read(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.value.map(|b| b.to_vec()))
}

// ---------------------------------------------------------------------------
// BatchRead
// ---------------------------------------------------------------------------

/// Issue a `BatchRead` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::batch_read`](crate::LedgerClient::batch_read) at
/// the dispatch layer. Returns a `Vec<(key, Option<value>)>` in the same
/// order as the request `keys` — missing keys carry `None`.
///
/// # Errors
///
/// Same shape as [`read`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn batch_read(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: Option<ws::VaultSlug>,
    keys: Vec<String>,
    consistency: ReadConsistency,
) -> Result<Vec<(String, Option<Vec<u8>>)>> {
    let client = ReadServiceClient::new(wire_client);
    let request = wr::BatchReadRequest {
        organization: Some(organization),
        vault,
        keys,
        consistency: domain_consistency_to_wire(consistency),
        caller: Some(caller),
    };
    let response = client.batch_read(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.results.into_iter().map(|r| (r.key, r.value.map(|b| b.to_vec()))).collect())
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Issue a `Write` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::write`](crate::LedgerClient::write) at the
/// dispatch layer. The caller owns idempotency-key generation (a 16-byte
/// UUID) so retries reuse the same key — the server's per-org idempotency
/// store rejects duplicates with a different payload.
///
/// `client_id` identifies the SDK instance issuing the call; it's
/// independent of `caller` (the user on whose behalf the call is made) and
/// is used by the server's per-client sequence numbering.
///
/// # Errors
///
/// - [`SdkError::Connection`] — transport / protocol failures.
/// - [`SdkError::Idempotency`] — `IdempotencyKeyReused` (caller reused the key with a different
///   payload).
/// - [`SdkError::Rpc`] — CAS failures, validation rejection, all other server-side write errors.
/// - Server-returned [`WireError`]s (rate-limit family, migration codes, etc.) flow through
///   [`rpc_error_to_sdk_error`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: ws::UserSlug,
    organization: ws::OrganizationSlug,
    vault: Option<ws::VaultSlug>,
    client_id: String,
    idempotency_key: [u8; 16],
    operations: Vec<Operation>,
) -> Result<WriteSuccess> {
    let client = WriteServiceClient::new(wire_client);
    let request = ww::WriteRequest {
        organization: Some(organization),
        vault,
        client_id: Some(ws::ClientIdMessage { id: client_id }),
        idempotency_key: Bytes::copy_from_slice(&idempotency_key),
        operations: operations.into_iter().map(domain_operation_to_wire).collect(),
        include_tx_proof: false,
        caller: Some(caller),
    };
    let response = client.write(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    process_write_response(response)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map the SDK domain `ReadConsistency` to the wire enum.
///
/// The wire enum is `#[repr(i32)]` with the same values as the proto enum;
/// the SDK domain enum has no `Unspecified` variant (Rust `Default` is
/// `Eventual`), so this is a total mapping with no fallback arm.
fn domain_consistency_to_wire(c: ReadConsistency) -> ws::ReadConsistency {
    match c {
        ReadConsistency::Eventual => ws::ReadConsistency::Eventual,
        ReadConsistency::Linearizable => ws::ReadConsistency::Linearizable,
    }
}

/// Convert a SDK domain [`Operation`] into a wire [`ws::Operation`].
///
/// Mirrors the proto-side `Operation::to_proto` in
/// [`crate::types::query`]. The SDK's `ExpireEntity` is server-internal —
/// the SDK domain enum doesn't expose it, so this translator only emits
/// the four caller-facing variants. The wire type's `Bytes` value field
/// adopts the `Vec<u8>` payload via `Bytes::from`.
fn domain_operation_to_wire(op: Operation) -> ws::Operation {
    let kind = match op {
        Operation::SetEntity { key, value, expires_at, condition } => {
            ws::OperationKind::SetEntity(ws::SetEntity {
                key,
                value: Bytes::from(value),
                expires_at,
                condition: condition.map(domain_set_condition_to_wire),
            })
        },
        Operation::DeleteEntity { key } => {
            ws::OperationKind::DeleteEntity(ws::DeleteEntity { key })
        },
        Operation::CreateRelationship { resource, relation, subject } => {
            ws::OperationKind::CreateRelationship(ws::CreateRelationship {
                resource,
                relation,
                subject,
            })
        },
        Operation::DeleteRelationship { resource, relation, subject } => {
            ws::OperationKind::DeleteRelationship(ws::DeleteRelationship {
                resource,
                relation,
                subject,
            })
        },
    };
    ws::Operation { op: Some(kind) }
}

/// Convert a SDK domain [`SetCondition`] into a wire [`ws::SetCondition`].
///
/// The SDK enum uses unit-style variants for `NotExists` / `MustExist`;
/// the wire enum encodes both as `bool(true)` (mirrors the proto oneof
/// which uses `bool` for the same reason — a oneof variant with no
/// payload is invalid in proto3). The wire `MustExists` (note: trailing
/// `s`) is the proto field name; the SDK domain spelling drops it.
fn domain_set_condition_to_wire(c: SetCondition) -> ws::SetCondition {
    let kind = match c {
        SetCondition::NotExists => ws::SetConditionKind::NotExists(true),
        SetCondition::MustExist => ws::SetConditionKind::MustExists(true),
        SetCondition::Version(v) => ws::SetConditionKind::Version(v),
        SetCondition::ValueEquals(v) => ws::SetConditionKind::ValueEquals(Bytes::from(v)),
    };
    ws::SetCondition { condition: Some(kind) }
}

/// Translate a wire [`ww::WriteResponse`] into the SDK's
/// `Result<WriteSuccess>`.
///
/// Mirrors the proto-side `LedgerClient::process_write_response` —
/// `Success` becomes `Ok(WriteSuccess)`; `AlreadyCommitted` is treated as
/// an idempotent retry success (carrying the original commit's tx_id /
/// height / sequence); `IdempotencyKeyReused` becomes
/// [`SdkError::Idempotency`]; everything else collapses into
/// [`SdkError::Rpc`] with `FailedPrecondition` (CAS-style errors). A
/// `WriteResponse` with a missing `result` oneof produces
/// [`SdkError::Rpc`] with `Internal` — same shape as the tonic path's
/// `missing_response_field`.
fn process_write_response(response: ww::WriteResponse) -> Result<WriteSuccess> {
    match response.result {
        Some(ww::WriteResponseResult::Success(success)) => Ok(WriteSuccess {
            tx_id: tx_id_to_hex(success.tx_id.as_ref()),
            block_height: success.block_height,
            assigned_sequence: success.assigned_sequence,
        }),
        Some(ww::WriteResponseResult::Error(error)) => match error.code {
            ww::WriteErrorCode::AlreadyCommitted => Ok(WriteSuccess {
                tx_id: tx_id_to_hex(error.committed_tx_id.as_ref()),
                block_height: error.committed_block_height.unwrap_or(0),
                assigned_sequence: error.assigned_sequence.unwrap_or(0),
            }),
            ww::WriteErrorCode::IdempotencyKeyReused => Err(SdkError::Idempotency {
                message: format!(
                    "Idempotency key reused with different payload: {}",
                    error.message
                ),
                conflict_key: None,
                original_tx_id: Some(tx_id_to_hex(error.committed_tx_id.as_ref())),
            }),
            _ => Err(SdkError::Rpc {
                code: inferadb_ledger_wire::ErrorCode::FailedPrecondition,
                message: error.message,
                request_id: None,
                trace_id: None,
                error_details: None,
            }),
        },
        None => Err(SdkError::Rpc {
            code: inferadb_ledger_wire::ErrorCode::Internal,
            message: "Missing result in WriteResponse".to_owned(),
            request_id: None,
            trace_id: None,
            error_details: None,
        }),
    }
}

/// Lowercase-hex encoding of a 16-byte transaction ID.
///
/// Mirrors `LedgerClient::tx_id_to_hex` exactly — empty / missing tx
/// collapses to `""` rather than panicking, and the format is
/// allocation-aware (capacity = 2 × byte count).
fn tx_id_to_hex(tx_id: Option<&ws::TxIdMessage>) -> String {
    use std::fmt::Write;
    let Some(tx) = tx_id else {
        return String::new();
    };
    tx.id.iter().fold(String::with_capacity(tx.id.len() * 2), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for oneof variant mappings and wire-response →
    //! domain-type / Result conversion. End-to-end tests against a real
    //! `WireClient` / `WireServer` are deferred to E.7 (TestCluster
    //! migration) — at that point the three entry points get exercised
    //! in full.

    use super::*;

    // -------------------------------------------------------------------
    // Round-trip: ReadRequest / ReadResponse via postcard.
    // -------------------------------------------------------------------

    #[test]
    fn read_request_postcard_round_trips() {
        // Constructed through the same field shape the production fn
        // produces; confirms the wire encoding is self-inverse.
        let req = wr::ReadRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            vault: Some(ws::VaultSlug::new(11)),
            key: "user:42".to_owned(),
            consistency: ws::ReadConsistency::Linearizable,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::ReadRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn read_response_some_value_round_trips_bytes() {
        // Bytes value must survive postcard round-trip without slicing
        // or truncation — a regression here would silently corrupt
        // multi-byte values.
        let resp = wr::ReadResponse {
            value: Some(Bytes::copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF])),
            block_height: 42,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::ReadResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
        assert_eq!(decoded.value.as_deref(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn read_response_none_value_round_trips() {
        let resp = wr::ReadResponse { value: None, block_height: 0 };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::ReadResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
        assert!(decoded.value.is_none());
    }

    // -------------------------------------------------------------------
    // Round-trip: BatchReadRequest / BatchReadResponse via postcard.
    // -------------------------------------------------------------------

    #[test]
    fn batch_read_request_postcard_round_trips() {
        let req = wr::BatchReadRequest {
            organization: Some(ws::OrganizationSlug::new(7)),
            vault: None,
            keys: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            consistency: ws::ReadConsistency::Eventual,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: wr::BatchReadRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn batch_read_response_round_trips_with_mixed_hits() {
        // Mix found / not-found entries; the SDK projection must
        // preserve order and collapse `value: None` to `None` on the
        // domain side.
        let resp = wr::BatchReadResponse {
            results: vec![
                wr::BatchReadResult {
                    key: "a".to_owned(),
                    value: Some(Bytes::from_static(b"alpha")),
                    found: true,
                },
                wr::BatchReadResult { key: "b".to_owned(), value: None, found: false },
                wr::BatchReadResult {
                    key: "c".to_owned(),
                    value: Some(Bytes::from_static(b"gamma")),
                    found: true,
                },
            ],
            block_height: 100,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: wr::BatchReadResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);

        // Now also check the SDK-shape projection.
        let projected: Vec<(String, Option<Vec<u8>>)> =
            decoded.results.into_iter().map(|r| (r.key, r.value.map(|b| b.to_vec()))).collect();
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0].0, "a");
        assert_eq!(projected[0].1.as_deref(), Some(&b"alpha"[..]));
        assert_eq!(projected[1].0, "b");
        assert!(projected[1].1.is_none());
        assert_eq!(projected[2].1.as_deref(), Some(&b"gamma"[..]));
    }

    // -------------------------------------------------------------------
    // ReadConsistency mapping.
    // -------------------------------------------------------------------

    #[test]
    fn domain_consistency_each_variant_maps_correctly() {
        // The SDK domain enum has only Eventual / Linearizable; the
        // wire enum's Unspecified variant is unreachable from this
        // path. Pin both arms explicitly so a future Default change
        // doesn't silently flip semantics.
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Eventual),
            ws::ReadConsistency::Eventual,
        );
        assert_eq!(
            domain_consistency_to_wire(ReadConsistency::Linearizable),
            ws::ReadConsistency::Linearizable,
        );
    }

    #[test]
    fn read_consistency_repr_values_match_proto() {
        // Lock in the integer tags so a cross-protocol audit (proto vs
        // wire) can grep for the same values. The wire enum is
        // #[repr(i32)] = 0/1/2 by design.
        assert_eq!(ws::ReadConsistency::Unspecified as i32, 0);
        assert_eq!(ws::ReadConsistency::Eventual as i32, 1);
        assert_eq!(ws::ReadConsistency::Linearizable as i32, 2);
    }

    // -------------------------------------------------------------------
    // OperationKind oneof variants.
    // -------------------------------------------------------------------

    #[test]
    fn operation_set_entity_to_wire_carries_all_fields() {
        let op = Operation::SetEntity {
            key: "user:42".to_owned(),
            value: b"payload".to_vec(),
            expires_at: Some(1_700_000_000),
            condition: Some(SetCondition::NotExists),
        };
        let wire = domain_operation_to_wire(op);
        match wire.op {
            Some(ws::OperationKind::SetEntity(se)) => {
                assert_eq!(se.key, "user:42");
                assert_eq!(se.value.as_ref(), b"payload");
                assert_eq!(se.expires_at, Some(1_700_000_000));
                match se.condition.unwrap().condition.unwrap() {
                    ws::SetConditionKind::NotExists(b) => assert!(b),
                    other => panic!("expected NotExists, got {other:?}"),
                }
            },
            other => panic!("expected SetEntity, got {other:?}"),
        }
    }

    #[test]
    fn operation_delete_entity_to_wire_round_trips() {
        let op = Operation::DeleteEntity { key: "user:42".to_owned() };
        let wire = domain_operation_to_wire(op);
        match wire.op {
            Some(ws::OperationKind::DeleteEntity(de)) => assert_eq!(de.key, "user:42"),
            other => panic!("expected DeleteEntity, got {other:?}"),
        }
    }

    #[test]
    fn operation_create_relationship_to_wire_round_trips() {
        let op = Operation::CreateRelationship {
            resource: "doc:1".to_owned(),
            relation: "viewer".to_owned(),
            subject: "user:alice".to_owned(),
        };
        let wire = domain_operation_to_wire(op);
        match wire.op {
            Some(ws::OperationKind::CreateRelationship(cr)) => {
                assert_eq!(cr.resource, "doc:1");
                assert_eq!(cr.relation, "viewer");
                assert_eq!(cr.subject, "user:alice");
            },
            other => panic!("expected CreateRelationship, got {other:?}"),
        }
    }

    #[test]
    fn operation_delete_relationship_to_wire_round_trips() {
        let op = Operation::DeleteRelationship {
            resource: "doc:1".to_owned(),
            relation: "viewer".to_owned(),
            subject: "user:alice".to_owned(),
        };
        let wire = domain_operation_to_wire(op);
        match wire.op {
            Some(ws::OperationKind::DeleteRelationship(dr)) => {
                assert_eq!(dr.resource, "doc:1");
                assert_eq!(dr.relation, "viewer");
                assert_eq!(dr.subject, "user:alice");
            },
            other => panic!("expected DeleteRelationship, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // SetConditionKind oneof variants.
    // -------------------------------------------------------------------

    #[test]
    fn set_condition_not_exists_to_wire() {
        let wire = domain_set_condition_to_wire(SetCondition::NotExists);
        match wire.condition.unwrap() {
            ws::SetConditionKind::NotExists(b) => assert!(b),
            other => panic!("expected NotExists, got {other:?}"),
        }
    }

    #[test]
    fn set_condition_must_exist_to_wire_uses_must_exists_variant() {
        // SDK enum spells `MustExist`; wire enum spells `MustExists`
        // (matches the proto field name). Pin the spelling bridge.
        let wire = domain_set_condition_to_wire(SetCondition::MustExist);
        match wire.condition.unwrap() {
            ws::SetConditionKind::MustExists(b) => assert!(b),
            other => panic!("expected MustExists, got {other:?}"),
        }
    }

    #[test]
    fn set_condition_version_to_wire() {
        let wire = domain_set_condition_to_wire(SetCondition::Version(7));
        match wire.condition.unwrap() {
            ws::SetConditionKind::Version(v) => assert_eq!(v, 7),
            other => panic!("expected Version, got {other:?}"),
        }
    }

    #[test]
    fn set_condition_value_equals_to_wire_preserves_bytes() {
        let wire = domain_set_condition_to_wire(SetCondition::ValueEquals(b"old".to_vec()));
        match wire.condition.unwrap() {
            ws::SetConditionKind::ValueEquals(v) => assert_eq!(v.as_ref(), b"old"),
            other => panic!("expected ValueEquals, got {other:?}"),
        }
    }

    #[test]
    fn set_condition_kind_round_trips_via_postcard() {
        // Each oneof variant must survive postcard round-trip without
        // tag collision — a regression here would silently misroute
        // CAS conditions on the wire.
        let cases = vec![
            ws::SetConditionKind::NotExists(true),
            ws::SetConditionKind::Version(1234),
            ws::SetConditionKind::ValueEquals(Bytes::from_static(b"old-value")),
            ws::SetConditionKind::MustExists(true),
        ];
        for kind in cases {
            let cond = ws::SetCondition { condition: Some(kind.clone()) };
            let bytes = postcard::to_allocvec(&cond).unwrap();
            let decoded: ws::SetCondition = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(cond, decoded);
        }
    }

    // -------------------------------------------------------------------
    // WriteRequest postcard round-trip.
    // -------------------------------------------------------------------

    #[test]
    fn write_request_round_trips_with_multiple_operations() {
        let req = ww::WriteRequest {
            organization: Some(ws::OrganizationSlug::new(1)),
            vault: Some(ws::VaultSlug::new(2)),
            client_id: Some(ws::ClientIdMessage { id: "client-x".to_owned() }),
            idempotency_key: Bytes::copy_from_slice(&[0xAA; 16]),
            operations: vec![
                domain_operation_to_wire(Operation::SetEntity {
                    key: "k".to_owned(),
                    value: b"v".to_vec(),
                    expires_at: None,
                    condition: None,
                }),
                domain_operation_to_wire(Operation::CreateRelationship {
                    resource: "doc:1".to_owned(),
                    relation: "viewer".to_owned(),
                    subject: "user:1".to_owned(),
                }),
            ],
            include_tx_proof: false,
            caller: Some(ws::UserSlug::new(99)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ww::WriteRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
        assert_eq!(decoded.idempotency_key.len(), 16);
    }

    // -------------------------------------------------------------------
    // process_write_response: success / AlreadyCommitted / IdempotencyKeyReused / others.
    // -------------------------------------------------------------------

    fn tx_id_msg(byte: u8) -> ws::TxIdMessage {
        ws::TxIdMessage { id: Bytes::copy_from_slice(&[byte; 16]) }
    }

    #[test]
    fn process_write_response_success_returns_write_success() {
        let resp = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Success(ww::WriteSuccess {
                tx_id: Some(tx_id_msg(0xAB)),
                block_height: 100,
                block_header: None,
                tx_proof: None,
                assigned_sequence: 7,
            })),
        };
        let result = process_write_response(resp).unwrap();
        // 16 bytes of 0xAB — hex form is 32 chars of "ab".
        assert_eq!(result.tx_id, "ab".repeat(16));
        assert_eq!(result.block_height, 100);
        assert_eq!(result.assigned_sequence, 7);
    }

    #[test]
    fn process_write_response_already_committed_treated_as_success() {
        // AlreadyCommitted is the idempotent-retry path — the original
        // success is reconstructed from the error's committed_* fields,
        // then surfaced as Ok.
        let resp = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Error(ww::WriteError {
                code: ww::WriteErrorCode::AlreadyCommitted,
                key: String::new(),
                current_version: None,
                current_value: None,
                message: "duplicate".to_owned(),
                committed_tx_id: Some(tx_id_msg(0xCD)),
                committed_block_height: Some(50),
                assigned_sequence: Some(3),
            })),
        };
        let result = process_write_response(resp).unwrap();
        assert_eq!(result.tx_id, "cd".repeat(16));
        assert_eq!(result.block_height, 50);
        assert_eq!(result.assigned_sequence, 3);
    }

    #[test]
    fn process_write_response_already_committed_with_missing_aux_fields_defaults_to_zero() {
        // Server may legitimately omit committed_block_height /
        // assigned_sequence when reconstructing the original commit
        // from a partial idempotency record. The helper must default
        // to 0 rather than panicking.
        let resp = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Error(ww::WriteError {
                code: ww::WriteErrorCode::AlreadyCommitted,
                key: String::new(),
                current_version: None,
                current_value: None,
                message: "duplicate".to_owned(),
                committed_tx_id: None,
                committed_block_height: None,
                assigned_sequence: None,
            })),
        };
        let result = process_write_response(resp).unwrap();
        assert!(result.tx_id.is_empty());
        assert_eq!(result.block_height, 0);
        assert_eq!(result.assigned_sequence, 0);
    }

    #[test]
    fn process_write_response_idempotency_key_reused_maps_to_idempotency_error() {
        let resp = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Error(ww::WriteError {
                code: ww::WriteErrorCode::IdempotencyKeyReused,
                key: String::new(),
                current_version: None,
                current_value: None,
                message: "different payload".to_owned(),
                committed_tx_id: Some(tx_id_msg(0xEF)),
                committed_block_height: None,
                assigned_sequence: None,
            })),
        };
        let err = process_write_response(resp).unwrap_err();
        match err {
            SdkError::Idempotency { message, conflict_key, original_tx_id } => {
                assert!(message.contains("different payload"), "unexpected message: {message}");
                assert!(conflict_key.is_none());
                assert_eq!(original_tx_id.as_deref(), Some("ef".repeat(16).as_str()));
            },
            other => panic!("expected Idempotency, got {other:?}"),
        }
    }

    #[test]
    fn process_write_response_other_error_codes_collapse_to_failed_precondition() {
        // CAS-style errors (KeyExists, KeyNotFound, VersionMismatch,
        // ValueMismatch, Unspecified) all flow into FailedPrecondition
        // — the SDK consumer treats these uniformly as "re-read and
        // retry with the updated value".
        let cases = vec![
            ww::WriteErrorCode::Unspecified,
            ww::WriteErrorCode::KeyExists,
            ww::WriteErrorCode::KeyNotFound,
            ww::WriteErrorCode::VersionMismatch,
            ww::WriteErrorCode::ValueMismatch,
        ];
        for code in cases {
            let resp = ww::WriteResponse {
                result: Some(ww::WriteResponseResult::Error(ww::WriteError {
                    code,
                    key: "k".to_owned(),
                    current_version: None,
                    current_value: None,
                    message: format!("err-for-{code:?}"),
                    committed_tx_id: None,
                    committed_block_height: None,
                    assigned_sequence: None,
                })),
            };
            let err = process_write_response(resp).unwrap_err();
            match err {
                SdkError::Rpc { code: rpc_code, message, .. } => {
                    assert_eq!(rpc_code, inferadb_ledger_wire::ErrorCode::FailedPrecondition);
                    assert!(message.contains("err-for-"), "unexpected message: {message}");
                },
                other => panic!("expected Rpc, got {other:?}"),
            }
        }
    }

    #[test]
    fn process_write_response_missing_result_maps_to_internal() {
        // A WriteResponse with result=None is a server-side bug; the
        // SDK surfaces it as Internal so the caller doesn't retry
        // forever (FailedPrecondition isn't retryable; Internal isn't
        // either).
        let resp = ww::WriteResponse { result: None };
        let err = process_write_response(resp).unwrap_err();
        match err {
            SdkError::Rpc { code, message, .. } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(
                    message.contains("Missing result in WriteResponse"),
                    "unexpected message: {message}",
                );
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // WriteResponseResult / WriteErrorCode oneof + repr coverage.
    // -------------------------------------------------------------------

    #[test]
    fn write_response_result_oneof_round_trips_via_postcard() {
        let success = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Success(ww::WriteSuccess {
                tx_id: Some(tx_id_msg(0x01)),
                block_height: 1,
                block_header: None,
                tx_proof: None,
                assigned_sequence: 1,
            })),
        };
        let error = ww::WriteResponse {
            result: Some(ww::WriteResponseResult::Error(ww::WriteError {
                code: ww::WriteErrorCode::VersionMismatch,
                key: "k".to_owned(),
                current_version: Some(7),
                current_value: Some(Bytes::from_static(b"old")),
                message: "mismatch".to_owned(),
                committed_tx_id: None,
                committed_block_height: None,
                assigned_sequence: None,
            })),
        };
        for resp in [success, error] {
            let bytes = postcard::to_allocvec(&resp).unwrap();
            let decoded: ww::WriteResponse = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(resp, decoded);
        }
    }

    #[test]
    fn write_error_code_repr_values_match_proto() {
        // Lock in the integer tags so a cross-protocol audit can verify
        // the wire enum's #[repr(i32)] layout matches the proto enum.
        assert_eq!(ww::WriteErrorCode::Unspecified as i32, 0);
        assert_eq!(ww::WriteErrorCode::KeyExists as i32, 1);
        assert_eq!(ww::WriteErrorCode::KeyNotFound as i32, 2);
        assert_eq!(ww::WriteErrorCode::VersionMismatch as i32, 3);
        assert_eq!(ww::WriteErrorCode::ValueMismatch as i32, 4);
        assert_eq!(ww::WriteErrorCode::AlreadyCommitted as i32, 5);
        assert_eq!(ww::WriteErrorCode::IdempotencyKeyReused as i32, 6);
    }

    // -------------------------------------------------------------------
    // tx_id_to_hex coverage.
    // -------------------------------------------------------------------

    #[test]
    fn tx_id_to_hex_none_returns_empty_string() {
        assert!(tx_id_to_hex(None).is_empty());
    }

    #[test]
    fn tx_id_to_hex_zero_byte_renders_with_leading_zero() {
        // The format string "{:02x}" must zero-pad single-digit bytes
        // so the resulting hex is fixed-length (32 chars for 16 bytes).
        let tx = ws::TxIdMessage { id: Bytes::copy_from_slice(&[0u8; 16]) };
        assert_eq!(tx_id_to_hex(Some(&tx)), "00".repeat(16));
    }

    #[test]
    fn tx_id_to_hex_mixed_bytes_round_trips() {
        let id_bytes: [u8; 16] = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let tx = ws::TxIdMessage { id: Bytes::copy_from_slice(&id_bytes) };
        assert_eq!(tx_id_to_hex(Some(&tx)), "0123456789abcdeffedcba9876543210");
    }
}
