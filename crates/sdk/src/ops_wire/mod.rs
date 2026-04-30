//! Wire-protocol-based SDK operations (Phase 0.0.E.5).
//!
//! Mirrors [`super::ops`] but constructs RPC calls via the wire-services
//! `*Client` structs (see [`inferadb_ledger_wire_services`]) over a
//! [`WireClient`](inferadb_ledger_wire_transport::WireClient) instead of a
//! [`tonic::transport::Channel`]. Coexists with `ops` during the migration —
//! [`LedgerClient`](crate::LedgerClient) keeps dispatching through `ops`
//! until F.1 flips the connection pool from tonic to wire-transport.
//!
//! Each wire op produces the same domain types (e.g.
//! [`HealthCheckResult`](crate::HealthCheckResult)) as the tonic op, so
//! swapping the dispatch path is mechanical.
//!
//! End-to-end tests against a real `WireClient` / `WireServer` are deferred
//! to E.7 (TestCluster migration); the unit tests here cover the
//! [`RpcError`](inferadb_ledger_wire_transport::RpcError) → [`SdkError`](crate::SdkError)
//! conversion plus the wire-response → domain-type mapping in isolation.

pub(crate) mod app;
pub(crate) mod credential;
pub(crate) mod data;
pub(crate) mod events;
pub(crate) mod health;
pub(crate) mod invitation;
pub(crate) mod list;
pub(crate) mod onboarding;
pub(crate) mod organization;
pub(crate) mod relationship;
pub(crate) mod schema;
pub(crate) mod token;
pub(crate) mod user;
pub(crate) mod vault;
pub(crate) mod verified_read;

use inferadb_ledger_wire_transport::RpcError;

use crate::{error::SdkError, wire_conversion::from_wire_error_with_correlation};

/// Maps a wire-transport [`RpcError`] into the SDK's [`SdkError`].
///
/// Shared by every `ops_wire/*.rs` op so the per-service files don't each
/// duplicate the five-arm match. Server-returned [`WireError`]s flow through
/// [`from_wire_error_with_correlation`] so they pick up the same
/// [`ServerErrorDetails`](crate::ServerErrorDetails) shape that the tonic
/// path produces. Everything else (transport, opcode mismatch, protocol
/// violation, decode failure) collapses into [`SdkError::Connection`] —
/// these are all "the call did not reach a real handler" failures from the
/// SDK consumer's perspective. The `RpcError` enum is `#[non_exhaustive]`,
/// so the catch-all arm stays generic against future variants.
///
/// `request_id` / `trace_id` correlation IDs are dropped here: the wire
/// transport does not surface the response frame's `FrameHeader` from
/// [`WireClient::unary`](inferadb_ledger_wire_transport::WireClient::unary),
/// so there is nothing to propagate. F.1 may revisit this when it threads
/// the frame header through.
pub(crate) fn rpc_error_to_sdk_error(err: RpcError) -> SdkError {
    match err {
        RpcError::WireError(wire_err) => from_wire_error_with_correlation(wire_err, None, None),
        RpcError::Transport(transport_err) => {
            SdkError::Connection { message: format!("wire transport error: {transport_err}") }
        },
        RpcError::OpcodeMismatch { expected, actual } => SdkError::Connection {
            message: format!("opcode mismatch: expected {expected:#06x}, got {actual:#06x}"),
        },
        RpcError::ProtocolViolation(reason) => {
            SdkError::Connection { message: format!("wire protocol violation: {reason}") }
        },
        RpcError::DecodeWireError(reason) => {
            SdkError::Connection { message: format!("failed to decode wire error: {reason}") }
        },
        // `RpcError` is `#[non_exhaustive]`; map any future variant to the
        // same generic Connection shape so SDK consumers see a
        // dispatch-layer failure rather than a panic.
        other => SdkError::Connection { message: format!("wire transport error: {other}") },
    }
}
