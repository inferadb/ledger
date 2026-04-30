//! Long-lived inter-node `Replicate` bidi-stream sub-protocol.
//!
//! The wire-macro DSL declares `RaftService::replicate` as a unary placeholder
//! at opcode `inferadb_ledger_wire::OPCODE_REPLICATE`, but the production
//! shape is a long-lived bidi stream multiplexing
//! [`inferadb_ledger_wire::services::raft::ConsensusEnvelope`] frames in both
//! directions. The peer-sender side opens an outbound bidi stream and writes
//! the protocol-version handshake frame
//! ([`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`], 0x0D04) as the
//! first frame; everything after is `OPCODE_REPLICATE` envelope frames.
//!
//! The transport's per-stream dispatch loop (see
//! `crate::server::dispatch::dispatch_stream`) recognises
//! `OPCODE_RAFT_PROTOCOL_HANDSHAKE` and hands the stream off to the
//! [`ReplicateHandler`] installed on the dispatcher. A bare dispatcher (no
//! Raft transport) returns `None` and the server replies
//! [`inferadb_ledger_wire::ErrorCode::Internal`] to any inbound handshake
//! frame.
//!
//! ## Frame shape on the wire
//!
//! All frames carry one of two opcodes:
//!
//! * **First frame**: [`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`] — 4-byte big-endian
//!   `u32` carrying the peer's [`inferadb_ledger_wire::RAFT_PROTOCOL_VERSION`]. Receiver replies
//!   with its own version on the same opcode (with [`crate::frame_io::FLAG_IS_RESPONSE`])
//!   regardless of match — peers see the mismatch and decide whether to retry.
//! * **Subsequent frames**: `OPCODE_REPLICATE` (0x0D02) carrying postcard-encoded
//!   [`inferadb_ledger_wire::services::raft::ConsensusEnvelope`] payloads. Receiver acks each
//!   envelope with an empty-payload response frame on the same opcode.
//!
//! Streams stay open for the lifetime of the inter-node connection; the
//! per-peer reconnect loop on the sender side re-opens the bidi after any
//! transport hiccup.

use bytes::Bytes;
use quinn::{RecvStream, SendStream};
use tokio_util::sync::CancellationToken;

use crate::error::TransportError;

/// Server-side hook a dispatcher exposes to participate in the inter-node
/// `Replicate` bidi-stream sub-protocol.
///
/// The [`crate::dispatcher::Dispatcher`] trait carries an optional
/// [`ReplicateHandler`] implementation; a dispatcher that returns `Some` from
/// [`crate::dispatcher::Dispatcher::replicate_handler`] will receive
/// `Replicate` streams routed off the
/// [`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`] dispatch branch.
/// Implementations own the version handshake, envelope drain, and ack
/// emission for the lifetime of the bidi stream.
///
/// A bare [`crate::dispatcher::Dispatcher`] (no Raft transport) returns
/// `None` from `replicate_handler` and the server replies with
/// [`inferadb_ledger_wire::ErrorCode::Internal`] to any inbound handshake
/// frame.
#[async_trait::async_trait]
pub trait ReplicateHandler: Send + Sync {
    /// Drive a single inbound `Replicate` bidi stream to completion.
    ///
    /// Called from the dispatch loop when an incoming bidi stream's first
    /// frame carries [`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`].
    /// The dispatch loop has already read the first frame off the wire and
    /// passes its payload as `first_frame` so the handshake can be verified
    /// without re-reading. The handler:
    ///
    /// 1. Validates the handshake payload (4-byte big-endian
    ///    [`inferadb_ledger_wire::RAFT_PROTOCOL_VERSION`]) and writes the local handshake response.
    /// 2. Loops on subsequent inbound `OPCODE_REPLICATE` frames, decoding each as a
    ///    [`inferadb_ledger_wire::services::raft::ConsensusEnvelope`] and writing an ack frame in
    ///    response.
    /// 3. Exits on cancel / read failure / write failure.
    ///
    /// Errors are surfaced as [`TransportError`] for ergonomic propagation;
    /// the dispatch loop only logs.
    async fn handle_replicate_stream(
        &self,
        send: SendStream,
        recv: RecvStream,
        first_frame: Bytes,
        cancel: CancellationToken,
    ) -> Result<(), TransportError>;
}
