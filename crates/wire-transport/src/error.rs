//! Transport-level errors surfaced by the wire-transport crate.
//!
//! [`TransportError`] is intentionally small in this skeleton; later tasks
//! (B.4 onward) will add variants as concrete failure paths emerge. The enum
//! is `#[non_exhaustive]` so consumers can't pattern-match exhaustively
//! across versions.

use inferadb_ledger_wire::CodecError;

/// Errors produced by the QUIC-based wire transport.
///
/// Wraps codec-level failures from [`inferadb_ledger_wire`] alongside
/// transport-specific failures (connection close, timeouts, auth handshake).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Frame encoding or decoding failed at the wire-protocol layer.
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    /// QUIC connection failed or was reset by the peer.
    #[error("quic connection error: {0}")]
    Quic(quinn::ConnectionError),

    /// The QUIC connection has already been closed locally.
    #[error("connection closed")]
    ConnectionClosed,

    /// An operation exceeded its deadline before completing.
    #[error("operation timed out")]
    Timeout,

    /// The authentication handshake on a new connection was rejected.
    #[error("authentication handshake failed: {reason}")]
    AuthHandshakeFailed {
        /// Human-readable cause; surfaced for diagnostics, not over the wire.
        reason: String,
    },

    /// Failed to read a frame from a QUIC stream.
    #[error("read frame failed: {reason}")]
    ReadFrame {
        /// Human-readable cause; surfaced for diagnostics, not over the wire.
        reason: String,
    },

    /// Failed to write a frame to a QUIC stream.
    #[error("write frame failed: {reason}")]
    WriteFrame {
        /// Human-readable cause; surfaced for diagnostics, not over the wire.
        reason: String,
    },

    /// An I/O error from the underlying socket or runtime.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}
