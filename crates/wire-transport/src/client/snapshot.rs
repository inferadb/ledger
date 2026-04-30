//! Client-side helper for the snapshot streaming sub-protocol (Task E.6a).
//!
//! Lives in `client/` rather than `snapshot_stream` so the public API on
//! [`WireClient`] reads naturally — same shape as
//! [`WireClient::unary`](super::WireClient::unary) and
//! [`WireClient::server_stream`](super::WireClient::server_stream).
//!
//! The implementation lives behind [`super::WireClient::snapshot_stream`].

use inferadb_ledger_wire::services::raft::InstallSnapshotHeader;

use super::{RpcError, WireClient};
use crate::{
    error::TransportError,
    snapshot_stream::{SnapshotSender, SnapshotStreamError},
};

impl WireClient {
    /// Open a snapshot streaming session.
    ///
    /// Acquires the connection (waiting for reconnect if necessary), opens
    /// a fresh bidi stream, writes the [`InstallSnapshotHeader`] frame, and
    /// returns a [`SnapshotSender`] the caller drives chunk-by-chunk. The
    /// session is committed via
    /// [`SnapshotSender::finish`](crate::snapshot_stream::SnapshotSender::finish);
    /// dropping the sender without `finish` is a sender-side abort and
    /// leaves the receiver's `.partial` file for crash-recovery cleanup.
    ///
    /// # Errors
    ///
    /// * [`RpcError::Transport`] if the connection cannot be acquired or the bi stream cannot be
    ///   opened, or the header frame cannot be written.
    /// * [`RpcError::ProtocolViolation`] if the wire codec rejects the encoded header (only
    ///   reachable for an internally malformed header).
    pub async fn snapshot_stream(
        &self,
        request_id: [u8; 16],
        header: InstallSnapshotHeader,
    ) -> Result<SnapshotSender, RpcError> {
        let connection = self.acquire().await?;
        SnapshotSender::open(&connection, request_id, header).await.map_err(snapshot_to_rpc_err)
    }
}

/// Project [`SnapshotStreamError`] onto [`RpcError`] for the client surface.
fn snapshot_to_rpc_err(err: SnapshotStreamError) -> RpcError {
    match err {
        SnapshotStreamError::Transport(t) => RpcError::Transport(t),
        SnapshotStreamError::Encode(reason) => {
            RpcError::ProtocolViolation(format!("snapshot encode: {reason}"))
        },
        SnapshotStreamError::Decode(reason) => {
            RpcError::ProtocolViolation(format!("snapshot decode: {reason}"))
        },
        SnapshotStreamError::WireError(w) => RpcError::WireError(w),
        // The remaining arms only fire mid-protocol, after `open` has
        // returned — they cannot reach this conversion path. Keep the
        // mapping exhaustive so a future arm doesn't get silently swallowed.
        other => RpcError::Transport(TransportError::WriteFrame {
            reason: format!("snapshot_stream open: {other}"),
        }),
    }
}
