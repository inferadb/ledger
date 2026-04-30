//! Trait abstraction over application-level RPC handling.
//!
//! The transport reads frames off a stream, matches the opcode to a service,
//! and hands the payload to a [`Dispatcher`]. Routing tables, typed request
//! decoding, and per-RPC error mapping all live behind this trait. Phase
//! 0.0.D's `define_protocol!` macro produces a concrete `Dispatcher` impl
//! that knows about all 14 services; the transport itself is agnostic.
//!
//! # Object safety
//!
//! Like [`AuthVerifier`](crate::auth::AuthVerifier), [`Dispatcher`] uses
//! return-position-impl-trait async fns (`fn dispatch(...) -> impl Future +
//! Send`). Rust 1.92's bare async-fn-in-trait support makes this **not**
//! dyn-compatible without `#[trait_variant::make]` or `#[async_trait]`,
//! both of which we explicitly avoid (Tier-1 fix #4). The server holds the
//! dispatcher behind a generic parameter (`WireServer<D: Dispatcher>`)
//! rather than `Arc<dyn Dispatcher>`. The `dispatcher_object_safe_via_arc`
//! test below documents the dyn-incompatibility check.

use bytes::Bytes;
use inferadb_ledger_wire::{RequestContext, WireError};
use tokio::sync::mpsc;

/// Routes an inbound frame to its handler and returns the response payload.
///
/// One [`Dispatcher`] is shared across every connection accepted by a
/// [`crate::server`] `WireServer`, so impls must be `Send + Sync + 'static`.
/// The transport already enforces decode-side framing invariants (header
/// well-formed, payload length within bounds) before invoking dispatch;
/// implementations only see the opcode and the variable-length payload.
///
/// Held as a generic parameter on the server type (not `dyn`) — see the
/// module-level note on object safety.
///
/// Cancellation: handlers MUST race their work against
/// [`RequestContext::cancellation`]. The transport wraps each call in
/// `tokio::select!` so a stream reset propagates without waiting for the
/// handler to notice on its own — but the handler still must yield often
/// enough for `select!` to interrupt cleanly.
pub trait Dispatcher: Send + Sync + 'static {
    /// Handle a unary RPC. Returns the response payload, or a
    /// [`WireError`] that the transport encodes as an error frame.
    fn dispatch(
        &self,
        opcode: u16,
        request: Bytes,
        ctx: RequestContext,
    ) -> impl std::future::Future<Output = Result<Bytes, WireError>> + Send;

    /// Handle a server-streaming RPC. The handler writes successive
    /// response payloads to `sink`; the transport sends each as a stream
    /// chunk frame and emits the terminal stream-end frame when this
    /// future resolves successfully.
    ///
    /// On `Err`, the transport encodes the error as the terminal frame
    /// (the in-flight stream chunks already on the wire are left as-is).
    fn dispatch_stream(
        &self,
        opcode: u16,
        request: Bytes,
        ctx: RequestContext,
        sink: StreamSink,
    ) -> impl std::future::Future<Output = Result<(), WireError>> + Send;

    /// Whether the given opcode dispatches to a server-streaming RPC.
    ///
    /// The transport calls this once per incoming stream to choose between
    /// [`Self::dispatch`] (unary) and [`Self::dispatch_stream`]. The default
    /// returns `false`; the macro-generated dispatcher in Phase 0.0.D
    /// overrides this with an exhaustive opcode → bool table.
    ///
    /// Implementations must be cheap — called on the hot path. A `match`
    /// over a small set of opcodes is the expected shape.
    fn is_streaming(&self, opcode: u16) -> bool {
        let _ = opcode;
        false
    }

    /// Optional hook into the snapshot streaming sub-protocol (Task E.6a).
    ///
    /// The wire-macro DSL doesn't model client-streaming RPCs, so the
    /// `RaftService::install_snapshot_stream` RPC is implemented as a
    /// transport-level escape hatch: the dispatcher recognises opcode
    /// [`inferadb_ledger_wire::OPCODE_INSTALL_SNAPSHOT_STREAM`] and routes
    /// the bidi stream into
    /// [`crate::snapshot_stream::run_snapshot_receive_loop`] instead of
    /// going through [`Self::dispatch`] / [`Self::dispatch_stream`].
    ///
    /// Implementations that don't expose Raft transport (everything except
    /// the production server) leave this as `None` (the default); inbound
    /// snapshot streams to a non-supporting dispatcher are answered with
    /// [`inferadb_ledger_wire::ErrorCode::Internal`].
    ///
    /// Returns `&dyn SnapshotHandler` rather than a concrete type so the
    /// transport can drive the receive loop without a generic parameter
    /// here — same dispatch shape as the rest of the trait.
    fn snapshot_handler(&self) -> Option<&dyn crate::snapshot_stream::SnapshotHandler> {
        None
    }

    /// Optional hook into the inter-node `Replicate` bidi-stream sub-protocol
    /// (Phase 0.0.E.6b).
    ///
    /// The wire-macro DSL doesn't model long-lived bidi streams, so the
    /// `RaftService::replicate` RPC is implemented as a transport-level
    /// escape hatch: the dispatcher recognises opcode
    /// [`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`] and routes
    /// the bidi stream into the [`crate::replicate_stream::ReplicateHandler`]
    /// implementation returned here instead of going through
    /// [`Self::dispatch`] / [`Self::dispatch_stream`].
    ///
    /// Implementations that don't expose Raft transport (everything except
    /// the production server) leave this as `None` (the default); inbound
    /// `Replicate` streams to a non-supporting dispatcher are answered with
    /// `inferadb_ledger_wire::ErrorCode::Unimplemented`.
    ///
    /// Returns `&dyn ReplicateHandler` rather than a concrete type so the
    /// transport can drive the bidi stream without a generic parameter
    /// here — same dispatch shape as [`Self::snapshot_handler`].
    fn replicate_handler(&self) -> Option<&dyn crate::replicate_stream::ReplicateHandler> {
        None
    }
}

/// Outbound channel a streaming handler writes response payloads to.
///
/// Wrapping the channel rather than exposing `mpsc::Sender<Bytes>` directly
/// lets the transport evolve the wire-side encoding (chunk framing,
/// backpressure policy) without touching handler code. For the handler,
/// [`StreamSink::send`] either succeeds or signals that the stream has been
/// torn down by the peer — at which point the handler should drop its work
/// and return.
#[derive(Debug)]
pub struct StreamSink {
    sender: mpsc::Sender<Bytes>,
}

impl StreamSink {
    /// Construct a new sink. Used by the dispatch loop (B.6) to hand a
    /// channel half to the handler; the loop holds the receiving half and
    /// drains it onto the QUIC send stream.
    #[must_use]
    pub fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self { sender }
    }

    /// Send one chunk of payload to the peer. Returns
    /// [`StreamSinkError::Closed`] if the receiver has dropped, which
    /// indicates the peer has reset the stream and the handler should
    /// abort.
    pub async fn send(&self, payload: Bytes) -> Result<(), StreamSinkError> {
        self.sender.send(payload).await.map_err(|_| StreamSinkError::Closed)
    }
}

/// Errors surfaced by [`StreamSink::send`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamSinkError {
    /// The peer reset the stream; subsequent sends will also fail. The
    /// handler should drop in-flight work and return — the dispatch loop
    /// will not encode a terminal stream-end frame in this case.
    #[error("stream closed by peer")]
    Closed,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use inferadb_ledger_wire::{
        AuthMethod, CallerIdentity, ConnectionMetrics, ErrorCode, RequestContext,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn sample_ctx() -> RequestContext {
        RequestContext {
            request_id: 0,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline: None,
            caller_identity: CallerIdentity {
                user_id: None,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::Anonymous,
                token_expiry: Instant::now() + Duration::from_secs(60),
                revocation_epoch: 0,
            },
            peer_addr: "127.0.0.1:0".parse().unwrap(),
            sdk_version: None,
            forwarded_for: None,
            correlation_id: 0,
            cancellation: CancellationToken::new(),
            conn_metrics: Arc::new(ConnectionMetrics),
        }
    }

    struct StubDispatcher;

    impl Dispatcher for StubDispatcher {
        async fn dispatch(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: RequestContext,
        ) -> Result<Bytes, WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
        }

        async fn dispatch_stream(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: RequestContext,
            _sink: StreamSink,
        ) -> Result<(), WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
        }
    }

    /// Compile-time check that `Dispatcher` impls work behind `Arc<T>` —
    /// the server holds one dispatcher and shares it across connection
    /// tasks. Generic, not `dyn`: see the module-level note on object
    /// safety.
    #[test]
    fn dispatcher_object_safe_via_arc() {
        fn assert_arc<T: Dispatcher>(_: Arc<T>) {}
        let d = Arc::new(StubDispatcher);
        assert_arc(d);
        // Force the stub's methods to typecheck without invoking them at
        // runtime — proves the trait shape is reachable.
        let _ = sample_ctx();
    }

    #[tokio::test]
    async fn stream_sink_send_after_close() {
        let (tx, rx) = mpsc::channel::<Bytes>(4);
        drop(rx);
        let sink = StreamSink::new(tx);
        let result = sink.send(Bytes::from_static(b"chunk")).await;
        assert!(matches!(result, Err(StreamSinkError::Closed)));
    }

    #[tokio::test]
    async fn stream_sink_send_succeeds() {
        let (tx, mut rx) = mpsc::channel::<Bytes>(4);
        let sink = StreamSink::new(tx);
        sink.send(Bytes::from_static(b"chunk")).await.expect("send to live receiver");
        let received = rx.recv().await.expect("receive sent chunk");
        assert_eq!(received.as_ref(), b"chunk");
    }
}
