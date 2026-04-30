//! Stream-level dispatch loop.
//!
//! Reads one request frame off the inbound stream, builds a [`RequestContext`],
//! invokes the configured [`Dispatcher`] (unary or streaming as indicated by
//! [`Dispatcher::is_streaming`]), and writes the response frame(s) back. Each
//! handler call is wrapped in `tokio::select!` against the per-stream
//! cancellation token so a stream reset propagates immediately rather than
//! waiting for the handler to notice on its own.
//!
//! # Invariants
//!
//! * Exactly one inbound request frame per stream. The protocol does not permit multiple requests
//!   on a single bi stream — each RPC opens a fresh stream.
//! * Every response or error path writes at least one response frame and then closes the send side.
//!   The exception is cancellation: when the per-stream token fires (peer reset or server shutdown)
//!   we drop without writing.
//! * Streaming responses emit zero-or-more chunk frames (`is_stream_chunk`) followed by exactly one
//!   terminal frame (`is_stream_end`). The terminal frame is either an empty-payload success frame
//!   or a `WireError` payload with `error_code != 0`.

use std::{net::SocketAddr, sync::Arc, time::Instant};

use bytes::Bytes;
use inferadb_ledger_wire::{
    CURRENT_PROTOCOL_VERSION, ConnectionMetrics, ErrorCode, Frame, FrameHeader,
    OPCODE_INSTALL_SNAPSHOT_STREAM, OPCODE_RAFT_PROTOCOL_HANDSHAKE, RequestContext, WireError,
    services::raft::{InstallSnapshotChunk, InstallSnapshotPayload},
};
use quinn::{RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    auth::VerifiedAuth,
    dispatcher::{Dispatcher, StreamSink},
    error::TransportError,
    frame_io::{
        FLAG_IS_RESPONSE, FLAG_IS_STREAM_CHUNK, FLAG_IS_STREAM_END, read_frame, send_error_frame,
        write_frame,
    },
    snapshot_stream::run_snapshot_receive_loop,
};

/// Buffer size for the in-memory channel between a streaming handler and
/// the drain loop. A small fixed bound applies backpressure to the handler
/// when the network can't keep up. Sized to absorb modest jitter without
/// inflating memory per stream.
const STREAM_SINK_BUFFER: usize = 64;

/// Per-stream dispatch loop.
///
/// 1. Read the inbound request frame.
/// 2. Build a [`RequestContext`] from the cached [`VerifiedAuth`], the peer address, and the
///    supplied per-stream cancellation token.
/// 3. Ask the dispatcher whether the opcode is unary or streaming.
/// 4. Race the handler against `cancel.cancelled()`. Encode the result as response frame(s) and
///    finish the send side.
///
/// On any error path that produces a response frame, the function uses
/// [`send_error_frame`] which already swallows write/finish errors at DEBUG.
pub(crate) async fn dispatch_stream<D>(
    mut send: SendStream,
    mut recv: RecvStream,
    auth: Arc<VerifiedAuth>,
    dispatcher: Arc<D>,
    peer_addr: SocketAddr,
    cancel: CancellationToken,
) where
    D: Dispatcher,
{
    let frame = match read_frame(&mut recv).await {
        Ok(f) => f,
        Err(err) => {
            tracing::debug!(error = %err, "request frame read failed");
            send_error_frame(&mut send, [0u8; 16], err_to_wire_error(&err)).await;
            return;
        },
    };

    let request_id = frame.header.request_id;
    let opcode = frame.header.opcode;

    if frame.header.version != CURRENT_PROTOCOL_VERSION {
        send_error_frame(
            &mut send,
            request_id,
            WireError {
                code: ErrorCode::InvalidArgument,
                message: format!("unsupported protocol version {:#04x}", frame.header.version),
                retryable: false,
                retry_after_ms: 0,
                context: Default::default(),
                suggested_action: "upgrade client SDK".into(),
            },
        )
        .await;
        return;
    }

    // Snapshot-streaming sub-protocol (Task E.6a). The wire-macro DSL
    // declares this RPC as a unary placeholder; at runtime we recognise the
    // opcode here and hand the bidi stream off to the dedicated receive
    // loop, which owns the rest of the protocol (header + chunks + footer
    // + acks + atomic rename).
    if opcode == OPCODE_INSTALL_SNAPSHOT_STREAM {
        dispatch_snapshot_stream(send, recv, frame.payload, request_id, dispatcher, cancel).await;
        return;
    }

    // Inter-node `Replicate` bidi-stream sub-protocol (Phase 0.0.E.6b). The
    // wire-macro DSL declares `RaftService::replicate` as a unary placeholder;
    // production traffic opens a long-lived bidi stream whose first frame is
    // the protocol-version handshake at this opcode. We hand the streams off
    // to the dispatcher's `ReplicateHandler` (when installed), which owns
    // the handshake, envelope drain, and ack emission for the lifetime of
    // the bidi.
    if opcode == OPCODE_RAFT_PROTOCOL_HANDSHAKE {
        dispatch_replicate_stream(send, recv, frame.payload, request_id, dispatcher, cancel).await;
        return;
    }

    let ctx = build_request_context(&frame.header, &auth, peer_addr, cancel.clone());

    if dispatcher.is_streaming(opcode) {
        dispatch_streaming(&mut send, opcode, frame.payload, ctx, dispatcher, &cancel).await;
    } else {
        dispatch_unary(&mut send, opcode, frame.payload, ctx, dispatcher, &cancel).await;
    }
}

/// Drive a unary RPC: race the handler against cancellation, then write
/// either a response frame (success) or an error frame (failure).
async fn dispatch_unary<D: Dispatcher>(
    send: &mut SendStream,
    opcode: u16,
    payload: Bytes,
    ctx: RequestContext,
    dispatcher: Arc<D>,
    cancel: &CancellationToken,
) {
    let request_id = ctx.request_id;
    let request_id_bytes = request_id.to_be_bytes();

    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => {
            tracing::debug!(opcode, "unary dispatch cancelled");
            return;
        }
        res = dispatcher.dispatch(opcode, payload, ctx) => res,
    };

    match result {
        Ok(response_payload) => {
            let payload_len = match u32::try_from(response_payload.len()) {
                Ok(n) => n,
                Err(_) => {
                    send_error_frame(
                        send,
                        request_id_bytes,
                        WireError {
                            code: ErrorCode::Internal,
                            message: "response payload exceeds u32::MAX".into(),
                            retryable: false,
                            retry_after_ms: 0,
                            context: Default::default(),
                            suggested_action: "report bug".into(),
                        },
                    )
                    .await;
                    return;
                },
            };
            let mut header = FrameHeader::default();
            header.request_id = request_id_bytes;
            header.opcode = opcode;
            header.version = CURRENT_PROTOCOL_VERSION;
            header.flags = FLAG_IS_RESPONSE;
            header.payload_length = payload_len;
            let frame = Frame { header, payload: response_payload };
            if let Err(err) = write_frame(send, &frame).await {
                tracing::debug!(error = %err, "failed to write unary response");
                return;
            }
            if let Err(err) = send.finish() {
                tracing::debug!(error = %err, "failed to finish unary response stream");
            }
        },
        Err(wire_err) => {
            send_error_frame(send, request_id_bytes, wire_err).await;
        },
    }
}

/// Drive a server-streaming RPC: race the handler against cancellation,
/// drain chunks onto the wire as they arrive, and write the terminal frame.
///
/// Concurrency note: the handler future and the drain are interleaved via
/// `tokio::select!` (not `tokio::join!`). When the handler resolves, its
/// captured [`StreamSink`] drops, which closes the channel, which makes the
/// next `rx.recv()` return `None` — that's how we know the handler is done
/// AND the queue is empty.
async fn dispatch_streaming<D: Dispatcher>(
    send: &mut SendStream,
    opcode: u16,
    payload: Bytes,
    ctx: RequestContext,
    dispatcher: Arc<D>,
    cancel: &CancellationToken,
) {
    let request_id = ctx.request_id;
    let request_id_bytes = request_id.to_be_bytes();
    let (tx, mut rx) = mpsc::channel::<Bytes>(STREAM_SINK_BUFFER);
    let sink = StreamSink::new(tx);

    let dispatch_fut = dispatcher.dispatch_stream(opcode, payload, ctx, sink);
    let mut dispatch_fut = std::pin::pin!(dispatch_fut);
    let mut dispatch_result: Option<Result<(), WireError>> = None;
    let mut wire_err_during_drain: Option<WireError> = None;

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::debug!(opcode, "stream dispatch cancelled");
                return;
            }
            res = &mut dispatch_fut, if dispatch_result.is_none() => {
                dispatch_result = Some(res);
                // Continue the loop — there may be queued chunks to drain
                // before we emit the terminal frame.
            }
            chunk = rx.recv() => {
                match chunk {
                    Some(payload) => {
                        if let Err(err) =
                            write_chunk_frame(send, request_id_bytes, opcode, payload).await
                        {
                            tracing::debug!(error = %err, "failed to write stream chunk");
                            // Cancel the per-stream token so the handler stops
                            // producing — its next `sink.send` (if any) will
                            // already fail because we're about to drop `rx`.
                            cancel.cancel();
                            wire_err_during_drain = Some(WireError {
                                code: ErrorCode::Internal,
                                message: format!("write stream chunk: {err}"),
                                retryable: false,
                                retry_after_ms: 0,
                                context: Default::default(),
                                suggested_action: "stream peer reset; retry idempotently".into(),
                            });
                            break;
                        }
                    }
                    None => {
                        // Channel closed: the handler dropped the StreamSink,
                        // which only happens when the handler future has
                        // returned. We MUST have a `dispatch_result` by now.
                        break;
                    }
                }
            }
        }
    }

    let terminal = if let Some(err) = wire_err_during_drain {
        terminal_error_frame(request_id_bytes, opcode, err)
    } else {
        match dispatch_result {
            Some(Ok(())) => terminal_success_frame(request_id_bytes, opcode),
            Some(Err(handler_err)) => terminal_error_frame(request_id_bytes, opcode, handler_err),
            None => {
                // Should be unreachable: the loop only `break`s when either
                // the channel closed (which implies the handler returned and
                // dropped the sink), or we set `wire_err_during_drain`. But
                // if we somehow hit this, fail closed.
                terminal_error_frame(
                    request_id_bytes,
                    opcode,
                    WireError {
                        code: ErrorCode::Internal,
                        message: "stream loop exited without handler result".into(),
                        retryable: false,
                        retry_after_ms: 0,
                        context: Default::default(),
                        suggested_action: "report bug".into(),
                    },
                )
            },
        }
    };

    if let Err(err) = write_frame(send, &terminal).await {
        tracing::debug!(error = %err, "failed to write terminal stream frame");
        return;
    }
    if let Err(err) = send.finish() {
        tracing::debug!(error = %err, "failed to finish stream response");
    }
}

/// Drive a snapshot-streaming sub-protocol session (Task E.6a).
///
/// The inbound frame's payload is expected to be a postcard-encoded
/// [`InstallSnapshotChunk`] carrying an
/// [`InstallSnapshotPayload::Header`]. This function:
///
/// 1. Decodes the header off the inbound payload. Malformed payloads or a non-`Header` first chunk
///    produce an `InvalidArgument` error frame.
/// 2. Asks the dispatcher for its [`crate::snapshot_stream::SnapshotHandler`] impl. A `None` return
///    (dispatcher doesn't expose Raft transport) produces an `Internal` error frame.
/// 3. Hands off to [`run_snapshot_receive_loop`], which owns the rest of the protocol.
async fn dispatch_snapshot_stream<D: Dispatcher>(
    mut send: SendStream,
    recv: RecvStream,
    inbound_payload: Bytes,
    request_id: [u8; 16],
    dispatcher: Arc<D>,
    cancel: CancellationToken,
) {
    let chunk: InstallSnapshotChunk = match postcard::from_bytes(&inbound_payload) {
        Ok(c) => c,
        Err(err) => {
            send_error_frame(
                &mut send,
                request_id,
                WireError {
                    code: ErrorCode::InvalidArgument,
                    message: format!("snapshot_stream: decode header chunk: {err}"),
                    retryable: false,
                    retry_after_ms: 0,
                    context: Default::default(),
                    suggested_action: "check sender encoding".into(),
                },
            )
            .await;
            return;
        },
    };

    let header = match chunk.payload {
        Some(InstallSnapshotPayload::Header(h)) => h,
        Some(_) => {
            send_error_frame(
                &mut send,
                request_id,
                WireError {
                    code: ErrorCode::InvalidArgument,
                    message: "snapshot_stream: first chunk was not a header".into(),
                    retryable: false,
                    retry_after_ms: 0,
                    context: Default::default(),
                    suggested_action: "send Header chunk first".into(),
                },
            )
            .await;
            return;
        },
        None => {
            send_error_frame(
                &mut send,
                request_id,
                WireError {
                    code: ErrorCode::InvalidArgument,
                    message: "snapshot_stream: chunk missing payload".into(),
                    retryable: false,
                    retry_after_ms: 0,
                    context: Default::default(),
                    suggested_action: "send Header chunk first".into(),
                },
            )
            .await;
            return;
        },
    };

    let Some(handler) = dispatcher.snapshot_handler() else {
        send_error_frame(
            &mut send,
            request_id,
            WireError {
                code: ErrorCode::Internal,
                message: "snapshot_stream: dispatcher does not expose a snapshot handler".into(),
                retryable: false,
                retry_after_ms: 0,
                context: Default::default(),
                suggested_action: "wire snapshot handler into dispatcher".into(),
            },
        )
        .await;
        return;
    };

    run_snapshot_receive_loop(send, recv, request_id, header, handler, cancel).await;
}

/// Drive an inter-node `Replicate` bidi stream (Phase 0.0.E.6b).
///
/// The dispatch loop has already read the first frame off the wire — its
/// payload (the 4-byte big-endian raft protocol version) is passed through
/// as `inbound_payload` so the handler can verify the handshake without
/// re-reading. Routing logic:
///
/// 1. Ask the dispatcher for its [`crate::replicate_stream::ReplicateHandler`] impl. A `None`
///    return (dispatcher doesn't expose Raft transport) produces an `Unimplemented` error frame and
///    ends the stream.
/// 2. Hand off to `handler.handle_replicate_stream`, which owns the rest of the protocol (handshake
///    response, envelope drain, ack emission).
async fn dispatch_replicate_stream<D: Dispatcher>(
    mut send: SendStream,
    recv: RecvStream,
    inbound_payload: Bytes,
    request_id: [u8; 16],
    dispatcher: Arc<D>,
    cancel: CancellationToken,
) {
    let Some(handler) = dispatcher.replicate_handler() else {
        send_error_frame(
            &mut send,
            request_id,
            WireError {
                code: ErrorCode::Internal,
                message: "replicate_stream: dispatcher does not expose a replicate handler".into(),
                retryable: false,
                retry_after_ms: 0,
                context: Default::default(),
                suggested_action: "wire replicate handler into dispatcher".into(),
            },
        )
        .await;
        return;
    };

    if let Err(err) = handler.handle_replicate_stream(send, recv, inbound_payload, cancel).await {
        // Errors are already best-effort surfaced by the handler (handshake
        // mismatch sends a version response then closes; envelope-drain
        // failures log + ack-and-continue or close cleanly). If something
        // bubbles up here, it indicates an unexpected transport-level
        // failure that the handler couldn't recover from — log + drop.
        tracing::debug!(error = %err, "replicate_stream: handler exited with error");
    }
}

/// Encode and write a single stream-chunk frame.
async fn write_chunk_frame(
    send: &mut SendStream,
    request_id: [u8; 16],
    opcode: u16,
    payload: Bytes,
) -> Result<(), TransportError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| TransportError::WriteFrame {
        reason: "stream chunk exceeds u32::MAX".into(),
    })?;
    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.opcode = opcode;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = FLAG_IS_RESPONSE | FLAG_IS_STREAM_CHUNK;
    header.payload_length = payload_len;
    let frame = Frame { header, payload };
    write_frame(send, &frame).await
}

/// Build the success terminal frame for a streaming response: empty payload
/// with `is_response | is_stream_end` flags set.
fn terminal_success_frame(request_id: [u8; 16], opcode: u16) -> Frame {
    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.opcode = opcode;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = FLAG_IS_RESPONSE | FLAG_IS_STREAM_END;
    Frame { header, payload: Bytes::new() }
}

/// Build the error terminal frame for a streaming response: postcard-encoded
/// [`WireError`] with `error_code != 0` and `is_response | is_stream_end`
/// flags set. If postcard fails (which would only happen for a malformed
/// `WireError`), falls back to a tiny canned `Internal` error.
fn terminal_error_frame(request_id: [u8; 16], opcode: u16, err: WireError) -> Frame {
    let payload_bytes = match postcard::to_allocvec(&err) {
        Ok(v) => Bytes::from(v),
        Err(encode_err) => {
            tracing::error!(error = %encode_err, "failed to encode WireError; using fallback");
            let fallback = WireError::new(ErrorCode::Internal, "encode error");
            Bytes::from(postcard::to_allocvec(&fallback).unwrap_or_default())
        },
    };
    let payload_length = u32::try_from(payload_bytes.len()).unwrap_or(u32::MAX);
    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.opcode = opcode;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = FLAG_IS_RESPONSE | FLAG_IS_STREAM_END;
    header.error_code = err.code.as_u32();
    header.payload_length = payload_length;
    Frame { header, payload: payload_bytes }
}

/// Build a [`RequestContext`] from the inbound frame header + cached
/// connection-level identity.
///
/// The deadline conversion is best-effort: `FrameHeader::deadline_unix_nanos`
/// is wall-clock UNIX nanos, which we convert to a monotonic [`Instant`] by
/// subtracting `SystemTime::now()` from the deadline. Wall-clock skew shifts
/// the monotonic deadline by the skew amount, but the alternative (carry
/// nanos through the dispatcher) requires every handler to do its own
/// conversion. This is the same approach `tonic` takes for its
/// `Request::deadline()` shim.
fn build_request_context(
    header: &FrameHeader,
    auth: &VerifiedAuth,
    peer_addr: SocketAddr,
    cancel: CancellationToken,
) -> RequestContext {
    let request_id = u128::from_be_bytes(header.request_id);
    let deadline = if header.deadline_unix_nanos == 0 {
        None
    } else {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        if header.deadline_unix_nanos > now_nanos {
            let remaining = std::time::Duration::from_nanos(header.deadline_unix_nanos - now_nanos);
            Some(Instant::now() + remaining)
        } else {
            // Deadline already past; pass an immediately-expired Instant so
            // the handler observes the timeout on its first deadline check.
            Some(Instant::now())
        }
    };

    RequestContext {
        request_id,
        idempotency_token: header.idempotency_token,
        trace_id: header.trace_id,
        span_id: header.span_id,
        trace_flags: header.trace_flags,
        causality_commit_index: header.causality_commit_index,
        deadline,
        caller_identity: auth.identity.clone(),
        peer_addr,
        // Populated by connection-level metadata in a future phase; B.6 has
        // no way to surface these without protocol-level metadata frames.
        sdk_version: None,
        forwarded_for: None,
        correlation_id: request_id,
        cancellation: cancel,
        conn_metrics: Arc::new(ConnectionMetrics),
    }
}

/// Map a [`TransportError`] from the read path to a [`WireError`] suitable
/// for the response frame. Codec errors that already carry a specific
/// failure reason are surfaced with `InvalidArgument`; everything else maps
/// to `Internal` so the SDK can retry.
fn err_to_wire_error(err: &TransportError) -> WireError {
    match err {
        TransportError::Codec(codec_err) => WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("codec error: {codec_err}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "check SDK frame encoding".into(),
        },
        TransportError::ReadFrame { reason } => WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("read frame: {reason}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "check SDK frame encoding".into(),
        },
        other => WireError {
            code: ErrorCode::Internal,
            message: format!("transport error: {other}"),
            retryable: true,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "retry".into(),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytes::{Bytes, BytesMut};
    use inferadb_ledger_wire::{
        AuthMethod, CURRENT_PROTOCOL_VERSION, CallerIdentity, ErrorCode, Frame, FrameHeader,
        HEADER_SIZE, OPCODE_AUTH_ESTABLISH, RequestContext, WireError, peek_payload_length,
    };
    use tokio::sync::watch;

    use super::*;
    use crate::{
        auth::{AuthError, AuthVerifier, VerifiedAuth},
        dispatcher::{Dispatcher, StreamSink},
        server::WireServer,
        tls,
    };

    /// Verifier that always authenticates the test caller with a future
    /// `token_expiry` so post-handshake streams aren't rejected on the
    /// expiry-check path.
    struct MockVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl MockVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for MockVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            Ok(VerifiedAuth {
                identity: CallerIdentity {
                    user_id: None,
                    app_id: None,
                    organization_id: None,
                    auth_method: AuthMethod::Anonymous,
                    token_expiry: std::time::Instant::now() + Duration::from_secs(60),
                    revocation_epoch: 0,
                },
                token_expiry: std::time::Instant::now() + Duration::from_secs(60),
                revocation_epoch: 0,
            })
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    /// Configurable dispatcher backing the B.6 tests. Each test composes
    /// the unary closure (or stream "action plan") it needs plus the
    /// `streaming_opcodes` allow-list.
    type UnaryFn = dyn Fn(u16, Bytes) -> Result<Bytes, WireError> + Send + Sync + 'static;
    type StreamFn = dyn Fn(u16, Bytes) -> StreamAction + Send + Sync + 'static;

    /// Action a stream handler asks the test harness to perform.
    enum StreamAction {
        /// Successively send `chunks`, then return `Ok(())`.
        SendThenOk(Vec<Bytes>),
        /// Successively send `chunks`, then return `Err(error)`.
        SendThenErr(Vec<Bytes>, WireError),
    }

    struct MockDispatcher {
        unary: Mutex<Option<Box<UnaryFn>>>,
        stream: Mutex<Option<Box<StreamFn>>>,
        streaming_opcodes: Vec<u16>,
    }

    impl MockDispatcher {
        fn unary(
            handler: impl Fn(u16, Bytes) -> Result<Bytes, WireError> + Send + Sync + 'static,
        ) -> Self {
            Self {
                unary: Mutex::new(Some(Box::new(handler))),
                stream: Mutex::new(None),
                streaming_opcodes: Vec::new(),
            }
        }

        fn streaming(
            opcode: u16,
            handler: impl Fn(u16, Bytes) -> StreamAction + Send + Sync + 'static,
        ) -> Self {
            Self {
                unary: Mutex::new(None),
                stream: Mutex::new(Some(Box::new(handler))),
                streaming_opcodes: vec![opcode],
            }
        }
    }

    impl Dispatcher for MockDispatcher {
        async fn dispatch(
            &self,
            opcode: u16,
            request: Bytes,
            _ctx: RequestContext,
        ) -> Result<Bytes, WireError> {
            self.unary
                .lock()
                .unwrap()
                .as_ref()
                .map(|h| h(opcode, request))
                .unwrap_or_else(|| Err(WireError::new(ErrorCode::Internal, "no unary handler")))
        }

        async fn dispatch_stream(
            &self,
            opcode: u16,
            request: Bytes,
            _ctx: RequestContext,
            sink: StreamSink,
        ) -> Result<(), WireError> {
            let action =
                self.stream.lock().unwrap().as_ref().map(|h| h(opcode, request)).unwrap_or_else(
                    || {
                        StreamAction::SendThenErr(
                            Vec::new(),
                            WireError::new(ErrorCode::Internal, "no stream handler"),
                        )
                    },
                );
            match action {
                StreamAction::SendThenOk(chunks) => {
                    for chunk in chunks {
                        if sink.send(chunk).await.is_err() {
                            return Err(WireError::new(ErrorCode::Internal, "sink closed"));
                        }
                    }
                    Ok(())
                },
                StreamAction::SendThenErr(chunks, err) => {
                    for chunk in chunks {
                        if sink.send(chunk).await.is_err() {
                            return Err(WireError::new(ErrorCode::Internal, "sink closed"));
                        }
                    }
                    Err(err)
                },
            }
        }

        fn is_streaming(&self, opcode: u16) -> bool {
            self.streaming_opcodes.contains(&opcode)
        }
    }

    fn loopback_zero() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn test_server_config() -> quinn::ServerConfig {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        let crypto = tls::rustls_server_crypto(chain, key).expect("build server crypto");
        tls::server_config(crypto)
    }

    fn build_test_client_endpoint() -> quinn::Endpoint {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let client_cfg = tls::client_config(crypto);
        let mut endpoint =
            quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .expect("bind client endpoint");
        endpoint.set_default_client_config(client_cfg);
        endpoint
    }

    fn build_server(
        dispatcher: Arc<MockDispatcher>,
    ) -> (WireServer<MockVerifier, MockDispatcher>, SocketAddr) {
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(MockVerifier::new()),
            dispatcher,
        )
        .expect("bind succeeds on ephemeral loopback port");
        let addr = server.local_addr().expect("local_addr after bind");
        (server, addr)
    }

    fn make_auth_establish_frame(payload: &[u8]) -> BytesMut {
        let payload = Bytes::copy_from_slice(payload);
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_AUTH_ESTABLISH;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.payload_length = u32::try_from(payload.len()).unwrap();
        let frame = Frame { header, payload };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode auth establish frame");
        buf
    }

    fn make_request_frame(opcode: u16, payload: &[u8], request_id: [u8; 16]) -> BytesMut {
        let payload = Bytes::copy_from_slice(payload);
        let mut header = FrameHeader::default();
        header.opcode = opcode;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.request_id = request_id;
        header.payload_length = u32::try_from(payload.len()).unwrap();
        let frame = Frame { header, payload };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode request frame");
        buf
    }

    /// Build a request frame with an explicitly-set version byte. Used to
    /// test the unsupported-version rejection path. Encodes through the
    /// normal codec, then patches byte 82.
    fn make_request_frame_with_version(
        opcode: u16,
        payload: &[u8],
        request_id: [u8; 16],
        version: u8,
    ) -> BytesMut {
        let mut buf = make_request_frame(opcode, payload, request_id);
        // The version byte sits at offset 82 in the header (see codec.rs).
        buf[82] = version;
        buf
    }

    async fn read_one_frame_full(recv: &mut quinn::RecvStream) -> Option<Frame> {
        let mut header_buf = [0u8; HEADER_SIZE];
        recv.read_exact(&mut header_buf).await.ok()?;
        let payload_length = peek_payload_length(&header_buf).ok()?;

        let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload_length as usize);
        buf.extend_from_slice(&header_buf);
        if payload_length > 0 {
            let start = buf.len();
            buf.resize(start + payload_length as usize, 0);
            recv.read_exact(&mut buf[start..]).await.ok()?;
        }

        let mut bytes = buf.freeze();
        Frame::decode(&mut bytes).ok()
    }

    /// `peek_payload_length` rejects frames with a non-current version byte.
    /// To get our test frame past the read path and exercise the
    /// dispatch-side version check, we'd need to bypass `peek_payload_length`
    /// entirely — which means writing a hand-rolled raw header. Easier: keep
    /// the version-rejection test in the read path itself (it surfaces as
    /// the same `InvalidArgument` error frame, just one layer earlier).
    async fn perform_handshake(
        client: &quinn::Endpoint,
        server_addr: SocketAddr,
    ) -> quinn::Connection {
        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        let (mut send, mut recv) = connection.open_bi().await.expect("open auth stream");
        let frame_buf = make_auth_establish_frame(b"credential");
        send.write_all(&frame_buf).await.expect("write auth frame");
        let _response = read_one_frame_full(&mut recv).await.expect("auth response");
        send.finish().expect("finish auth stream");
        connection
    }

    #[tokio::test]
    async fn dispatch_unary_round_trip() {
        // Echo dispatcher: respond with the request payload verbatim.
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, payload| Ok(payload)));
        let (_server, server_addr) = build_server(dispatcher);
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let mut request_id = [0u8; 16];
        request_id[15] = 7;
        let req = make_request_frame(0x0010, b"hello-world", request_id);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        let response = tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
            .await
            .expect("response within timeout")
            .expect("read response frame");

        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        assert_eq!(response.header.error_code, 0);
        assert_eq!(response.header.request_id, request_id);
        assert_eq!(response.header.opcode, 0x0010);
        assert_eq!(&response.payload[..], b"hello-world");

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn dispatch_unary_propagates_handler_error() {
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, _payload| {
            Err(WireError::new(ErrorCode::FailedPrecondition, "test failure"))
        }));
        let (_server, server_addr) = build_server(dispatcher);
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let request_id = [0xAA; 16];
        let req = make_request_frame(0x0011, b"input", request_id);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        let response = tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
            .await
            .expect("response within timeout")
            .expect("read response frame");

        assert_eq!(response.header.error_code, ErrorCode::FailedPrecondition.as_u32());
        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        let wire_err: WireError =
            postcard::from_bytes(&response.payload).expect("decode WireError");
        assert_eq!(wire_err.code, ErrorCode::FailedPrecondition);
        assert_eq!(wire_err.message, "test failure");

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn dispatch_unary_handles_cancellation() {
        // Handler that never resolves. We open a stream, write a request,
        // wait briefly to confirm no response is in flight, then close the
        // connection. The per-connection cancel fires, the per-stream
        // child token fires, and `dispatch_unary`'s `tokio::select!` drops
        // the pending handler future without panicking. Successful test
        // exit (no hang, no panic) is the assertion.
        struct PendingDispatcher;
        impl Dispatcher for PendingDispatcher {
            async fn dispatch(
                &self,
                _opcode: u16,
                _request: Bytes,
                _ctx: RequestContext,
            ) -> Result<Bytes, WireError> {
                std::future::pending().await
            }

            async fn dispatch_stream(
                &self,
                _opcode: u16,
                _request: Bytes,
                _ctx: RequestContext,
                _sink: StreamSink,
            ) -> Result<(), WireError> {
                Err(WireError::new(ErrorCode::Internal, "n/a"))
            }
        }

        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(MockVerifier::new()),
            Arc::new(PendingDispatcher),
        )
        .expect("bind");
        let server_addr = server.local_addr().expect("local_addr");
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let req = make_request_frame(0x0012, b"never-resolves", [0u8; 16]);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        // Confirm no response within a short window — the dispatcher is
        // intentionally pending, so a response would indicate a regression.
        let racing =
            tokio::time::timeout(Duration::from_millis(200), read_one_frame_full(&mut recv)).await;
        assert!(
            racing.is_err() || racing.unwrap().is_none(),
            "pending dispatcher must not produce a response",
        );

        // Trigger cancellation via connection close.
        connection.close(0u32.into(), b"test done");
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn dispatch_streaming_round_trip() {
        let dispatcher = Arc::new(MockDispatcher::streaming(0x0020, |_opcode, _payload| {
            StreamAction::SendThenOk(vec![
                Bytes::from_static(b"chunk-a"),
                Bytes::from_static(b"chunk-b"),
                Bytes::from_static(b"chunk-c"),
            ])
        }));
        let (_server, server_addr) = build_server(dispatcher);
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let request_id = [0x42; 16];
        let req = make_request_frame(0x0020, b"start", request_id);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        // Read until we see the terminal frame.
        let mut chunks = Vec::new();
        let mut terminal = None;
        for _ in 0..10 {
            let frame =
                tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
                    .await
                    .expect("frame within timeout")
                    .expect("read frame");
            assert_eq!(frame.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
            assert_eq!(frame.header.request_id, request_id);
            if frame.header.flags & FLAG_IS_STREAM_END != 0 {
                terminal = Some(frame);
                break;
            }
            assert_eq!(frame.header.flags & FLAG_IS_STREAM_CHUNK, FLAG_IS_STREAM_CHUNK);
            chunks.push(frame);
        }

        let terminal = terminal.expect("terminal frame must arrive");
        assert_eq!(chunks.len(), 3, "expected 3 chunk frames before terminal");
        assert_eq!(&chunks[0].payload[..], b"chunk-a");
        assert_eq!(&chunks[1].payload[..], b"chunk-b");
        assert_eq!(&chunks[2].payload[..], b"chunk-c");
        assert_eq!(terminal.header.error_code, 0, "success terminal must have error_code=0");
        assert_eq!(terminal.payload.len(), 0, "success terminal carries empty payload");

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn dispatch_streaming_terminal_error() {
        let dispatcher = Arc::new(MockDispatcher::streaming(0x0021, |_opcode, _payload| {
            StreamAction::SendThenErr(
                vec![Bytes::from_static(b"chunk-x")],
                WireError::new(ErrorCode::Internal, "stream failed"),
            )
        }));
        let (_server, server_addr) = build_server(dispatcher);
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let request_id = [0x55; 16];
        let req = make_request_frame(0x0021, b"start", request_id);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        // Drain frames until the stream-end frame arrives.
        let mut saw_chunk = false;
        let terminal = loop {
            let frame =
                tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
                    .await
                    .expect("frame within timeout")
                    .expect("read frame");
            if frame.header.flags & FLAG_IS_STREAM_END != 0 {
                break frame;
            }
            saw_chunk = true;
            assert_eq!(frame.header.flags & FLAG_IS_STREAM_CHUNK, FLAG_IS_STREAM_CHUNK);
        };
        assert!(saw_chunk, "expected at least one chunk before terminal error");
        assert_eq!(terminal.header.flags & FLAG_IS_STREAM_END, FLAG_IS_STREAM_END);
        assert_eq!(terminal.header.error_code, ErrorCode::Internal.as_u32());
        let wire_err: WireError =
            postcard::from_bytes(&terminal.payload).expect("decode terminal WireError");
        assert_eq!(wire_err.code, ErrorCode::Internal);
        assert_eq!(wire_err.message, "stream failed");

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn dispatch_rejects_unsupported_version() {
        // The version-rejection path lives one layer above
        // `dispatch_streaming`/`dispatch_unary` — `read_frame` invokes
        // `peek_payload_length`, which rejects bad version bytes via
        // `CodecError::UnsupportedVersion`. The dispatch loop catches that
        // and emits a single InvalidArgument error frame.
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, payload| Ok(payload)));
        let (_server, server_addr) = build_server(dispatcher);
        let client = build_test_client_endpoint();
        let connection = perform_handshake(&client, server_addr).await;

        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let request_id = [0x99; 16];
        let req = make_request_frame_with_version(0x0010, b"x", request_id, 0x99);
        send.write_all(&req).await.expect("write request frame");
        send.finish().expect("finish request side");

        let response = tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
            .await
            .expect("response within timeout")
            .expect("read response frame");

        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        assert_eq!(response.header.error_code, ErrorCode::InvalidArgument.as_u32());
        let wire_err: WireError =
            postcard::from_bytes(&response.payload).expect("decode WireError");
        assert_eq!(wire_err.code, ErrorCode::InvalidArgument);

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }
}
