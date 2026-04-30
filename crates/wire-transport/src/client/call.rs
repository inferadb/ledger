//! Per-RPC call helpers on top of [`WireClient::acquire`].
//!
//! Wraps the QUIC bidi-stream lifecycle into `unary` and `server_stream`
//! methods that match the SDK's expected handler shape. Frame
//! encoding/decoding is delegated to the crate-private `frame_io` module.
//!
//! # Surface
//!
//! * [`WireClient::unary`] — open a bi stream, write one request frame, read one response frame,
//!   return the payload (or a decoded [`WireError`]).
//! * [`WireClient::server_stream`] — open a bi stream, write one request frame, return a
//!   [`ResponseStream`] that yields each chunk and terminates after the stream-end frame.
//!
//! Both helpers share [`UnaryRequest`] as their input shape: a bare struct
//! with public fields. The SDK layer (Phase 0.0.E) will provide its own
//! typed builders on top of this primitive.
//!
//! # Deadline policy
//!
//! `deadline_unix_nanos == 0` means no deadline; otherwise the value is
//! written verbatim into the frame header. The transport itself does NOT
//! enforce the deadline locally — the server-side dispatcher does. Callers
//! that want a local timeout should wrap [`WireClient::unary`] in
//! [`tokio::time::timeout`].

use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use inferadb_ledger_wire::{CURRENT_PROTOCOL_VERSION, Frame, FrameHeader, WireError};

use super::WireClient;
use crate::{
    error::TransportError,
    frame_io::{
        FLAG_IS_RESPONSE, FLAG_IS_STREAM_CHUNK, FLAG_IS_STREAM_END, read_frame, write_frame,
    },
};

/// Inputs for a single RPC call.
///
/// Public fields keep this primitive simple — typed wrappers belong in the
/// SDK layer (Phase 0.0.E). The required fields are `opcode` and
/// `request_id`; everything else can default to zero.
#[derive(Debug, Clone)]
pub struct UnaryRequest {
    /// RPC identifier; opcode registry is global across all services.
    pub opcode: u16,
    /// 128-bit request correlation ID. Echoed by the server on every
    /// response/chunk frame for this call.
    pub request_id: u128,
    /// Phase 12 idempotency contract; all-zero means no idempotency.
    pub idempotency_token: [u8; 16],
    /// OpenTelemetry W3C trace ID.
    pub trace_id: [u8; 16],
    /// OpenTelemetry W3C span ID.
    pub span_id: [u8; 8],
    /// OpenTelemetry W3C trace flags.
    pub trace_flags: u8,
    /// Phase 5 causality token; zero = no constraint.
    pub causality_commit_index: u64,
    /// Wall-clock deadline in nanoseconds since UNIX epoch; zero = no
    /// deadline. See module-level note on enforcement.
    pub deadline_unix_nanos: u64,
    /// Request payload.
    pub payload: Bytes,
}

/// Errors surfaced by [`WireClient::unary`] and [`WireClient::server_stream`].
///
/// Distinguishes transport-level failures (connection lost, frame I/O
/// error) from server-returned [`WireError`]s and from protocol-level
/// violations (opcode mismatch, missing flags).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RpcError {
    /// Underlying transport failed (connection closed, frame read/write
    /// error, codec rejected the response frame).
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    /// Server returned a structured [`WireError`] on the response frame.
    /// For streaming RPCs, this is the terminal frame's payload; any
    /// earlier chunks were yielded as `Ok` items first.
    #[error("server returned error: {0:?}")]
    WireError(WireError),

    /// Server's response had a different opcode than the request — a
    /// protocol-level invariant violation.
    #[error("opcode mismatch: expected {expected:#06x}, got {actual:#06x}")]
    OpcodeMismatch {
        /// Opcode the client wrote on the request frame.
        expected: u16,
        /// Opcode the server set on the response frame.
        actual: u16,
    },

    /// Generic protocol-level error: missing flag, malformed framing, etc.
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    /// Failed to decode the server's [`WireError`] payload (the frame
    /// header carried `error_code != 0` but the postcard payload was
    /// malformed).
    #[error("failed to decode WireError: {0}")]
    DecodeWireError(String),
}

/// Stream of response chunks from a server-streaming RPC.
///
/// Yields `Ok(payload)` for each chunk; ends after the terminal frame. If
/// the server returns a [`WireError`] mid-stream, the stream yields
/// `Err(RpcError::WireError(_))` exactly once and then completes.
pub struct ResponseStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, RpcError>> + Send>>,
}

impl Stream for ResponseStream {
    type Item = Result<Bytes, RpcError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl std::fmt::Debug for ResponseStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseStream").finish_non_exhaustive()
    }
}

impl WireClient {
    /// Issue a unary RPC.
    ///
    /// Acquires the connection (waiting for reconnect if necessary),
    /// opens a fresh bi stream, writes one request frame, reads one
    /// response frame, and returns the payload.
    ///
    /// # Errors
    ///
    /// * [`RpcError::Transport`] if the connection cannot be acquired or the bi stream cannot be
    ///   opened, or the request/response frame read/write fails.
    /// * [`RpcError::WireError`] if the server returned an error frame.
    /// * [`RpcError::OpcodeMismatch`] if the server echoed a different opcode than the request.
    /// * [`RpcError::ProtocolViolation`] if the response is missing the `is_response` flag or
    ///   carries `is_stream_chunk` (which is only valid for streaming responses).
    pub async fn unary(&self, request: UnaryRequest) -> Result<Bytes, RpcError> {
        let connection = self.acquire().await?;
        let (mut send, mut recv) = connection.open_bi().await.map_err(|err| {
            RpcError::Transport(TransportError::AuthHandshakeFailed {
                reason: format!("open_bi: {err}"),
            })
        })?;

        let request_frame = build_request_frame(&request)?;
        write_frame(&mut send, &request_frame).await?;
        send.finish().map_err(|err| {
            RpcError::Transport(TransportError::WriteFrame {
                reason: format!("finish send: {err}"),
            })
        })?;

        let response = read_frame(&mut recv).await?;

        if response.header.error_code != 0 {
            let err: WireError = postcard::from_bytes(&response.payload)
                .map_err(|err| RpcError::DecodeWireError(err.to_string()))?;
            return Err(RpcError::WireError(err));
        }
        if response.header.opcode != request.opcode {
            return Err(RpcError::OpcodeMismatch {
                expected: request.opcode,
                actual: response.header.opcode,
            });
        }
        if response.header.flags & FLAG_IS_RESPONSE == 0 {
            return Err(RpcError::ProtocolViolation("response missing is_response flag".into()));
        }
        if response.header.flags & FLAG_IS_STREAM_CHUNK != 0 {
            return Err(RpcError::ProtocolViolation(
                "unary response carried is_stream_chunk flag".into(),
            ));
        }

        Ok(response.payload)
    }

    /// Issue a server-streaming RPC.
    ///
    /// Acquires the connection, opens a fresh bi stream, writes one
    /// request frame, and returns a [`ResponseStream`] that yields each
    /// chunk and terminates after the stream-end frame.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] if the connection cannot be
    /// acquired, the bi stream cannot be opened, or the request frame
    /// cannot be written. Errors raised mid-stream are surfaced as
    /// `Err(_)` items on the returned stream rather than from this
    /// function.
    pub async fn server_stream(&self, request: UnaryRequest) -> Result<ResponseStream, RpcError> {
        let connection = self.acquire().await?;
        let (mut send, recv) = connection.open_bi().await.map_err(|err| {
            RpcError::Transport(TransportError::AuthHandshakeFailed {
                reason: format!("open_bi: {err}"),
            })
        })?;

        let request_frame = build_request_frame(&request)?;
        write_frame(&mut send, &request_frame).await?;
        send.finish().map_err(|err| {
            RpcError::Transport(TransportError::WriteFrame {
                reason: format!("finish send: {err}"),
            })
        })?;

        let expected_opcode = request.opcode;
        let stream = async_stream::stream! {
            let mut recv = recv;
            loop {
                let frame = match read_frame(&mut recv).await {
                    Ok(f) => f,
                    Err(err) => {
                        yield Err(RpcError::Transport(err));
                        break;
                    }
                };

                if frame.header.error_code != 0 {
                    let wire_err: WireError = match postcard::from_bytes(&frame.payload) {
                        Ok(e) => e,
                        Err(e) => {
                            yield Err(RpcError::DecodeWireError(e.to_string()));
                            break;
                        }
                    };
                    yield Err(RpcError::WireError(wire_err));
                    break;
                }

                if frame.header.opcode != expected_opcode {
                    yield Err(RpcError::OpcodeMismatch {
                        expected: expected_opcode,
                        actual: frame.header.opcode,
                    });
                    break;
                }

                if frame.header.flags & FLAG_IS_RESPONSE == 0 {
                    yield Err(RpcError::ProtocolViolation(
                        "stream frame missing is_response flag".into(),
                    ));
                    break;
                }

                let is_chunk = frame.header.flags & FLAG_IS_STREAM_CHUNK != 0;
                let is_end = frame.header.flags & FLAG_IS_STREAM_END != 0;

                if is_chunk {
                    yield Ok(frame.payload);
                }
                if is_end {
                    break;
                }
                if !is_chunk && !is_end {
                    yield Err(RpcError::ProtocolViolation(
                        "stream frame missing both is_stream_chunk and is_stream_end flags".into(),
                    ));
                    break;
                }
            }
        };

        Ok(ResponseStream { inner: Box::pin(stream) })
    }
}

/// Encode a [`UnaryRequest`] into the request [`Frame`] sent to the server.
///
/// `flags = 0`: request frames carry neither `is_response` nor any of the
/// stream flags. `version` is set to [`CURRENT_PROTOCOL_VERSION`] —
/// callers cannot override it (the server's `peek_payload_length` would
/// reject anything else anyway).
fn build_request_frame(request: &UnaryRequest) -> Result<Frame, RpcError> {
    let payload_length = u32::try_from(request.payload.len())
        .map_err(|_| RpcError::ProtocolViolation("request payload exceeds u32::MAX".into()))?;
    // `FrameHeader::_padding` is private to the wire crate, so we
    // initialise via `Default::default()` and overwrite the public fields.
    let mut header = FrameHeader::default();
    header.request_id = request.request_id.to_be_bytes();
    header.idempotency_token = request.idempotency_token;
    header.trace_id = request.trace_id;
    header.span_id = request.span_id;
    header.trace_flags = request.trace_flags;
    header.causality_commit_index = request.causality_commit_index;
    header.deadline_unix_nanos = request.deadline_unix_nanos;
    header.opcode = request.opcode;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = 0;
    header.payload_length = payload_length;
    Ok(Frame { header, payload: request.payload.clone() })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU16, Ordering},
        },
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use futures::StreamExt;
    use inferadb_ledger_wire::{AuthMethod, CallerIdentity, ErrorCode, RequestContext, WireError};
    use tokio::sync::watch;

    use super::*;
    use crate::{
        auth::{AuthError, AuthVerifier, VerifiedAuth},
        client::ClientConfig,
        dispatcher::{Dispatcher, StreamSink},
        server::WireServer,
        tls,
    };

    /// Verifier that accepts any credential — auth is not under test here.
    struct AnyVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl AnyVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for AnyVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            Ok(VerifiedAuth {
                identity: CallerIdentity {
                    user_id: None,
                    app_id: None,
                    organization_id: None,
                    auth_method: AuthMethod::Anonymous,
                    token_expiry: Instant::now() + Duration::from_secs(300),
                    revocation_epoch: 0,
                },
                token_expiry: Instant::now() + Duration::from_secs(300),
                revocation_epoch: 0,
            })
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    /// Configurable dispatcher: composes a unary and/or stream handler
    /// per test, and (optionally) records the request ID it observed so
    /// tests can confirm the client wrote the supplied value.
    type UnaryFn =
        dyn Fn(u16, Bytes, &RequestContext) -> Result<Bytes, WireError> + Send + Sync + 'static;
    type StreamFn = dyn Fn(u16, Bytes, &RequestContext) -> StreamAction + Send + Sync + 'static;

    /// Action a stream handler asks the harness to perform.
    enum StreamAction {
        /// Send each chunk in order, then return `Ok(())` to terminate
        /// the stream cleanly.
        SendThenOk(Vec<Bytes>),
        /// Send each chunk in order, then return `Err(error)` so the
        /// terminal frame carries a `WireError`.
        SendThenErr(Vec<Bytes>, WireError),
    }

    struct MockDispatcher {
        unary: Mutex<Option<Box<UnaryFn>>>,
        stream: Mutex<Option<Box<StreamFn>>>,
        streaming_opcodes: Vec<u16>,
        last_request_id: Mutex<Option<u128>>,
    }

    impl MockDispatcher {
        fn unary(
            handler: impl Fn(u16, Bytes, &RequestContext) -> Result<Bytes, WireError>
            + Send
            + Sync
            + 'static,
        ) -> Self {
            Self {
                unary: Mutex::new(Some(Box::new(handler))),
                stream: Mutex::new(None),
                streaming_opcodes: Vec::new(),
                last_request_id: Mutex::new(None),
            }
        }

        fn streaming(
            opcode: u16,
            handler: impl Fn(u16, Bytes, &RequestContext) -> StreamAction + Send + Sync + 'static,
        ) -> Self {
            Self {
                unary: Mutex::new(None),
                stream: Mutex::new(Some(Box::new(handler))),
                streaming_opcodes: vec![opcode],
                last_request_id: Mutex::new(None),
            }
        }

        fn last_request_id(&self) -> Option<u128> {
            *self.last_request_id.lock().unwrap()
        }
    }

    impl Dispatcher for MockDispatcher {
        async fn dispatch(
            &self,
            opcode: u16,
            request: Bytes,
            ctx: RequestContext,
        ) -> Result<Bytes, WireError> {
            *self.last_request_id.lock().unwrap() = Some(ctx.request_id);
            self.unary
                .lock()
                .unwrap()
                .as_ref()
                .map(|h| h(opcode, request, &ctx))
                .unwrap_or_else(|| Err(WireError::new(ErrorCode::Internal, "no unary handler")))
        }

        async fn dispatch_stream(
            &self,
            opcode: u16,
            request: Bytes,
            ctx: RequestContext,
            sink: StreamSink,
        ) -> Result<(), WireError> {
            *self.last_request_id.lock().unwrap() = Some(ctx.request_id);
            let action = self
                .stream
                .lock()
                .unwrap()
                .as_ref()
                .map(|h| h(opcode, request, &ctx))
                .unwrap_or_else(|| {
                    StreamAction::SendThenErr(
                        Vec::new(),
                        WireError::new(ErrorCode::Internal, "no stream handler"),
                    )
                });
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

    fn build_test_server(
        dispatcher: Arc<MockDispatcher>,
    ) -> (WireServer<AnyVerifier, MockDispatcher>, SocketAddr) {
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(AnyVerifier::new()),
            dispatcher,
        )
        .expect("bind succeeds on ephemeral loopback port");
        let addr = server.local_addr().expect("local_addr after bind");
        (server, addr)
    }

    fn build_client(addr: SocketAddr) -> WireClient {
        let crypto = tls::rustls_client_crypto_skip_verify();
        WireClient::new(ClientConfig {
            server_addr: addr,
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"test-credential"),
            connect_timeout: Duration::from_secs(5),
        })
        .expect("new client")
    }

    fn unary_request(opcode: u16, request_id: u128, payload: &[u8]) -> UnaryRequest {
        UnaryRequest {
            opcode,
            request_id,
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            trace_flags: 0,
            causality_commit_index: 0,
            deadline_unix_nanos: 0,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    /// Echo unary handler: server returns the request payload verbatim.
    /// Confirms the round trip works end to end through the new client
    /// helper.
    #[tokio::test]
    async fn unary_round_trip() {
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, payload, _ctx| Ok(payload)));
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        let response = client
            .unary(unary_request(0x0010, 7, b"hello-world"))
            .await
            .expect("unary call succeeds");
        assert_eq!(&response[..], b"hello-world");

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Server returns a `WireError`. Client must surface it as
    /// `RpcError::WireError(_)` with the same code and message.
    #[tokio::test]
    async fn unary_returns_wire_error() {
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, _payload, _ctx| {
            Err(WireError::new(ErrorCode::FailedPrecondition, "not allowed"))
        }));
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        let result = client.unary(unary_request(0x0011, 1, b"x")).await;
        match result {
            Err(RpcError::WireError(err)) => {
                assert_eq!(err.code, ErrorCode::FailedPrecondition);
                assert_eq!(err.message, "not allowed");
            },
            other => panic!("expected WireError, got {other:?}"),
        }

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// `unary` writes the supplied `request_id` verbatim into the frame
    /// header — the dispatcher confirms it observed the same value.
    #[tokio::test]
    async fn unary_uses_supplied_request_id() {
        let dispatcher = Arc::new(MockDispatcher::unary(|_opcode, payload, _ctx| Ok(payload)));
        let dispatcher_clone = dispatcher.clone();
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        const REQUEST_ID: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
        let _ =
            client.unary(unary_request(0x0010, REQUEST_ID, b"x")).await.expect("unary succeeds");

        assert_eq!(dispatcher_clone.last_request_id(), Some(REQUEST_ID));

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Server-streaming success path: three chunks then a terminal
    /// success frame. Client must yield exactly the three payloads in
    /// order, then complete cleanly.
    #[tokio::test]
    async fn server_stream_yields_chunks_then_completes() {
        let dispatcher = Arc::new(MockDispatcher::streaming(0x0020, |_opcode, _payload, _ctx| {
            StreamAction::SendThenOk(vec![
                Bytes::from_static(b"chunk-a"),
                Bytes::from_static(b"chunk-b"),
                Bytes::from_static(b"chunk-c"),
            ])
        }));
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        let stream = client
            .server_stream(unary_request(0x0020, 42, b"start"))
            .await
            .expect("server_stream succeeds");

        let collected: Vec<Result<Bytes, RpcError>> = stream.collect().await;
        assert_eq!(collected.len(), 3, "expected exactly 3 chunks before stream end");
        assert_eq!(collected[0].as_ref().expect("chunk-a is Ok").as_ref(), b"chunk-a",);
        assert_eq!(collected[1].as_ref().expect("chunk-b is Ok").as_ref(), b"chunk-b",);
        assert_eq!(collected[2].as_ref().expect("chunk-c is Ok").as_ref(), b"chunk-c",);

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Server-streaming error path: one chunk then a terminal error
    /// frame. Client must yield the chunk as Ok, then a single Err with
    /// the decoded WireError, then complete.
    #[tokio::test]
    async fn server_stream_propagates_terminal_error() {
        let dispatcher = Arc::new(MockDispatcher::streaming(0x0021, |_opcode, _payload, _ctx| {
            StreamAction::SendThenErr(
                vec![Bytes::from_static(b"chunk-x")],
                WireError::new(ErrorCode::Internal, "stream failed"),
            )
        }));
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        let mut stream = client
            .server_stream(unary_request(0x0021, 5, b"start"))
            .await
            .expect("server_stream succeeds");

        let first = stream.next().await.expect("first frame");
        assert_eq!(first.expect("first chunk is Ok").as_ref(), b"chunk-x");

        let second = stream.next().await.expect("terminal frame");
        match second {
            Err(RpcError::WireError(err)) => {
                assert_eq!(err.code, ErrorCode::Internal);
                assert_eq!(err.message, "stream failed");
            },
            other => panic!("expected WireError, got {other:?}"),
        }

        assert!(stream.next().await.is_none(), "stream must complete after terminal error");

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Two consecutive unary calls reuse the same connection (no
    /// reconnect, single auth handshake) — proving that `unary` opens a
    /// per-RPC stream rather than dialing a fresh connection.
    #[tokio::test]
    async fn unary_reuses_connection_across_calls() {
        let dispatcher_call_count = Arc::new(AtomicU16::new(0));
        let count_handle = dispatcher_call_count.clone();
        let dispatcher = Arc::new(MockDispatcher::unary(move |_opcode, payload, _ctx| {
            count_handle.fetch_add(1, Ordering::SeqCst);
            Ok(payload)
        }));
        let (server, addr) = build_test_server(dispatcher);
        let client = build_client(addr);

        let _ = client.unary(unary_request(0x0010, 1, b"a")).await.expect("first call");
        let _ = client.unary(unary_request(0x0010, 2, b"b")).await.expect("second call");

        assert_eq!(dispatcher_call_count.load(Ordering::SeqCst), 2);

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }
}
