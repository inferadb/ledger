//! Per-connection task: auth handshake, revocation watchdog, accept_bi loop.
//!
//! `handle_connection` is the entry point spawned by the accept loop in
//! [`crate::server::accept`]. It drives the QUIC handshake, performs the
//! auth handshake on the first inbound bi stream, spawns a revocation
//! watchdog tied to the connection's cancellation token, then loops on
//! `connection.accept_bi()` handing each accepted stream to
//! `crate::server::dispatch::dispatch_stream`.
//!
//! # Lifecycle
//!
//! 1. Drive `quinn::Connecting` to a `quinn::Connection`.
//! 2. Accept the first bi stream; expect an `OpAuthEstablish` frame.
//! 3. Verify the credential via [`AuthVerifier::verify`]; cache the [`VerifiedAuth`] on the
//!    connection. On failure, send a single `WireError::Unauthenticated` error frame on the auth
//!    stream and close the connection.
//! 4. Spawn a revocation watchdog that subscribes to [`AuthVerifier::subscribe_epoch`] and cancels
//!    the connection root token when `current_epoch > auth.revocation_epoch`.
//! 5. Loop on `accept_bi()`. For each accepted stream:
//!    - If `Instant::now() >= auth.token_expiry`, write a `WireError:: Unauthenticated` error frame
//!      and drop the stream.
//!    - Otherwise hand it to `crate::server::dispatch::dispatch_stream` under a child cancellation
//!      token.
//! 6. When the loop exits (cancel fired, peer closed, or accept error), cancel the root token
//!    explicitly so the watchdog tears down with us.
//!
//! Stream-level dispatch is a B.5 placeholder; real frame decode +
//! [`Dispatcher::dispatch`] routing lands in B.6. Public `WireServer::
//! shutdown` lands in B.7.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use bytes::Bytes;
use inferadb_ledger_wire::{
    CURRENT_PROTOCOL_VERSION, ErrorCode, Frame, FrameHeader, OPCODE_AUTH_ESTABLISH, WireError,
};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    auth::{AuthError, AuthVerifier, VerifiedAuth},
    dispatcher::Dispatcher,
    error::TransportError,
    frame_io::{FLAG_IS_RESPONSE, read_frame, write_frame},
};

/// QUIC application close code: auth handshake failed.
///
/// QUIC application close codes are 62-bit `VarInt`s opaque to the protocol;
/// we pick small fixed values per close reason so operators can grep them.
const WIRE_ERROR_AUTH_FAILED: u32 = 1;

/// QUIC application close code: server-initiated shutdown (cancel fired).
///
/// The revocation watchdog also surfaces here: when the watchdog fires it
/// cancels the connection root token, which trips the cancel branch in
/// `accept_streams_loop` and closes the connection with this code. From the
/// peer's perspective, "revoked" and "server shutting down" are
/// indistinguishable at the QUIC layer; the difference is only visible in
/// server-side logs (the watchdog logs `revocation epoch advanced` at INFO
/// before cancelling).
const WIRE_ERROR_SHUTDOWN: u32 = 2;

/// Per-connection task spawned by the accept loop.
///
/// Drives QUIC handshake → auth handshake → accept_bi loop. Logs at DEBUG on
/// success, WARN on auth failure or unexpected accept errors. Never panics —
/// every error path drops cleanly so the parent [`TaskTracker`] sees a
/// completed (not aborted) task at shutdown.
///
/// The `cancel` token is the per-connection root: cancelling it stops the
/// accept_bi loop and the revocation watchdog. Per-stream tasks receive
/// `cancel.child_token()`.
pub(crate) async fn handle_connection<V, D>(
    connecting: quinn::Connecting,
    verifier: Arc<V>,
    dispatcher: Arc<D>,
    cancel: CancellationToken,
    tasks: TaskTracker,
) where
    V: AuthVerifier,
    D: Dispatcher,
{
    // 1. Drive QUIC + TLS handshake.
    let connection = match connecting.await {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(error = %err, "wire-transport connection handshake failed");
            return;
        },
    };
    let remote = connection.remote_address();

    // 2. Auth handshake. The first inbound bi stream must carry an `OpAuthEstablish` frame;
    //    everything else is rejected.
    let auth = match auth_handshake(&connection, &*verifier).await {
        Ok(a) => a,
        Err(err) => {
            tracing::warn!(error = %err, %remote, "wire-transport auth handshake failed");
            connection.close(WIRE_ERROR_AUTH_FAILED.into(), b"auth failed");
            return;
        },
    };
    tracing::debug!(%remote, "wire-transport connection authenticated");

    // 3. Revocation watchdog. Subscribed receivers yield the current epoch on first
    //    `borrow_and_update`; the watchdog only fires `cancel` when the sender publishes a value
    //    strictly greater than the cached `auth.revocation_epoch`.
    let watchdog_cancel = cancel.clone();
    let watchdog_epoch = auth.revocation_epoch;
    let mut epoch_rx = verifier.subscribe_epoch();
    let watchdog_task = tasks.spawn(async move {
        revocation_watchdog(&mut epoch_rx, watchdog_epoch, &watchdog_cancel).await;
    });

    // 4. accept_bi loop. Shared `Arc<VerifiedAuth>` so each per-stream task can read `token_expiry`
    //    without cloning the entire identity.
    let auth = Arc::new(auth);
    accept_streams_loop(&connection, &auth, &dispatcher, &cancel, &tasks).await;

    // 5. Tear down. The accept loop already called `connection.close(...)` on its own exit paths;
    //    cancelling the root token is idempotent and ensures the watchdog wakes up if it was
    //    blocked on `changed()`.
    cancel.cancel();
    let _ = watchdog_task.await;

    tracing::debug!(%remote, "wire-transport connection closed");
}

/// Run the auth handshake on the connection's first inbound bi stream.
///
/// Reads exactly one frame, validates its opcode/version/error_code, hands
/// the payload to [`AuthVerifier::verify`], and writes a success-response
/// frame back on the same stream. The success response carries the same
/// `request_id` as the inbound frame (so the client can correlate),
/// `flags = FLAG_IS_RESPONSE`, and an empty payload — credential metadata
/// (e.g., the resolved `revocation_epoch`) is not echoed back; the client
/// already knows what it sent.
async fn auth_handshake<V: AuthVerifier>(
    connection: &quinn::Connection,
    verifier: &V,
) -> Result<VerifiedAuth, AuthError> {
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| AuthError::MalformedCredential(format!("accept_bi failed: {err}")))?;

    let frame = read_frame(&mut recv)
        .await
        .map_err(|err| AuthError::MalformedCredential(format!("frame read: {err}")))?;

    if frame.header.opcode != OPCODE_AUTH_ESTABLISH {
        return Err(AuthError::MalformedCredential(format!(
            "first frame opcode {:#06x}; expected OpAuthEstablish ({:#06x})",
            frame.header.opcode, OPCODE_AUTH_ESTABLISH
        )));
    }
    if frame.header.version != CURRENT_PROTOCOL_VERSION {
        return Err(AuthError::MalformedCredential(format!(
            "first frame version {:#04x}; expected {:#04x}",
            frame.header.version, CURRENT_PROTOCOL_VERSION
        )));
    }
    if frame.header.error_code != 0 {
        return Err(AuthError::MalformedCredential(
            "first frame is an error frame; expected OpAuthEstablish request".into(),
        ));
    }

    let auth = verifier.verify(frame.payload).await?;

    // Send a success response on the same stream so the client can confirm
    // the handshake completed before opening RPC streams.
    //
    // `FrameHeader::_padding` is private to the wire crate, so we initialise
    // via `Default::default()` and overwrite the public fields rather than
    // using struct-update syntax (which would require naming `_padding`).
    let mut response_header = FrameHeader::default();
    response_header.request_id = frame.header.request_id;
    response_header.opcode = OPCODE_AUTH_ESTABLISH;
    response_header.version = CURRENT_PROTOCOL_VERSION;
    response_header.flags = FLAG_IS_RESPONSE;
    let response = Frame { header: response_header, payload: Bytes::new() };

    write_frame(&mut send, &response)
        .await
        .map_err(|err| AuthError::Internal(format!("write auth response: {err}")))?;
    send.finish().map_err(|err| AuthError::Internal(format!("close auth stream: {err}")))?;

    Ok(auth)
}

/// Watch the cluster signing-key epoch and cancel the connection root if it
/// advances past the cached `cached_epoch`.
///
/// `watch::Receiver::changed` returns `Err` only when the sender drops, which
/// for our model means the [`AuthVerifier`] was dropped — exits silently.
async fn revocation_watchdog(
    epoch_rx: &mut watch::Receiver<u64>,
    cached_epoch: u64,
    cancel: &CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                // Connection already tearing down; don't wait for an epoch
                // change that may never come.
                return;
            }
            res = epoch_rx.changed() => {
                if res.is_err() {
                    // Sender dropped (verifier dropped). Stop watching.
                    return;
                }
            }
        }
        let current = *epoch_rx.borrow_and_update();
        if current > cached_epoch {
            tracing::info!(
                connection_epoch = cached_epoch,
                cluster_epoch = current,
                "revocation epoch advanced — closing connection",
            );
            cancel.cancel();
            return;
        }
    }
}

/// Drive the per-connection `accept_bi` loop until cancellation, peer close,
/// or an unexpected accept error.
///
/// Per accepted stream:
/// * If `Instant::now() >= auth.token_expiry`, spawn a small task that writes a single
///   `WireError::Unauthenticated` error frame and drops the stream — the client must reconnect.
/// * Otherwise hand the stream to [`crate::server::dispatch::dispatch_stream`] under a child
///   cancellation token.
async fn accept_streams_loop<D>(
    connection: &quinn::Connection,
    auth: &Arc<VerifiedAuth>,
    dispatcher: &Arc<D>,
    cancel: &CancellationToken,
    tasks: &TaskTracker,
) where
    D: Dispatcher,
{
    let peer_addr = connection.remote_address();
    loop {
        let streams = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::debug!(%peer_addr, "wire-transport connection: shutdown signal received");
                connection.close(WIRE_ERROR_SHUTDOWN.into(), b"server shutting down");
                return;
            }
            res = connection.accept_bi() => match res {
                Ok(streams) => streams,
                Err(quinn::ConnectionError::ApplicationClosed { .. })
                | Err(quinn::ConnectionError::LocallyClosed) => {
                    tracing::debug!(%peer_addr, "wire-transport connection closed cleanly");
                    return;
                }
                Err(err) => {
                    tracing::warn!(error = %err, %peer_addr, "wire-transport accept_bi failed");
                    return;
                }
            },
        };

        // Per-RPC token-expiry check (decision D1).
        if Instant::now() >= auth.token_expiry {
            tasks.spawn(async move {
                let (send, _recv) = streams;
                if let Err(err) = send_unauthenticated_error(send).await {
                    tracing::debug!(
                        error = %err,
                        "failed to send Unauthenticated error frame on expired-token stream",
                    );
                }
            });
            continue;
        }

        let (send, recv) = streams;
        let auth = auth.clone();
        let dispatcher = dispatcher.clone();
        let stream_cancel = cancel.child_token();
        tasks.spawn(async move {
            crate::server::dispatch::dispatch_stream(
                send,
                recv,
                auth,
                dispatcher,
                peer_addr,
                stream_cancel,
            )
            .await;
        });
    }
}

/// Encode and send a single `WireError::Unauthenticated` frame, then close
/// the send side of the stream.
///
/// Used in two places: per-RPC token-expiry rejection (called from
/// [`accept_streams_loop`]) and any future spot where the transport itself
/// needs to surface an auth failure outside the auth handshake.
async fn send_unauthenticated_error(mut send: quinn::SendStream) -> Result<(), TransportError> {
    let wire_err = WireError {
        code: ErrorCode::Unauthenticated,
        message: "credential expired; please refresh".into(),
        retryable: true,
        retry_after_ms: 0,
        context: BTreeMap::new(),
        suggested_action: "reconnect with a fresh credential".into(),
    };
    let payload_vec = postcard::to_allocvec(&wire_err).map_err(|err| {
        TransportError::AuthHandshakeFailed { reason: format!("encode WireError: {err}") }
    })?;
    let payload_length = u32::try_from(payload_vec.len()).map_err(|_| {
        TransportError::AuthHandshakeFailed { reason: "WireError payload too large".into() }
    })?;
    let payload_bytes = Bytes::from(payload_vec);

    let mut header = FrameHeader::default();
    header.version = CURRENT_PROTOCOL_VERSION;
    header.error_code = ErrorCode::Unauthenticated.as_u32();
    header.flags = FLAG_IS_RESPONSE;
    header.payload_length = payload_length;
    let frame = Frame { header, payload: payload_bytes };
    write_frame(&mut send, &frame).await?;
    send.finish().map_err(|err| TransportError::AuthHandshakeFailed {
        reason: format!("close stream: {err}"),
    })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Mutex,
        time::Duration,
    };

    use bytes::{Bytes, BytesMut};
    use inferadb_ledger_wire::{
        AuthMethod, CallerIdentity, ErrorCode, Frame, FrameHeader, HEADER_SIZE,
        OPCODE_AUTH_ESTABLISH, OPCODE_PING, WireError, peek_payload_length,
    };
    use tokio::sync::watch;

    use super::*;
    use crate::{
        auth::{AuthError, VerifiedAuth},
        dispatcher::{Dispatcher, StreamSink},
        server::WireServer,
        tls,
    };

    /// Verifier that returns a configurable result, with a controllable
    /// epoch-watch sender so tests can drive the revocation watchdog.
    struct MockVerifier {
        result: Mutex<Result<VerifiedAuth, AuthError>>,
        epoch_tx: watch::Sender<u64>,
    }

    impl MockVerifier {
        fn new(result: Result<VerifiedAuth, AuthError>) -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { result: Mutex::new(result), epoch_tx: tx }
        }

        fn with_epoch(result: Result<VerifiedAuth, AuthError>, epoch: u64) -> Self {
            let (tx, _rx) = watch::channel(epoch);
            Self { result: Mutex::new(result), epoch_tx: tx }
        }

        fn publish_epoch(&self, epoch: u64) {
            let _ = self.epoch_tx.send(epoch);
        }
    }

    impl AuthVerifier for MockVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            // Cloning the cached result lets tests `verify` repeatedly.
            match &*self.result.lock().unwrap() {
                Ok(auth) => Ok(auth.clone()),
                Err(AuthError::MalformedCredential(s)) => {
                    Err(AuthError::MalformedCredential(s.clone()))
                },
                Err(AuthError::InvalidSignature) => Err(AuthError::InvalidSignature),
                Err(AuthError::Expired) => Err(AuthError::Expired),
                Err(AuthError::PrincipalNotAuthorized(s)) => {
                    Err(AuthError::PrincipalNotAuthorized(s.clone()))
                },
                Err(AuthError::Internal(s)) => Err(AuthError::Internal(s.clone())),
            }
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    struct StubDispatcher;

    impl Dispatcher for StubDispatcher {
        async fn dispatch(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: inferadb_ledger_wire::RequestContext,
        ) -> Result<Bytes, WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
        }

        async fn dispatch_stream(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: inferadb_ledger_wire::RequestContext,
            _sink: StreamSink,
        ) -> Result<(), WireError> {
            Err(WireError::new(ErrorCode::Internal, "stub"))
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

    fn sample_caller_identity() -> CallerIdentity {
        CallerIdentity {
            user_id: None,
            app_id: None,
            organization_id: None,
            auth_method: AuthMethod::Anonymous,
            token_expiry: Instant::now() + Duration::from_secs(60),
            revocation_epoch: 0,
        }
    }

    fn sample_auth_future() -> VerifiedAuth {
        VerifiedAuth {
            identity: sample_caller_identity(),
            token_expiry: Instant::now() + Duration::from_secs(60),
            revocation_epoch: 0,
        }
    }

    fn sample_auth_expired() -> VerifiedAuth {
        VerifiedAuth {
            identity: CallerIdentity {
                token_expiry: Instant::now() - Duration::from_secs(1),
                ..sample_caller_identity()
            },
            token_expiry: Instant::now() - Duration::from_secs(1),
            revocation_epoch: 0,
        }
    }

    /// Bind a server with the given verifier. Returns the server (so the
    /// caller can keep it alive) and its bound `SocketAddr`.
    fn build_server(
        verifier: Arc<MockVerifier>,
    ) -> (WireServer<MockVerifier, StubDispatcher>, SocketAddr) {
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            verifier,
            Arc::new(StubDispatcher),
        )
        .expect("bind succeeds on ephemeral loopback port");
        let addr = server.local_addr().expect("local_addr after bind");
        (server, addr)
    }

    /// Encode a well-formed `OpAuthEstablish` request frame.
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

    /// Encode a frame with a non-auth opcode (used for the "wrong opcode"
    /// rejection test).
    fn make_ping_frame() -> BytesMut {
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_PING;
        header.version = CURRENT_PROTOCOL_VERSION;
        let frame = Frame { header, payload: Bytes::new() };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode ping frame");
        buf
    }

    /// Encode an arbitrary post-auth frame (request body — opcode is opaque
    /// to the B.5 placeholder dispatch).
    fn make_arbitrary_request_frame() -> BytesMut {
        let mut header = FrameHeader::default();
        // Any allocated opcode; the placeholder ignores it.
        header.opcode = 0x0010;
        header.version = CURRENT_PROTOCOL_VERSION;
        let frame = Frame { header, payload: Bytes::new() };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode request frame");
        buf
    }

    /// Read exactly one full frame off `recv` (header + declared payload).
    /// Returns `None` on early stream close before the frame completes.
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

    /// Open the auth stream, send a credential, and wait for the server's
    /// success response. Returns the connection (kept alive) and the
    /// response frame.
    async fn perform_handshake(
        client: &quinn::Endpoint,
        server_addr: SocketAddr,
    ) -> (quinn::Connection, Frame) {
        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        let (mut send, mut recv) = connection.open_bi().await.expect("open auth stream");
        let frame_buf = make_auth_establish_frame(b"credential");
        send.write_all(&frame_buf).await.expect("write auth frame");
        // Don't `finish` yet — the server replies on the same stream.

        let response = read_one_frame_full(&mut recv).await.expect("auth response");
        // Now we're done with the auth stream.
        send.finish().expect("finish auth stream");
        (connection, response)
    }

    #[tokio::test]
    async fn auth_handshake_succeeds_with_valid_credential() {
        let verifier = Arc::new(MockVerifier::new(Ok(sample_auth_future())));
        let (_server, server_addr) = build_server(verifier);
        let client = build_test_client_endpoint();

        let (connection, response) =
            tokio::time::timeout(Duration::from_secs(5), perform_handshake(&client, server_addr))
                .await
                .expect("handshake completes within timeout");

        assert_eq!(response.header.opcode, OPCODE_AUTH_ESTABLISH);
        assert_eq!(response.header.version, CURRENT_PROTOCOL_VERSION);
        assert_eq!(response.header.error_code, 0);
        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        assert_eq!(response.payload.len(), 0);

        // Connection stays up after handshake (server is now in accept_bi
        // mode). Confirm by checking it's not closed yet.
        // We give the runtime a tick to let the server transition.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            connection.close_reason().is_none(),
            "connection should remain open in accept_bi mode after handshake",
        );

        // Tear down cleanly.
        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn auth_handshake_rejects_wrong_opcode() {
        let verifier = Arc::new(MockVerifier::new(Ok(sample_auth_future())));
        let (_server, server_addr) = build_server(verifier);
        let client = build_test_client_endpoint();

        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        let (mut send, mut recv) = connection.open_bi().await.expect("open auth stream");
        let frame_buf = make_ping_frame();
        send.write_all(&frame_buf).await.expect("write ping frame");
        send.finish().expect("finish stream");

        // The server should reject the handshake and close with WIRE_ERROR_AUTH_FAILED.
        // The peer-side recv stream returns EOF / error since the connection drops.
        let _ = read_one_frame_full(&mut recv).await;

        let close_reason = tokio::time::timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("connection closes within timeout");
        match close_reason {
            quinn::ConnectionError::ApplicationClosed(close) => {
                assert_eq!(
                    close.error_code,
                    quinn::VarInt::from_u32(WIRE_ERROR_AUTH_FAILED),
                    "close code should be WIRE_ERROR_AUTH_FAILED",
                );
            },
            other => panic!("expected ApplicationClosed, got {other:?}"),
        }

        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn auth_handshake_rejects_invalid_credential() {
        let verifier = Arc::new(MockVerifier::new(Err(AuthError::InvalidSignature)));
        let (_server, server_addr) = build_server(verifier);
        let client = build_test_client_endpoint();

        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        let (mut send, mut recv) = connection.open_bi().await.expect("open auth stream");
        let frame_buf = make_auth_establish_frame(b"bogus credential");
        send.write_all(&frame_buf).await.expect("write auth frame");
        send.finish().expect("finish stream");

        // Drain anything the server might have written before close.
        let _ = read_one_frame_full(&mut recv).await;

        let close_reason = tokio::time::timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("connection closes within timeout");
        match close_reason {
            quinn::ConnectionError::ApplicationClosed(close) => {
                assert_eq!(close.error_code, quinn::VarInt::from_u32(WIRE_ERROR_AUTH_FAILED));
            },
            other => panic!("expected ApplicationClosed, got {other:?}"),
        }

        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn token_expiry_blocks_subsequent_streams() {
        // Verify cached `token_expiry` is in the past — the per-RPC check in
        // `accept_streams_loop` should reject every post-handshake stream
        // with a `WireError::Unauthenticated` frame.
        let verifier = Arc::new(MockVerifier::new(Ok(sample_auth_expired())));
        let (_server, server_addr) = build_server(verifier);
        let client = build_test_client_endpoint();

        let (connection, _auth_response) = perform_handshake(&client, server_addr).await;

        // Open a post-auth stream. Server should reply with an
        // `Unauthenticated` error frame on it.
        //
        // QUIC streams are lazy: the server sees `accept_bi` complete only
        // when the client sends the first byte. Write any minimal payload
        // (a placeholder request frame) so the server picks the stream up.
        let (mut send, mut recv) = connection.open_bi().await.expect("open RPC stream");
        let req = make_arbitrary_request_frame();
        send.write_all(&req).await.expect("write request frame");
        // Don't `finish` — the server doesn't read the request payload on
        // the rejection path; it just writes the error frame and drops.

        let response = tokio::time::timeout(Duration::from_secs(5), read_one_frame_full(&mut recv))
            .await
            .expect("error frame arrives within timeout")
            .expect("read error frame");

        assert_eq!(response.header.error_code, ErrorCode::Unauthenticated.as_u32());
        assert_eq!(response.header.flags & FLAG_IS_RESPONSE, FLAG_IS_RESPONSE);
        assert!(
            response.header.payload_length > 0,
            "Unauthenticated error frame must carry a WireError payload",
        );

        // Decode the WireError payload to confirm the code.
        let wire_err: WireError =
            postcard::from_bytes(&response.payload).expect("decode WireError payload");
        assert_eq!(wire_err.code, ErrorCode::Unauthenticated);

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    #[tokio::test]
    async fn revocation_watchdog_closes_on_epoch_advance() {
        // Issue auth at epoch 0; then advance the cluster epoch to 1.
        // The watchdog should cancel the connection root, which fires the
        // shutdown branch in `accept_streams_loop` and closes the connection
        // with `WIRE_ERROR_SHUTDOWN`.
        let mut auth = sample_auth_future();
        auth.revocation_epoch = 0;
        auth.identity.revocation_epoch = 0;

        let verifier = Arc::new(MockVerifier::with_epoch(Ok(auth), 0));
        let (_server, server_addr) = build_server(verifier.clone());
        let client = build_test_client_endpoint();

        let (connection, _auth_response) = perform_handshake(&client, server_addr).await;

        // Confirm the connection survived the handshake.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(connection.close_reason().is_none(), "connection should be open before revoke");

        // Trip the watchdog.
        verifier.publish_epoch(1);

        let close_reason = tokio::time::timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("connection closes within timeout");
        match close_reason {
            quinn::ConnectionError::ApplicationClosed(close) => {
                assert_eq!(
                    close.error_code,
                    quinn::VarInt::from_u32(WIRE_ERROR_SHUTDOWN),
                    "watchdog-triggered close uses shutdown code",
                );
            },
            other => panic!("expected ApplicationClosed, got {other:?}"),
        }

        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    // The B.5-era placeholder test
    // `stream_dispatch_placeholder_returns_empty_response` was removed in B.6
    // when `dispatch_stream` gained real frame-based routing. See
    // `crates/wire-transport/src/server/dispatch.rs` for the replacement
    // test suite.
}
