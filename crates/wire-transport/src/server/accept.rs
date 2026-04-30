//! `WireServer::bind` and the accept loop.
//!
//! Binds a `quinn::Endpoint` to a UDP address and drives an accept loop in
//! the background. Per-incoming-packet, the loop enforces address validation
//! (quinn 0.11's replacement for the old `ServerConfig::use_retry(true)`):
//! any first-packet `Incoming` whose remote address is not yet validated is
//! sent a Retry token via [`quinn::Incoming::retry`] and the loop moves on.
//! Only validated incoming connections proceed to the QUIC handshake and
//! per-connection task spawn.
//!
//! Per-connection task body lives in `crate::connection::handle_connection`:
//! the auth handshake + revocation watchdog landed in B.5, stream-level
//! dispatch in B.6, and the public [`WireServer::shutdown`] API in B.7. The
//! shutdown flow cancels the root token, calls [`quinn::Endpoint::close`] so
//! peers see a QUIC `CONNECTION_CLOSE`, then awaits all in-flight tasks
//! through the [`TaskTracker`] bounded by a caller-supplied timeout.
//!
//! # Type shape
//!
//! `WireServer<V, D>` parameterises the [`AuthVerifier`] and [`Dispatcher`]
//! impls as generics — never `dyn`. The trait methods return
//! `impl Future + Send`, which Rust 1.92's bare async-fn-in-trait support
//! makes dyn-incompatible without `#[trait_variant::make]` or `#[async_trait]`,
//! both of which we explicitly avoid (Tier-1 fix #4 of the SDK rewrite plan).
//! Generic parameters give us static dispatch and avoid an extra crate
//! dependency. The accept loop receives `Arc<V>` / `Arc<D>` clones and hands
//! them to per-connection tasks; B.5 will pass the same `Arc`s into
//! `crate::connection::handle_connection` for cache lookup and dispatch.

use std::{net::SocketAddr, sync::Arc};

use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{auth::AuthVerifier, dispatcher::Dispatcher, error::TransportError};

/// QUIC server bound to a UDP address with a background accept loop.
///
/// Construction succeeds once the endpoint is bound; accept-loop errors are
/// logged but never propagated. Call [`WireServer::shutdown`] to drain
/// cleanly — that fires the root cancellation token, sends a QUIC
/// `CONNECTION_CLOSE` to every active peer, and awaits the [`TaskTracker`]
/// bounded by a caller-supplied timeout.
///
/// The verifier and dispatcher are held as `Arc<V>` / `Arc<D>` so the accept
/// loop and every per-connection task can share one instance — see
/// [module docs](self) for why these are generic, not `dyn`.
pub struct WireServer<V, D>
where
    V: AuthVerifier,
    D: Dispatcher,
{
    endpoint: quinn::Endpoint,
    /// Kept on `Self` so the [`Arc`] reference count covers the lifetime of
    /// every per-connection task; the spawned tasks already hold their own
    /// clones, so `WireServer` itself never reads through this field.
    #[allow(dead_code)]
    verifier: Arc<V>,
    /// Kept on `Self` for the same lifetime reason as `verifier`.
    #[allow(dead_code)]
    dispatcher: Arc<D>,
    /// Root cancellation token. The accept loop watches it; per-connection
    /// tasks receive `cancel.child_token()` so a single `cancel.cancel()` call
    /// fans out through the whole server. Fired by [`WireServer::shutdown`].
    cancel: CancellationToken,
    /// Tracks the accept-loop task and every per-connection task spawned by
    /// it. [`WireServer::shutdown`] calls `tasks.close()` then awaits
    /// `tasks.wait()` to join cleanly. **Not closed at bind time** — the
    /// accept loop must remain able to spawn new per-connection tasks until
    /// shutdown begins.
    tasks: TaskTracker,
}

impl<V, D> WireServer<V, D>
where
    V: AuthVerifier,
    D: Dispatcher,
{
    /// Bind a new QUIC server to `addr` and start the accept loop in the
    /// background.
    ///
    /// Returns once the [`quinn::Endpoint`] is bound; the accept loop runs
    /// as a tracked task and only stops when the cancellation token fires
    /// (B.7) or the endpoint is closed externally. Accept errors inside the
    /// loop are logged at WARN/DEBUG and do not propagate.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Io`] if the underlying UDP socket cannot
    /// be bound (typically `EADDRINUSE` or insufficient privilege for a
    /// privileged port).
    pub fn bind(
        addr: SocketAddr,
        server_config: quinn::ServerConfig,
        verifier: Arc<V>,
        dispatcher: Arc<D>,
    ) -> Result<Self, TransportError> {
        let endpoint = quinn::Endpoint::server(server_config, addr).map_err(TransportError::Io)?;

        let cancel = CancellationToken::new();
        let tasks = TaskTracker::new();

        // Spawn the accept loop. It owns clones of everything it needs so
        // the `WireServer` value can be moved freely after `bind` returns.
        // The accept loop itself is a tracked task — `tasks.wait()` (B.7)
        // joins it along with every per-connection task it spawned.
        tasks.spawn(accept_loop(
            endpoint.clone(),
            verifier.clone(),
            dispatcher.clone(),
            cancel.clone(),
            tasks.clone(),
        ));

        // NOTE: do NOT call `tasks.close()` here. `TaskTracker::close` makes
        // subsequent `spawn()` calls panic, and the accept loop spawns one
        // per-connection task per validated `Incoming`. Closing happens in
        // the public `shutdown` (B.7), after `cancel` is fired and the
        // accept loop has broken out of its `select!`.

        Ok(Self { endpoint, verifier, dispatcher, cancel, tasks })
    }

    /// Local address the QUIC endpoint is bound to.
    ///
    /// Useful in tests that bind to `127.0.0.1:0` and need the
    /// kernel-assigned port to dial back in.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Io`] only if the underlying socket has been
    /// closed concurrently — fresh `WireServer`s never see this.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.endpoint.local_addr().map_err(TransportError::Io)
    }

    /// Initiate graceful shutdown.
    ///
    /// 1. Cancels the root cancellation token, signalling the accept loop, every per-connection
    ///    task, and every per-stream dispatch task to drop in-flight work.
    /// 2. Calls [`quinn::Endpoint::close`] which sends a QUIC `CONNECTION_CLOSE` to every connected
    ///    peer with the supplied application close code and reason. Connected peers see this as a
    ///    transport-level shutdown rather than a TCP-style RST.
    /// 3. Closes the [`TaskTracker`] so no new tasks can be spawned, then awaits all in-flight
    ///    tasks bounded by `timeout`.
    /// 4. On clean drain, briefly awaits [`quinn::Endpoint::wait_idle`] (capped at 500 ms) so the
    ///    final datagrams flush before the UDP socket is dropped. The cap prevents a misbehaving OS
    ///    path from hanging shutdown.
    ///
    /// Returns [`ShutdownOutcome::Clean`] if all tasks completed within the timeout, or
    /// [`ShutdownOutcome::TimedOut`] otherwise. In the timed-out case the in-flight tasks are
    /// still alive — the runtime continues to drive them on whatever scheduler it has, and they
    /// keep observing the cancellation token. Callers that want a hard kill must drop the runtime
    /// (e.g., `Runtime::shutdown_timeout`).
    ///
    /// `close_code` is opaque to QUIC — peers see it as the application close code on
    /// [`quinn::ConnectionError::ApplicationClosed`]. Pass `0u32.into()` for a generic clean
    /// shutdown, or a transport-defined code from a workspace error space.
    ///
    /// `shutdown` consumes `self`. Calling it twice is impossible by type — the underlying UDP
    /// socket is being torn down, the server can't be reused, and the type system reflects that.
    pub async fn shutdown(
        self,
        timeout: std::time::Duration,
        close_code: quinn::VarInt,
        close_reason: &[u8],
    ) -> ShutdownOutcome {
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        tracing::info!(timeout_ms, "wire-transport server shutdown initiated");

        // 1. Cancel root token — accept loop exits at next select; per-stream tasks observe
        //    cancellation at their next select-yield.
        self.cancel.cancel();

        // 2. Send QUIC CONNECTION_CLOSE to every active connection. `Endpoint::close` is
        //    fire-and-forget; peers receive the close asynchronously.
        self.endpoint.close(close_code, close_reason);

        // 3. Stop the tracker accepting new spawns. Existing spawns keep running. Order matters:
        //    cancel before close so any spawn racing in from the accept loop sees the cancel and
        //    exits cleanly rather than being rejected at the spawn boundary.
        self.tasks.close();

        // 4. Await all in-flight tasks bounded by `timeout`.
        match tokio::time::timeout(timeout, self.tasks.wait()).await {
            Ok(()) => {
                tracing::info!("wire-transport server shutdown complete");
                // Final cleanup: drain in-flight datagrams so the OS releases
                // the UDP socket cleanly. `Endpoint::wait_idle` returns once
                // every datagram is flushed; bound it tightly so we don't
                // hang here if the OS path is misbehaving.
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    self.endpoint.wait_idle(),
                )
                .await;
                ShutdownOutcome::Clean
            },
            Err(_elapsed) => {
                tracing::warn!(
                    "wire-transport server shutdown exceeded timeout — \
                     in-flight tasks still observing cancellation"
                );
                ShutdownOutcome::TimedOut
            },
        }
    }
}

/// Result of [`WireServer::shutdown`].
///
/// `Clean` and `TimedOut` are intentionally informational — neither variant
/// triggers any further action on the server's behalf. The Rust async runtime
/// can't safely cancel arbitrary user code, so a handler stuck in a tight
/// non-yielding loop will simply not be reachable from `TimedOut`. Callers
/// that need a hard kill must drop the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// All in-flight tasks completed within the timeout. Endpoint flushed
    /// and dropped.
    Clean,
    /// One or more tasks did not complete within the timeout. They remain
    /// alive on the runtime's scheduler, observing cancellation; further
    /// progress depends on whether they yield voluntarily.
    TimedOut,
}

/// Drive the accept loop until cancellation or endpoint close.
///
/// Spawns one tracked per-connection task per validated `Incoming`. Address
/// validation happens here, not in [`crate::tls::server_config`], because
/// quinn 0.11 moved the retry-token decision to a per-`Incoming` call after
/// removing the `ServerConfig::use_retry(true)` knob.
async fn accept_loop<V, D>(
    endpoint: quinn::Endpoint,
    verifier: Arc<V>,
    dispatcher: Arc<D>,
    cancel: CancellationToken,
    tasks: TaskTracker,
) where
    V: AuthVerifier,
    D: Dispatcher,
{
    loop {
        let incoming = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::debug!("wire-transport accept loop: shutdown signal received");
                break;
            }
            incoming = endpoint.accept() => match incoming {
                Some(inc) => inc,
                None => {
                    tracing::debug!("wire-transport accept loop: endpoint closed");
                    break;
                }
            }
        };

        // H-3 SYN-cookie equivalent. quinn 0.11 removed
        // `ServerConfig::use_retry(true)`; address validation now happens
        // per-`Incoming`. Refusing to start handshake work for an
        // unvalidated peer prevents amplification and per-connection state
        // exhaustion attacks.
        if !incoming.remote_address_validated() {
            // `retry()` consumes the `Incoming` and writes a Retry packet;
            // the client must come back with the resulting token. If the
            // peer sent a previously issued (already-validated) token, the
            // call returns `RetryError` — which means we shouldn't have hit
            // this branch in the first place, so just log and move on.
            if let Err(err) = incoming.retry() {
                tracing::warn!(error = %err, "incoming.retry() failed (already validated?)");
            }
            continue;
        }

        let connecting = match incoming.accept() {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(error = %err, "wire-transport accept failed");
                continue;
            },
        };

        // Per-connection task. The body lives in [`handle_connection`];
        // for now (B.4) it just completes the handshake and closes. B.5
        // replaces the body with the auth-handshake / revocation-watchdog
        // / `accept_bi()` loop, B.6 wires stream-level dispatch into it.
        let verifier = verifier.clone();
        let dispatcher = dispatcher.clone();
        let cancel_child = cancel.child_token();
        let tasks_inner = tasks.clone();
        tasks.spawn(async move {
            crate::connection::handle_connection(
                connecting,
                verifier,
                dispatcher,
                cancel_child,
                tasks_inner,
            )
            .await;
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    };

    use bytes::{Bytes, BytesMut};
    use inferadb_ledger_wire::{
        AuthMethod, CURRENT_PROTOCOL_VERSION, CallerIdentity, Frame, FrameHeader, HEADER_SIZE,
        OPCODE_AUTH_ESTABLISH, WireError, peek_payload_length,
    };
    use tokio::sync::watch;

    use super::*;
    use crate::{
        auth::{AuthError, VerifiedAuth},
        dispatcher::StreamSink,
        tls,
    };

    /// Build a self-signed cert + key pair for `localhost` and the matching
    /// rustls server config. Used by every server-bound test in this module.
    fn test_server_crypto() -> rustls::ServerConfig {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        tls::rustls_server_crypto(chain, key).expect("build server crypto")
    }

    fn test_server_config() -> quinn::ServerConfig {
        tls::server_config(test_server_crypto())
    }

    /// Stub verifier — never actually invoked at this phase (B.4 placeholder
    /// closes the connection before doing any auth handshake), but the
    /// generic signature requires a real impl.
    struct StubVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl StubVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for StubVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            Err(AuthError::Internal("stub".into()))
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
            Err(WireError::new(inferadb_ledger_wire::ErrorCode::Internal, "stub"))
        }

        async fn dispatch_stream(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: inferadb_ledger_wire::RequestContext,
            _sink: StreamSink,
        ) -> Result<(), WireError> {
            Err(WireError::new(inferadb_ledger_wire::ErrorCode::Internal, "stub"))
        }
    }

    /// Verifier that accepts any credential with a far-future `token_expiry`.
    /// Used by the active-connection shutdown tests where we need the auth
    /// handshake to succeed so the per-connection task is in the `accept_bi`
    /// loop when shutdown fires.
    struct PermissiveVerifier {
        epoch_tx: watch::Sender<u64>,
    }

    impl PermissiveVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { epoch_tx: tx }
        }
    }

    impl AuthVerifier for PermissiveVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            let identity = CallerIdentity {
                user_id: None,
                app_id: None,
                organization_id: None,
                auth_method: AuthMethod::Anonymous,
                token_expiry: Instant::now() + Duration::from_secs(300),
                revocation_epoch: 0,
            };
            Ok(VerifiedAuth {
                identity,
                token_expiry: Instant::now() + Duration::from_secs(300),
                revocation_epoch: 0,
            })
        }

        fn subscribe_epoch(&self) -> watch::Receiver<u64> {
            self.epoch_tx.subscribe()
        }
    }

    /// Dispatcher whose unary handler never resolves — used to verify that
    /// shutdown's root cancellation drains an in-flight dispatch via the
    /// `tokio::select!` in `dispatch_unary`.
    struct PendingDispatcher;

    impl Dispatcher for PendingDispatcher {
        async fn dispatch(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: inferadb_ledger_wire::RequestContext,
        ) -> Result<Bytes, WireError> {
            std::future::pending().await
        }

        async fn dispatch_stream(
            &self,
            _opcode: u16,
            _request: Bytes,
            _ctx: inferadb_ledger_wire::RequestContext,
            _sink: StreamSink,
        ) -> Result<(), WireError> {
            Err(WireError::new(inferadb_ledger_wire::ErrorCode::Internal, "n/a"))
        }
    }

    fn loopback_zero() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn build_server() -> WireServer<StubVerifier, StubDispatcher> {
        WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(StubVerifier::new()),
            Arc::new(StubDispatcher),
        )
        .expect("bind succeeds on ephemeral loopback port")
    }

    /// Build a quinn client endpoint with the test-only insecure verifier.
    /// We deliberately don't use `WireClient` (that's B.8) — these tests
    /// exercise the server, not the SDK transport.
    fn build_test_client_endpoint() -> quinn::Endpoint {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let client_cfg = tls::client_config(crypto);
        let mut endpoint =
            quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .expect("bind client endpoint");
        endpoint.set_default_client_config(client_cfg);
        endpoint
    }

    #[tokio::test]
    async fn bind_succeeds_on_loopback_ephemeral_port() {
        let server = build_server();
        let bound = server.local_addr().expect("local_addr after bind");
        assert!(bound.ip().is_loopback(), "bound to loopback");
        assert_ne!(bound.port(), 0, "kernel assigned a real port");
    }

    /// End-to-end smoke test against the accept loop.
    ///
    /// As of B.5, `handle_connection` drives the QUIC handshake and then
    /// awaits the client's first inbound bi stream (the auth handshake).
    /// This test does NOT send the auth frame — it only confirms the
    /// handshake completes (proving the accept loop picked up the
    /// connection, validated the address, and spawned the per-connection
    /// task). End-to-end auth flow tests live in
    /// `crates/wire-transport/src/connection.rs`.
    #[tokio::test]
    async fn accept_loop_completes_handshake() {
        let server = build_server();
        let server_addr = server.local_addr().expect("local_addr");

        let client = build_test_client_endpoint();
        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        // Confirm the connection is alive after handshake; the server is
        // now blocked awaiting the auth-establish frame on the first bi
        // stream.
        assert!(
            connection.close_reason().is_none(),
            "connection should remain open awaiting auth handshake",
        );

        // Tear down endpoints so the test exits promptly. The server's
        // per-connection task observes ApplicationClosed and unwinds.
        connection.close(0u32.into(), b"test done");
        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    /// Bind to a privileged port (1) on loopback. On every supported dev
    /// platform this fails with `EACCES` (Linux/macOS) when running as
    /// non-root. CI runs as non-root; if a future CI image runs as root,
    /// this test will pass instead of fail — surface that by asserting on
    /// the error class, not the message.
    ///
    /// Note: this test is intentionally light. The `Endpoint::server` call
    /// path simply forwards the underlying `bind()` `io::Error` through
    /// `TransportError::Io`, so this is the cleanest way to exercise that
    /// arm without relying on simulated allocator failures.
    #[tokio::test]
    async fn bind_fails_on_privileged_port() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
        let result: Result<WireServer<StubVerifier, StubDispatcher>, _> = WireServer::bind(
            addr,
            test_server_config(),
            Arc::new(StubVerifier::new()),
            Arc::new(StubDispatcher),
        );
        // Accept either the typical EACCES on non-root dev/CI, or any other
        // I/O variant if the host happens to grant the bind. We only assert
        // it didn't return Ok — picking up port 1 silently would be the
        // genuinely surprising outcome.
        match result {
            Ok(_) => panic!("binding to port 1 unexpectedly succeeded"),
            Err(TransportError::Io(_)) => {},
            Err(other) => panic!("expected TransportError::Io, got {other:?}"),
        }
    }

    /// `WireServer::shutdown` on a freshly-bound server with no connected
    /// clients should fire the cancel token, drain the accept-loop task, and
    /// return [`ShutdownOutcome::Clean`] within a tight bound. This replaces
    /// the B.4-era `shutdown_signal_breaks_accept_loop` test that drove the
    /// same path via the (now-removed) `cancel_for_test` / `tasks_for_test`
    /// accessors.
    #[tokio::test]
    async fn shutdown_completes_cleanly_with_no_connections() {
        let server = build_server();
        let outcome = server.shutdown(Duration::from_secs(1), 0u32.into(), b"test shutdown").await;
        assert_eq!(outcome, ShutdownOutcome::Clean);
    }

    /// With one authenticated client connected and the per-connection task
    /// parked in `accept_bi`, `shutdown` must (a) cancel the per-connection
    /// task via the root token, (b) close the endpoint so the peer sees a
    /// QUIC `CONNECTION_CLOSE`, and (c) drain the tracker to
    /// [`ShutdownOutcome::Clean`].
    #[tokio::test]
    async fn shutdown_drains_active_connection() {
        // Bind with the permissive verifier so the auth handshake succeeds
        // and the per-connection task reaches the `accept_bi` loop.
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(PermissiveVerifier::new()),
            Arc::new(StubDispatcher),
        )
        .expect("bind");
        let server_addr = server.local_addr().expect("local_addr");

        let client = build_test_client_endpoint();
        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        // Drive the auth handshake to completion so the server's
        // per-connection task is parked in `accept_bi`. Without this the
        // connection task would still be inside `auth_handshake.accept_bi`
        // and shutdown would still drain — but the more realistic case is
        // a fully-authenticated connection.
        let (mut send, mut recv) = connection.open_bi().await.expect("open auth stream");
        let frame_buf = make_auth_establish_frame(b"credential");
        send.write_all(&frame_buf).await.expect("write auth frame");
        let _response =
            read_one_frame_full(&mut recv).await.expect("auth response arrives before shutdown");
        send.finish().expect("finish auth stream");

        // Shutdown with a distinctive close code so we can assert the peer
        // observes it via the application close.
        const TEST_CLOSE_CODE: u32 = 7;
        let outcome =
            server.shutdown(Duration::from_secs(5), TEST_CLOSE_CODE.into(), b"drain test").await;
        assert_eq!(outcome, ShutdownOutcome::Clean);

        // The peer should see the connection close. The exact close code may
        // be the one we passed to `Endpoint::close` (if the OS got the
        // CONNECTION_CLOSE frame to the peer first) OR `WIRE_ERROR_SHUTDOWN`
        // (if the per-connection task's own `connection.close(...)` raced
        // ahead via the cancel branch in `accept_streams_loop`). Both are
        // valid outcomes — we only assert the connection observed *some*
        // application close, not which one won the race.
        let close_reason = tokio::time::timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("client observes connection close within timeout");
        match close_reason {
            quinn::ConnectionError::ApplicationClosed(_)
            | quinn::ConnectionError::LocallyClosed
            | quinn::ConnectionError::ConnectionClosed(_)
            | quinn::ConnectionError::Reset => {},
            other => panic!("expected an application-level close, got {other:?}"),
        }

        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    /// Mid-flight unary dispatch that never resolves on its own MUST exit via
    /// the per-stream cancel branch when shutdown fires. Confirms that
    /// `shutdown` cooperates with the `tokio::select!` cancel arm in
    /// `dispatch_unary`, not just with handlers that voluntarily yield.
    ///
    /// Note: this is the strongest cancellation case the runtime can express.
    /// A handler that runs CPU-bound code without ever awaiting cannot be
    /// preempted by `select!` — that's a Rust async invariant, not a
    /// transport bug. `std::future::pending()` is the right stand-in: it
    /// never resolves but cooperates with `select!` on every poll.
    #[tokio::test]
    async fn shutdown_with_in_flight_dispatch_completes_via_cancellation() {
        let server = WireServer::bind(
            loopback_zero(),
            test_server_config(),
            Arc::new(PermissiveVerifier::new()),
            Arc::new(PendingDispatcher),
        )
        .expect("bind");
        let server_addr = server.local_addr().expect("local_addr");

        let client = build_test_client_endpoint();
        let connection = client
            .connect(server_addr, "localhost")
            .expect("client connect call")
            .await
            .expect("handshake completes");

        // Auth handshake.
        let (mut auth_send, mut auth_recv) = connection.open_bi().await.expect("open auth stream");
        let auth_frame = make_auth_establish_frame(b"credential");
        auth_send.write_all(&auth_frame).await.expect("write auth frame");
        let _auth_response = read_one_frame_full(&mut auth_recv).await.expect("auth response");
        auth_send.finish().expect("finish auth stream");

        // Open an RPC stream and write a request that the dispatcher will
        // park forever on. Use any allocated unary opcode — `PendingDispatcher`
        // routes everything through `dispatch`.
        let (mut rpc_send, _rpc_recv) = connection.open_bi().await.expect("open RPC stream");
        let rpc_frame = make_arbitrary_request_frame();
        rpc_send.write_all(&rpc_frame).await.expect("write request frame");
        rpc_send.finish().expect("finish request side");

        // Give the server a moment to read the request and enter the
        // pending dispatch — without this we may shut down before the
        // dispatch task is even spawned, which is a less interesting test.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let outcome = server.shutdown(Duration::from_secs(5), 0u32.into(), b"cancel test").await;
        assert_eq!(
            outcome,
            ShutdownOutcome::Clean,
            "pending handler must drain via select cancellation, not time out",
        );

        client.close(0u32.into(), b"test done");
        client.wait_idle().await;
    }

    /// Sanity check: the two outcome variants compare unequal. Trivial guard
    /// against an accidental derive change that collapses them.
    #[test]
    fn shutdown_outcome_variants_distinct() {
        assert_ne!(ShutdownOutcome::Clean, ShutdownOutcome::TimedOut);
    }

    /// Belt-and-suspenders: verify the construction shape works through
    /// the generic accept loop. We can't observe the unvalidated->retry
    /// branch directly without raw QUIC packet manipulation; the assertion
    /// here is that all generic parameters compose without trait-bound
    /// errors at the spawn site. End-to-end retry coverage is in B.11.
    #[tokio::test]
    async fn generic_compose_smoke() {
        // Construct then shut down through the public API — proves all
        // generic parameters compose at the spawn site and that the
        // shutdown path drains cleanly with no connections.
        let server = build_server();
        let _addr = server.local_addr().expect("local_addr");
        let outcome = server.shutdown(Duration::from_secs(1), 0u32.into(), b"compose smoke").await;
        assert_eq!(outcome, ShutdownOutcome::Clean);
    }

    /// Sanity-check that `VerifiedAuth` with future `token_expiry` survives
    /// past the test boundary — purely a guard against accidentally writing
    /// tests that depend on now-vs-now timing in a way that races. Held
    /// here because B.5 exercises it; cheap to keep.
    #[test]
    fn verified_auth_future_expiry_holds() {
        let future = Instant::now() + Duration::from_secs(60);
        assert!(future > Instant::now());
    }

    /// Encode a well-formed `OpAuthEstablish` request frame with the supplied
    /// credential payload. Mirrors the helper in `crate::connection::tests`.
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

    /// Encode an arbitrary post-auth request frame. The opcode is allocated
    /// but otherwise opaque — the dispatcher under test routes it.
    fn make_arbitrary_request_frame() -> BytesMut {
        let mut header = FrameHeader::default();
        header.opcode = 0x0010;
        header.version = CURRENT_PROTOCOL_VERSION;
        let frame = Frame { header, payload: Bytes::new() };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode request frame");
        buf
    }

    /// Read exactly one full frame off `recv` (header + declared payload).
    /// Returns `None` if the stream closes before the frame completes.
    /// Mirrors the helper in `crate::connection::tests`.
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
}
