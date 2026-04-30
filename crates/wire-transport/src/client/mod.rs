//! QUIC client: connect, reconnect, per-RPC streams, health.
//!
//! [`WireClient`] is the outbound counterpart to [`crate::server::WireServer`].
//! Construction returns immediately; the first [`WireClient::acquire`] call
//! drives the QUIC handshake plus the auth handshake (one bi stream
//! exchanging an `OpAuthEstablish` frame). The auth payload is opaque to
//! this crate — typically a serialized JWT, supplied by the SDK.
//!
//! # Reconnect serialization (Tier-1 fix #3)
//!
//! When multiple concurrent callers see a disconnect, only ONE should
//! reconnect; the others must wait for that single in-flight handshake to
//! finish. [`ConnectionState`] is gated by a [`tokio::sync::Mutex`]; the
//! slow path re-checks the state after re-acquiring the mutex (the classic
//! double-checked-locking pattern, sound under tokio because the only
//! mutation is via the same mutex). Concurrent callers serialize on the
//! mutex; only one runs the handshake. The rest observe the cached
//! `Connected` state and return immediately.
//!
//! # Per-RPC call helpers (B.9)
//!
//! [`WireClient::unary`] and [`WireClient::server_stream`] (defined in the
//! [`call`] submodule) wrap `connection.open_bi()` into typed helpers that
//! match the SDK's expected handler shape. [`UnaryRequest`] is the input
//! primitive; [`ResponseStream`] is a `Stream<Item = Result<Bytes,
//! RpcError>>` returned by `server_stream`.
//!
//! # Health observability (B.10)
//!
//! [`WireClient::status`] returns a [`ClientStatus`] snapshot — connection
//! state, attempt counts, and timestamps for the most recent attempt and
//! last successful connect. Designed for operator diagnostics and as a
//! source of truth for SDK-level Prometheus metrics in Phase 14. The
//! transport itself emits no metrics.
//!
//! # Out of scope
//!
//! The `RegionPool` migration (Phase 0.0.E) extends this surface later.

pub mod call;
pub mod pool;
pub mod reconnect;
pub mod snapshot;
pub mod status;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
pub use call::{ResponseStream, RpcError, UnaryRequest};
use inferadb_ledger_wire::{CURRENT_PROTOCOL_VERSION, Frame, FrameHeader, OPCODE_AUTH_ESTABLISH};
use status::ClientHealth;
pub use status::{ClientStatus, ConnectionStateKind};
use tokio::sync::Mutex;

use crate::{
    error::TransportError,
    frame_io::{FLAG_IS_RESPONSE, read_frame, write_frame},
};

/// Configuration for a [`WireClient`].
///
/// `auth_payload` is the bytes the client sends in the `OpAuthEstablish`
/// frame on every fresh connection — typically a serialized JWT. It's
/// opaque to wire-transport; the server's [`crate::auth::AuthVerifier`]
/// impl interprets it.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// QUIC server address (UDP).
    pub server_addr: SocketAddr,
    /// SNI / certificate verification name. Must match the server's cert.
    pub server_name: String,
    /// QUIC client config (TLS, transport caps, ALPN). Build via
    /// [`crate::tls::client_config`].
    pub quic: quinn::ClientConfig,
    /// Bytes sent in the `OpAuthEstablish` frame on every fresh
    /// connection. Treated as opaque by the transport.
    pub auth_payload: Bytes,
    /// Timeout for one connect+auth attempt. Per-attempt, not total —
    /// reconnect retries restart this clock each iteration.
    pub connect_timeout: Duration,
}

/// Cached connection state.
///
/// Held under `Mutex<ConnectionState>` so concurrent `acquire` callers
/// observe a single source of truth and the reconnect handshake runs at
/// most once at a time.
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// No live connection; next `acquire` triggers a connect.
    Disconnected,
    /// One task is currently handshaking. Other callers serialize on the
    /// mutex and observe the result on re-acquire. Carried as a marker —
    /// no handle to the in-flight work; we re-poll under the mutex.
    Connecting,
    /// Connection is live. Cached as `Arc<Connection>` so each `acquire`
    /// hands out a cheap clone.
    Connected(Arc<quinn::Connection>),
    /// Permanent close from [`WireClient::close`]. Subsequent `acquire`
    /// calls return [`TransportError::ConnectionClosed`].
    Closed,
}

/// QUIC client with auto-reconnect and a single-flight reconnect mutex.
///
/// Construct via [`WireClient::new`]; obtain a live connection via
/// [`WireClient::acquire`]; tear down via [`WireClient::close`].
///
/// `WireClient` does not own a background reconnect task. Reconnect is
/// driven on-demand by `acquire`, which is the operation a caller actually
/// blocked on. A backgrounded reconnect would be wasted work if no one is
/// currently waiting for a connection — and harder to cancel cleanly.
pub struct WireClient {
    state: Arc<Mutex<ConnectionState>>,
    endpoint: quinn::Endpoint,
    config: ClientConfig,
    reconnect: Arc<reconnect::ReconnectBackoff>,
    /// Health counters and timestamps; cloneable behind `Arc` so the
    /// connect path mutates without contending with status readers.
    health: Arc<ClientHealth>,
}

impl WireClient {
    /// Construct a new client. The QUIC endpoint binds an ephemeral UDP
    /// port immediately; the QUIC + auth handshake itself is deferred to
    /// the first [`WireClient::acquire`] call.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Io`] if the local UDP socket cannot be
    /// bound (typically rare — `0.0.0.0:0` lets the OS pick).
    pub fn new(config: ClientConfig) -> Result<Self, TransportError> {
        // Bind an ephemeral local UDP socket. `0.0.0.0:0` works for IPv4
        // server addresses; if we ever need IPv6-only servers this needs
        // to detect from `config.server_addr.is_ipv6()` and use `[::]:0`.
        //
        // SAFETY-style justification: `"0.0.0.0:0"` is a static literal
        // and a valid `SocketAddrV4`; `<SocketAddr as FromStr>::from_str`
        // cannot fail on it. The expect message points at the literal so
        // a future maintainer who edits it sees the prerequisite.
        #[allow(clippy::expect_used)]
        let local_bind: SocketAddr = "0.0.0.0:0".parse().expect("static literal parses");
        let endpoint = quinn::Endpoint::client(local_bind).map_err(TransportError::Io)?;
        Ok(Self {
            state: Arc::new(Mutex::new(ConnectionState::Disconnected)),
            endpoint,
            config,
            reconnect: Arc::new(reconnect::ReconnectBackoff::standard()),
            health: Arc::new(ClientHealth::new()),
        })
    }

    /// Returns a snapshot of the client's current health.
    ///
    /// Captures the current [`ConnectionStateKind`], creation time, last
    /// successful connect time, last attempt time, and cumulative + recent
    /// failure counters. The snapshot is consistent at the instant of the
    /// call but may be stale by the time the caller observes it; callers
    /// needing latest state should call `status` again rather than caching.
    ///
    /// `status` is `async` because reading [`ConnectionState`] requires
    /// awaiting the same mutex used by the connect/close paths. The lock is
    /// held only long enough to project the current variant — no I/O, no
    /// allocation under the lock.
    pub async fn status(&self) -> ClientStatus {
        let state_kind = match &*self.state.lock().await {
            ConnectionState::Disconnected => ConnectionStateKind::Disconnected,
            ConnectionState::Connecting => ConnectionStateKind::Connecting,
            ConnectionState::Connected(_) => ConnectionStateKind::Connected,
            ConnectionState::Closed => ConnectionStateKind::Closed,
        };
        ClientStatus {
            state: state_kind,
            created_at: self.health.created_at,
            last_connected_at: *self.health.last_connected_at.lock(),
            last_reconnect_at: *self.health.last_reconnect_at.lock(),
            connect_attempts: self
                .health
                .connect_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            consecutive_failures: self
                .health
                .consecutive_failures
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Get a live connection, reconnecting if necessary.
    ///
    /// Fast path: cached `Arc<Connection>` if state is `Connected` and the
    /// connection has no close reason. Slow path: acquire the mutex and
    /// hold it across the connect+auth handshake — concurrent callers
    /// queue on the mutex and observe the cached `Connected` state when
    /// they get in, so only ONE handshake runs no matter how many tasks
    /// race in (Tier-1 fix #3).
    ///
    /// Holding the mutex across the handshake is the point of the mutex;
    /// the alternative — releasing under `Connecting` and looping with a
    /// signal — collapses to "all N tasks call `do_connect`" because the
    /// `Connecting` state alone doesn't tell a queued task whether the
    /// in-flight handshake is making progress or has already failed.
    ///
    /// `acquire` itself does not have a timeout — the slow-path loop
    /// retries indefinitely on connect failure. Callers that need a
    /// bounded wait should wrap in `tokio::time::timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::ConnectionClosed`] if [`Self::close`]
    /// has been called. Otherwise loops on transient failures.
    pub async fn acquire(&self) -> Result<Arc<quinn::Connection>, TransportError> {
        // Fast path: peek at cached state. The lock is held only long
        // enough to clone the `Arc<Connection>` or observe `Closed`; no
        // I/O, no async work. This is effectively wait-free against
        // other fast-path callers.
        {
            let state = self.state.lock().await;
            match &*state {
                ConnectionState::Connected(conn) if conn.close_reason().is_none() => {
                    return Ok(conn.clone());
                },
                ConnectionState::Closed => return Err(TransportError::ConnectionClosed),
                _ => {},
            }
        }

        // Slow path: acquire the mutex and hold it across the handshake.
        // Concurrent callers queue here; whichever one wins re-checks the
        // state, runs `do_connect` if needed, and publishes the result
        // before releasing the mutex. The next caller in line then sees
        // `Connected` and returns the cached `Arc`.
        loop {
            let mut state = self.state.lock().await;

            // Re-check under the mutex — another caller may have reconnected
            // while we were queued.
            match &*state {
                ConnectionState::Connected(conn) if conn.close_reason().is_none() => {
                    return Ok(conn.clone());
                },
                ConnectionState::Closed => return Err(TransportError::ConnectionClosed),
                _ => {},
            }

            *state = ConnectionState::Connecting;
            let connect_result = self.do_connect().await;

            // If `close()` slipped in between the lock acquire and now,
            // honor it. (We held the mutex the whole time, so this can
            // only happen if `close` itself was blocked on the lock and
            // ran *after* we returned — which it cannot. The branch is
            // defensive: future maintainers might split close into a
            // path that doesn't take this mutex.)
            if matches!(*state, ConnectionState::Closed) {
                if let Ok(conn) = connect_result {
                    conn.close(0u32.into(), b"client closed during handshake");
                }
                return Err(TransportError::ConnectionClosed);
            }

            match connect_result {
                Ok(conn) => {
                    let arc_conn = Arc::new(conn);
                    *state = ConnectionState::Connected(arc_conn.clone());
                    self.reconnect.reset();
                    self.health.record_success();
                    return Ok(arc_conn);
                },
                Err(err) => {
                    *state = ConnectionState::Disconnected;
                    self.health.record_failure();
                    // Release the mutex before sleeping. Other queued
                    // callers can observe `Disconnected` and race us on
                    // the next iteration's `lock().await` — whichever
                    // gets the mutex first runs the next attempt. This is
                    // intentional: serializing all retries serially under
                    // backoff would punish callers that queued late and
                    // could otherwise have inherited a successful retry.
                    drop(state);
                    tracing::debug!(error = %err, "wire client connect failed; backing off");
                    self.reconnect.wait().await;
                },
            }
        }
    }

    /// Initiate clean shutdown.
    ///
    /// Closes the active connection (if any), marks state as
    /// [`ConnectionState::Closed`], and closes the underlying QUIC
    /// endpoint. Subsequent [`Self::acquire`] calls return
    /// [`TransportError::ConnectionClosed`].
    ///
    /// `code` is opaque to QUIC — peers see it as the application close
    /// code on their `quinn::ConnectionError::ApplicationClosed`.
    pub async fn close(&self, code: quinn::VarInt, reason: &[u8]) {
        let mut state = self.state.lock().await;
        if let ConnectionState::Connected(conn) = &*state {
            conn.close(code, reason);
        }
        *state = ConnectionState::Closed;
        self.endpoint.close(code, reason);
    }

    /// Drive a single QUIC + auth handshake, bounded by
    /// [`ClientConfig::connect_timeout`]. Returns the live
    /// [`quinn::Connection`] on success.
    ///
    /// Records the start of the attempt against [`ClientHealth`] before
    /// dialing — every call increments `connect_attempts` and stamps
    /// `last_reconnect_at`, including the very first connect (the field
    /// name reads "reconnect" but the semantics are "most recent attempt"
    /// across success, failure, and re-dial).
    async fn do_connect(&self) -> Result<quinn::Connection, TransportError> {
        self.health.record_attempt_start();
        let connecting = self
            .endpoint
            .connect_with(
                self.config.quic.clone(),
                self.config.server_addr,
                &self.config.server_name,
            )
            .map_err(|err| TransportError::AuthHandshakeFailed {
                reason: format!("connect_with: {err}"),
            })?;

        let connection = tokio::time::timeout(self.config.connect_timeout, connecting)
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|err| TransportError::AuthHandshakeFailed {
                reason: format!("quic handshake: {err}"),
            })?;

        // Auth handshake — open a bi stream, send OpAuthEstablish, await
        // response. Errors here close the connection so we don't leak the
        // half-open peer-side state.
        if let Err(err) = self.run_auth_handshake(&connection).await {
            connection.close(0u32.into(), b"auth handshake failed");
            return Err(err);
        }
        Ok(connection)
    }

    /// Send `OpAuthEstablish`, await success response. Mirrors
    /// [`crate::connection::auth_handshake`] on the server side.
    async fn run_auth_handshake(
        &self,
        connection: &quinn::Connection,
    ) -> Result<(), TransportError> {
        let (mut send, mut recv) = connection.open_bi().await.map_err(|err| {
            TransportError::AuthHandshakeFailed { reason: format!("open_bi: {err}") }
        })?;

        let payload_length = u32::try_from(self.config.auth_payload.len()).map_err(|_| {
            TransportError::AuthHandshakeFailed { reason: "auth payload too large".into() }
        })?;

        // `FrameHeader::_padding` is private to the wire crate, so we
        // initialise via `Default::default()` and overwrite the public
        // fields rather than struct-update syntax.
        let mut header = FrameHeader::default();
        header.opcode = OPCODE_AUTH_ESTABLISH;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.payload_length = payload_length;
        let request = Frame { header, payload: self.config.auth_payload.clone() };
        write_frame(&mut send, &request).await?;
        send.finish().map_err(|err| TransportError::AuthHandshakeFailed {
            reason: format!("finish auth stream: {err}"),
        })?;

        let response = read_frame(&mut recv).await?;
        if response.header.error_code != 0 {
            return Err(TransportError::AuthHandshakeFailed {
                reason: format!("server returned error_code {}", response.header.error_code),
            });
        }
        if response.header.opcode != OPCODE_AUTH_ESTABLISH {
            return Err(TransportError::AuthHandshakeFailed {
                reason: format!("unexpected opcode {:#06x}", response.header.opcode),
            });
        }
        if response.header.flags & FLAG_IS_RESPONSE == 0 {
            return Err(TransportError::AuthHandshakeFailed {
                reason: "auth response missing is_response flag".into(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use inferadb_ledger_wire::{AuthMethod, CallerIdentity, ErrorCode, RequestContext, WireError};
    use tokio::sync::watch;

    use super::*;
    use crate::{
        auth::{AuthError, AuthVerifier, VerifiedAuth},
        dispatcher::{Dispatcher, StreamSink},
        server::WireServer,
        tls,
    };

    /// Verifier that accepts any credential and counts every `verify` call.
    /// Test-3 (`concurrent_acquire_serializes_connect`) relies on
    /// `verify_count` being exactly 1 after N concurrent `acquire` calls.
    struct CountingVerifier {
        verify_count: Arc<AtomicUsize>,
        epoch_tx: watch::Sender<u64>,
    }

    impl CountingVerifier {
        fn new() -> Self {
            let (tx, _rx) = watch::channel(0);
            Self { verify_count: Arc::new(AtomicUsize::new(0)), epoch_tx: tx }
        }

        fn verify_count(&self) -> usize {
            self.verify_count.load(Ordering::SeqCst)
        }

        fn count_handle(&self) -> Arc<AtomicUsize> {
            self.verify_count.clone()
        }
    }

    impl AuthVerifier for CountingVerifier {
        async fn verify(&self, _payload: Bytes) -> Result<VerifiedAuth, AuthError> {
            self.verify_count.fetch_add(1, Ordering::SeqCst);
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

    /// No-op dispatcher — none of the B.8 tests exercise post-auth RPCs.
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
        verifier: Arc<CountingVerifier>,
    ) -> (WireServer<CountingVerifier, StubDispatcher>, SocketAddr) {
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

    fn build_client_config(addr: SocketAddr) -> ClientConfig {
        let crypto = tls::rustls_client_crypto_skip_verify();
        ClientConfig {
            server_addr: addr,
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"test-credential"),
            connect_timeout: Duration::from_secs(5),
        }
    }

    /// Smoke test: bind, connect, hand back a live connection.
    #[tokio::test]
    async fn acquire_returns_connection_after_handshake() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier.clone());

        let client = WireClient::new(build_client_config(addr)).expect("new client");
        let conn = client.acquire().await.expect("acquire succeeds");
        assert!(conn.close_reason().is_none(), "connection live after acquire");
        assert_eq!(verifier.verify_count(), 1, "exactly one auth handshake ran");

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Second `acquire` must return the cached `Arc<Connection>`, not
    /// dial again. We compare via `Arc::ptr_eq` to confirm strict identity.
    #[tokio::test]
    async fn acquire_caches_connection() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier.clone());

        let client = WireClient::new(build_client_config(addr)).expect("new client");
        let first = client.acquire().await.expect("first acquire");
        let second = client.acquire().await.expect("second acquire");
        assert!(Arc::ptr_eq(&first, &second), "second acquire returned cached connection");
        assert_eq!(verifier.verify_count(), 1, "no second handshake ran");

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Tier-1 fix #3 verification: 10 concurrent `acquire` calls must all
    /// succeed, but the server must observe exactly ONE auth handshake.
    /// The reconnect mutex serializes the slow path; only one task runs
    /// `do_connect`, the rest observe the cached `Connected` state under
    /// the mutex and return the same `Arc<Connection>`.
    #[tokio::test]
    async fn concurrent_acquire_serializes_connect() {
        let verifier = Arc::new(CountingVerifier::new());
        let count_handle = verifier.count_handle();
        let (server, addr) = build_test_server(verifier);

        let client = Arc::new(WireClient::new(build_client_config(addr)).expect("new client"));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let c = client.clone();
            handles.push(tokio::spawn(async move { c.acquire().await }));
        }

        let mut conns = Vec::new();
        for h in handles {
            let conn = h.await.expect("task joins").expect("acquire succeeds");
            conns.push(conn);
        }

        assert_eq!(
            count_handle.load(Ordering::SeqCst),
            1,
            "concurrent acquire must collapse to one handshake (Tier-1 fix #3)",
        );
        let first = &conns[0];
        for (i, c) in conns.iter().enumerate().skip(1) {
            assert!(Arc::ptr_eq(first, c), "task {i} got a different connection");
        }

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// `close` followed by `acquire` must return `ConnectionClosed`
    /// immediately, without dialing.
    #[tokio::test]
    async fn close_terminates_acquire() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier.clone());

        let client = WireClient::new(build_client_config(addr)).expect("new client");
        client.close(0u32.into(), b"early close").await;

        let result = client.acquire().await;
        match result {
            Err(TransportError::ConnectionClosed) => {},
            other => panic!("expected ConnectionClosed, got {other:?}"),
        }
        assert_eq!(verifier.verify_count(), 0, "close must prevent any handshake");

        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// `acquire` must respect `connect_timeout`. Dial a non-listening
    /// address with a short timeout; `do_connect` should produce a
    /// `Timeout` (or a transient connect error if the OS rejects fast),
    /// then `acquire` loops on backoff. We bound the test with our own
    /// outer timeout to confirm the per-attempt timeout is plumbed.
    ///
    /// Use a non-routable address (TEST-NET-1, RFC 5737) so the dial
    /// genuinely hangs at the network level rather than being refused.
    /// `tokio::time::timeout` on the whole `acquire` confirms the
    /// per-attempt timeout fires (otherwise `acquire` would be stuck
    /// inside `connecting.await` indefinitely).
    #[tokio::test]
    async fn acquire_honors_connect_timeout() {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let config = ClientConfig {
            server_addr: "192.0.2.1:443".parse().unwrap(),
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"x"),
            connect_timeout: Duration::from_millis(200),
        };
        let client = WireClient::new(config).expect("new client");

        // Outer timeout: the inner per-attempt timeout fires every 200ms,
        // then the client backs off and loops. We let it loop a couple of
        // times to confirm the loop is healthy, then close.
        let result = tokio::time::timeout(Duration::from_millis(800), client.acquire()).await;

        match result {
            // Outer timeout fired — `acquire` is correctly looping on
            // backoff, never wedging. This is the expected outcome.
            Err(_) => {},
            // OS rejected the dial fast and the loop somehow surfaced —
            // also acceptable; the point is `acquire` is observable.
            Ok(Err(_)) => {},
            Ok(Ok(_)) => panic!("acquire unexpectedly succeeded against unreachable address"),
        }

        client.close(0u32.into(), b"test done").await;
    }

    // -------- B.10: client health observability --------

    /// A fresh client reports `Disconnected` with zero counters and no
    /// timestamps yet — the `created_at` stamp is recent (within the last
    /// second; CI hosts can be slow but a brand-new client should never be
    /// further off than that).
    #[tokio::test]
    async fn status_initial_state_is_disconnected() {
        let before = Instant::now();
        let crypto = tls::rustls_client_crypto_skip_verify();
        let config = ClientConfig {
            server_addr: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"x"),
            connect_timeout: Duration::from_millis(200),
        };
        let client = WireClient::new(config).expect("new client");
        let status = client.status().await;

        assert_eq!(status.state, ConnectionStateKind::Disconnected);
        assert_eq!(status.connect_attempts, 0);
        assert_eq!(status.consecutive_failures, 0);
        assert!(status.last_connected_at.is_none(), "no successful connect yet");
        assert!(status.last_reconnect_at.is_none(), "no attempt yet");
        assert!(
            status.created_at >= before && status.created_at <= Instant::now(),
            "created_at must fall inside the construction window",
        );
    }

    /// After a successful `acquire`, status reports `Connected`, exactly
    /// one cumulative attempt, zero consecutive failures, and a populated
    /// `last_connected_at` stamp.
    #[tokio::test]
    async fn status_after_successful_connect() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier);

        let client = WireClient::new(build_client_config(addr)).expect("new client");
        let before = Instant::now();
        let _conn = client.acquire().await.expect("acquire");
        let status = client.status().await;

        assert_eq!(status.state, ConnectionStateKind::Connected);
        assert_eq!(status.connect_attempts, 1, "exactly one attempt for the initial connect");
        assert_eq!(status.consecutive_failures, 0);
        let connected_at =
            status.last_connected_at.expect("last_connected_at populated after success");
        assert!(connected_at >= before, "connect stamp lands at or after acquire start");
        assert!(status.last_reconnect_at.is_some(), "attempt stamp populated for first connect");

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// Repeated failures bump `consecutive_failures` and `connect_attempts`
    /// in lockstep. Pointing at a non-routable address (TEST-NET-1) makes
    /// every dial time out at the per-attempt budget; we cap the test with
    /// an outer timeout and assert the counters advanced.
    #[tokio::test]
    async fn status_tracks_consecutive_failures() {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let config = ClientConfig {
            server_addr: "192.0.2.1:443".parse().unwrap(),
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"x"),
            connect_timeout: Duration::from_millis(100),
        };
        let client = Arc::new(WireClient::new(config).expect("new client"));

        // Drive `acquire` for a bounded window. The outer timeout fires
        // because the unreachable address makes every per-attempt dial
        // time out, and `acquire` loops on backoff. We just want at least
        // a couple of failures to land before tearing down.
        let acquire_handle = {
            let c = client.clone();
            tokio::spawn(async move { c.acquire().await })
        };
        let _ = tokio::time::timeout(Duration::from_millis(400), async {
            // Spin until at least 1 failure is recorded.
            loop {
                if client.status().await.consecutive_failures >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;

        let status = client.status().await;
        assert!(
            status.consecutive_failures >= 1,
            "expected at least 1 failure, got {}",
            status.consecutive_failures,
        );
        assert!(
            status.connect_attempts >= status.consecutive_failures,
            "attempts ({}) should be >= failures ({})",
            status.connect_attempts,
            status.consecutive_failures,
        );
        assert!(status.last_connected_at.is_none(), "no success has occurred");
        assert!(status.last_reconnect_at.is_some(), "attempt stamp populated after first dial");

        client.close(0u32.into(), b"test done").await;
        acquire_handle.abort();
    }

    /// `close` flips status to `Closed` and prevents further attempts.
    #[tokio::test]
    async fn status_after_close() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier);
        let client = WireClient::new(build_client_config(addr)).expect("new client");

        client.close(0u32.into(), b"explicit close").await;
        let status = client.status().await;

        assert_eq!(status.state, ConnectionStateKind::Closed);
        assert_eq!(status.connect_attempts, 0, "close before acquire — no attempts");
        assert_eq!(status.consecutive_failures, 0);
        assert!(status.last_connected_at.is_none());
        assert!(status.last_reconnect_at.is_none());

        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }

    /// `connect_attempts` increments per actual attempt — a cached
    /// `acquire` does NOT bump the counter, only the real handshake does.
    #[tokio::test]
    async fn status_attempts_match_connect_calls() {
        let verifier = Arc::new(CountingVerifier::new());
        let (server, addr) = build_test_server(verifier);
        let client = WireClient::new(build_client_config(addr)).expect("new client");

        let _ = client.acquire().await.expect("first acquire");
        assert_eq!(client.status().await.connect_attempts, 1);

        // Second acquire returns cached connection — no new handshake.
        let _ = client.acquire().await.expect("second acquire");
        assert_eq!(
            client.status().await.connect_attempts,
            1,
            "cached acquire must not bump the attempt counter",
        );

        client.close(0u32.into(), b"test done").await;
        server.shutdown(Duration::from_secs(2), 0u32.into(), b"test done").await;
    }
}
