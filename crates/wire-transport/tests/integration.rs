//! End-to-end integration tests for the wire-transport crate.
//!
//! Each test stands up a real `WireServer` + `WireClient` over QUIC on
//! loopback with rcgen-generated certs. The test-only
//! `tls::rustls_client_crypto_skip_verify` short-circuits cert verification
//! so we don't need to bundle a CA — that helper is gated behind the
//! `insecure-skip-verify` feature, and `Cargo.toml` lists this test target
//! with `required-features = ["insecure-skip-verify"]` so cargo skips
//! silently when the feature isn't on.
//!
//! These tests live in the `tests/` integration-test directory rather than
//! inline `#[cfg(test)]` modules because they exercise the public crate
//! surface end-to-end (server + client + dispatcher + cancellation) at a
//! higher concurrency and integration level than the unit tests. Inline
//! `#[cfg(test)] mod tests` blocks in `src/**` cover the per-surface
//! behavior; this file covers stack-wide invariants — the kind of bug a
//! single-surface test cannot catch on its own.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use inferadb_ledger_wire::{AuthMethod, CallerIdentity, ErrorCode, RequestContext, WireError};
use inferadb_ledger_wire_transport::{
    AuthError, AuthVerifier, ClientConfig, Dispatcher, ShutdownOutcome, StreamSink, UnaryRequest,
    VerifiedAuth, WireClient, WireServer, tls,
};
use tokio::sync::watch;

// ─── Common test infrastructure ────────────────────────────────────────────

/// Verifier that accepts every credential. Identity is anonymous, expiry is
/// far in the future, revocation epoch is zero. Adequate for exercising the
/// stack end-to-end where auth is not the system under test.
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
            token_expiry: Instant::now() + Duration::from_secs(3600),
            revocation_epoch: 0,
        };
        Ok(VerifiedAuth {
            identity,
            token_expiry: Instant::now() + Duration::from_secs(3600),
            revocation_epoch: 0,
        })
    }

    fn subscribe_epoch(&self) -> watch::Receiver<u64> {
        self.epoch_tx.subscribe()
    }
}

/// Dispatcher that echoes the request payload back as the response.
struct EchoDispatcher;

impl Dispatcher for EchoDispatcher {
    async fn dispatch(
        &self,
        _opcode: u16,
        request: Bytes,
        _ctx: RequestContext,
    ) -> Result<Bytes, WireError> {
        Ok(request)
    }

    async fn dispatch_stream(
        &self,
        _opcode: u16,
        _request: Bytes,
        _ctx: RequestContext,
        _sink: StreamSink,
    ) -> Result<(), WireError> {
        Err(WireError::new(ErrorCode::Internal, "EchoDispatcher does not support streaming"))
    }
}

/// Build the rustls + quinn server config from a freshly-generated
/// self-signed cert for `localhost`. Mirrors the helper used inline in the
/// `src/**` unit tests.
fn build_server_config() -> quinn::ServerConfig {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert");
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()),
    );
    let crypto = tls::rustls_server_crypto(vec![cert_der], key_der).expect("server crypto");
    tls::server_config(crypto)
}

fn loopback_zero() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

/// Bring up a `WireServer<V, D>` bound to an ephemeral loopback port plus a
/// `WireClient` configured to dial it. Returns both so the test can drive
/// the round trip and tear down cleanly.
async fn build_test_stack<V, D>(verifier: V, dispatcher: D) -> (WireServer<V, D>, WireClient)
where
    V: AuthVerifier,
    D: Dispatcher,
{
    let server = WireServer::bind(
        loopback_zero(),
        build_server_config(),
        Arc::new(verifier),
        Arc::new(dispatcher),
    )
    .expect("bind succeeds on ephemeral loopback port");
    let server_addr = server.local_addr().expect("local_addr after bind");

    let client_crypto = tls::rustls_client_crypto_skip_verify();
    let client_quic = tls::client_config(client_crypto);

    let client_config = ClientConfig {
        server_addr,
        server_name: "localhost".to_string(),
        quic: client_quic,
        auth_payload: Bytes::from_static(b"integration-test-credential"),
        connect_timeout: Duration::from_secs(2),
    };
    let client = WireClient::new(client_config).expect("new wire client");

    (server, client)
}

/// Build a minimal `UnaryRequest` for an integer index `i`. Opcode is held
/// constant (0x0010 — an arbitrary unary opcode), payload is `b"hello-{i}"`.
fn echo_request(i: usize) -> (UnaryRequest, Bytes) {
    let payload = Bytes::from(format!("hello-{i}"));
    let req = UnaryRequest {
        opcode: 0x0010,
        request_id: i as u128,
        idempotency_token: [0u8; 16],
        trace_id: [0u8; 16],
        span_id: [0u8; 8],
        trace_flags: 0,
        causality_commit_index: 0,
        deadline_unix_nanos: 0,
        payload: payload.clone(),
    };
    (req, payload)
}

// ─── Test 1 — round-trip echo at high concurrency ──────────────────────────

/// Issue many concurrent unary calls against a single connection, asserting
/// that every payload round-trips correctly. Stresses stream multiplexing on
/// a single QUIC connection.
///
/// `N_CALLS` stays well under the QUIC `MAX_CONCURRENT_BIDI_STREAMS = 1024`
/// cap configured in `tls::server_config` — exercising multiplexing under
/// realistic load without poking the cap-saturation boundary, which has its
/// own brittle test profile (see `integration_high_stream_count_completes`).
#[tokio::test]
async fn integration_echo_high_concurrency() {
    let (server, client) = build_test_stack(PermissiveVerifier::new(), EchoDispatcher).await;

    const N_CALLS: usize = 256;
    let client = Arc::new(client);

    let mut handles = Vec::with_capacity(N_CALLS);
    for i in 0..N_CALLS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let (req, expected) = echo_request(i);
            let result = client.unary(req).await;
            (i, expected, result)
        }));
    }

    for handle in handles {
        let (i, expected, result) = handle.await.expect("task joins");
        let response = result.unwrap_or_else(|err| panic!("call {i} failed: {err:?}"));
        assert_eq!(response, expected, "call {i} echo mismatch");
    }

    client.close(0u32.into(), b"test complete").await;
    let outcome = server.shutdown(Duration::from_secs(2), 0u32.into(), b"test complete").await;
    assert_eq!(outcome, ShutdownOutcome::Clean);
}

// ─── Test 2 — high stream count completes ──────────────────────────────────

/// Issue 1000 concurrent unary calls — well under the 1024 stream cap but
/// substantially higher than `N_CALLS` in `integration_echo_high_concurrency`
/// — and assert all complete cleanly. Catches multiplexing regressions that
/// only manifest near the cap boundary without tripping into the
/// (test-fragile) cap-saturation path.
///
/// We do NOT attempt to exercise the cap itself end-to-end. quinn's behavior
/// at `MAX_CONCURRENT_BIDI_STREAMS` is that `connection.open_bi()` returns
/// successfully past the cap (the stream is "opened" client-side); the
/// actual backpressure manifests as the request frame's `write_all` blocking
/// until a slot frees. Asserting precisely on that timing is brittle. The
/// cap itself is enforced at the QUIC layer by the `transport_config` set
/// up in `tls::server_config`; that constant is verified in B.3 unit tests.
#[tokio::test]
async fn integration_high_stream_count_completes() {
    let (server, client) = build_test_stack(PermissiveVerifier::new(), EchoDispatcher).await;

    const N_CALLS: usize = 1000;
    let client = Arc::new(client);

    let mut handles = Vec::with_capacity(N_CALLS);
    for i in 0..N_CALLS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let (req, expected) = echo_request(i);
            let result = client.unary(req).await;
            (i, expected, result)
        }));
    }

    let mut completed = 0usize;
    for handle in handles {
        let (i, expected, result) = handle.await.expect("task joins");
        let response = result.unwrap_or_else(|err| panic!("call {i} failed: {err:?}"));
        assert_eq!(response, expected, "call {i} echo mismatch");
        completed += 1;
    }
    assert_eq!(completed, N_CALLS, "every call must complete");

    client.close(0u32.into(), b"test complete").await;
    let outcome = server.shutdown(Duration::from_secs(5), 0u32.into(), b"test complete").await;
    assert_eq!(outcome, ShutdownOutcome::Clean);
}

// ─── Test 3 — cancellation propagation ─────────────────────────────────────

/// Drop sentinel that records (a) that the handler future was dropped and
/// (b) the wall-clock instant of the drop. The drop path is how the
/// transport's outer `tokio::select!` actually surfaces cancellation: when
/// the root cancel fires, the cancel arm wins (under `biased` selection)
/// and the handler future is dropped mid-await — see `dispatch_unary` in
/// `crates/wire-transport/src/server/dispatch.rs`.
///
/// Asserting on the drop path is the only way to observe cancellation from
/// inside a handler. The handler's own `ctx.cancellation.cancelled().await`
/// loses its race against the outer `select!`, so any path that records
/// observation *after* the await would never run on the cancellation case.
struct CancellationSentinel {
    cancelled_at: Arc<Mutex<Option<Instant>>>,
    observed_cancellation: Arc<AtomicBool>,
}

impl Drop for CancellationSentinel {
    fn drop(&mut self) {
        *self.cancelled_at.lock().expect("lock not poisoned") = Some(Instant::now());
        self.observed_cancellation.store(true, Ordering::SeqCst);
    }
}

/// Dispatcher whose unary handler parks on `std::future::pending()` after
/// arming a `CancellationSentinel`. The sentinel records the moment the
/// handler future is dropped (i.e., the outer `select!`'s cancel arm wins).
struct CancelObserverDispatcher {
    /// Set to `true` and stamps `handler_entered_at` once the handler has
    /// entered. Lets the test confirm the handler is parked before
    /// triggering the cancellation source.
    handler_entered: Arc<AtomicBool>,
    handler_entered_at: Arc<Mutex<Option<Instant>>>,
    /// Set to `true` when the handler future is dropped (cancellation
    /// observed), and stamps `cancelled_at` at the same moment.
    observed_cancellation: Arc<AtomicBool>,
    cancelled_at: Arc<Mutex<Option<Instant>>>,
}

impl Dispatcher for CancelObserverDispatcher {
    async fn dispatch(
        &self,
        _opcode: u16,
        _request: Bytes,
        _ctx: RequestContext,
    ) -> Result<Bytes, WireError> {
        *self.handler_entered_at.lock().expect("lock not poisoned") = Some(Instant::now());
        self.handler_entered.store(true, Ordering::SeqCst);
        let _sentinel = CancellationSentinel {
            cancelled_at: self.cancelled_at.clone(),
            observed_cancellation: self.observed_cancellation.clone(),
        };
        // Park forever; the outer `select!` in `dispatch_unary` drops this
        // future when the per-stream cancel token fires. The sentinel's
        // `Drop` impl runs at that moment.
        std::future::pending::<()>().await;
        // Unreachable on the cancellation path; kept so the function has a
        // valid return type in case the dispatcher is invoked outside a
        // cancellation context (defensive).
        Err(WireError::new(ErrorCode::Internal, "unreachable"))
    }

    async fn dispatch_stream(
        &self,
        _opcode: u16,
        _request: Bytes,
        _ctx: RequestContext,
        _sink: StreamSink,
    ) -> Result<(), WireError> {
        Err(WireError::new(ErrorCode::Internal, "streaming not supported"))
    }
}

/// Cancellation must reach an in-flight handler within the SLA when the
/// server initiates shutdown.
///
/// # What this asserts
///
/// 1. The handler future is actually dropped when the cancel fires (proven by the sentinel's `Drop`
///    running).
/// 2. The drop happens within the SLA budget after shutdown begins.
/// 3. `WireServer::shutdown` returns `Clean` — i.e., the per-stream task drained via cancellation,
///    not via the timeout fallback.
///
/// # Why drop-based observation, not `ctx.cancellation.cancelled().await`
///
/// `dispatch_unary` (`crates/wire-transport/src/server/dispatch.rs`) wraps
/// the handler future in
///
/// ```text
/// tokio::select! {
///     biased;
///     () = cancel.cancelled() => return;
///     res = dispatcher.dispatch(...) => res,
/// }
/// ```
///
/// When the cancel fires, the cancel arm wins under `biased`. The handler
/// future is dropped before any code following its own
/// `ctx.cancellation.cancelled().await` can run. The drop *is* the
/// observation. Sentinels in `Drop` impls are the standard pattern for
/// asserting on a dropped future from outside the future itself.
///
/// # Why server shutdown rather than client stream reset
///
/// The original B.11 spec phrases the trigger as a client-driven stream
/// reset. The current transport doesn't wire stream-level resets into the
/// per-stream cancellation token — `accept_streams_loop` only fires
/// `cancel.cancel()` when the connection-level cancel fires, and
/// `dispatch_unary` doesn't observe `SendStream::stopped()` while the
/// handler is running. The reliable cancellation source today is
/// `WireServer::shutdown`, which fires the root token; per-stream child
/// tokens cascade.
///
/// That's the right correctness contract to test here: handlers MUST drop
/// their work when the transport says the connection is going away.
/// Whether the source is server shutdown or per-stream reset is a future
/// design question; the handler-side observation is the same.
///
/// # SLA
///
/// 100ms is the aspirational target from the SDK rewrite plan. CI variance
/// (especially cold starts on shared runners) makes that brittle, so we
/// assert <500ms — well below any user-visible budget while leaving
/// headroom for noisy runners.
#[tokio::test]
async fn integration_cancellation_propagates_within_sla() {
    let handler_entered = Arc::new(AtomicBool::new(false));
    let handler_entered_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let observed_cancellation = Arc::new(AtomicBool::new(false));
    let cancelled_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let dispatcher = CancelObserverDispatcher {
        handler_entered: handler_entered.clone(),
        handler_entered_at: handler_entered_at.clone(),
        observed_cancellation: observed_cancellation.clone(),
        cancelled_at: cancelled_at.clone(),
    };

    let (server, client) = build_test_stack(PermissiveVerifier::new(), dispatcher).await;
    let client = Arc::new(client);

    // Issue a unary call on a background task. The dispatcher parks
    // forever, so this future only resolves once the connection is torn
    // down. We don't await its result; the test asserts on the server-side
    // sentinel.
    let in_flight = {
        let client = client.clone();
        tokio::spawn(async move {
            let (req, _expected) = echo_request(0);
            client.unary(req).await
        })
    };

    // Wait for the handler to enter — without this, shutdown could race
    // ahead of the dispatch task's spawn and the test would record an
    // accidental "handler never started" pass.
    let entered_in_time =
        tokio::time::timeout(Duration::from_secs(2), wait_until_set(&handler_entered)).await;
    assert!(
        entered_in_time.is_ok(),
        "dispatcher never entered the handler — request frame did not reach the server",
    );

    // Trigger cancellation via server shutdown. This fires the root cancel
    // token; per-stream child tokens cascade. The dispatcher's outer
    // `tokio::select!` then drops the handler future, which runs the
    // sentinel's `Drop` impl.
    let shutdown_started = Instant::now();
    let outcome = server.shutdown(Duration::from_secs(5), 0u32.into(), b"cancellation test").await;
    let shutdown_elapsed = shutdown_started.elapsed();
    assert_eq!(
        outcome,
        ShutdownOutcome::Clean,
        "shutdown must drain via the per-stream cancellation arm; \
         a TimedOut here means the dispatch task isn't observing the cancel token",
    );

    // The handler future must have been dropped by the outer `select!`'s
    // cancel arm. The sentinel's `Drop` records this.
    assert!(
        observed_cancellation.load(Ordering::SeqCst),
        "handler future was never dropped — the cancellation token isn't \
         propagating through the dispatch loop",
    );

    // Bound the per-handler latency: the time from "handler entered" to
    // "handler dropped" (sentinel `Drop`). This is tighter than total
    // `shutdown` wall time, which also includes task-tracker drain.
    const SLA: Duration = Duration::from_millis(500);
    let entered = handler_entered_at
        .lock()
        .expect("lock not poisoned")
        .expect("handler_entered_at stamped before this assertion runs");
    let cancelled = cancelled_at
        .lock()
        .expect("lock not poisoned")
        .expect("cancelled_at stamped before this assertion runs");
    let latency = cancelled.duration_since(entered);
    assert!(
        latency < SLA,
        "handler dropped after {latency:?} (SLA: {SLA:?}); \
         total shutdown wall time was {shutdown_elapsed:?}",
    );

    // The client-side `unary` call observes either a transport error (the
    // connection close reaches the client first) or an opcode-mismatch /
    // protocol-violation if the server's terminal write somehow lands. We
    // don't assert on which — the cancellation contract is server-side.
    let _ = in_flight.await;
    client.close(0u32.into(), b"test complete").await;
}

/// Spin until `flag` flips to `true`, yielding briefly between checks. Used
/// to sequence "the handler entered" before the cancellation trigger fires.
async fn wait_until_set(flag: &AtomicBool) {
    while !flag.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
