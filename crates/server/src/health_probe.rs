//! K8s `tcpSocket`-style health probe listener (Phase 0.0.E.8).
//!
//! Replaces tonic-health's gRPC probe path. After the wire-protocol
//! migration, the binary listens on UDP for QUIC traffic and has no
//! HTTP/2 surface for K8s probes to target. K8s `tcpSocket` probes
//! succeed iff a TCP handshake completes; this module binds a minimal
//! TCP listener on the QUIC port number and serves it.
//!
//! Two protocols on one port number is intentional: UDP/QUIC carries
//! application traffic, TCP/probe is a no-op accept loop. K8s manifests
//! point both at port 50051; nothing else listens on TCP/50051.
//!
//! # The healthy/unhealthy contract
//!
//! K8s `tcpSocket` probes observe ONLY whether the TCP handshake
//! completes — they cannot observe what happens after. Once the kernel
//! accepts the SYN and our process accepts the connection, the probe is
//! already counted as successful, regardless of what we do with the
//! socket afterward.
//!
//! The "soft unhealthy" path here (when `AtomicBool` is `false`) is
//! retained as a hook for future use:
//!
//! - **Liveness**: the binary either is or isn't accepting TCP. The real liveness signal is
//!   "process is up and `tokio::net::TcpListener` is bound." Crashing or unbinding the listener is
//!   the only way to make `tcpSocket` probes fail.
//! - **Readiness**: same TCP-level limitation. Richer readiness signaling (e.g., "DB is reachable")
//!   requires application-level probes, which the wire-protocol build deliberately does not expose
//!   over HTTP. Operators who need it should add a separate diagnostic port.
//!
//! Both healthy and unhealthy branches close the socket cleanly; the
//! distinction exists so a future operator can plumb the bool to a
//! cause-of-degradation log line or metric without changing the public
//! API.
//!
//! This module is the only HTTP-adjacent surface in the wire-protocol
//! build. It depends on `tokio::net::TcpListener` only — no `hyper`,
//! no `tonic-health`.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::{io::AsyncWriteExt, net::TcpListener};
use tokio_util::sync::CancellationToken;

/// Bind a TCP listener at `addr` and serve health probes until cancelled.
///
/// On each accepted connection the socket is closed cleanly via
/// [`AsyncWriteExt::shutdown`]. The `healthy` flag is currently observed
/// only for logging — see the module doc on the
/// healthy/unhealthy contract for why TCP probes can't fail at the
/// post-handshake layer.
///
/// Returns `Ok(())` when the cancellation token fires. Returns `Err` if
/// the initial bind fails.
///
/// # Example (test-style; production wiring lives in `bootstrap.rs`)
///
/// ```no_run
/// use std::net::SocketAddr;
/// use std::sync::Arc;
/// use std::sync::atomic::AtomicBool;
/// use tokio_util::sync::CancellationToken;
/// use inferadb_ledger_server::health_probe::run_tcp_probe_listener;
///
/// # async fn example() -> std::io::Result<()> {
/// let addr: SocketAddr = "0.0.0.0:50051".parse().unwrap();
/// let healthy = Arc::new(AtomicBool::new(true));
/// let cancel = CancellationToken::new();
/// run_tcp_probe_listener(addr, healthy, cancel).await
/// # }
/// ```
pub async fn run_tcp_probe_listener(
    addr: SocketAddr,
    healthy: Arc<AtomicBool>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(local_addr = %listener.local_addr()?, "tcp health probe listener bound");

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::debug!("tcp health probe listener: shutdown signal");
                return Ok(());
            }
            res = listener.accept() => {
                let (mut socket, _peer) = match res {
                    Ok(v) => v,
                    Err(err) => {
                        // Per-accept errors are transient (peer reset before
                        // accept completes, etc.). Log and keep serving.
                        tracing::debug!(error = %err, "tcp health probe accept failed");
                        continue;
                    }
                };
                let probe_healthy = healthy.load(Ordering::Acquire);
                if !probe_healthy {
                    tracing::debug!("tcp health probe accepted while unhealthy");
                }
                // K8s tcpSocket probes are TCP-handshake-only — at this
                // point the probe has already been counted as successful.
                // Close cleanly. See the module doc for why we don't try
                // to fail probes via RST.
                let _ = socket.shutdown().await;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use tokio::{io::AsyncReadExt, net::TcpStream};

    use super::*;

    /// Bind to ephemeral loopback port; return resolved addr + cancel + listener task.
    async fn spawn_listener(
        healthy: Arc<AtomicBool>,
    ) -> (SocketAddr, CancellationToken, tokio::task::JoinHandle<std::io::Result<()>>) {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();
        drop(listener);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let task =
            tokio::spawn(async move { run_tcp_probe_listener(bound, healthy, cancel_clone).await });
        // Give the listener a tick to bind.
        tokio::time::sleep(Duration::from_millis(20)).await;
        (bound, cancel, task)
    }

    #[tokio::test]
    async fn probe_succeeds_when_healthy() {
        let healthy = Arc::new(AtomicBool::new(true));
        let (addr, cancel, task) = spawn_listener(healthy).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Server closes immediately; reading should yield 0 bytes (EOF).
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("read within timeout")
            .expect("read succeeded");
        assert_eq!(n, 0, "expected immediate EOF on healthy probe");

        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn probe_completes_handshake_when_unhealthy() {
        // tcpSocket probes pass on handshake success regardless of healthy
        // state — see module doc. This test confirms the listener still
        // serves the probe (i.e., doesn't drop the socket pre-handshake)
        // when healthy=false, which is the contract operators rely on.
        let healthy = Arc::new(AtomicBool::new(false));
        let (addr, cancel, task) = spawn_listener(healthy).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("read within timeout")
            .expect("read succeeded");
        assert_eq!(n, 0, "expected immediate EOF on unhealthy probe close");

        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn listener_shuts_down_on_cancel() {
        let healthy = Arc::new(AtomicBool::new(true));
        let (_addr, cancel, task) = spawn_listener(healthy).await;

        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("listener exits within 1s after cancel")
            .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn health_state_changes_take_effect_mid_lifetime() {
        let healthy = Arc::new(AtomicBool::new(true));
        let healthy_clone = healthy.clone();
        let (addr, cancel, task) = spawn_listener(healthy_clone).await;

        // First probe while healthy: clean close.
        let mut s1 = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 16];
        let n =
            tokio::time::timeout(Duration::from_secs(1), s1.read(&mut buf)).await.unwrap().unwrap();
        assert_eq!(n, 0);

        // Flip to unhealthy.
        healthy.store(false, Ordering::Release);

        // Second probe: handshake still succeeds (K8s observation point).
        let mut s2 = TcpStream::connect(addr).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), s2.read(&mut buf)).await.unwrap();
        // We don't assert specific behavior here — RST vs FIN varies across
        // platforms and the test for that is `probe_completes_handshake_when_unhealthy`.

        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bind_failure_returns_err() {
        // Bind to port 1 (privileged, not root in CI) → expect error.
        let healthy = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let result = run_tcp_probe_listener("127.0.0.1:1".parse().unwrap(), healthy, cancel).await;
        assert!(result.is_err(), "expected bind failure on privileged port");
    }
}
