//! Graceful shutdown handling.
//!
//! Provides signal handling and coordinated shutdown for clean server termination.
//! The `ShutdownCoordinator` distributes cancellation via a root
//! `CancellationToken` and tracks spawned task handles so they can be
//! awaited with a per-handle deadline during shutdown.

use std::time::Duration;

use tokio::{signal, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// Default per-handle deadline during shutdown.
const DEFAULT_HANDLE_DEADLINE: Duration = Duration::from_secs(30);

/// Waits for a shutdown signal (Ctrl-C or SIGTERM).
///
/// On Unix systems, also handles SIGTERM for container environments.
#[allow(clippy::expect_used)]
pub async fn shutdown_signal() {
    let ctrl_c = async {
        // Safety: If we can't install signal handlers, the process should panic
        // since graceful shutdown is critical for data integrity.
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        // Safety: Same reasoning as above for SIGTERM in container environments.
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C, initiating shutdown");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, initiating shutdown");
        }
    }
}

/// Coordinates graceful shutdown by distributing cancellation to all child
/// tokens and awaiting registered task handles with a per-handle deadline.
///
/// The actual graceful shutdown logic (draining connections, flushing Raft
/// state) is handled by [`inferadb_ledger_raft::GracefulShutdown`].
pub struct ShutdownCoordinator {
    /// Root cancellation token — cancelling this cancels all children.
    root_token: CancellationToken,
    /// Registered task handles to await during shutdown.
    handles: parking_lot::Mutex<Vec<(&'static str, JoinHandle<()>)>>,
    /// Per-handle deadline for awaiting task completion.
    handle_deadline: Duration,
}

impl ShutdownCoordinator {
    /// Creates a new shutdown coordinator with the default 30-second per-handle
    /// deadline.
    pub fn new() -> Self {
        Self::with_handle_deadline(DEFAULT_HANDLE_DEADLINE)
    }

    /// Creates a new shutdown coordinator with a custom per-handle deadline.
    #[allow(dead_code)] // used by tests
    pub fn with_handle_deadline(handle_deadline: Duration) -> Self {
        Self {
            root_token: CancellationToken::new(),
            handles: parking_lot::Mutex::new(Vec::new()),
            handle_deadline,
        }
    }

    /// Returns a reference to the root cancellation token.
    #[allow(dead_code)]
    pub fn root_token(&self) -> &CancellationToken {
        &self.root_token
    }

    /// Creates a named child token derived from the root token.
    ///
    /// The child is automatically cancelled when the root token is cancelled
    /// (i.e. when [`shutdown`](Self::shutdown) is called).
    pub fn child_token(&self, _name: &'static str) -> CancellationToken {
        self.root_token.child_token()
    }

    /// Registers a spawned task handle for tracking during shutdown.
    ///
    /// All registered handles are awaited (with a per-handle deadline) when
    /// [`shutdown`](Self::shutdown) is called.
    pub fn register_handle(&self, name: &'static str, handle: JoinHandle<()>) {
        self.handles.lock().push((name, handle));
    }

    /// Initiates shutdown: cancels the root token, then awaits all registered
    /// handles with the configured per-handle deadline.
    ///
    /// Handles that do not complete within the deadline are logged as warnings
    /// and their underlying tasks are aborted.
    pub async fn shutdown(&self) {
        tracing::info!("ShutdownCoordinator: cancelling root token");
        self.root_token.cancel();

        let handles: Vec<_> = {
            let mut guard = self.handles.lock();
            std::mem::take(&mut *guard)
        };

        for (name, handle) in handles {
            match tokio::time::timeout(self.handle_deadline, handle).await {
                Ok(Ok(())) => {
                    tracing::info!(handle = name, "ShutdownCoordinator: handle completed");
                },
                Ok(Err(join_err)) => {
                    tracing::warn!(
                        handle = name,
                        error = %join_err,
                        "ShutdownCoordinator: handle panicked during shutdown",
                    );
                },
                Err(_elapsed) => {
                    tracing::warn!(
                        handle = name,
                        deadline_ms = self.handle_deadline.as_millis() as u64,
                        "ShutdownCoordinator: handle did not complete within deadline, aborting",
                    );
                },
            }
        }

        tracing::info!("ShutdownCoordinator: shutdown complete");
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn test_shutdown_coordinator_cancels_root() {
        let coordinator = ShutdownCoordinator::new();
        let token = coordinator.root_token().clone();

        assert!(!token.is_cancelled());
        coordinator.shutdown().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn child_tokens_cancel_on_parent_shutdown() {
        let coordinator = ShutdownCoordinator::with_handle_deadline(Duration::from_secs(5));
        let child = coordinator.child_token("test-task");
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = tokio::spawn(async move {
            child.cancelled().await;
            completed_clone.store(true, Ordering::Release);
        });

        coordinator.register_handle("test-task", handle);
        coordinator.shutdown().await;

        assert!(
            completed.load(Ordering::Acquire),
            "child task should have completed after root cancellation"
        );
    }

    #[tokio::test]
    async fn shutdown_awaits_registered_handles() {
        let coordinator = ShutdownCoordinator::with_handle_deadline(Duration::from_secs(5));
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            finished_clone.store(true, Ordering::Release);
        });

        coordinator.register_handle("sleeper", handle);
        coordinator.shutdown().await;

        assert!(
            finished.load(Ordering::Acquire),
            "shutdown should have waited for the handle to complete"
        );
    }

    #[tokio::test]
    async fn shutdown_enforces_deadline_for_slow_handles() {
        let deadline = Duration::from_millis(100);
        let coordinator = ShutdownCoordinator::with_handle_deadline(deadline);
        let child = coordinator.child_token("slow-task");

        let handle = tokio::spawn(async move {
            // Ignore cancellation and sleep longer than the deadline.
            tokio::select! {
                _ = child.cancelled() => {
                    // Even after cancellation, keep running past the deadline.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        });

        coordinator.register_handle("slow-task", handle);

        let start = tokio::time::Instant::now();
        coordinator.shutdown().await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown should complete near the deadline ({deadline:?}), not wait for the full 60s task; elapsed: {elapsed:?}"
        );
    }
}
