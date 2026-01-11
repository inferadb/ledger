//! Graceful shutdown handling.
//!
//! Provides signal handling for clean server shutdown.

use tokio::signal;

/// Wait for a shutdown signal (Ctrl-C or SIGTERM).
///
/// This function blocks until a shutdown signal is received.
/// On Unix systems, it also handles SIGTERM for container environments.
#[allow(clippy::expect_used)]
pub async fn shutdown_signal() {
    let ctrl_c = async {
        // Safety: If we can't install signal handlers, the process should panic
        // since graceful shutdown is critical for data integrity.
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
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

/// Shutdown coordinator for graceful termination.
///
/// Manages the shutdown sequence to ensure:
/// - In-flight requests complete
/// - Raft state is flushed
/// - Connections are closed cleanly
pub struct ShutdownCoordinator {
    /// Notify channel for shutdown signal.
    notify: tokio::sync::broadcast::Sender<()>,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator.
    pub fn new() -> Self {
        let (notify, _) = tokio::sync::broadcast::channel(1);
        Self { notify }
    }

    /// Subscribe to shutdown notifications.
    #[allow(dead_code)]
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.notify.subscribe()
    }

    /// Trigger shutdown.
    pub fn shutdown(&self) {
        let _ = self.notify.send(());
    }

    /// Wait for shutdown signal and trigger coordinator.
    #[allow(dead_code)]
    pub async fn wait_for_signal(&self) {
        shutdown_signal().await;
        self.shutdown();
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_shutdown_coordinator() {
        let coordinator = ShutdownCoordinator::new();
        let mut receiver = coordinator.subscribe();

        // Spawn task to trigger shutdown after a short delay
        let coord_clone = coordinator.notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = coord_clone.send(());
        });

        // Wait for shutdown signal
        let result = tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await;
        assert!(result.is_ok(), "should receive shutdown signal");
    }
}
