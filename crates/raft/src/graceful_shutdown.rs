//! Graceful shutdown and health state management.
//!
//! Provides three-probe health checking (startup, liveness, readiness) following
//! Kubernetes conventions, and orchestrates graceful server shutdown with
//! connection draining and Raft leadership handoff.
//!
//! # Health Probes
//!
//! - **Startup**: Passes once the node has completed initialization (Raft recovery, shard setup).
//!   Failing startup probe tells Kubernetes to keep waiting.
//! - **Liveness**: Passes when the event loop is responsive. Failing liveness probe triggers pod
//!   restart.
//! - **Readiness**: Passes when the node can serve traffic (startup complete AND not shutting
//!   down). Failing readiness probe removes the pod from service endpoints.
//!
//! # Shutdown Sequence
//!
//! 1. Mark readiness as failing (stops new traffic from load balancer)
//! 2. Wait `grace_period_secs` for load balancer to drain
//! 3. Trigger final Raft snapshots for leader shards
//! 4. Shut down all Raft instances (triggers re-election)
//! 5. Signal the gRPC server to stop

use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_types::config::ShutdownConfig;
use tokio::sync::watch;
use tracing::{info, warn};

/// Node lifecycle phase.
///
/// Transitions: `Starting` → `Ready` → `ShuttingDown`.
/// These are one-way transitions; a node never goes back to `Starting` or `Ready`
/// from a later state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodePhase {
    /// Node is initializing (Raft recovery, shard setup).
    Starting = 0,
    /// Node is fully operational and serving traffic.
    Ready = 1,
    /// Node is shutting down and draining connections.
    ShuttingDown = 2,
}

impl NodePhase {
    fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Starting,
            1 => Self::Ready,
            2 => Self::ShuttingDown,
            _ => Self::Starting,
        }
    }
}

/// Shared health state for the node.
///
/// Thread-safe, lock-free state that tracks the current lifecycle phase.
/// Used by health probes and the shutdown coordinator.
#[derive(Debug, Clone)]
pub struct HealthState {
    phase: Arc<AtomicU8>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Creates a new `HealthState` in the `Starting` phase.
    pub fn new() -> Self {
        Self { phase: Arc::new(AtomicU8::new(NodePhase::Starting as u8)) }
    }

    /// Returns the current lifecycle phase.
    pub fn phase(&self) -> NodePhase {
        NodePhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// Transitions to the `Ready` phase.
    ///
    /// Only succeeds if the current phase is `Starting`.
    pub fn mark_ready(&self) -> bool {
        self.phase
            .compare_exchange(
                NodePhase::Starting as u8,
                NodePhase::Ready as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transitions to the `ShuttingDown` phase.
    ///
    /// Succeeds from any phase (idempotent).
    pub fn mark_shutting_down(&self) {
        self.phase.store(NodePhase::ShuttingDown as u8, Ordering::Release);
    }

    /// Startup probe: passes once initialization is complete.
    ///
    /// Returns `true` when the node has left the `Starting` phase.
    pub fn startup_check(&self) -> bool {
        self.phase() != NodePhase::Starting
    }

    /// Liveness probe: passes when the event loop is responsive.
    ///
    /// Currently always returns `true` since the ability to execute this
    /// function proves the async runtime is alive. A future enhancement
    /// could add a watchdog timer for background tasks.
    pub fn liveness_check(&self) -> bool {
        // If we can execute this function, the event loop is responsive.
        // The gRPC framework handles the actual request/response, so reaching
        // this code proves the async runtime is not deadlocked.
        true
    }

    /// Readiness probe: passes when the node can serve traffic.
    ///
    /// Returns `true` when startup is complete AND the node is not shutting down.
    pub fn readiness_check(&self) -> bool {
        self.phase() == NodePhase::Ready
    }
}

/// Orchestrates graceful server shutdown.
///
/// Coordinates the shutdown sequence: marking readiness as failing,
/// waiting for load balancer drain, triggering final snapshots,
/// and signaling the gRPC server to stop.
pub struct GracefulShutdown {
    config: ShutdownConfig,
    health_state: HealthState,
    /// Sender to signal the gRPC server to stop accepting connections.
    server_shutdown_tx: watch::Sender<bool>,
}

impl GracefulShutdown {
    /// Creates a new shutdown coordinator.
    ///
    /// Returns the coordinator and a receiver that the gRPC server should
    /// use with `serve_with_shutdown()`.
    pub fn new(config: ShutdownConfig, health_state: HealthState) -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { config, health_state, server_shutdown_tx: tx }, rx)
    }

    /// Execute the graceful shutdown sequence.
    ///
    /// This method should be called when the node receives a termination signal
    /// (e.g., SIGTERM). It orchestrates the full shutdown sequence and then
    /// signals the gRPC server to stop.
    ///
    /// The `pre_shutdown` callback is invoked after the grace period, before
    /// the server is signaled to stop. Use it to trigger Raft snapshots and
    /// shut down shard groups.
    pub async fn execute<F, Fut>(self, pre_shutdown: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Step 1: Mark readiness as failing
        info!("Graceful shutdown initiated, marking readiness as failing");
        self.health_state.mark_shutting_down();

        // Step 2: Wait for load balancer to drain
        let grace_period = Duration::from_secs(self.config.grace_period_secs);
        info!(grace_period_secs = self.config.grace_period_secs, "Waiting for load balancer drain");
        tokio::time::sleep(grace_period).await;

        // Step 3: Run pre-shutdown tasks (snapshots, Raft shutdown)
        info!("Running pre-shutdown tasks");
        pre_shutdown().await;

        // Step 4: Signal gRPC server to stop
        info!("Signaling gRPC server to stop");
        if self.server_shutdown_tx.send(true).is_err() {
            warn!("gRPC server already stopped");
        }

        info!("Graceful shutdown sequence complete");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_starts_in_starting_phase() {
        let state = HealthState::new();
        assert_eq!(state.phase(), NodePhase::Starting);
        assert!(!state.startup_check());
        assert!(state.liveness_check());
        assert!(!state.readiness_check());
    }

    #[test]
    fn test_health_state_mark_ready() {
        let state = HealthState::new();
        assert!(state.mark_ready());
        assert_eq!(state.phase(), NodePhase::Ready);
        assert!(state.startup_check());
        assert!(state.liveness_check());
        assert!(state.readiness_check());
    }

    #[test]
    fn test_health_state_mark_ready_only_from_starting() {
        let state = HealthState::new();
        assert!(state.mark_ready());
        // Cannot go back to Ready from Ready
        assert!(!state.mark_ready());
    }

    #[test]
    fn test_health_state_mark_shutting_down() {
        let state = HealthState::new();
        state.mark_ready();
        state.mark_shutting_down();
        assert_eq!(state.phase(), NodePhase::ShuttingDown);
        assert!(state.startup_check()); // startup complete
        assert!(state.liveness_check()); // still alive
        assert!(!state.readiness_check()); // not accepting traffic
    }

    #[test]
    fn test_health_state_shutdown_from_starting() {
        let state = HealthState::new();
        state.mark_shutting_down();
        assert_eq!(state.phase(), NodePhase::ShuttingDown);
        assert!(state.startup_check()); // left Starting
        assert!(!state.readiness_check()); // not ready
    }

    #[test]
    fn test_health_state_shutdown_idempotent() {
        let state = HealthState::new();
        state.mark_shutting_down();
        state.mark_shutting_down();
        assert_eq!(state.phase(), NodePhase::ShuttingDown);
    }

    #[test]
    fn test_health_state_cannot_mark_ready_after_shutdown() {
        let state = HealthState::new();
        state.mark_shutting_down();
        assert!(!state.mark_ready());
        assert_eq!(state.phase(), NodePhase::ShuttingDown);
    }

    #[test]
    fn test_health_state_default() {
        let state = HealthState::default();
        assert_eq!(state.phase(), NodePhase::Starting);
    }

    #[test]
    fn test_health_state_clone_shares_state() {
        let state = HealthState::new();
        let clone = state.clone();
        state.mark_ready();
        assert_eq!(clone.phase(), NodePhase::Ready);
    }

    #[test]
    fn test_node_phase_from_u8_unknown() {
        assert_eq!(NodePhase::from_u8(255), NodePhase::Starting);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_signals_server() {
        let health = HealthState::new();
        health.mark_ready();

        let config = ShutdownConfig { grace_period_secs: 1, drain_timeout_secs: 5 };
        let (shutdown, mut rx) = GracefulShutdown::new(config, health.clone());

        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = executed.clone();

        // Run shutdown in background
        let handle = tokio::spawn(async move {
            shutdown
                .execute(|| async move {
                    executed_clone.store(true, Ordering::Release);
                })
                .await;
        });

        // Wait for completion
        handle.await.unwrap();

        // Verify state
        assert_eq!(health.phase(), NodePhase::ShuttingDown);
        assert!(executed.load(Ordering::Acquire));
        assert!(*rx.borrow_and_update());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_marks_not_ready() {
        let health = HealthState::new();
        health.mark_ready();
        assert!(health.readiness_check());

        let config = ShutdownConfig { grace_period_secs: 1, drain_timeout_secs: 5 };
        let (shutdown, _rx) = GracefulShutdown::new(config, health.clone());

        tokio::spawn(async move {
            shutdown.execute(|| async {}).await;
        });

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Health should immediately show not ready
        assert!(!health.readiness_check());
        assert_eq!(health.phase(), NodePhase::ShuttingDown);
    }
}
