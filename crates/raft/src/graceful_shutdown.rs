//! Graceful shutdown, health state management, and connection draining.
//!
//! Provides three-probe health checking (startup, liveness, readiness) following
//! Kubernetes conventions, orchestrates graceful server shutdown with connection
//! draining and Raft leadership handoff, and tracks background job health via
//! a watchdog.
//!
//! # Health Probes
//!
//! - **Startup**: Passes once the node has completed initialization (Raft recovery, shard setup).
//!   Failing startup probe tells Kubernetes to keep waiting.
//! - **Liveness**: Passes when the event loop is responsive AND all background jobs are
//!   heartbeating within their expected intervals. Failing liveness probe triggers pod restart.
//! - **Readiness**: Passes when the node can serve traffic (startup complete AND not shutting
//!   down). Failing readiness probe removes the pod from service endpoints.
//!
//! # Shutdown Sequence
//!
//! 1. Mark readiness as failing (stops new traffic from load balancer)
//! 2. Wait `pre_stop_delay_secs` for Kubernetes to remove pod from endpoints
//! 3. Wait `grace_period_secs` for load balancer to drain existing connections
//! 4. Wait up to `drain_timeout_secs` for in-flight requests to finish
//! 5. Run pre-shutdown tasks (snapshots, Raft shutdown) with `pre_shutdown_timeout_secs` limit
//! 6. Signal the gRPC server to stop
//!
//! # Connection Tracking
//!
//! [`ConnectionTracker`] provides a lock-free `AtomicUsize` counter incremented
//! on every inbound gRPC request and decremented when the response completes.
//! During shutdown, [`ConnectionTracker::wait_for_zero`] polls until the counter
//! reaches zero or the timeout expires.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
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

// ─── Connection Tracker ──────────────────────────────────────────

/// Tracks the number of in-flight gRPC requests.
///
/// Thread-safe, lock-free counter using `AtomicUsize`. The counter is
/// incremented when a request arrives and decremented when the response
/// is sent. During shutdown, [`wait_for_zero`](Self::wait_for_zero) blocks
/// until all in-flight requests complete or the timeout expires.
#[derive(Debug, Clone)]
pub struct ConnectionTracker {
    active: Arc<AtomicUsize>,
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionTracker {
    /// Creates a new tracker with zero active connections.
    pub fn new() -> Self {
        Self { active: Arc::new(AtomicUsize::new(0)) }
    }

    /// Increments the active connection count.
    pub fn increment(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the active connection count.
    pub fn decrement(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the current number of active connections.
    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Waits until the active count reaches zero or the timeout expires.
    ///
    /// Returns `true` if all connections drained, `false` if timed out.
    pub async fn wait_for_zero(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = Duration::from_millis(100);

        loop {
            if self.active_count() == 0 {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

// ─── Background Job Watchdog ─────────────────────────────────────

/// Entry tracking a single background job.
struct WatchdogEntry {
    /// Expected interval between heartbeats (seconds).
    expected_interval_secs: u64,
    /// Unix timestamp (seconds) of last heartbeat.
    last_heartbeat: Arc<AtomicU64>,
}

/// Tracks background job health via periodic heartbeats.
///
/// Each background job registers with a name and expected interval, then
/// calls [`heartbeat`](Self::heartbeat) on each cycle. The liveness probe
/// consults [`check_all`](Self::check_all) to detect stuck jobs.
#[derive(Clone)]
pub struct BackgroundJobWatchdog {
    entries: Arc<parking_lot::RwLock<HashMap<String, WatchdogEntry>>>,
    /// Multiplier for expected interval; job is stale if
    /// `now - last_heartbeat > multiplier * expected_interval`.
    multiplier: u64,
}

impl std::fmt::Debug for BackgroundJobWatchdog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundJobWatchdog").field("multiplier", &self.multiplier).finish()
    }
}

impl BackgroundJobWatchdog {
    /// Creates a watchdog with the given staleness multiplier.
    pub fn new(multiplier: u64) -> Self {
        Self { entries: Arc::new(parking_lot::RwLock::new(HashMap::new())), multiplier }
    }

    /// Registers a background job. Returns the `AtomicU64` handle the job
    /// should use for heartbeating (write current unix timestamp).
    pub fn register(&self, name: impl Into<String>, expected_interval_secs: u64) -> Arc<AtomicU64> {
        let now = current_unix_secs();
        let handle = Arc::new(AtomicU64::new(now));
        let entry = WatchdogEntry { expected_interval_secs, last_heartbeat: handle.clone() };
        self.entries.write().insert(name.into(), entry);
        handle
    }

    /// Records a heartbeat for the named job.
    pub fn heartbeat(&self, name: &str) {
        let entries = self.entries.read();
        if let Some(entry) = entries.get(name) {
            entry.last_heartbeat.store(current_unix_secs(), Ordering::Relaxed);
        }
    }

    /// Checks all registered jobs.
    ///
    /// # Errors
    ///
    /// Returns the name of the first stale job if any registered job has
    /// not sent a heartbeat within its expected interval multiplied by
    /// the staleness multiplier.
    pub fn check_all(&self) -> Result<(), String> {
        let now = current_unix_secs();
        let entries = self.entries.read();
        for (name, entry) in entries.iter() {
            let last = entry.last_heartbeat.load(Ordering::Relaxed);
            let max_age = entry.expected_interval_secs.saturating_mul(self.multiplier);
            if now.saturating_sub(last) > max_age {
                return Err(name.clone());
            }
        }
        Ok(())
    }

    /// Returns the number of registered jobs.
    pub fn job_count(&self) -> usize {
        self.entries.read().len()
    }
}

/// Returns the current Unix timestamp in seconds.
fn current_unix_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ─── Health State ────────────────────────────────────────────────

/// Shared health state for the node.
///
/// Thread-safe, lock-free state that tracks the current lifecycle phase.
/// Used by health probes and the shutdown coordinator.
#[derive(Debug, Clone)]
pub struct HealthState {
    phase: Arc<AtomicU8>,
    /// Connection tracker for drain monitoring.
    connection_tracker: ConnectionTracker,
    /// Background job watchdog for liveness monitoring.
    watchdog: Option<BackgroundJobWatchdog>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Creates a new `HealthState` in the `Starting` phase.
    pub fn new() -> Self {
        Self {
            phase: Arc::new(AtomicU8::new(NodePhase::Starting as u8)),
            connection_tracker: ConnectionTracker::new(),
            watchdog: None,
        }
    }

    /// Attaches a background job watchdog to the health state.
    #[must_use]
    pub fn with_watchdog(mut self, watchdog: BackgroundJobWatchdog) -> Self {
        self.watchdog = Some(watchdog);
        self
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

    /// Returns a reference to the connection tracker.
    pub fn connection_tracker(&self) -> &ConnectionTracker {
        &self.connection_tracker
    }

    /// Returns a reference to the watchdog, if configured.
    pub fn watchdog(&self) -> Option<&BackgroundJobWatchdog> {
        self.watchdog.as_ref()
    }

    /// Startup probe: passes once initialization is complete.
    ///
    /// Returns `true` when the node has left the `Starting` phase.
    pub fn startup_check(&self) -> bool {
        self.phase() != NodePhase::Starting
    }

    /// Liveness probe: passes when the event loop is responsive and all
    /// background jobs are healthy.
    ///
    /// If a watchdog is configured, checks that every registered background
    /// job has heartbeated within its expected interval. Without a watchdog,
    /// always returns `true` (reaching this code proves the async runtime
    /// is alive).
    pub fn liveness_check(&self) -> bool {
        if let Some(watchdog) = &self.watchdog
            && let Err(stale_job) = watchdog.check_all()
        {
            warn!(job = %stale_job, "Background job missed heartbeat, liveness failing");
            return false;
        }
        true
    }

    /// Readiness probe: passes when the node can serve traffic.
    ///
    /// Returns `true` when startup is complete AND the node is not shutting down.
    pub fn readiness_check(&self) -> bool {
        self.phase() == NodePhase::Ready
    }
}

// ─── Graceful Shutdown ───────────────────────────────────────────

/// Orchestrates graceful server shutdown.
///
/// Coordinates the shutdown sequence: marking readiness as failing,
/// waiting for load balancer drain, draining in-flight connections,
/// running pre-shutdown tasks with a timeout, and signaling the gRPC
/// server to stop.
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

    /// Executes the graceful shutdown sequence.
    ///
    /// This method should be called when the node receives a termination signal
    /// (e.g., SIGTERM). It orchestrates the full shutdown sequence and then
    /// signals the gRPC server to stop.
    ///
    /// The `pre_shutdown` callback is invoked after connection drain, before
    /// the server is signaled to stop. Use it to trigger Raft snapshots and
    /// shut down shard groups.
    pub async fn execute<F, Fut>(self, pre_shutdown: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Phase 1: Mark readiness as failing
        info!("Graceful shutdown initiated, marking readiness as failing");
        self.health_state.mark_shutting_down();

        // Phase 2: Pre-stop delay (K8s endpoint removal)
        let pre_stop_delay = Duration::from_secs(self.config.pre_stop_delay_secs);
        if pre_stop_delay > Duration::ZERO {
            info!(
                delay_secs = self.config.pre_stop_delay_secs,
                "Waiting for Kubernetes endpoint removal"
            );
            tokio::time::sleep(pre_stop_delay).await;
        }

        // Phase 3: Load balancer grace period
        let grace_period = Duration::from_secs(self.config.grace_period_secs);
        info!(grace_period_secs = self.config.grace_period_secs, "Waiting for load balancer drain");
        tokio::time::sleep(grace_period).await;

        // Phase 4: Drain in-flight connections
        let drain_timeout = Duration::from_secs(self.config.drain_timeout_secs);
        let active = self.health_state.connection_tracker().active_count();
        if active > 0 {
            info!(
                active_connections = active,
                drain_timeout_secs = self.config.drain_timeout_secs,
                "Draining in-flight connections"
            );
            let drained = self.health_state.connection_tracker().wait_for_zero(drain_timeout).await;
            if !drained {
                let remaining = self.health_state.connection_tracker().active_count();
                warn!(
                    remaining_connections = remaining,
                    "Drain timeout expired, forcefully terminating connections"
                );
            } else {
                info!("All in-flight connections drained");
            }
        }

        // Phase 5: Run pre-shutdown tasks with timeout
        let pre_shutdown_timeout = Duration::from_secs(self.config.pre_shutdown_timeout_secs);
        info!(timeout_secs = self.config.pre_shutdown_timeout_secs, "Running pre-shutdown tasks");
        match tokio::time::timeout(pre_shutdown_timeout, pre_shutdown()).await {
            Ok(()) => info!("Pre-shutdown tasks completed"),
            Err(_) => warn!(
                timeout_secs = self.config.pre_shutdown_timeout_secs,
                "Pre-shutdown tasks timed out, proceeding with shutdown"
            ),
        }

        // Phase 6: Signal gRPC server to stop
        info!("Signaling gRPC server to stop");
        if self.server_shutdown_tx.send(true).is_err() {
            warn!("gRPC server already stopped");
        }

        info!("Graceful shutdown sequence complete");
    }
}

// ─── Connection Tracking Tower Layer ─────────────────────────────

/// Tower layer that tracks in-flight requests via a [`ConnectionTracker`].
///
/// When added to the gRPC server's layer stack, this intercepts every request
/// to increment the counter and decrements it when the response future completes.
#[derive(Clone, Debug)]
pub struct ConnectionTrackingLayer {
    tracker: ConnectionTracker,
}

impl ConnectionTrackingLayer {
    /// Creates a new layer wrapping the given tracker.
    pub fn new(tracker: ConnectionTracker) -> Self {
        Self { tracker }
    }
}

impl<S> tower::Layer<S> for ConnectionTrackingLayer {
    type Service = ConnectionTrackingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectionTrackingService { inner, tracker: self.tracker.clone() }
    }
}

/// Tower service wrapper that increments/decrements connection counter.
#[derive(Clone, Debug)]
pub struct ConnectionTrackingService<S> {
    inner: S,
    tracker: ConnectionTracker,
}

impl<S, ReqBody> tower::Service<tonic::codegen::http::Request<ReqBody>>
    for ConnectionTrackingService<S>
where
    S: tower::Service<tonic::codegen::http::Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        self.tracker.increment();
        let tracker = self.tracker.clone();
        let future = self.inner.call(req);
        Box::pin(async move {
            let result = future.await;
            tracker.decrement();
            result
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ─── NodePhase Tests ─────────────────────────────────────

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

    // ─── ConnectionTracker Tests ─────────────────────────────

    #[test]
    fn test_connection_tracker_increment_decrement() {
        let tracker = ConnectionTracker::new();
        assert_eq!(tracker.active_count(), 0);
        tracker.increment();
        tracker.increment();
        assert_eq!(tracker.active_count(), 2);
        tracker.decrement();
        assert_eq!(tracker.active_count(), 1);
        tracker.decrement();
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn test_connection_tracker_clone_shares_state() {
        let tracker = ConnectionTracker::new();
        let clone = tracker.clone();
        tracker.increment();
        assert_eq!(clone.active_count(), 1);
        clone.decrement();
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn test_connection_tracker_wait_for_zero_immediate() {
        let tracker = ConnectionTracker::new();
        let drained = tracker.wait_for_zero(Duration::from_millis(100)).await;
        assert!(drained);
    }

    #[tokio::test]
    async fn test_connection_tracker_wait_for_zero_with_drain() {
        tokio::time::pause();
        let tracker = ConnectionTracker::new();
        tracker.increment();

        let tracker_clone = tracker.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tracker_clone.decrement();
        });

        let drained = tracker.wait_for_zero(Duration::from_secs(1)).await;
        assert!(drained);
    }

    #[tokio::test]
    async fn test_connection_tracker_wait_for_zero_timeout() {
        tokio::time::pause();
        let tracker = ConnectionTracker::new();
        tracker.increment();

        let drained = tracker.wait_for_zero(Duration::from_millis(200)).await;
        assert!(!drained);
        assert_eq!(tracker.active_count(), 1);
    }

    #[test]
    fn test_connection_tracker_default() {
        let tracker = ConnectionTracker::default();
        assert_eq!(tracker.active_count(), 0);
    }

    // ─── BackgroundJobWatchdog Tests ─────────────────────────

    #[test]
    fn test_watchdog_no_jobs_is_healthy() {
        let watchdog = BackgroundJobWatchdog::new(2);
        assert!(watchdog.check_all().is_ok());
        assert_eq!(watchdog.job_count(), 0);
    }

    #[test]
    fn test_watchdog_register_and_heartbeat() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let _handle = watchdog.register("gc", 60);
        assert_eq!(watchdog.job_count(), 1);
        // Just registered — within window, so healthy
        assert!(watchdog.check_all().is_ok());
    }

    #[test]
    fn test_watchdog_stale_job_detected() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let handle = watchdog.register("gc", 1);
        // Simulate a stale heartbeat (5 seconds ago, with expected_interval=1, multiplier=2)
        let stale_time = current_unix_secs().saturating_sub(5);
        handle.store(stale_time, Ordering::Relaxed);
        let result = watchdog.check_all();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "gc");
    }

    #[test]
    fn test_watchdog_heartbeat_refreshes() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let handle = watchdog.register("gc", 1);
        // Make it stale
        let stale_time = current_unix_secs().saturating_sub(5);
        handle.store(stale_time, Ordering::Relaxed);
        assert!(watchdog.check_all().is_err());
        // Heartbeat should fix it
        watchdog.heartbeat("gc");
        assert!(watchdog.check_all().is_ok());
    }

    #[test]
    fn test_watchdog_multiple_jobs() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let _handle1 = watchdog.register("gc", 60);
        let handle2 = watchdog.register("compactor", 1);
        assert_eq!(watchdog.job_count(), 2);
        // All healthy initially
        assert!(watchdog.check_all().is_ok());
        // Make compactor stale
        let stale_time = current_unix_secs().saturating_sub(5);
        handle2.store(stale_time, Ordering::Relaxed);
        let result = watchdog.check_all();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "compactor");
    }

    #[test]
    fn test_watchdog_clone_shares_state() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let clone = watchdog.clone();
        let _handle = watchdog.register("gc", 60);
        assert_eq!(clone.job_count(), 1);
    }

    // ─── Health State with Watchdog ──────────────────────────

    #[test]
    fn test_liveness_without_watchdog() {
        let state = HealthState::new();
        assert!(state.liveness_check());
    }

    #[test]
    fn test_liveness_with_healthy_watchdog() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let _handle = watchdog.register("gc", 60);
        let state = HealthState::new().with_watchdog(watchdog);
        assert!(state.liveness_check());
    }

    #[test]
    fn test_liveness_with_stale_watchdog() {
        let watchdog = BackgroundJobWatchdog::new(2);
        let handle = watchdog.register("gc", 1);
        let stale_time = current_unix_secs().saturating_sub(5);
        handle.store(stale_time, Ordering::Relaxed);
        let state = HealthState::new().with_watchdog(watchdog);
        assert!(!state.liveness_check());
    }

    #[test]
    fn test_health_state_connection_tracker_accessible() {
        let state = HealthState::new();
        state.connection_tracker().increment();
        assert_eq!(state.connection_tracker().active_count(), 1);
        state.connection_tracker().decrement();
        assert_eq!(state.connection_tracker().active_count(), 0);
    }

    // ─── GracefulShutdown Tests ──────────────────────────────

    fn test_shutdown_config() -> ShutdownConfig {
        ShutdownConfig {
            grace_period_secs: 1,
            drain_timeout_secs: 5,
            pre_stop_delay_secs: 0,
            pre_shutdown_timeout_secs: 10,
            watchdog_multiplier: 2,
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown_signals_server() {
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();

        let (shutdown, mut rx) = GracefulShutdown::new(test_shutdown_config(), health.clone());

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
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();
        assert!(health.readiness_check());

        let (shutdown, _rx) = GracefulShutdown::new(test_shutdown_config(), health.clone());

        tokio::spawn(async move {
            shutdown.execute(|| async {}).await;
        });

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Health should immediately show not ready
        assert!(!health.readiness_check());
        assert_eq!(health.phase(), NodePhase::ShuttingDown);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_drains_connections() {
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();

        // Simulate an in-flight connection
        health.connection_tracker().increment();

        let (shutdown, _rx) = GracefulShutdown::new(test_shutdown_config(), health.clone());

        // Release the connection after a short delay
        let tracker = health.connection_tracker().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            tracker.decrement();
        });

        let start = tokio::time::Instant::now();
        shutdown.execute(|| async {}).await;
        let elapsed = start.elapsed();

        // Should have waited for connection drain (grace_period + drain wait)
        assert!(elapsed >= Duration::from_secs(1));
        assert_eq!(health.connection_tracker().active_count(), 0);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_drain_timeout_expires() {
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();

        // Simulate a stuck connection that never finishes
        health.connection_tracker().increment();

        let (shutdown, mut rx) = GracefulShutdown::new(test_shutdown_config(), health.clone());

        shutdown.execute(|| async {}).await;

        // Server should still be signaled even with stuck connections
        assert!(*rx.borrow_and_update());
        // Connection count is still 1 (never decremented)
        assert_eq!(health.connection_tracker().active_count(), 1);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_pre_shutdown_timeout() {
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();

        let config = ShutdownConfig {
            grace_period_secs: 1,
            drain_timeout_secs: 5,
            pre_stop_delay_secs: 0,
            pre_shutdown_timeout_secs: 5,
            watchdog_multiplier: 2,
        };
        let (shutdown, mut rx) = GracefulShutdown::new(config, health.clone());

        let start = tokio::time::Instant::now();
        shutdown
            .execute(|| async {
                // Simulate a hung Raft shutdown
                tokio::time::sleep(Duration::from_secs(300)).await;
            })
            .await;
        let elapsed = start.elapsed();

        // Should have timed out the pre-shutdown, not waited 300s
        assert!(elapsed < Duration::from_secs(15));
        // Server still gets signaled
        assert!(*rx.borrow_and_update());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_pre_stop_delay() {
        tokio::time::pause();
        let health = HealthState::new();
        health.mark_ready();

        let config = ShutdownConfig {
            grace_period_secs: 1,
            drain_timeout_secs: 5,
            pre_stop_delay_secs: 1,
            pre_shutdown_timeout_secs: 10,
            watchdog_multiplier: 2,
        };
        let (shutdown, _rx) = GracefulShutdown::new(config, health.clone());

        let start = tokio::time::Instant::now();
        shutdown.execute(|| async {}).await;
        let elapsed = start.elapsed();

        // Should have waited pre_stop_delay + grace_period = at least 2s
        assert!(elapsed >= Duration::from_secs(2));
    }
}
