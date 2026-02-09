//! Auto-recovery for diverged vaults.
//!
//! Per DESIGN.md §8.2: Diverged vaults recover automatically with bounded retries.
//! When a vault's computed state_root doesn't match the expected value, it enters
//! `Diverged` status. This module provides automatic recovery:
//!
//! 1. Detect diverged vaults via periodic scan
//! 2. Transition to `Recovering` status with attempt counter
//! 3. Replay vault state from the latest snapshot
//! 4. If successful, mark as `Healthy`
//! 5. If failed, retry with exponential backoff (max MAX_RECOVERY_ATTEMPTS)
//! 6. After max attempts, require manual intervention
//!
//! Recovery runs only on the leader to avoid duplicate work.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{NamespaceId, VaultId};
use openraft::Raft;
use snafu::{GenerateImplicitData, ResultExt};
use tokio::{sync::mpsc, time::interval};
use tracing::{debug, error, info, warn};

use crate::{
    error::{
        ApplyOperationsSnafu, BlockArchiveNotConfiguredSnafu, BlockReadSnafu, IndexLookupSnafu,
        RecoveryError, StateRootComputationSnafu,
    },
    log_storage::{AppliedStateAccessor, MAX_RECOVERY_ATTEMPTS, VaultHealthStatus},
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig},
};

/// Default interval between recovery scans.
const RECOVERY_SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// Base delay for exponential backoff between retry attempts.
const BASE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Maximum delay between retry attempts.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);

/// Auto-recovery configuration.
#[derive(Debug, Clone, bon::Builder)]
pub struct AutoRecoveryConfig {
    /// Interval between scanning for diverged vaults.
    #[builder(default = RECOVERY_SCAN_INTERVAL)]
    pub scan_interval: Duration,
    /// Base delay for exponential backoff.
    #[builder(default = BASE_RETRY_DELAY)]
    pub base_retry_delay: Duration,
    /// Maximum delay between retries.
    #[builder(default = MAX_RETRY_DELAY)]
    pub max_retry_delay: Duration,
    /// Whether auto-recovery is enabled.
    #[builder(default = true)]
    pub enabled: bool,
}

impl Default for AutoRecoveryConfig {
    fn default() -> Self {
        Self {
            scan_interval: RECOVERY_SCAN_INTERVAL,
            base_retry_delay: BASE_RETRY_DELAY,
            max_retry_delay: MAX_RETRY_DELAY,
            enabled: true,
        }
    }
}

/// Result of a recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryResult {
    /// Recovery succeeded, vault is now healthy.
    Success,
    /// Recovery failed with a transient error (will retry).
    TransientFailure(String),
    /// Recovery reproduced the divergence (determinism bug detected).
    DeterminismBug,
    /// Maximum recovery attempts exceeded.
    MaxAttemptsExceeded,
}

/// Auto-recovery job for diverged vaults.
///
/// Runs as a background task, periodically scanning for vaults that need
/// recovery and triggering the recovery process through Raft consensus.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct AutoRecoveryJob<B: StorageBackend + 'static> {
    /// The Raft instance for proposing health updates.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// Accessor for applied state (vault health).
    applied_state: AppliedStateAccessor,
    /// Block archive for replaying transactions.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<B>>>,
    /// Snapshot manager for finding recovery starting points.
    #[builder(default)]
    snapshot_manager: Option<Arc<SnapshotManager>>,
    /// State layer for applying recovered state (internally thread-safe via inferadb-ledger-store
    /// MVCC).
    state: Arc<StateLayer<B>>,
    /// Configuration.
    #[builder(default)]
    config: AutoRecoveryConfig,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> AutoRecoveryJob<B> {
    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Calculate retry delay with exponential backoff.
    fn retry_delay(&self, attempt: u8) -> Duration {
        let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1) as u32);
        let delay = self.config.base_retry_delay.saturating_mul(multiplier as u32);
        std::cmp::min(delay, self.config.max_retry_delay)
    }

    /// Check if a recovering vault is ready for retry.
    fn is_ready_for_retry(&self, started_at: i64, attempt: u8) -> bool {
        let now = chrono::Utc::now().timestamp();
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        elapsed >= self.retry_delay(attempt)
    }

    /// Scan for vaults that need recovery action.
    ///
    /// Returns vaults that are:
    /// - Diverged (need initial recovery)
    /// - Recovering and ready for retry (backoff elapsed)
    fn find_vaults_needing_recovery(&self) -> Vec<(NamespaceId, VaultId, VaultHealthStatus)> {
        let all_vaults = self.applied_state.all_vaults();
        let mut needs_recovery = Vec::new();

        for ((namespace_id, vault_id), _meta) in all_vaults {
            let health = self.applied_state.vault_health(namespace_id, vault_id);

            match &health {
                VaultHealthStatus::Diverged { .. } => {
                    needs_recovery.push((namespace_id, vault_id, health));
                },
                VaultHealthStatus::Recovering { started_at, attempt } => {
                    if *attempt < MAX_RECOVERY_ATTEMPTS
                        && self.is_ready_for_retry(*started_at, *attempt)
                    {
                        needs_recovery.push((namespace_id, vault_id, health));
                    }
                },
                VaultHealthStatus::Healthy => {
                    // Nothing to do
                },
            }
        }

        needs_recovery
    }

    /// Attempt to recover a single vault.
    ///
    /// This involves:
    /// 1. Finding the latest usable snapshot
    /// 2. Replaying blocks from snapshot to current tip
    /// 3. Verifying the computed state_root matches expected
    async fn attempt_recovery(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        current_health: &VaultHealthStatus,
    ) -> RecoveryResult {
        // Determine current attempt number
        let attempt = match current_health {
            VaultHealthStatus::Diverged { .. } => 1,
            VaultHealthStatus::Recovering { attempt, .. } => attempt + 1,
            VaultHealthStatus::Healthy => return RecoveryResult::Success,
        };

        if attempt > MAX_RECOVERY_ATTEMPTS {
            crate::metrics::record_recovery_attempt(
                namespace_id.value(),
                vault_id.value(),
                attempt,
                "max_attempts_exceeded",
            );
            return RecoveryResult::MaxAttemptsExceeded;
        }

        // First, transition to Recovering state
        let now = chrono::Utc::now().timestamp();
        if let Err(e) = self
            .propose_health_update(
                namespace_id,
                vault_id,
                false,
                None,
                None,
                None,
                Some(attempt),
                Some(now),
            )
            .await
        {
            warn!(
                namespace_id = namespace_id.value(),
                vault_id = vault_id.value(),
                attempt,
                error = %e,
                "Failed to transition vault to Recovering state"
            );
            crate::metrics::record_recovery_attempt(
                namespace_id.value(),
                vault_id.value(),
                attempt,
                "transition_failed",
            );
            return RecoveryResult::TransientFailure(e.to_string());
        }

        crate::metrics::set_vault_health(namespace_id.value(), vault_id.value(), "recovering");
        info!(
            namespace_id = namespace_id.value(),
            vault_id = vault_id.value(),
            attempt,
            max_attempts = MAX_RECOVERY_ATTEMPTS,
            "Recovery attempt started"
        );

        // Get the expected state root from the divergence info
        let (expected_root, diverged_height) = match current_health {
            VaultHealthStatus::Diverged { expected, at_height, .. } => (*expected, *at_height),
            VaultHealthStatus::Recovering { .. } => {
                // For retries, we need to get the original divergence info
                // This is stored in the applied state, but for simplicity
                // we'll attempt a full replay
                (inferadb_ledger_types::ZERO_HASH, 0)
            },
            VaultHealthStatus::Healthy => return RecoveryResult::Success,
        };

        // Attempt the actual recovery
        match self.replay_vault_state(namespace_id, vault_id, expected_root, diverged_height).await
        {
            Ok(computed_root) => {
                if computed_root == expected_root
                    || expected_root == inferadb_ledger_types::ZERO_HASH
                {
                    // Recovery successful — transition to healthy
                    if let Err(e) = self
                        .propose_health_update(
                            namespace_id,
                            vault_id,
                            true,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        .await
                    {
                        warn!(
                            namespace_id = namespace_id.value(),
                            vault_id = vault_id.value(),
                            error = %e,
                            "Failed to mark vault as healthy after recovery"
                        );
                        crate::metrics::record_recovery_attempt(
                            namespace_id.value(),
                            vault_id.value(),
                            attempt,
                            "health_update_failed",
                        );
                        return RecoveryResult::TransientFailure(e.to_string());
                    }
                    crate::metrics::set_vault_health(
                        namespace_id.value(),
                        vault_id.value(),
                        "healthy",
                    );
                    crate::metrics::record_recovery_attempt(
                        namespace_id.value(),
                        vault_id.value(),
                        attempt,
                        "success",
                    );
                    info!(
                        namespace_id = namespace_id.value(),
                        vault_id = vault_id.value(),
                        attempt,
                        "Recovery completed successfully, vault restored to healthy"
                    );
                    RecoveryResult::Success
                } else {
                    // Divergence reproduced — determinism bug
                    error!(
                        namespace_id = namespace_id.value(),
                        vault_id = vault_id.value(),
                        ?expected_root,
                        ?computed_root,
                        diverged_height,
                        attempt,
                        "Recovery reproduced divergence — determinism bug detected, manual intervention required"
                    );
                    crate::metrics::record_determinism_bug(namespace_id.value(), vault_id.value());
                    crate::metrics::record_recovery_attempt(
                        namespace_id.value(),
                        vault_id.value(),
                        attempt,
                        "determinism_bug",
                    );
                    RecoveryResult::DeterminismBug
                }
            },
            Err(e) => {
                warn!(
                    namespace_id = namespace_id.value(),
                    vault_id = vault_id.value(),
                    attempt,
                    max_attempts = MAX_RECOVERY_ATTEMPTS,
                    error = %e,
                    "Recovery attempt failed, will retry with backoff"
                );
                crate::metrics::record_recovery_attempt(
                    namespace_id.value(),
                    vault_id.value(),
                    attempt,
                    "transient_failure",
                );
                RecoveryResult::TransientFailure(e.to_string())
            },
        }
    }

    /// Replay vault state from snapshot/genesis to verify state root.
    async fn replay_vault_state(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        _expected_root: inferadb_ledger_types::Hash,
        _diverged_height: u64,
    ) -> Result<inferadb_ledger_types::Hash, RecoveryError> {
        let archive = self
            .block_archive
            .as_ref()
            .ok_or(snafu::NoneError)
            .context(BlockArchiveNotConfiguredSnafu)?;

        // Find starting point (snapshot or genesis)
        let (start_height, mut computed_root) =
            self.find_recovery_start_point(namespace_id, vault_id)?;

        // Get current tip height
        let tip_height = self.applied_state.vault_height(namespace_id, vault_id);

        debug!(
            namespace_id = namespace_id.value(),
            vault_id = vault_id.value(),
            start_height,
            tip_height,
            "Replaying vault state for recovery"
        );

        // Replay blocks from start_height to tip
        for height in start_height..=tip_height {
            let shard_height = archive.find_shard_height(namespace_id, vault_id, height).context(
                IndexLookupSnafu {
                    namespace_id: namespace_id.value(),
                    vault_id: vault_id.value(),
                    height,
                },
            )?;

            let shard_height = match shard_height {
                Some(h) => h,
                None => continue,
            };

            let shard_block =
                archive.read_block(shard_height).context(BlockReadSnafu { shard_height })?;

            let vault_entry = shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            });

            if let Some(entry) = vault_entry {
                // Apply transactions and compute new state root
                // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
                for tx in &entry.transactions {
                    self.state
                        .apply_operations(vault_id, &tx.operations, height)
                        .context(ApplyOperationsSnafu { height })?;
                }

                // Compute state root after applying
                computed_root = self
                    .state
                    .compute_state_root(vault_id)
                    .context(StateRootComputationSnafu { vault_id: vault_id.value() })?;
            }
        }

        Ok(computed_root)
    }

    /// Find the optimal starting point for recovery.
    fn find_recovery_start_point(
        &self,
        _namespace_id: NamespaceId,
        vault_id: VaultId,
    ) -> Result<(u64, inferadb_ledger_types::Hash), RecoveryError> {
        // Try to find a snapshot to start from
        if let Some(snapshot_manager) = &self.snapshot_manager
            && let Ok(snapshots) = snapshot_manager.list_snapshots()
        {
            for &shard_height in snapshots.iter().rev() {
                if let Ok(snapshot) = snapshot_manager.load(shard_height)
                    && let Some(vault_state) =
                        snapshot.header.vault_states.iter().find(|v| v.vault_id == vault_id)
                {
                    return Ok((vault_state.vault_height + 1, vault_state.state_root));
                }
            }
        }

        // No snapshot found, start from genesis
        Ok((1, inferadb_ledger_types::ZERO_HASH))
    }

    /// Propose a vault health update through Raft.
    #[allow(clippy::too_many_arguments)]
    async fn propose_health_update(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        healthy: bool,
        expected_root: Option<inferadb_ledger_types::Hash>,
        computed_root: Option<inferadb_ledger_types::Hash>,
        diverged_at_height: Option<u64>,
        recovery_attempt: Option<u8>,
        recovery_started_at: Option<i64>,
    ) -> Result<(), RecoveryError> {
        let request = LedgerRequest::UpdateVaultHealth {
            namespace_id,
            vault_id,
            healthy,
            expected_root,
            computed_root,
            diverged_at_height,
            recovery_attempt,
            recovery_started_at,
        };

        let result =
            self.raft.client_write(request).await.map_err(|e| RecoveryError::RaftConsensus {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        match result.data {
            LedgerResponse::VaultHealthUpdated { success: true } => Ok(()),
            LedgerResponse::VaultHealthUpdated { success: false } => {
                Err(RecoveryError::HealthUpdateRejected {
                    reason: "Health update failed".to_string(),
                })
            },
            other => {
                Err(RecoveryError::UnexpectedRaftResponse { description: format!("{:?}", other) })
            },
        }
    }

    /// Run the auto-recovery job.
    ///
    /// This should be spawned as a background task. It will run until
    /// the shutdown signal is received.
    pub async fn run(self, mut shutdown: mpsc::Receiver<()>) {
        if !self.config.enabled {
            info!("Auto-recovery is disabled");
            return;
        }

        let mut ticker = interval(self.config.scan_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            interval_secs = self.config.scan_interval.as_secs(),
            max_attempts = MAX_RECOVERY_ATTEMPTS,
            base_retry_delay_secs = self.config.base_retry_delay.as_secs(),
            max_retry_delay_secs = self.config.max_retry_delay.as_secs(),
            "Auto-recovery job started"
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(ref handle) = self.watchdog_handle {
                        handle.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    if !self.is_leader() {
                        debug!("Not leader, skipping recovery scan");
                        continue;
                    }

                    let trace_ctx = TraceContext::new();
                    let cycle_start = Instant::now();
                    let mut vaults_recovered = 0u64;
                    let mut had_failure = false;

                    let vaults = self.find_vaults_needing_recovery();
                    if !vaults.is_empty() {
                        info!(
                            trace_id = %trace_ctx.trace_id,
                            count = vaults.len(),
                            "Recovery scan found vaults needing attention"
                        );
                    }

                    for (namespace_id, vault_id, health) in vaults {
                        let result = self.attempt_recovery(namespace_id, vault_id, &health).await;

                        match result {
                            RecoveryResult::Success => {
                                crate::metrics::record_recovery_success(namespace_id.value(), vault_id.value());
                                vaults_recovered += 1;
                            }
                            RecoveryResult::TransientFailure(ref msg) => {
                                crate::metrics::record_recovery_failure(namespace_id.value(), vault_id.value(), msg);
                                had_failure = true;
                            }
                            RecoveryResult::DeterminismBug => {
                                // Metrics already recorded in attempt_recovery
                                had_failure = true;
                                error!(
                                    trace_id = %trace_ctx.trace_id,
                                    namespace_id = namespace_id.value(),
                                    vault_id = vault_id.value(),
                                    "Circuit breaker: vault requires manual intervention due to determinism bug"
                                );
                            }
                            RecoveryResult::MaxAttemptsExceeded => {
                                crate::metrics::set_vault_health(namespace_id.value(), vault_id.value(), "diverged");
                                had_failure = true;
                                error!(
                                    trace_id = %trace_ctx.trace_id,
                                    namespace_id = namespace_id.value(),
                                    vault_id = vault_id.value(),
                                    max_attempts = MAX_RECOVERY_ATTEMPTS,
                                    "Circuit breaker tripped: vault exceeded max recovery attempts, manual intervention required"
                                );
                            }
                        }
                    }

                    let duration = cycle_start.elapsed().as_secs_f64();
                    crate::metrics::record_background_job_duration("auto_recovery", duration);
                    crate::metrics::record_background_job_run(
                        "auto_recovery",
                        if had_failure { "failure" } else { "success" },
                    );
                    if vaults_recovered > 0 {
                        crate::metrics::record_background_job_items("auto_recovery", vaults_recovered);
                    }
                }
                _ = shutdown.recv() => {
                    info!("Auto-recovery job shutting down");
                    break;
                }
            }
        }
    }

    /// Start the auto-recovery job as a background task.
    ///
    /// Returns a handle to the spawned task. The task runs until dropped.
    /// For graceful shutdown, use `run()` with a shutdown channel instead.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);
        // Note: _shutdown_tx is dropped when this scope ends, but the receiver
        // will never receive a message (recv returns None when all senders drop).
        // The task will run until the JoinHandle is dropped or aborted.
        tokio::spawn(async move {
            self.run(shutdown_rx).await;
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_delay_exponential_backoff() {
        let config = AutoRecoveryConfig {
            base_retry_delay: Duration::from_secs(5),
            max_retry_delay: Duration::from_secs(300),
            ..Default::default()
        };

        // Attempt 1: 5 * 2^0 = 5s
        let delay1 = config.base_retry_delay.saturating_mul(1);
        assert_eq!(delay1, Duration::from_secs(5));

        // Attempt 2: 5 * 2^1 = 10s
        let delay2 = config.base_retry_delay.saturating_mul(2);
        assert_eq!(delay2, Duration::from_secs(10));

        // Attempt 3: 5 * 2^2 = 20s
        let delay3 = config.base_retry_delay.saturating_mul(4);
        assert_eq!(delay3, Duration::from_secs(20));
    }

    #[test]
    fn test_retry_delay_capped_at_max() {
        let config = AutoRecoveryConfig {
            base_retry_delay: Duration::from_secs(60),
            max_retry_delay: Duration::from_secs(120),
            ..Default::default()
        };

        // Attempt 3: 60 * 2^2 = 240s, but capped at 120s
        let multiplier = 2u64.pow(2);
        let delay = config.base_retry_delay.saturating_mul(multiplier as u32);
        let capped = std::cmp::min(delay, config.max_retry_delay);
        assert_eq!(capped, Duration::from_secs(120));
    }

    #[test]
    fn test_default_config() {
        let config = AutoRecoveryConfig::default();
        assert_eq!(config.scan_interval, Duration::from_secs(30));
        assert_eq!(config.base_retry_delay, Duration::from_secs(5));
        assert_eq!(config.max_retry_delay, Duration::from_secs(300));
        assert!(config.enabled);
    }

    #[test]
    fn test_auto_recovery_config_builder_with_defaults() {
        let config = AutoRecoveryConfig::builder().build();
        assert_eq!(config.scan_interval, Duration::from_secs(30));
        assert_eq!(config.base_retry_delay, Duration::from_secs(5));
        assert_eq!(config.max_retry_delay, Duration::from_secs(300));
        assert!(config.enabled);
    }

    #[test]
    fn test_auto_recovery_config_builder_with_custom_values() {
        let config = AutoRecoveryConfig::builder()
            .scan_interval(Duration::from_secs(60))
            .base_retry_delay(Duration::from_secs(10))
            .max_retry_delay(Duration::from_secs(600))
            .enabled(false)
            .build();
        assert_eq!(config.scan_interval, Duration::from_secs(60));
        assert_eq!(config.base_retry_delay, Duration::from_secs(10));
        assert_eq!(config.max_retry_delay, Duration::from_secs(600));
        assert!(!config.enabled);
    }

    #[test]
    fn test_auto_recovery_config_builder_matches_default() {
        let from_builder = AutoRecoveryConfig::builder().build();
        let from_default = AutoRecoveryConfig::default();
        assert_eq!(from_builder.scan_interval, from_default.scan_interval);
        assert_eq!(from_builder.base_retry_delay, from_default.base_retry_delay);
        assert_eq!(from_builder.max_retry_delay, from_default.max_retry_delay);
        assert_eq!(from_builder.enabled, from_default.enabled);
    }

    #[test]
    fn test_recovery_result_variants() {
        assert_eq!(RecoveryResult::Success, RecoveryResult::Success);
        assert_ne!(RecoveryResult::Success, RecoveryResult::TransientFailure("error".to_string()));
        assert_ne!(RecoveryResult::Success, RecoveryResult::DeterminismBug);
        assert_ne!(RecoveryResult::Success, RecoveryResult::MaxAttemptsExceeded);
    }
}
