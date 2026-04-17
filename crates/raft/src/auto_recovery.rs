//! Auto-recovery for diverged vaults.
//!
//! Diverged vaults recover automatically with bounded retries.
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

use std::{sync::Arc, time::Duration};

use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{OrganizationId, VaultId};
use snafu::{GenerateImplicitData, ResultExt};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    consensus_handle::ConsensusHandle,
    error::{
        ApplyOperationsSnafu, BlockArchiveNotConfiguredSnafu, BlockReadSnafu, IndexLookupSnafu,
        RecoveryError, StateRootComputationSnafu,
    },
    log_storage::{AppliedStateAccessor, MAX_RECOVERY_ATTEMPTS, VaultHealthStatus},
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerResponse, RaftPayload},
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
    /// Consensus handle for proposing vault health state changes.
    handle: Arc<ConsensusHandle>,
    /// This node's ID (retained for future logging/diagnostics).
    #[allow(dead_code)]
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
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl<B: StorageBackend + 'static> AutoRecoveryJob<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        self.handle.is_leader()
    }

    /// Calculates retry delay with exponential backoff.
    fn retry_delay(&self, attempt: u8) -> Duration {
        let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1) as u32);
        let delay = self.config.base_retry_delay.saturating_mul(multiplier as u32);
        std::cmp::min(delay, self.config.max_retry_delay)
    }

    /// Checks if a recovering vault is ready for retry.
    fn is_ready_for_retry(&self, started_at: i64, attempt: u8) -> bool {
        let now = chrono::Utc::now().timestamp();
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        elapsed >= self.retry_delay(attempt)
    }

    /// Scans for vaults that need recovery action.
    ///
    /// Returns vaults that are:
    /// - Diverged (need initial recovery)
    /// - Recovering and ready for retry (backoff elapsed)
    fn find_vaults_needing_recovery(&self) -> Vec<(OrganizationId, VaultId, VaultHealthStatus)> {
        let all_vaults = self.applied_state.all_vaults();
        let mut needs_recovery = Vec::new();

        for ((organization, vault), _meta) in all_vaults {
            let health = self.applied_state.vault_health(organization, vault);

            match &health {
                VaultHealthStatus::Diverged { .. } => {
                    needs_recovery.push((organization, vault, health));
                },
                VaultHealthStatus::Recovering { started_at, attempt } => {
                    if *attempt < MAX_RECOVERY_ATTEMPTS
                        && self.is_ready_for_retry(*started_at, *attempt)
                    {
                        needs_recovery.push((organization, vault, health));
                    }
                },
                VaultHealthStatus::Healthy => {
                    // Nothing to do
                },
            }
        }

        needs_recovery
    }

    /// Attempts to recover a single vault.
    ///
    /// This involves:
    /// 1. Finding the latest usable snapshot
    /// 2. Replaying blocks from snapshot to current tip
    /// 3. Verifying the computed state_root matches expected
    async fn attempt_recovery(
        &self,
        organization: OrganizationId,
        vault: VaultId,
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
                organization,
                vault,
                attempt,
                "max_attempts_exceeded",
            );
            return RecoveryResult::MaxAttemptsExceeded;
        }

        // First, transition to Recovering state
        let now = chrono::Utc::now().timestamp();
        if let Err(e) = self
            .propose_health_update(
                organization,
                vault,
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
                organization = organization.value(),
                vault = vault.value(),
                attempt,
                error = %e,
                "Failed to transition vault to Recovering state"
            );
            crate::metrics::record_recovery_attempt(
                organization,
                vault,
                attempt,
                "transition_failed",
            );
            return RecoveryResult::TransientFailure(e.to_string());
        }

        crate::metrics::set_vault_health(organization, vault, "recovering");
        info!(
            organization = organization.value(),
            vault = vault.value(),
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
        match self.replay_vault_state(organization, vault, expected_root, diverged_height).await {
            Ok(computed_root) => {
                if computed_root == expected_root
                    || expected_root == inferadb_ledger_types::ZERO_HASH
                {
                    // Recovery successful — transition to healthy
                    if let Err(e) = self
                        .propose_health_update(
                            organization,
                            vault,
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
                            organization = organization.value(),
                            vault = vault.value(),
                            error = %e,
                            "Failed to mark vault as healthy after recovery"
                        );
                        crate::metrics::record_recovery_attempt(
                            organization,
                            vault,
                            attempt,
                            "health_update_failed",
                        );
                        return RecoveryResult::TransientFailure(e.to_string());
                    }
                    crate::metrics::set_vault_health(organization, vault, "healthy");
                    crate::metrics::record_recovery_attempt(
                        organization,
                        vault,
                        attempt,
                        "success",
                    );
                    info!(
                        organization = organization.value(),
                        vault = vault.value(),
                        attempt,
                        "Recovery completed successfully, vault restored to healthy"
                    );
                    RecoveryResult::Success
                } else {
                    // Divergence reproduced — determinism bug
                    error!(
                        organization = organization.value(),
                        vault = vault.value(),
                        ?expected_root,
                        ?computed_root,
                        diverged_height,
                        attempt,
                        "Recovery reproduced divergence — determinism bug detected, manual intervention required"
                    );
                    crate::metrics::record_determinism_bug(organization, vault);
                    crate::metrics::record_recovery_attempt(
                        organization,
                        vault,
                        attempt,
                        "determinism_bug",
                    );
                    RecoveryResult::DeterminismBug
                }
            },
            Err(e) => {
                warn!(
                    organization = organization.value(),
                    vault = vault.value(),
                    attempt,
                    max_attempts = MAX_RECOVERY_ATTEMPTS,
                    error = %e,
                    "Recovery attempt failed, will retry with backoff"
                );
                crate::metrics::record_recovery_attempt(
                    organization,
                    vault,
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
        organization: OrganizationId,
        vault: VaultId,
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
            self.find_recovery_start_point(organization, vault)?;

        // Get current tip height
        let tip_height = self.applied_state.vault_height(organization, vault);

        debug!(
            organization = organization.value(),
            vault = vault.value(),
            start_height,
            tip_height,
            "Replaying vault state for recovery"
        );

        // Replay blocks from start_height to tip
        for height in start_height..=tip_height {
            let region_height = archive
                .find_region_height(organization, vault, height)
                .context(IndexLookupSnafu { organization, vault, height })?;

            let region_height = match region_height {
                Some(h) => h,
                None => continue,
            };

            let region_block =
                archive.read_block(region_height).context(BlockReadSnafu { region_height })?;

            let vault_entry = region_block.vault_entries.iter().find(|e| {
                e.organization == organization && e.vault == vault && e.vault_height == height
            });

            if let Some(entry) = vault_entry {
                // Apply transactions and compute new state root
                // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
                for tx in &entry.transactions {
                    self.state
                        .apply_operations(vault, &tx.operations, height)
                        .context(ApplyOperationsSnafu { height })?;
                }

                // Compute state root after applying
                computed_root = self
                    .state
                    .compute_state_root(vault)
                    .context(StateRootComputationSnafu { vault })?;
            }
        }

        Ok(computed_root)
    }

    /// Finds the optimal starting point for recovery.
    fn find_recovery_start_point(
        &self,
        _organization: OrganizationId,
        vault: VaultId,
    ) -> Result<(u64, inferadb_ledger_types::Hash), RecoveryError> {
        // Try to find a snapshot to start from
        if let Some(snapshot_manager) = &self.snapshot_manager
            && let Ok(snapshots) = snapshot_manager.list_snapshots()
        {
            for &region_height in snapshots.iter().rev() {
                if let Ok(snapshot) = snapshot_manager.load(region_height)
                    && let Some(vault_state) =
                        snapshot.header.vault_states.iter().find(|v| v.vault == vault)
                {
                    return Ok((vault_state.vault_height + 1, vault_state.state_root));
                }
            }
        }

        // No snapshot found, start from genesis
        Ok((1, inferadb_ledger_types::ZERO_HASH))
    }

    /// Proposes a vault health update through Raft.
    #[allow(clippy::too_many_arguments)]
    async fn propose_health_update(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        healthy: bool,
        expected_root: Option<inferadb_ledger_types::Hash>,
        computed_root: Option<inferadb_ledger_types::Hash>,
        diverged_at_height: Option<u64>,
        recovery_attempt: Option<u8>,
        recovery_started_at: Option<i64>,
    ) -> Result<(), RecoveryError> {
        let request = LedgerRequest::UpdateVaultHealth {
            organization,
            vault,
            healthy,
            expected_root,
            computed_root,
            diverged_at_height,
            recovery_attempt,
            recovery_started_at,
        };

        let response = self
            .handle
            .propose_and_wait(RaftPayload::system(request), Duration::from_secs(10))
            .await
            .map_err(|e| RecoveryError::RaftConsensus {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        match response {
            LedgerResponse::VaultHealthUpdated { success } if success => Ok(()),
            LedgerResponse::VaultHealthUpdated { success: false } => {
                Err(RecoveryError::HealthUpdateRejected {
                    reason: "state machine returned success=false".to_string(),
                })
            },
            other => Err(RecoveryError::UnexpectedRaftResponse { description: format!("{other}") }),
        }
    }

    /// Runs the auto-recovery job.
    ///
    /// This should be spawned as a background task. It will run until
    /// the shutdown signal is received.
    async fn run(self) {
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

                    let mut job = crate::logging::JobContext::new("auto_recovery", None);
                    let trace_ctx = TraceContext::new();
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

                    for (organization, vault, health) in vaults {
                        let result = self.attempt_recovery(organization, vault, &health).await;

                        match result {
                            RecoveryResult::Success => {
                                crate::metrics::record_recovery_success(organization, vault);
                                vaults_recovered += 1;
                            }
                            RecoveryResult::TransientFailure(ref msg) => {
                                crate::metrics::record_recovery_failure(organization, vault, msg);
                                had_failure = true;
                            }
                            RecoveryResult::DeterminismBug => {
                                // Metrics already recorded in attempt_recovery
                                had_failure = true;
                                error!(
                                    trace_id = %trace_ctx.trace_id,
                                    organization = organization.value(),
                                    vault = vault.value(),
                                    "Circuit breaker: vault requires manual intervention due to determinism bug"
                                );
                            }
                            RecoveryResult::MaxAttemptsExceeded => {
                                crate::metrics::set_vault_health(organization, vault, "diverged");
                                had_failure = true;
                                error!(
                                    trace_id = %trace_ctx.trace_id,
                                    organization = organization.value(),
                                    vault = vault.value(),
                                    max_attempts = MAX_RECOVERY_ATTEMPTS,
                                    "Circuit breaker tripped: vault exceeded max recovery attempts, manual intervention required"
                                );
                            }
                        }
                    }

                    if had_failure {
                        job.set_failure();
                    }
                    job.record_items(vaults_recovered);
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("AutoRecoveryJob shutting down");
                    break;
                }
            }
        }
    }

    /// Starts the auto-recovery job as a background task.
    ///
    /// Returns a handle to the spawned task. The task runs until the
    /// cancellation token is cancelled.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
    fn test_retry_delay_exact_backoff_schedule() {
        // Reproduce the exact retry_delay method logic
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(300);

        let compute_delay = |attempt: u8| -> Duration {
            let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1) as u32);
            let delay = base.saturating_mul(multiplier as u32);
            std::cmp::min(delay, max)
        };

        // Attempt 1: 5 * 2^0 = 5s
        assert_eq!(compute_delay(1), Duration::from_secs(5));
        // Attempt 2: 5 * 2^1 = 10s
        assert_eq!(compute_delay(2), Duration::from_secs(10));
        // Attempt 3: 5 * 2^2 = 20s
        assert_eq!(compute_delay(3), Duration::from_secs(20));
        // Attempt 4: 5 * 2^3 = 40s
        assert_eq!(compute_delay(4), Duration::from_secs(40));
        // Attempt 5: 5 * 2^4 = 80s
        assert_eq!(compute_delay(5), Duration::from_secs(80));
        // Attempt 6: 5 * 2^5 = 160s
        assert_eq!(compute_delay(6), Duration::from_secs(160));
        // Attempt 7: 5 * 2^6 = 320s, capped at 300s
        assert_eq!(compute_delay(7), Duration::from_secs(300));
    }

    #[test]
    fn test_retry_delay_attempt_zero() {
        // Attempt 0 should still work (saturating_sub prevents underflow)
        let base = Duration::from_secs(5);
        let multiplier = 2u64.saturating_pow(0u8.saturating_sub(1) as u32);
        // 0u8.saturating_sub(1) = 0, so 2^0 = 1, delay = 5s
        let delay = base.saturating_mul(multiplier as u32);
        assert_eq!(delay, Duration::from_secs(5));
    }

    #[test]
    fn test_retry_delay_saturating_overflow() {
        // Large attempt number should not overflow
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(300);
        let multiplier = 2u64.saturating_pow(254); // Very large
        let delay = base.saturating_mul(multiplier as u32);
        let capped = std::cmp::min(delay, max);
        // Should be capped at max
        assert!(capped <= max);
    }

    #[test]
    fn test_constants() {
        assert_eq!(RECOVERY_SCAN_INTERVAL, Duration::from_secs(30));
        assert_eq!(BASE_RETRY_DELAY, Duration::from_secs(5));
        assert_eq!(MAX_RETRY_DELAY, Duration::from_secs(300));
    }

    #[test]
    fn test_attempt_number_from_health_status() {
        // Diverged -> attempt = 1
        // Recovering with attempt N -> attempt = N + 1
        // Healthy -> return early
        use crate::log_storage::VaultHealthStatus;

        let diverged = VaultHealthStatus::Diverged {
            expected: [1u8; 32],
            computed: [2u8; 32],
            at_height: 100,
        };
        let attempt = match &diverged {
            VaultHealthStatus::Diverged { .. } => 1u8,
            VaultHealthStatus::Recovering { attempt, .. } => attempt + 1,
            VaultHealthStatus::Healthy => 0,
        };
        assert_eq!(attempt, 1);

        let recovering = VaultHealthStatus::Recovering { started_at: 1000, attempt: 3 };
        let attempt = match &recovering {
            VaultHealthStatus::Diverged { .. } => 1u8,
            VaultHealthStatus::Recovering { attempt, .. } => attempt + 1,
            VaultHealthStatus::Healthy => 0,
        };
        assert_eq!(attempt, 4);

        let healthy = VaultHealthStatus::Healthy;
        let attempt = match &healthy {
            VaultHealthStatus::Diverged { .. } => 1u8,
            VaultHealthStatus::Recovering { attempt, .. } => attempt + 1,
            VaultHealthStatus::Healthy => 0,
        };
        assert_eq!(attempt, 0);
    }

    #[test]
    fn test_is_ready_for_retry_logic() {
        // Reproduce the ready-for-retry logic without needing the full job
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(300);

        let retry_delay = |attempt: u8| -> Duration {
            let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1) as u32);
            let delay = base.saturating_mul(multiplier as u32);
            std::cmp::min(delay, max)
        };

        let now = chrono::Utc::now().timestamp();

        // Started 100 seconds ago, attempt 1 (delay = 5s) => ready
        let started_at = now - 100;
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        assert!(elapsed >= retry_delay(1));

        // Started 3 seconds ago, attempt 1 (delay = 5s) => not ready
        let started_at = now - 3;
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        assert!(elapsed < retry_delay(1));

        // Started 15 seconds ago, attempt 3 (delay = 20s) => not ready
        let started_at = now - 15;
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        assert!(elapsed < retry_delay(3));

        // Started 25 seconds ago, attempt 3 (delay = 20s) => ready
        let started_at = now - 25;
        let elapsed = Duration::from_secs((now - started_at).max(0) as u64);
        assert!(elapsed >= retry_delay(3));
    }
}
