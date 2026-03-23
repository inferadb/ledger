//! Background token maintenance job.
//!
//! Periodically cleans up expired refresh tokens and transitions rotated
//! signing keys past their grace period. Both phases propose through Raft
//! for deterministic state machine replay.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
        record_onboarding_gc_accounts, record_onboarding_gc_codes, record_totp_gc_challenges,
    },
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Default interval between token maintenance cycles (5 minutes).
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

/// Result of a token maintenance cycle.
#[derive(Debug, Default)]
pub struct MaintenanceResult {
    /// Number of expired refresh tokens deleted (includes poisoned family cleanup).
    pub expired_tokens_deleted: u64,
    /// Number of signing keys transitioned from Rotated to Revoked.
    pub signing_keys_revoked: u64,
    /// Number of expired onboarding verification codes deleted across all regions.
    pub onboarding_codes_deleted: u64,
    /// Number of expired onboarding accounts deleted across all regions.
    pub onboarding_accounts_deleted: u64,
    /// Number of expired TOTP challenges deleted across all regions.
    pub totp_challenges_deleted: u64,
}

/// Background job for token lifecycle maintenance.
///
/// Runs two phases each cycle:
/// 1. Proposes `DeleteExpiredRefreshTokens` through Raft (the apply handler performs the actual
///    scan and deletion, including poisoned family GC).
/// 2. Scans state for rotated signing keys past their grace period, then proposes
///    `TransitionSigningKeyRevoked` for each through Raft.
///
/// Only runs on the leader node.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct TokenMaintenanceJob<B: StorageBackend + 'static> {
    /// Raft consensus handle for proposing changes and verifying leadership.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer for reading signing key status (Phase 2 scan).
    state: Arc<StateLayer<B>>,
    /// Interval between maintenance cycles.
    #[builder(default = DEFAULT_MAINTENANCE_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<AtomicU64>>,
    /// Multi-region Raft manager for proposing cleanup to regional groups.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
}

impl<B: StorageBackend + 'static> TokenMaintenanceJob<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single maintenance cycle.
    ///
    /// Phase 1: Propose `DeleteExpiredRefreshTokens` through Raft.
    /// Phase 2: Scan for rotated signing keys past grace, propose
    ///          `TransitionSigningKeyRevoked` for each.
    pub async fn run_cycle(&self) -> MaintenanceResult {
        if !self.is_leader() {
            debug!("Skipping token maintenance cycle (not leader)");
            return MaintenanceResult::default();
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting token maintenance cycle");

        let mut result = MaintenanceResult::default();
        let mut had_errors = false;

        // Phase 1: Delete expired refresh tokens (the apply handler does the actual work)
        match self
            .raft
            .client_write(RaftPayload::system(LedgerRequest::DeleteExpiredRefreshTokens))
            .await
        {
            Ok(response) => {
                if let crate::types::LedgerResponse::ExpiredRefreshTokensDeleted { count } =
                    response.data
                {
                    result.expired_tokens_deleted = count;
                    if count > 0 {
                        info!(
                            trace_id = %trace_ctx.trace_id,
                            count,
                            "Deleted expired refresh tokens"
                        );
                    }
                }
            },
            Err(e) => {
                had_errors = true;
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    "Failed to delete expired refresh tokens"
                );
            },
        }

        // Phase 2: Transition rotated signing keys past grace period
        let now = chrono::Utc::now();
        let sys = inferadb_ledger_state::system::SystemOrganizationService::new(self.state.clone());
        match sys.list_rotated_keys_past_grace(now) {
            Ok(kids) => {
                for kid in kids {
                    match self
                        .raft
                        .client_write(RaftPayload::new(
                            LedgerRequest::TransitionSigningKeyRevoked { kid: kid.clone() },
                            0,
                        ))
                        .await
                    {
                        Ok(_) => {
                            result.signing_keys_revoked += 1;
                            info!(
                                trace_id = %trace_ctx.trace_id,
                                kid = %kid,
                                "Transitioned rotated signing key to revoked"
                            );
                        },
                        Err(e) => {
                            had_errors = true;
                            warn!(
                                trace_id = %trace_ctx.trace_id,
                                kid = %kid,
                                error = %e,
                                "Failed to transition signing key"
                            );
                        },
                    }
                }
            },
            Err(e) => {
                had_errors = true;
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    "Failed to scan for rotated signing keys"
                );
            },
        }

        // Phase 3: Cleanup expired onboarding records in each regional Raft group
        if let Some(ref manager) = self.manager {
            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                match manager.get_region_group(region) {
                    Ok(group) => {
                        let request =
                            LedgerRequest::System(SystemRequest::CleanupExpiredOnboarding);
                        match group.raft().client_write(RaftPayload::system(request)).await {
                            Ok(response) => {
                                if let crate::types::LedgerResponse::OnboardingCleanedUp {
                                    verification_codes_deleted,
                                    onboarding_accounts_deleted,
                                    totp_challenges_deleted,
                                } = response.data
                                {
                                    let codes = u64::from(verification_codes_deleted);
                                    let accounts = u64::from(onboarding_accounts_deleted);
                                    let totp = u64::from(totp_challenges_deleted);
                                    result.onboarding_codes_deleted += codes;
                                    result.onboarding_accounts_deleted += accounts;
                                    result.totp_challenges_deleted += totp;
                                    if codes > 0 || accounts > 0 || totp > 0 {
                                        info!(
                                            trace_id = %trace_ctx.trace_id,
                                            region = %region,
                                            codes_deleted = codes,
                                            accounts_deleted = accounts,
                                            totp_challenges_deleted = totp,
                                            "Cleaned up expired onboarding records"
                                        );
                                    }
                                }
                            },
                            Err(e) => {
                                had_errors = true;
                                warn!(
                                    trace_id = %trace_ctx.trace_id,
                                    region = %region,
                                    error = %e,
                                    "Failed to clean up onboarding records"
                                );
                            },
                        }
                    },
                    Err(e) => {
                        had_errors = true;
                        warn!(
                            trace_id = %trace_ctx.trace_id,
                            region = %region,
                            error = %e,
                            "Failed to get region group for onboarding cleanup"
                        );
                    },
                }
            }
            if result.onboarding_codes_deleted > 0 {
                record_onboarding_gc_codes(result.onboarding_codes_deleted);
            }
            if result.onboarding_accounts_deleted > 0 {
                record_onboarding_gc_accounts(result.onboarding_accounts_deleted);
            }
            if result.totp_challenges_deleted > 0 {
                record_totp_gc_challenges(result.totp_challenges_deleted);
            }
        }

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("token_maintenance", duration);
        let status = if had_errors { "failure" } else { "success" };
        record_background_job_run("token_maintenance", status);
        record_background_job_items(
            "token_maintenance",
            result.expired_tokens_deleted
                + result.signing_keys_revoked
                + result.onboarding_codes_deleted
                + result.onboarding_accounts_deleted
                + result.totp_challenges_deleted,
        );

        debug!(
            trace_id = %trace_ctx.trace_id,
            expired_deleted = result.expired_tokens_deleted,
            keys_revoked = result.signing_keys_revoked,
            onboarding_codes = result.onboarding_codes_deleted,
            onboarding_accounts = result.onboarding_accounts_deleted,
            totp_challenges = result.totp_challenges_deleted,
            duration_secs = duration,
            "Token maintenance cycle complete"
        );

        result
    }

    /// Starts the token maintenance background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                if let Some(ref handle) = self.watchdog_handle {
                    handle.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        Ordering::Relaxed,
                    );
                }
                self.run_cycle().await;
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_result_default() {
        let result = MaintenanceResult::default();
        assert_eq!(result.expired_tokens_deleted, 0);
        assert_eq!(result.signing_keys_revoked, 0);
        assert_eq!(result.onboarding_codes_deleted, 0);
        assert_eq!(result.onboarding_accounts_deleted, 0);
        assert_eq!(result.totp_challenges_deleted, 0);
    }

    #[test]
    fn maintenance_result_accumulates_onboarding_counts() {
        let mut result = MaintenanceResult::default();
        result.onboarding_codes_deleted += 5;
        result.onboarding_accounts_deleted += 3;
        result.totp_challenges_deleted += 7;
        assert_eq!(result.onboarding_codes_deleted, 5);
        assert_eq!(result.onboarding_accounts_deleted, 3);
        assert_eq!(result.totp_challenges_deleted, 7);
    }
}
