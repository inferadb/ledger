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
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload},
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
            .client_write(RaftPayload::new(LedgerRequest::DeleteExpiredRefreshTokens))
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

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("token_maintenance", duration);
        let status = if had_errors { "failure" } else { "success" };
        record_background_job_run("token_maintenance", status);
        record_background_job_items(
            "token_maintenance",
            result.expired_tokens_deleted + result.signing_keys_revoked,
        );

        debug!(
            trace_id = %trace_ctx.trace_id,
            expired_deleted = result.expired_tokens_deleted,
            keys_revoked = result.signing_keys_revoked,
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
    }
}
