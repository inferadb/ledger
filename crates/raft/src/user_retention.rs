//! Background user retention reaper.
//!
//! Periodically scans for soft-deleted users whose retention period has
//! elapsed and submits `EraseUser` Raft proposals to finalize deletion.
//! Only runs on the leader node.

use std::{sync::Arc, time::Instant};

use chrono::Utc;
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{UserStatus, config::UserRetentionConfig};
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Background job that erases users whose soft-delete retention period has expired.
///
/// Runs periodically on the leader, scanning for `Deleting` users whose
/// `deleted_at + region.retention_days()` is in the past. For each expired
/// user, proposes an `EraseUser` Raft request to finalize crypto-shredding.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct UserRetentionReaper<B: StorageBackend + 'static> {
    /// Raft consensus handle for leadership checks and proposals.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer for scanning user records.
    state: Arc<StateLayer<B>>,
    /// Reaper configuration.
    #[builder(default)]
    config: UserRetentionConfig,
}

impl<B: StorageBackend + 'static> UserRetentionReaper<B> {
    /// Creates a reaper from a configuration struct.
    pub fn from_config(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        config: &UserRetentionConfig,
    ) -> Self {
        Self { raft, node_id, state, config: config.clone() }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single reaper cycle, returning the number of users erased.
    async fn run_cycle(&self) -> usize {
        if !self.is_leader() {
            debug!("Skipping user retention reaper cycle (not leader)");
            return 0;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting user retention reaper cycle");

        let sys = inferadb_ledger_state::system::SystemOrganizationService::new(self.state.clone());

        // Scan users in batches
        let users = match sys.list_users(None, self.config.batch_size) {
            Ok(users) => users,
            Err(e) => {
                let duration = cycle_start.elapsed().as_secs_f64();
                record_background_job_duration("user_retention_reaper", duration);
                record_background_job_run("user_retention_reaper", "failure");
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    "Failed to scan users for retention reaper"
                );
                return 0;
            },
        };

        let now = Utc::now();
        let mut erased_count = 0;

        for user in &users {
            if user.status != UserStatus::Deleting {
                continue;
            }

            let deleted_at = match user.deleted_at {
                Some(ts) => ts,
                None => continue,
            };

            let retention = chrono::Duration::days(i64::from(user.region.retention_days()));
            let expiry = deleted_at + retention;

            if now < expiry {
                continue;
            }

            // Retention period has elapsed — submit erasure proposal
            let request = LedgerRequest::System(SystemRequest::EraseUser {
                user_id: user.id,
                erased_by: "retention_reaper".to_string(),
                region: user.region,
            });

            match self.raft.client_write(RaftPayload::new(request)).await {
                Ok(_) => {
                    info!(
                        trace_id = %trace_ctx.trace_id,
                        user_id = %user.id,
                        region = %user.region,
                        deleted_at = %deleted_at,
                        "User retention expired, erasure proposed"
                    );
                    erased_count += 1;
                },
                Err(e) => {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        user_id = %user.id,
                        error = ?e,
                        "Failed to propose user erasure"
                    );
                },
            }
        }

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("user_retention_reaper", duration);
        record_background_job_items("user_retention_reaper", erased_count as u64);

        if erased_count > 0 {
            record_background_job_run("user_retention_reaper", "success");
            info!(
                trace_id = %trace_ctx.trace_id,
                erased_count,
                scanned = users.len(),
                duration_secs = duration,
                "User retention reaper cycle complete"
            );
        } else {
            record_background_job_run("user_retention_reaper", "success");
            debug!(
                trace_id = %trace_ctx.trace_id,
                scanned = users.len(),
                duration_secs = duration,
                "User retention reaper cycle complete (no expired users)"
            );
        }

        erased_count
    }

    /// Starts the retention reaper background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let tick_interval = std::time::Duration::from_secs(self.config.interval_secs);
        tokio::spawn(async move {
            let mut ticker = interval(tick_interval);

            loop {
                ticker.tick().await;
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
    fn test_default_config_values() {
        let config = UserRetentionConfig::default();
        assert_eq!(config.interval_secs, 3600);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_config_validation_interval_too_small() {
        let result = UserRetentionConfig::builder().interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validation_batch_size_zero() {
        let result = UserRetentionConfig::builder().batch_size(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validation_minimum_values() {
        let config = UserRetentionConfig::builder()
            .interval_secs(60)
            .batch_size(1)
            .build()
            .expect("valid at minimum");
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.batch_size, 1);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = UserRetentionConfig { interval_secs: 7200, batch_size: 50 };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: UserRetentionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.interval_secs, 7200);
        assert_eq!(deserialized.batch_size, 50);
    }

    #[test]
    fn test_config_deserialization_defaults() {
        let config: UserRetentionConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.interval_secs, 3600);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_background_job_metrics_functions() {
        // Verify metric recording functions accept expected arguments.
        // No-ops without a recorder — confirms call signatures match run_cycle().
        record_background_job_duration("user_retention_reaper", 1.5);
        record_background_job_run("user_retention_reaper", "success");
        record_background_job_items("user_retention_reaper", 5);
    }
}
