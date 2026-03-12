//! Background organization purge job.
//!
//! Periodically scans for organizations in `Deleted` status whose retention
//! cooldown has elapsed and submits `PurgeOrganization` Raft proposals to
//! finalize removal. Only runs on the leader node.

use std::{sync::Arc, time::Instant};

use chrono::Utc;
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::config::OrganizationPurgeConfig;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Background job that purges organizations whose soft-delete retention
/// cooldown has expired.
///
/// Runs periodically on the leader, scanning for `Deleted` organizations
/// whose `deleted_at + region.retention_days()` is in the past. For each
/// expired organization, proposes a `PurgeOrganization` Raft request to
/// finalize removal of all organization data.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct OrganizationPurgeJob<B: StorageBackend + 'static> {
    /// Raft consensus handle for leadership checks and proposals.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer for scanning organization records.
    state: Arc<StateLayer<B>>,
    /// Purge job configuration.
    #[builder(default)]
    config: OrganizationPurgeConfig,
    /// Raft manager for proposing REGIONAL cleanup before GLOBAL purge.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> OrganizationPurgeJob<B> {
    /// Creates a purge job from a configuration struct.
    pub fn from_config(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        config: &OrganizationPurgeConfig,
    ) -> Self {
        Self { raft, node_id, state, config: config.clone(), manager: None, watchdog_handle: None }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single purge cycle, returning the number of organizations purged.
    async fn run_cycle(&self) -> usize {
        if !self.is_leader() {
            debug!("Skipping organization purge cycle (not leader)");
            return 0;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting organization purge cycle");

        let sys = inferadb_ledger_state::system::SystemOrganizationService::new(self.state.clone());

        let organizations = match sys.list_organizations() {
            Ok(orgs) => orgs,
            Err(e) => {
                let duration = cycle_start.elapsed().as_secs_f64();
                record_background_job_duration("organization_purge", duration);
                record_background_job_run("organization_purge", "failure");
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    "Failed to scan organizations for purge job"
                );
                return 0;
            },
        };

        let now = Utc::now();
        let mut purged_count = 0;

        let expired_orgs = organizations.iter().filter(|org| {
            org.status == inferadb_ledger_state::system::OrganizationStatus::Deleted
                && org.deleted_at.is_some_and(|deleted_at| {
                    let retention = chrono::Duration::days(i64::from(org.region.retention_days()));
                    now >= deleted_at + retention
                })
        });

        for org in expired_orgs.take(self.config.batch_size) {
            // Step 1: Propose REGIONAL cleanup (profiles, name indices)
            if let Some(ref manager) = self.manager {
                let regional_request =
                    LedgerRequest::System(SystemRequest::PurgeOrganizationRegional {
                        organization: org.organization_id,
                    });
                match manager.get_region_group(org.region) {
                    Ok(group) => {
                        if let Err(e) =
                            group.raft().client_write(RaftPayload::new(regional_request)).await
                        {
                            warn!(
                                trace_id = %trace_ctx.trace_id,
                                organization_id = %org.organization_id,
                                region = %org.region,
                                error = ?e,
                                "Failed to propose REGIONAL organization purge"
                            );
                            // Continue to GLOBAL purge — REGIONAL data is orphaned
                            // but not a blocker for structural cleanup.
                        }
                    },
                    Err(e) => {
                        warn!(
                            trace_id = %trace_ctx.trace_id,
                            organization_id = %org.organization_id,
                            region = %org.region,
                            error = ?e,
                            "Region group not found for REGIONAL purge"
                        );
                    },
                }
            }

            // Step 2: Propose GLOBAL purge (slug indices, structural records)
            let request = LedgerRequest::PurgeOrganization { organization: org.organization_id };

            match self.raft.client_write(RaftPayload::new(request)).await {
                Ok(_) => {
                    info!(
                        trace_id = %trace_ctx.trace_id,
                        organization_id = %org.organization_id,
                        region = %org.region,
                        deleted_at = ?org.deleted_at,
                        "Organization retention expired, purge proposed"
                    );
                    purged_count += 1;
                },
                Err(e) => {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        organization_id = %org.organization_id,
                        error = ?e,
                        "Failed to propose organization purge"
                    );
                },
            }
        }

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("organization_purge", duration);
        record_background_job_items("organization_purge", purged_count as u64);

        if purged_count > 0 {
            record_background_job_run("organization_purge", "success");
            info!(
                trace_id = %trace_ctx.trace_id,
                purged_count,
                scanned = organizations.len(),
                duration_secs = duration,
                "Organization purge cycle complete"
            );
        } else {
            record_background_job_run("organization_purge", "success");
            debug!(
                trace_id = %trace_ctx.trace_id,
                scanned = organizations.len(),
                duration_secs = duration,
                "Organization purge cycle complete (no expired organizations)"
            );
        }

        // Update watchdog heartbeat
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                std::sync::atomic::Ordering::Release,
            );
        }

        purged_count
    }

    /// Starts the organization purge background task.
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
        let config = OrganizationPurgeConfig::default();
        assert_eq!(config.interval_secs, 3600);
        assert_eq!(config.batch_size, 50);
    }

    #[test]
    fn test_config_validation_interval_too_small() {
        let result = OrganizationPurgeConfig::builder().interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validation_batch_size_zero() {
        let result = OrganizationPurgeConfig::builder().batch_size(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validation_minimum_values() {
        let config = OrganizationPurgeConfig::builder()
            .interval_secs(60)
            .batch_size(1)
            .build()
            .expect("valid at minimum");
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.batch_size, 1);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = OrganizationPurgeConfig { interval_secs: 7200, batch_size: 25 };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: OrganizationPurgeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.interval_secs, 7200);
        assert_eq!(deserialized.batch_size, 25);
    }

    #[test]
    fn test_config_deserialization_defaults() {
        let config: OrganizationPurgeConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.interval_secs, 3600);
        assert_eq!(config.batch_size, 50);
    }
}
