//! Background organization purge job.
//!
//! Periodically scans for organizations in `Deleted` status whose retention
//! cooldown has elapsed and submits `PurgeOrganization` Raft proposals to
//! finalize removal. Only runs on the leader node.
//!
//! Failed purge steps are retried up to 3 times with exponential backoff
//! within a cycle. Organizations that exhaust all retries are tracked in a
//! priority set and retried on every subsequent tick until successful.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{OrganizationId, config::OrganizationPurgeConfig};
use openraft::Raft;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
        record_org_purge_global_failure, record_org_purge_regional_failure,
        record_org_purge_retry_exhausted,
    },
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Maximum number of retry attempts per purge step.
const MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (100ms, 500ms, 2.5s).
const BACKOFF_BASE: Duration = Duration::from_millis(100);

/// Backoff multiplier between retry attempts.
const BACKOFF_MULTIPLIER: u32 = 5;

/// Background job that purges organizations whose soft-delete retention
/// cooldown has expired.
///
/// Runs periodically on the leader, scanning for `Deleted` organizations
/// whose `deleted_at + region.retention_days()` is in the past. For each
/// expired organization, proposes a `PurgeOrganization` Raft request to
/// finalize removal of all organization data.
///
/// Failed purges are tracked and retried on every subsequent tick until
/// successful (bypassing the retention cooldown check).
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

    /// Proposes a REGIONAL purge with retry and backoff.
    ///
    /// Returns `true` if the regional purge succeeded (or no manager is
    /// configured), `false` if all retries were exhausted.
    async fn propose_regional_with_retry(
        &self,
        org_id: OrganizationId,
        region: inferadb_ledger_types::Region,
        trace_id: &str,
    ) -> bool {
        let Some(ref manager) = self.manager else {
            return true;
        };

        let group = match manager.get_region_group(region) {
            Ok(g) => g,
            Err(e) => {
                warn!(
                    trace_id = %trace_id,
                    organization_id = %org_id,
                    region = %region,
                    error = ?e,
                    "Region group not found for REGIONAL purge"
                );
                return false;
            },
        };

        for attempt in 0..MAX_RETRIES {
            let regional_request =
                LedgerRequest::System(SystemRequest::PurgeOrganizationRegional {
                    organization: org_id,
                });

            match group.raft().client_write(RaftPayload::system(regional_request)).await {
                Ok(_) => return true,
                Err(e) => {
                    record_org_purge_regional_failure(&region.to_string());
                    let remaining = MAX_RETRIES - attempt - 1;
                    if remaining > 0 {
                        let delay = BACKOFF_BASE * BACKOFF_MULTIPLIER.pow(attempt);
                        warn!(
                            trace_id = %trace_id,
                            organization_id = %org_id,
                            region = %region,
                            attempt = attempt + 1,
                            retries_remaining = remaining,
                            error = ?e,
                            "REGIONAL purge failed, retrying after {}ms",
                            delay.as_millis(),
                        );
                        sleep(delay).await;
                    } else {
                        error!(
                            trace_id = %trace_id,
                            organization_id = %org_id,
                            region = %region,
                            error = ?e,
                            "REGIONAL purge failed after {MAX_RETRIES} attempts"
                        );
                    }
                },
            }
        }

        false
    }

    /// Proposes a GLOBAL purge with retry and backoff.
    ///
    /// Returns `true` if the global purge succeeded, `false` if all retries
    /// were exhausted.
    async fn propose_global_with_retry(&self, org_id: OrganizationId, trace_id: &str) -> bool {
        for attempt in 0..MAX_RETRIES {
            let request = LedgerRequest::PurgeOrganization { organization: org_id };

            match self.raft.client_write(RaftPayload::system(request)).await {
                Ok(_) => return true,
                Err(e) => {
                    record_org_purge_global_failure();
                    let remaining = MAX_RETRIES - attempt - 1;
                    if remaining > 0 {
                        let delay = BACKOFF_BASE * BACKOFF_MULTIPLIER.pow(attempt);
                        warn!(
                            trace_id = %trace_id,
                            organization_id = %org_id,
                            attempt = attempt + 1,
                            retries_remaining = remaining,
                            error = ?e,
                            "GLOBAL purge failed, retrying after {}ms",
                            delay.as_millis(),
                        );
                        sleep(delay).await;
                    } else {
                        error!(
                            trace_id = %trace_id,
                            organization_id = %org_id,
                            error = ?e,
                            "GLOBAL purge failed after {MAX_RETRIES} attempts"
                        );
                    }
                },
            }
        }

        false
    }

    /// Runs a single purge cycle, returning the number of organizations purged.
    ///
    /// Priority organizations (those that failed in a previous cycle) are
    /// processed first and are not gated by the retention cooldown.
    async fn run_cycle(&self, priority: &mut HashSet<OrganizationId>) -> usize {
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

        // Phase 1: Retry priority organizations (previously failed).
        let priority_snapshot: Vec<OrganizationId> = priority.iter().copied().collect();
        for org_id in &priority_snapshot {
            let org = organizations.iter().find(|o| o.organization_id == *org_id);
            let Some(org) = org else {
                // Organization no longer exists in state — remove from priority set.
                priority.remove(org_id);
                continue;
            };

            if self.purge_organization(org, &trace_ctx.trace_id).await {
                priority.remove(org_id);
                purged_count += 1;
            }
        }

        // Phase 2: Scan for newly eligible organizations.
        // Collect indices first to avoid borrow conflict with `priority`.
        let expired_indices: Vec<usize> = organizations
            .iter()
            .enumerate()
            .filter(|(_, org)| {
                !priority.contains(&org.organization_id)
                    && org.status == inferadb_ledger_state::system::OrganizationStatus::Deleted
                    && org.deleted_at.is_some_and(|deleted_at| {
                        let retention =
                            chrono::Duration::days(i64::from(org.region.retention_days()));
                        now >= deleted_at + retention
                    })
            })
            .take(self.config.batch_size)
            .map(|(i, _)| i)
            .collect();

        for idx in expired_indices {
            let org = &organizations[idx];
            if self.purge_organization(org, &trace_ctx.trace_id).await {
                purged_count += 1;
            } else {
                priority.insert(org.organization_id);
                record_org_purge_retry_exhausted();
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
                priority_pending = priority.len(),
                duration_secs = duration,
                "Organization purge cycle complete"
            );
        } else {
            record_background_job_run("organization_purge", "success");
            debug!(
                trace_id = %trace_ctx.trace_id,
                scanned = organizations.len(),
                priority_pending = priority.len(),
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

    /// Attempts to purge a single organization (REGIONAL + GLOBAL).
    ///
    /// Returns `true` if the GLOBAL purge succeeded. REGIONAL failures are
    /// tolerated (orphaned data is cleaned up on subsequent cycles).
    async fn purge_organization(
        &self,
        org: &inferadb_ledger_state::system::OrganizationRegistry,
        trace_id: &str,
    ) -> bool {
        // Step 1: Propose REGIONAL cleanup (profiles, name indices).
        // REGIONAL failure is tolerated — orphaned data will be cleaned on retry.
        let regional_ok =
            self.propose_regional_with_retry(org.organization_id, org.region, trace_id).await;

        if !regional_ok {
            warn!(
                trace_id = %trace_id,
                organization_id = %org.organization_id,
                region = %org.region,
                "REGIONAL purge exhausted retries, proceeding to GLOBAL purge"
            );
        }

        // Step 2: Propose GLOBAL purge (slug indices, structural records).
        if self.propose_global_with_retry(org.organization_id, trace_id).await {
            info!(
                trace_id = %trace_id,
                organization_id = %org.organization_id,
                region = %org.region,
                deleted_at = ?org.deleted_at,
                regional_ok,
                "Organization retention expired, purge proposed"
            );
            true
        } else {
            false
        }
    }

    /// Starts the organization purge background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let tick_interval = std::time::Duration::from_secs(self.config.interval_secs);
        tokio::spawn(async move {
            let mut ticker = interval(tick_interval);
            let mut priority_retries: HashSet<OrganizationId> = HashSet::new();

            loop {
                ticker.tick().await;
                self.run_cycle(&mut priority_retries).await;
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

    #[test]
    fn test_backoff_schedule() {
        // Verify exponential backoff: 100ms, 500ms, 2500ms
        let delays: Vec<Duration> = (0..MAX_RETRIES)
            .map(|attempt| BACKOFF_BASE * BACKOFF_MULTIPLIER.pow(attempt))
            .collect();
        assert_eq!(delays[0], Duration::from_millis(100));
        assert_eq!(delays[1], Duration::from_millis(500));
        assert_eq!(delays[2], Duration::from_millis(2500));
    }

    #[test]
    fn test_constants() {
        assert_eq!(MAX_RETRIES, 3);
        assert_eq!(BACKOFF_BASE, Duration::from_millis(100));
        assert_eq!(BACKOFF_MULTIPLIER, 5);
    }

    #[test]
    fn test_backoff_total_wait() {
        // Total wait across all retries: 100 + 500 + 2500 = 3100ms
        let total: Duration =
            (0..MAX_RETRIES).map(|attempt| BACKOFF_BASE * BACKOFF_MULTIPLIER.pow(attempt)).sum();
        assert_eq!(total, Duration::from_millis(3100));
    }

    #[test]
    fn test_config_large_batch_size() {
        let config = OrganizationPurgeConfig::builder()
            .interval_secs(3600)
            .batch_size(10_000)
            .build()
            .expect("large batch is valid");
        assert_eq!(config.batch_size, 10_000);
    }

    #[test]
    fn test_retention_window_calculation() {
        use inferadb_ledger_types::Region;

        // GLOBAL has 90-day retention
        let retention_global = chrono::Duration::days(i64::from(Region::GLOBAL.retention_days()));
        assert_eq!(retention_global.num_days(), 90);

        let now = Utc::now();

        // Deleted 100 days ago: past retention
        let deleted_long_ago = now - chrono::Duration::days(100);
        assert!(now >= deleted_long_ago + retention_global);

        // Deleted 10 days ago: within retention
        let deleted_recently = now - chrono::Duration::days(10);
        assert!(now < deleted_recently + retention_global);
    }

    #[test]
    fn test_backoff_no_overflow_at_max_retries() {
        // Verify the last retry attempt doesn't overflow
        let last_attempt = MAX_RETRIES - 1;
        let delay = BACKOFF_BASE * BACKOFF_MULTIPLIER.pow(last_attempt);
        assert_eq!(delay, Duration::from_millis(2500));
    }
}
