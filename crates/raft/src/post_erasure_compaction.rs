//! Post-erasure Raft log compaction job.
//!
//! Enforces a maximum Raft log retention period by triggering proactive
//! snapshots when the time since last snapshot exceeds a configurable
//! threshold. This bounds how long encrypted PII entries remain in the
//! Raft log after user erasure or organization purge.
//!
//! Without this job, log compaction only occurs when `snapshot_threshold`
//! entries accumulate (default 10,000). On low-traffic regional groups,
//! this can take days or weeks — leaving crypto-shredded entries on disk
//! far longer than GDPR erasure SLAs require.
//!
//! The job runs on every node but only triggers snapshots on Raft groups
//! where this node is the leader.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_types::{Region, config::PostErasureCompactionConfig};
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_run,
        record_post_erasure_compaction_triggered,
    },
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Background job that enforces maximum Raft log retention by triggering
/// proactive snapshots on all Raft groups (GLOBAL + regional).
///
/// On each cycle, checks whether the time since the last snapshot trigger
/// exceeds `max_log_retention_secs`. If so, calls `trigger().snapshot()`
/// on the affected Raft group (leader-only).
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct PostErasureCompactionJob {
    /// This node's ID for leadership checks.
    node_id: LedgerNodeId,
    /// GLOBAL Raft handle.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Raft manager for accessing regional Raft groups.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Job configuration.
    #[builder(default)]
    config: PostErasureCompactionConfig,
    /// Watchdog heartbeat handle.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl PostErasureCompactionJob {
    /// Checks if this node is the leader of the given Raft group.
    fn is_leader_of(&self, raft: &Raft<LedgerTypeConfig>) -> bool {
        let metrics = raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Triggers a snapshot on a Raft group if this node is leader and
    /// the retention threshold has been exceeded.
    async fn maybe_trigger_snapshot(
        &self,
        region_label: &str,
        raft: &Raft<LedgerTypeConfig>,
        last_snapshot: &mut Option<Instant>,
        threshold: Duration,
    ) -> bool {
        if !self.is_leader_of(raft) {
            return false;
        }

        let should_trigger = match *last_snapshot {
            Some(ts) => ts.elapsed() >= threshold,
            None => {
                // First cycle after startup — record baseline without
                // triggering an immediate snapshot burst across all groups.
                *last_snapshot = Some(Instant::now());
                false
            },
        };

        if !should_trigger {
            return false;
        }

        match raft.trigger().snapshot().await {
            Ok(_) => {
                *last_snapshot = Some(Instant::now());
                record_post_erasure_compaction_triggered(region_label);
                info!(region = region_label, "Post-erasure compaction: triggered snapshot");
                true
            },
            Err(e) => {
                warn!(
                    region = region_label,
                    error = %e,
                    "Post-erasure compaction: failed to trigger snapshot"
                );
                false
            },
        }
    }

    /// Runs a single compaction check cycle across all Raft groups.
    async fn run_cycle(&self, last_snapshots: &mut HashMap<String, Option<Instant>>) {
        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting post-erasure compaction cycle");

        let threshold = Duration::from_secs(self.config.max_log_retention_secs);
        let mut triggered = 0u64;

        // Check GLOBAL Raft group.
        let global_last = last_snapshots.entry("GLOBAL".to_string()).or_insert(None);
        if self.maybe_trigger_snapshot("GLOBAL", &self.raft, global_last, threshold).await {
            triggered += 1;
        }

        // Check each regional Raft group.
        if let Some(ref manager) = self.manager {
            let regions: Vec<Region> = manager.list_regions();
            for region in regions {
                if region == Region::GLOBAL {
                    continue; // Already checked above.
                }
                let region_label = region.as_str().to_string();
                match manager.get_region_group(region) {
                    Ok(group) => {
                        let region_last =
                            last_snapshots.entry(region_label.clone()).or_insert(None);
                        if self
                            .maybe_trigger_snapshot(
                                &region_label,
                                group.raft(),
                                region_last,
                                threshold,
                            )
                            .await
                        {
                            triggered += 1;
                        }
                    },
                    Err(e) => {
                        debug!(
                            region = region.as_str(),
                            error = ?e,
                            "Skipping region group (not available)"
                        );
                    },
                }
            }
        }

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("post_erasure_compaction", duration);

        record_background_job_run("post_erasure_compaction", "success");
        if triggered > 0 {
            info!(
                trace_id = %trace_ctx.trace_id,
                triggered,
                duration_secs = duration,
                "Post-erasure compaction cycle complete"
            );
        } else {
            debug!(
                trace_id = %trace_ctx.trace_id,
                duration_secs = duration,
                "Post-erasure compaction cycle complete (no snapshots triggered)"
            );
        }

        // Update watchdog heartbeat.
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    /// Starts the post-erasure compaction background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let tick_interval = Duration::from_secs(self.config.check_interval_secs);
        tokio::spawn(async move {
            let mut ticker = interval(tick_interval);
            let mut last_snapshots: HashMap<String, Option<Instant>> = HashMap::new();

            loop {
                ticker.tick().await;
                self.run_cycle(&mut last_snapshots).await;
            }
        })
    }
}

// Config validation tests live in `types/src/config/mod.rs` (canonical location).

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = PostErasureCompactionConfig::default();
        assert_eq!(config.max_log_retention_secs, 3600);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn config_serde_roundtrip() {
        let config =
            PostErasureCompactionConfig { max_log_retention_secs: 7200, check_interval_secs: 600 };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PostErasureCompactionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_log_retention_secs, 7200);
        assert_eq!(deserialized.check_interval_secs, 600);
    }

    #[test]
    fn config_deserialization_defaults() {
        let config: PostErasureCompactionConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.max_log_retention_secs, 3600);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn config_clone() {
        let config =
            PostErasureCompactionConfig { max_log_retention_secs: 1800, check_interval_secs: 120 };
        let cloned = config.clone();
        assert_eq!(cloned.max_log_retention_secs, config.max_log_retention_secs);
        assert_eq!(cloned.check_interval_secs, config.check_interval_secs);
    }

    #[test]
    fn threshold_duration_from_config() {
        let config =
            PostErasureCompactionConfig { max_log_retention_secs: 3600, check_interval_secs: 300 };
        let threshold = Duration::from_secs(config.max_log_retention_secs);
        assert_eq!(threshold, Duration::from_secs(3600));
    }

    #[test]
    fn tick_interval_from_config() {
        let config =
            PostErasureCompactionConfig { max_log_retention_secs: 3600, check_interval_secs: 300 };
        let tick = Duration::from_secs(config.check_interval_secs);
        assert_eq!(tick, Duration::from_secs(300));
    }

    #[test]
    fn last_snapshots_map_operations() {
        // Exercises the HashMap<String, Option<Instant>> used in run_cycle
        let mut last_snapshots: HashMap<String, Option<Instant>> = HashMap::new();

        // First access returns None
        let global_last = last_snapshots.entry("GLOBAL".to_string()).or_insert(None);
        assert!(global_last.is_none());

        // After snapshot, it should have a value
        *global_last = Some(Instant::now());
        assert!(last_snapshots["GLOBAL"].is_some());

        // Multiple regions tracked independently
        last_snapshots.entry("US_EAST_VA".to_string()).or_insert(None);
        last_snapshots.entry("EU_WEST_IE".to_string()).or_insert(Some(Instant::now()));

        assert_eq!(last_snapshots.len(), 3);
        assert!(last_snapshots["US_EAST_VA"].is_none());
        assert!(last_snapshots["EU_WEST_IE"].is_some());
    }

    #[test]
    fn snapshot_trigger_decision_first_cycle() {
        // First cycle: no previous snapshot timestamp — sets baseline, does NOT trigger
        let mut last_snapshot: Option<Instant> = None;
        let threshold = Duration::from_secs(3600);

        let should_trigger = match last_snapshot {
            Some(ts) => ts.elapsed() >= threshold,
            None => {
                last_snapshot = Some(Instant::now());
                false
            },
        };

        assert!(!should_trigger);
        assert!(last_snapshot.is_some());
    }

    #[test]
    fn snapshot_trigger_decision_within_threshold() {
        // Recent snapshot — should NOT trigger
        let last_snapshot = Some(Instant::now());
        let threshold = Duration::from_secs(3600);

        let should_trigger = match last_snapshot {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };

        assert!(!should_trigger);
    }

    #[test]
    fn snapshot_trigger_decision_past_threshold() {
        // Old snapshot — SHOULD trigger
        let last_snapshot = Some(Instant::now() - Duration::from_secs(7200));
        let threshold = Duration::from_secs(3600);

        let should_trigger = match last_snapshot {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };

        assert!(should_trigger);
    }

    #[test]
    fn config_validation_rejects_too_small_retention() {
        let result = PostErasureCompactionConfig::builder().max_log_retention_secs(299).build();
        assert!(result.is_err());
    }

    #[test]
    fn config_validation_rejects_too_small_interval() {
        let result = PostErasureCompactionConfig::builder().check_interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn config_validation_accepts_minimum_values() {
        let config = PostErasureCompactionConfig::builder()
            .max_log_retention_secs(300)
            .check_interval_secs(60)
            .build()
            .expect("minimum values should be valid");
        assert_eq!(config.max_log_retention_secs, 300);
        assert_eq!(config.check_interval_secs, 60);
    }
}
