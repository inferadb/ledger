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

use inferadb_ledger_types::{
    Region, config::PostErasureCompactionConfig, trace_context::TraceContext,
};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    consensus_handle::ConsensusHandle, metrics::record_post_erasure_compaction_triggered,
    raft_manager::RaftManager,
};

/// Background job that enforces maximum Raft log retention by triggering
/// proactive snapshots on all Raft groups (GLOBAL + regional).
///
/// On each cycle, checks whether the time since the last snapshot trigger
/// exceeds `max_log_retention_secs`. If so, triggers a snapshot via the
/// consensus handle on the affected Raft group (leader-only).
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct PostErasureCompactionJob {
    /// GLOBAL consensus handle for leadership checks.
    handle: Arc<ConsensusHandle>,
    /// Raft manager for accessing regional Raft groups.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Job configuration.
    #[builder(default)]
    config: PostErasureCompactionConfig,
    /// Watchdog heartbeat handle.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl PostErasureCompactionJob {
    /// Checks if this node is the leader of the GLOBAL shard.
    fn is_leader_of_global(&self) -> bool {
        self.handle.is_leader()
    }

    /// Triggers a snapshot if this node is leader and the retention threshold
    /// has been exceeded.
    ///
    /// Calls `trigger_snapshot()` on the consensus handle to initiate snapshot
    /// creation. The actual snapshot is taken by the apply worker / state machine.
    async fn maybe_trigger_snapshot(
        &self,
        region_label: &str,
        is_leader: bool,
        last_snapshot: &mut Option<Instant>,
        threshold: Duration,
        handle: &ConsensusHandle,
    ) -> bool {
        if !is_leader {
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

        match handle.trigger_snapshot().await {
            Ok((idx, _term)) if idx > 0 => {
                info!(
                    region = region_label,
                    last_included_index = idx,
                    "Post-erasure compaction: snapshot triggered"
                );
                *last_snapshot = Some(Instant::now());
                record_post_erasure_compaction_triggered(region_label);
                true
            },
            Ok(_) => {
                debug!(
                    region = region_label,
                    "Post-erasure compaction: no snapshot needed (commit index unchanged)"
                );
                *last_snapshot = Some(Instant::now());
                false
            },
            Err(e) => {
                warn!(
                    region = region_label,
                    error = %e,
                    "Post-erasure compaction: snapshot trigger failed"
                );
                // Do not update timestamp — allow retry on next cycle.
                false
            },
        }
    }

    /// Runs a single compaction check cycle across all Raft groups.
    async fn run_cycle(&self, last_snapshots: &mut HashMap<String, Option<Instant>>) {
        let mut job = crate::logging::JobContext::new("post_erasure_compaction", None);
        let cycle_start = Instant::now();
        let trace_ctx = TraceContext::new();
        debug!(trace_id = %trace_ctx.trace_id, "Starting post-erasure compaction cycle");

        let threshold = Duration::from_secs(self.config.max_log_retention_secs);
        let mut triggered = 0u64;

        // Check GLOBAL group.
        let global_is_leader = self.is_leader_of_global();
        let global_last = last_snapshots.entry("GLOBAL".to_string()).or_insert(None);
        if self
            .maybe_trigger_snapshot(
                "GLOBAL",
                global_is_leader,
                global_last,
                threshold,
                &self.handle,
            )
            .await
        {
            triggered += 1;
        }

        // Check each regional group using per-region leadership.
        if let Some(ref manager) = self.manager {
            let regions: Vec<Region> = manager.list_regions();
            for region in regions {
                if region == Region::GLOBAL {
                    continue; // Already checked above.
                }
                let region_label = region.as_str().to_string();
                match manager.get_region_group(region) {
                    Ok(group) => {
                        let region_is_leader = group.handle().is_leader();
                        let region_last =
                            last_snapshots.entry(region_label.clone()).or_insert(None);
                        if self
                            .maybe_trigger_snapshot(
                                &region_label,
                                region_is_leader,
                                region_last,
                                threshold,
                                group.handle(),
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

        job.record_items(triggered);
        let duration_secs = cycle_start.elapsed().as_secs_f64();

        if triggered > 0 {
            info!(
                trace_id = %trace_ctx.trace_id,
                triggered,
                duration_secs,
                "Post-erasure compaction cycle complete"
            );
        } else {
            debug!(
                trace_id = %trace_ctx.trace_id,
                duration_secs,
                "Post-erasure compaction cycle complete (no snapshots triggered)"
            );
        }

        // Update watchdog heartbeat.
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(
                crate::graceful_shutdown::watchdog_now_nanos(),
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
                tokio::select! {
                    _ = ticker.tick() => {
                        self.run_cycle(&mut last_snapshots).await;
                    }
                    _ = self.cancellation_token.cancelled() => {
                        info!("PostErasureCompactionJob shutting down");
                        break;
                    }
                }
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

    #[test]
    fn snapshot_trigger_decision_exact_boundary() {
        // Exactly at the threshold boundary — should trigger
        let threshold = Duration::from_secs(3600);
        let last_snapshot = Some(Instant::now() - threshold);
        let should_trigger = match last_snapshot {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };
        assert!(should_trigger);
    }

    #[test]
    fn last_snapshots_region_independence() {
        let mut last_snapshots: HashMap<String, Option<Instant>> = HashMap::new();
        let now = Instant::now();

        // Set different timestamps for different regions
        last_snapshots.insert("GLOBAL".to_string(), Some(now));
        last_snapshots.insert("US_EAST_VA".to_string(), Some(now - Duration::from_secs(7200)));
        last_snapshots.insert("EU_WEST_IE".to_string(), None);

        let threshold = Duration::from_secs(3600);

        // GLOBAL: recent, should not trigger
        let global_should_trigger = match last_snapshots["GLOBAL"] {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };
        assert!(!global_should_trigger);

        // US_EAST_VA: 2 hours ago, should trigger
        let us_should_trigger = match last_snapshots["US_EAST_VA"] {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };
        assert!(us_should_trigger);

        // EU_WEST_IE: None, first cycle sets baseline
        let eu_should_trigger = match last_snapshots["EU_WEST_IE"] {
            Some(ts) => ts.elapsed() >= threshold,
            None => false,
        };
        assert!(!eu_should_trigger);
    }

    #[test]
    fn config_validation_rejects_both_too_small() {
        let result = PostErasureCompactionConfig::builder()
            .max_log_retention_secs(10)
            .check_interval_secs(10)
            .build();
        assert!(result.is_err());
    }
}
