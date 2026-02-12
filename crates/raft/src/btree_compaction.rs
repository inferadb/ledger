//! Background B+ tree compaction job.
//!
//! Periodically scans all B+ tree tables and merges underfull leaf nodes
//! to reclaim space after deletions. Only runs on the leader node.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::config::BTreeCompactionConfig;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
        record_btree_compaction,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Default interval between B+ tree compaction cycles (1 hour).
const DEFAULT_COMPACTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Default minimum fill factor threshold for compaction.
const DEFAULT_MIN_FILL_FACTOR: f64 = 0.4;

/// Background job that compacts B+ tree tables by merging underfull leaf nodes.
///
/// Runs periodically, checking if this node is the leader before performing
/// compaction. Merges adjacent sibling leaves under the same parent when their
/// combined fill factor allows it.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct BTreeCompactor<B: StorageBackend + 'static> {
    /// Raft consensus handle for verifying leadership before compacting.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// State layer providing database access.
    state: Arc<StateLayer<B>>,
    /// Minimum fill factor threshold (leaves below this are candidates for merging).
    #[builder(default = DEFAULT_MIN_FILL_FACTOR)]
    min_fill_factor: f64,
    /// Interval between compaction cycles.
    #[builder(default = DEFAULT_COMPACTION_INTERVAL)]
    interval: Duration,
}

impl<B: StorageBackend + 'static> BTreeCompactor<B> {
    /// Creates a compactor from a configuration struct.
    pub fn from_config(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<StateLayer<B>>,
        config: &BTreeCompactionConfig,
    ) -> Self {
        Self {
            raft,
            node_id,
            state,
            min_fill_factor: config.min_fill_factor,
            interval: Duration::from_secs(config.interval_secs),
        }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single compaction cycle.
    ///
    /// Compacts all B+ tree tables, merging underfull leaf nodes.
    fn run_cycle(&self) {
        if !self.is_leader() {
            debug!("Skipping B+ tree compaction cycle (not leader)");
            return;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting B+ tree compaction cycle");

        match self.state.compact_tables(self.min_fill_factor) {
            Ok(stats) => {
                let duration = cycle_start.elapsed().as_secs_f64();
                record_btree_compaction(stats.pages_merged, stats.pages_freed);
                record_background_job_duration("compaction", duration);
                record_background_job_run("compaction", "success");
                record_background_job_items("compaction", stats.pages_merged);

                if stats.pages_merged > 0 {
                    info!(
                        trace_id = %trace_ctx.trace_id,
                        pages_merged = stats.pages_merged,
                        pages_freed = stats.pages_freed,
                        duration_secs = duration,
                        "B+ tree compaction cycle complete"
                    );
                } else {
                    debug!(
                        trace_id = %trace_ctx.trace_id,
                        duration_secs = duration,
                        "B+ tree compaction cycle complete (no pages merged)"
                    );
                }
            },
            Err(e) => {
                let duration = cycle_start.elapsed().as_secs_f64();
                record_background_job_duration("compaction", duration);
                record_background_job_run("compaction", "failure");

                warn!(
                    trace_id = %trace_ctx.trace_id,
                    error = %e,
                    duration_secs = duration,
                    "B+ tree compaction cycle failed"
                );
            },
        }
    }

    /// Starts the B+ tree compactor background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                self.run_cycle();
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
        assert_eq!(DEFAULT_COMPACTION_INTERVAL, Duration::from_secs(3600));
        assert!((DEFAULT_MIN_FILL_FACTOR - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_interval_conversion() {
        let config = BTreeCompactionConfig::default();
        let duration = Duration::from_secs(config.interval_secs);
        assert_eq!(duration, Duration::from_secs(3600));
    }

    #[test]
    fn test_background_job_metrics_emitted_on_compaction() {
        // Verify metric recording functions accept expected arguments for compaction.
        // These are no-ops without a recorder — the test confirms call signatures
        // match what run_cycle() uses.
        use crate::metrics::{
            record_background_job_duration, record_background_job_items, record_background_job_run,
        };

        // Success path: pages were merged
        record_background_job_duration("compaction", 2.5);
        record_background_job_run("compaction", "success");
        record_background_job_items("compaction", 16);

        // Failure path
        record_background_job_duration("compaction", 0.01);
        record_background_job_run("compaction", "failure");
    }
}
