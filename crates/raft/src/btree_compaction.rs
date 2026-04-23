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
use inferadb_ledger_types::{config::BTreeCompactionConfig, trace_context::TraceContext};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{consensus_handle::ConsensusHandle, metrics::record_btree_compaction};

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
    /// Consensus handle for verifying leadership before compacting.
    handle: Arc<ConsensusHandle>,
    /// State layer providing database access.
    state: Arc<StateLayer<B>>,
    /// Minimum fill factor threshold (leaves below this are candidates for merging).
    #[builder(default = DEFAULT_MIN_FILL_FACTOR)]
    min_fill_factor: f64,
    /// Interval between compaction cycles.
    #[builder(default = DEFAULT_COMPACTION_INTERVAL)]
    interval: Duration,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl<B: StorageBackend + 'static> BTreeCompactor<B> {
    /// Creates a compactor from a configuration struct.
    pub fn from_config(
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<B>>,
        config: &BTreeCompactionConfig,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            handle,
            state,
            min_fill_factor: config.min_fill_factor,
            interval: Duration::from_secs(config.interval_secs),
            cancellation_token,
        }
    }

    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        self.handle.is_leader()
    }

    /// Runs a single compaction cycle across every live vault DB.
    ///
    /// Slice 2c routes B+ tree compaction per-vault — `compact_tables`
    /// addresses one vault at a time so a corruption or contention
    /// spike in one vault DB does not block compaction of the rest.
    /// Aggregates per-vault `pages_merged` / `pages_freed` for the
    /// cycle metric.
    fn run_cycle(&self) {
        if !self.is_leader() {
            debug!("Skipping B+ tree compaction cycle (not leader)");
            return;
        }

        let mut job = crate::logging::JobContext::new("btree_compaction", None);
        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting B+ tree compaction cycle");

        let vaults = self.state.live_vault_dbs();
        let mut total_merged = 0u64;
        let mut total_freed = 0u64;
        let mut had_failure = false;

        for (vault, _db) in &vaults {
            match self.state.compact_tables(*vault, self.min_fill_factor) {
                Ok(stats) => {
                    total_merged = total_merged.saturating_add(stats.pages_merged);
                    total_freed = total_freed.saturating_add(stats.pages_freed);
                },
                Err(e) => {
                    had_failure = true;
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        vault_id = vault.value(),
                        error = %e,
                        "B+ tree compaction failed for vault"
                    );
                },
            }
        }

        let duration_secs = cycle_start.elapsed().as_secs_f64();
        record_btree_compaction(total_merged, total_freed);
        job.record_items(total_merged);
        if had_failure {
            job.set_failure();
        }

        if total_merged > 0 {
            info!(
                trace_id = %trace_ctx.trace_id,
                vaults = vaults.len(),
                pages_merged = total_merged,
                pages_freed = total_freed,
                duration_secs,
                "B+ tree compaction cycle complete"
            );
        } else {
            debug!(
                trace_id = %trace_ctx.trace_id,
                vaults = vaults.len(),
                duration_secs,
                "B+ tree compaction cycle complete (no pages merged)"
            );
        }
    }

    /// Starts the B+ tree compactor background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.run_cycle();
                    }
                    _ = self.cancellation_token.cancelled() => {
                        info!("BTreeCompactor shutting down");
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_config_interval_conversion() {
        let config = BTreeCompactionConfig::default();
        let duration = Duration::from_secs(config.interval_secs);
        assert_eq!(duration, Duration::from_secs(3600));
    }
}
