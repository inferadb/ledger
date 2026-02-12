//! Block compaction for COMPACTED retention mode.
//!
//! Per DESIGN.md §4.4, vaults can be configured with COMPACTED retention mode
//! where transaction bodies are removed from old blocks while preserving headers
//! (state_root, tx_merkle_root) for verification.
//!
//! Compaction behavior:
//! - Runs only on leader (followers skip)
//! - Respects per-vault retention_policy settings
//! - Removes transaction bodies for blocks older than (tip - retention_blocks)
//! - Headers are always preserved for chain verification

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_state::BlockArchive;
use inferadb_ledger_store::StorageBackend;
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    log_storage::AppliedStateAccessor,
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    trace_context::TraceContext,
    types::{BlockRetentionMode, LedgerNodeId, LedgerTypeConfig},
};

/// Default interval between compaction cycles.
const COMPACTION_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Block compactor for COMPACTED retention mode.
///
/// Runs as a background task, periodically checking vault retention policies
/// and compacting old blocks to remove transaction bodies.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct BlockCompactor<B: StorageBackend + 'static> {
    /// Raft consensus handle for leader checks.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// Block archive for compaction.
    block_archive: Arc<BlockArchive<B>>,
    /// Accessor for applied state (vault registry and metadata).
    applied_state: AppliedStateAccessor,
    /// Compaction interval.
    #[builder(default = COMPACTION_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> BlockCompactor<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Runs a single compaction cycle.
    ///
    /// Scans all vaults with COMPACTED retention mode and compacts blocks
    /// older than (current_height - retention_blocks).
    async fn run_cycle(&self) {
        // Only leader performs compaction
        if !self.is_leader() {
            debug!("Skipping compaction cycle (not leader)");
            return;
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting block compaction cycle");

        // Get all vault metadata to check retention policies
        let vaults = self.applied_state.all_vaults();
        let vault_heights = self.applied_state.all_vault_heights();

        let mut total_compacted = 0u64;
        let mut had_error = false;

        for ((namespace_id, vault_id), meta) in vaults {
            // Skip vaults not in COMPACTED mode
            if meta.retention_policy.mode != BlockRetentionMode::Compacted {
                continue;
            }

            // Get current vault height
            let current_height = match vault_heights.get(&(namespace_id, vault_id)) {
                Some(h) => *h,
                None => continue,
            };

            // Calculate compaction watermark
            let retention_blocks = meta.retention_policy.retention_blocks;
            if current_height <= retention_blocks {
                // Not enough blocks to compact
                continue;
            }

            let compact_before = current_height - retention_blocks;

            // Check if we've already compacted past this point
            match self.block_archive.compaction_watermark() {
                Ok(Some(watermark)) if watermark >= compact_before => {
                    // Already compacted
                    continue;
                },
                Err(e) => {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        namespace_id = namespace_id.value(),
                        vault_id = vault_id.value(),
                        error = %e,
                        "Failed to get compaction watermark"
                    );
                    had_error = true;
                    continue;
                },
                _ => {},
            }

            // Perform compaction
            match self.block_archive.compact_before(compact_before) {
                Ok(count) => {
                    if count > 0 {
                        info!(
                            trace_id = %trace_ctx.trace_id,
                            namespace_id = namespace_id.value(),
                            vault_id = vault_id.value(),
                            compact_before,
                            count,
                            "Compacted blocks"
                        );
                        total_compacted += count;
                    }
                },
                Err(e) => {
                    warn!(
                        trace_id = %trace_ctx.trace_id,
                        namespace_id = namespace_id.value(),
                        vault_id = vault_id.value(),
                        compact_before,
                        error = %e,
                        "Block compaction failed"
                    );
                    had_error = true;
                },
            }
        }

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("gc", duration);
        record_background_job_run("gc", if had_error { "failure" } else { "success" });
        if total_compacted > 0 {
            record_background_job_items("gc", total_compacted);
            info!(
                trace_id = %trace_ctx.trace_id,
                total_compacted,
                duration_secs = duration,
                "Block compaction cycle complete"
            );
        } else {
            debug!(
                trace_id = %trace_ctx.trace_id,
                duration_secs = duration,
                "Block compaction cycle complete (no blocks compacted)"
            );
        }
    }

    /// Starts the block compactor background task.
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
                        std::sync::atomic::Ordering::Relaxed,
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
    use crate::types::BlockRetentionPolicy;

    #[test]
    fn test_compaction_calculation() {
        // Vault height 15000, retention_blocks 10000
        // Should compact blocks before height 5000
        let current_height = 15000u64;
        let retention_blocks = 10000u64;
        let compact_before = current_height - retention_blocks;
        assert_eq!(compact_before, 5000);

        // Vault height 5000, retention_blocks 10000
        // Should not compact (not enough blocks)
        let current_height = 5000u64;
        let retention_blocks = 10000u64;
        assert!(current_height <= retention_blocks);

        // Vault height 10001, retention_blocks 10000
        // Should compact blocks before height 1
        let current_height = 10001u64;
        let retention_blocks = 10000u64;
        let compact_before = current_height - retention_blocks;
        assert_eq!(compact_before, 1);
    }

    #[test]
    fn test_retention_policy_check() {
        // Full mode - should not compact
        let full_policy = BlockRetentionPolicy::default();
        assert_eq!(full_policy.mode, BlockRetentionMode::Full);
        assert!(full_policy.mode != BlockRetentionMode::Compacted);

        // Compacted mode - should compact
        let compacted_policy =
            BlockRetentionPolicy { mode: BlockRetentionMode::Compacted, retention_blocks: 5000 };
        assert_eq!(compacted_policy.mode, BlockRetentionMode::Compacted);
        assert_eq!(compacted_policy.retention_blocks, 5000);
    }
}
