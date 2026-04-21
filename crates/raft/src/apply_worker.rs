//! Decoupled apply worker for the consensus engine.
//!
//! Receives committed entry batches from the consensus reactor and applies
//! them to the state machine via [`RaftLogStore::apply_committed_entries`].

use std::time::Instant;

use inferadb_ledger_consensus::committed::CommittedBatch;
use inferadb_ledger_state::shard_routing::ShardIdx;
use inferadb_ledger_store::FileBackend;
use tokio::sync::mpsc;

use crate::{
    consensus_handle::{ResponseMap, SpilloverMap},
    log_storage::RaftLogStore,
    metrics,
};

/// Runs the apply loop, applying consensus engine entries to the state machine.
///
/// One apply worker per `RegionGroup` in the current Phase A layout; once
/// Task 5 fans `start_region` across `0..shards_per_region`, there will be
/// one per `(region, shard)` pair. The `region` + `shard` fields below feed
/// the Prometheus labels on every [`metrics::record_apply_batch`] — the
/// headline Phase A scaling metric.
pub struct ApplyWorker {
    store: RaftLogStore<FileBackend>,
    response_map: ResponseMap,
    spillover: SpilloverMap,
    /// When set, sends a signal after every committed batch is applied. Used
    /// by the GLOBAL region's apply worker to wake the PlacementController —
    /// ensures data region membership is updated within one apply cycle of
    /// a GLOBAL membership change.
    dr_event_tx: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    /// Region label for apply-batch metrics.
    region: String,
    /// Shard label for apply-batch metrics, pre-stringified from a
    /// [`ShardIdx`]. Phase A always emits `"0"`; Task 5 fans workers out.
    shard: String,
}

impl ApplyWorker {
    /// Creates a new apply worker.
    ///
    /// - `store` — the Raft log store containing state layer, block archive, etc.
    /// - `response_map` — shared map for delivering responses back to proposers.
    /// - `spillover` — buffer for responses when no waiter is registered yet.
    /// - `region` / `shard_idx` — labels stamped on every [`metrics::record_apply_batch`] emission.
    pub fn new(
        store: RaftLogStore<FileBackend>,
        response_map: ResponseMap,
        spillover: SpilloverMap,
        region: impl Into<String>,
        shard_idx: ShardIdx,
    ) -> Self {
        Self {
            store,
            response_map,
            spillover,
            dr_event_tx: None,
            region: region.into(),
            shard: shard_idx.0.to_string(),
        }
    }

    /// Attaches a DR event sender for this worker. When set, the worker
    /// sends a signal after every committed batch is applied, waking the
    /// PlacementController on this node. Only the GLOBAL region's worker
    /// should have this set — data region workers leave it `None`.
    #[must_use]
    pub fn with_dr_event_tx(mut self, tx: tokio::sync::mpsc::UnboundedSender<()>) -> Self {
        self.dr_event_tx = Some(tx);
        self
    }

    /// Runs the apply loop until the channel is closed (engine shutdown).
    pub async fn run(mut self, mut rx: mpsc::Receiver<CommittedBatch>) {
        use tracing::Instrument;
        while let Some(batch) = rx.recv().await {
            if batch.entries.is_empty() {
                continue;
            }
            let batch_size = batch.entries.len();
            let span = tracing::debug_span!(
                "apply_worker_batch",
                shard = batch.shard.0,
                entry_count = batch_size,
            );
            let apply_start = Instant::now();
            let apply_result = self
                .store
                .apply_committed_entries(&batch.entries, batch.leader_node)
                .instrument(span)
                .await;
            let apply_latency = apply_start.elapsed().as_secs_f64();
            match apply_result {
                Ok(responses) => {
                    metrics::record_apply_batch(
                        &self.region,
                        &self.shard,
                        "ok",
                        batch_size,
                        apply_latency,
                    );
                    let mut map = self.response_map.lock();
                    for (entry, response) in batch.entries.iter().zip(responses.into_iter()) {
                        if let Some(tx) = map.remove(&entry.index) {
                            let _ = tx.send(response);
                        } else {
                            // No waiter registered yet — store for late pickup.
                            self.spillover.lock().insert(entry.index, response);
                        }
                    }
                },
                Err(e) => {
                    metrics::record_apply_batch(
                        &self.region,
                        &self.shard,
                        "error",
                        batch_size,
                        apply_latency,
                    );
                    tracing::error!(
                        error = %e,
                        shard = batch.shard.0,
                        "Apply worker error"
                    );
                    let mut map = self.response_map.lock();
                    for entry in &batch.entries {
                        if let Some(tx) = map.remove(&entry.index) {
                            let _ = tx.send(crate::types::LedgerResponse::Error {
                                code: inferadb_ledger_types::ErrorCode::Internal,
                                message: format!("Apply failed: {e}"),
                            });
                        } else {
                            self.spillover.lock().insert(
                                entry.index,
                                crate::types::LedgerResponse::Error {
                                    code: inferadb_ledger_types::ErrorCode::Internal,
                                    message: format!("Apply failed: {e}"),
                                },
                            );
                        }
                    }
                },
            }
            // Wake the PlacementController so data region membership is reconciled
            // promptly after GLOBAL state changes (new voter, decommission, etc.).
            // Only the GLOBAL region's worker has this set; DR workers skip.
            if let Some(ref tx) = self.dr_event_tx {
                let _ = tx.send(());
            }

            // Prune stale spillover entries. No-op responses from become_leader
            // accumulate because no proposer registered for them. Keep only
            // entries within 1000 indices of the latest committed entry.
            if let Some(last_entry) = batch.entries.last() {
                let cutoff = last_entry.index.saturating_sub(1000);
                if cutoff > 0 {
                    self.spillover.lock().retain(|&index, _| index > cutoff);
                }
            }
        }
        tracing::info!("Apply worker shutting down");
    }
}
