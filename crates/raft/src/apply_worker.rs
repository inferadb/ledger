//! Decoupled apply worker for the consensus engine.
//!
//! Receives committed entry batches from the consensus reactor and applies
//! them to the state machine via [`RaftLogStore::apply_committed_entries`].

use inferadb_ledger_consensus::committed::CommittedBatch;
use inferadb_ledger_store::FileBackend;
use tokio::sync::mpsc;

use crate::{
    consensus_handle::{ResponseMap, SpilloverMap},
    log_storage::RaftLogStore,
};

/// Runs the apply loop, applying consensus engine entries to the state machine.
pub struct ApplyWorker {
    store: RaftLogStore<FileBackend>,
    response_map: ResponseMap,
    spillover: SpilloverMap,
    /// When set, fires after every committed batch is applied. Used by the
    /// GLOBAL region's apply worker to wake the DR scheduler on all nodes —
    /// ensures data region membership is updated within one apply cycle of
    /// a GLOBAL membership change, regardless of which node is the DR leader.
    dr_notify: Option<std::sync::Arc<tokio::sync::Notify>>,
}

impl ApplyWorker {
    /// Creates a new apply worker.
    ///
    /// - `store` — the Raft log store containing state layer, block archive, etc.
    /// - `response_map` — shared map for delivering responses back to proposers.
    /// - `spillover` — buffer for responses when no waiter is registered yet.
    pub fn new(
        store: RaftLogStore<FileBackend>,
        response_map: ResponseMap,
        spillover: SpilloverMap,
    ) -> Self {
        Self { store, response_map, spillover, dr_notify: None }
    }

    /// Attaches a DR membership notification for this worker. When set, the
    /// worker fires the notification after every committed batch is applied,
    /// waking the DR scheduler on this node. Only the GLOBAL region's worker
    /// should have this set — data region workers leave it `None`.
    #[must_use]
    pub fn with_dr_notify(mut self, notify: std::sync::Arc<tokio::sync::Notify>) -> Self {
        self.dr_notify = Some(notify);
        self
    }

    /// Runs the apply loop until the channel is closed (engine shutdown).
    pub async fn run(mut self, mut rx: mpsc::Receiver<CommittedBatch>) {
        while let Some(batch) = rx.recv().await {
            if batch.entries.is_empty() {
                continue;
            }
            match self.store.apply_committed_entries(&batch.entries, batch.leader_node).await {
                Ok(responses) => {
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
            // Wake the DR scheduler so data region membership is reconciled
            // promptly after GLOBAL state changes (new voter, decommission, etc.).
            // Only the GLOBAL region's worker has this set; DR workers skip.
            if let Some(ref notify) = self.dr_notify {
                notify.notify_one();
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
