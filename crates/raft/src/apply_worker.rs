//! Decoupled apply workers — one per Raft tier.
//!
//! Each tier in the B.1 three-tier model has its own typed apply pipeline:
//!
//! - [`SystemApplyWorker`] applies [`SystemRequest`](crate::types::SystemRequest) committed entries
//!   against the `(GLOBAL, 0)` [`OrganizationGroup`](crate::raft_manager::OrganizationGroup)
//!   (the cluster control-plane group until B.1.6 ships a distinct SystemGroup storage path).
//! - [`RegionApplyWorker`] applies [`RegionRequest`](crate::types::RegionRequest) committed entries
//!   against per-region `(region, 0)` [`OrganizationGroup`](crate::raft_manager::OrganizationGroup)s
//!   (the regional control-plane group until B.1.6 ships a distinct RegionGroup storage path).
//! - [`OrganizationApplyWorker`] applies [`OrganizationRequest`](crate::types::OrganizationRequest)
//!   committed entries against a per-organization
//!   [`OrganizationGroup`](crate::raft_manager::OrganizationGroup) via
//!   [`RaftLogStore::apply_committed_entries`].
//!
//! Tier discipline: each apply worker is typed to its tier's request enum
//! via the generic [`ApplyWorker<R>`] parameter. Cross-tier misrouting is a
//! compile error — `SystemApplyWorker` cannot be handed an
//! `OrganizationRequest`-bearing batch because the decode type is fixed at
//! construction.

use std::{marker::PhantomData, time::Instant};

use inferadb_ledger_consensus::committed::CommittedBatch;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::OrganizationId;
use tokio::sync::{mpsc, oneshot};

use crate::{
    consensus_handle::{ResponseMap, SpilloverMap},
    log_storage::{RaftLogStore, operations::ApplyableRequest},
    metrics,
};

/// Cluster control-plane apply worker, typed to `SystemRequest`.
pub type SystemApplyWorker = ApplyWorker<crate::types::SystemRequest>;

/// Regional control-plane apply worker (one per data region), typed to
/// `RegionRequest`.
pub type RegionApplyWorker = ApplyWorker<crate::types::RegionRequest>;

/// Organization data-plane apply worker (one per organization, per region),
/// typed to `OrganizationRequest`.
pub type OrganizationApplyWorker = ApplyWorker<crate::types::OrganizationRequest>;

/// Generic apply worker parameterised by the tier-specific request type `R`.
///
/// Receives committed entry batches from the consensus reactor and applies
/// them to the tier-specific state machine via
/// [`RaftLogStore::apply_committed_entries::<R>`]. The `R` type parameter
/// enforces compile-time tier discipline: the wire decoder, the apply
/// dispatch, and the response fan-out all share the same `R` — there is
/// no runtime branching on a shared wrapper enum.
///
/// Construction picks the right `R`: `(GLOBAL, 0)` gets
/// `ApplyWorker::<SystemRequest>`; `(region, 0)` for region != GLOBAL gets
/// `ApplyWorker::<SystemRequest>` (B.1 transitional — RegionRequest routing
/// pending B.1.6); `(region, org_id > 0)` gets
/// `ApplyWorker::<OrganizationRequest>`.
pub struct ApplyWorker<R: ApplyableRequest> {
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
    /// ConsensusState label for apply-batch metrics, pre-stringified from a
    /// [`OrganizationId`].
    shard: String,
    _marker: PhantomData<R>,
}

impl<R: ApplyableRequest> ApplyWorker<R> {
    /// Creates a new apply worker typed to `R`.
    pub fn new(
        store: RaftLogStore<FileBackend>,
        response_map: ResponseMap,
        spillover: SpilloverMap,
        region: impl Into<String>,
        organization_id: OrganizationId,
    ) -> Self {
        Self {
            store,
            response_map,
            spillover,
            dr_event_tx: None,
            region: region.into(),
            shard: organization_id.value().to_string(),
            _marker: PhantomData,
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
                .apply_committed_entries::<R>(&batch.entries, batch.leader_node)
                .instrument(span)
                .await;
            let apply_latency = apply_start.elapsed().as_secs_f64();
            // Response fan-out: partition (entry, response) pairs into
            // waiter-registered (send now) and unregistered (spillover
            // insert). Both locks are acquired briefly: `response_map`
            // only for the take phase, `spillover` only for the insert
            // phase. Channel sends happen lock-free between the two
            // phases so a blocked proposer racing to register a waiter
            // never contends with the apply loop.
            let status_label = if apply_result.is_ok() { "ok" } else { "error" };
            metrics::record_apply_batch(
                &self.region,
                &self.shard,
                status_label,
                batch_size,
                apply_latency,
            );

            let responses: Vec<crate::types::LedgerResponse> = match apply_result {
                Ok(responses) => responses,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        shard = batch.shard.0,
                        "Apply worker error"
                    );
                    let err = crate::types::LedgerResponse::Error {
                        code: inferadb_ledger_types::ErrorCode::Internal,
                        message: format!("Apply failed: {e}"),
                    };
                    batch.entries.iter().map(|_| err.clone()).collect()
                },
            };

            // Phase 1: remove waiters under a short `response_map` lock.
            let mut to_send: Vec<(oneshot::Sender<crate::types::LedgerResponse>, crate::types::LedgerResponse)> =
                Vec::with_capacity(batch_size);
            let mut to_spillover: Vec<(u64, crate::types::LedgerResponse)> =
                Vec::with_capacity(batch_size);
            {
                let mut map = self.response_map.lock();
                for (entry, response) in batch.entries.iter().zip(responses.into_iter()) {
                    match map.remove(&entry.index) {
                        Some(tx) => to_send.push((tx, response)),
                        None => to_spillover.push((entry.index, response)),
                    }
                }
            }

            // Phase 2: lock-free delivery. Channel sends don't need any
            // crate-internal lock, so proposers racing to register can
            // acquire `response_map` here without contention.
            for (tx, response) in to_send {
                let _ = tx.send(response);
            }

            // Phase 3: batch-insert spillover under a single `spillover` lock.
            if !to_spillover.is_empty() {
                let mut spillover = self.spillover.lock();
                for (index, response) in to_spillover {
                    spillover.insert(index, response);
                }
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
