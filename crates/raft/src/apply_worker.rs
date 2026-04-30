//! Typed apply workers — one per Raft tier.
//!
//! Each tier has its own apply pipeline, parameterised by its request enum:
//!
//! - [`SystemApplyWorker`] applies [`SystemRequest`](crate::types::SystemRequest) entries for the
//!   cluster control-plane group (`GLOBAL, 0`).
//! - [`RegionApplyWorker`] applies [`RegionRequest`](crate::types::RegionRequest) entries for
//!   per-region control-plane groups (`(region, 0)`).
//! - [`OrganizationApplyWorker`] applies [`OrganizationRequest`](crate::types::OrganizationRequest)
//!   entries for per-organization data-plane groups (`(region, org_id > 0)`) via
//!   [`log_storage::RaftLogStore::apply_committed_entries`](crate::log_storage::RaftLogStore).
//!
//! Tier discipline: cross-tier misrouting is a compile error. `SystemApplyWorker` cannot
//! receive an `OrganizationRequest`-bearing batch because the decode type is fixed at
//! construction by the [`ApplyWorker<R>`] type parameter.

use std::{marker::PhantomData, time::Instant};

use inferadb_ledger_consensus::committed::CommittedBatch;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::OrganizationId;
use tokio::sync::{mpsc, oneshot};

use crate::{
    apply_command::ApplyCommand,
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
/// `RaftLogStore::apply_committed_entries::<R>`. The `R` type parameter
/// enforces compile-time tier discipline: the wire decoder, the apply
/// dispatch, and the response fan-out all share the same `R` — there is
/// no runtime branching on a shared wrapper enum.
///
/// Use the typed aliases instead of constructing this directly:
/// - [`SystemApplyWorker`] for the cluster control-plane group `(GLOBAL, 0)`
/// - [`RegionApplyWorker`] for a regional control-plane group `(region, 0)`
/// - [`OrganizationApplyWorker`] for a per-organization group `(region, org_id > 0)`
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

    /// Runs the apply loop until the commit channel is closed (engine
    /// shutdown).
    ///
    /// `ctrl_rx` is the per-shard out-of-band [`ApplyCommand`] channel drained alongside `rx`
    /// via `tokio::select!`. [`crate::snapshot_installer`]'s `RaftManagerSnapshotInstaller`
    /// emits [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// here so the install happens on the same task that owns the `RaftLogStore`, preventing
    /// any race between an in-flight batch apply and a snapshot install.
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<CommittedBatch>,
        mut ctrl_rx: crate::apply_command::ApplyCommandReceiver,
    ) {
        use tracing::Instrument;
        loop {
            let batch = tokio::select! {
                biased;
                cmd = ctrl_rx.recv() => {
                    match cmd {
                        Some(ApplyCommand::InstallSnapshot {
                            meta,
                            plaintext,
                            completion,
                            _backend: _,
                        }) => {
                            // Install the snapshot synchronously on the apply
                            // task. Concurrent batch apply during this window
                            // is impossible because we own `&mut self` and
                            // the select arm runs to completion before we
                            // poll `rx` again.
                            let install_result =
                                self.store.install_snapshot(&meta, plaintext).await;
                            // The completion channel may be closed if the
                            // installer task was cancelled while we held the
                            // command — drop silently in that case.
                            let _ = completion.send(install_result);
                            continue;
                        },
                        None => {
                            // Control channel closed (manager dropped).
                            // Continue draining the commit channel until it
                            // closes too, but silently ignore further
                            // control commands (the recv() returns None
                            // immediately on subsequent polls).
                            continue;
                        },
                    }
                },
                maybe_batch = rx.recv() => {
                    match maybe_batch {
                        Some(b) => b,
                        None => break,
                    }
                },
            };
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
            // Record per-vault and org-rollup throughput metrics.
            // `org_apply_throughput_ops_total` is always-on; per-vault apply latency
            // is gated on `metrics::vault_metrics_enabled()`. `vault_id` is `Some`
            // only for vault-tier Raft groups; org-scoped groups emit the rollup with `None`.
            metrics::record_vault_apply_batch(
                &self.region,
                &self.shard,
                self.store.vault_id(),
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

            // Response fan-out: remove waiters from the map, deliver lock-free,
            // then batch-insert any spillover. The lock-free send dominates
            // unless contention is pathological; map locks are only held for
            // short critical sections between sends.
            let fanout_start = Instant::now();
            let mut to_send: Vec<(
                oneshot::Sender<crate::types::LedgerResponse>,
                crate::types::LedgerResponse,
            )> = Vec::with_capacity(batch_size);
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

            // Lock-free delivery — no crate-internal lock needed, so proposers racing to
            // register can acquire `response_map` without contention.
            for (tx, response) in to_send {
                let _ = tx.send(response);
            }

            // Batch-insert spillover under a single `spillover` lock.
            if !to_spillover.is_empty() {
                let mut spillover = self.spillover.lock();
                for (index, response) in to_spillover {
                    spillover.insert(index, response);
                }
            }
            metrics::record_apply_phase(
                &self.region,
                &self.shard,
                metrics::ApplyPhase::ResponseFanout,
                fanout_start.elapsed().as_secs_f64(),
            );
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
