//! Parallel per-vault WAL replay for newly-joined organization voters.
//!
//! ## Background
//!
//! Today, when a node is added as a voter to an organization with N vault
//! groups, every vault group independently catches up via its own
//! `InstallSnapshot` RPC: at 1000 vaults that is a 1000-snapshot storm
//! per voter add. The Phase 5 spec (M4 deliverable —
//! `docs/superpowers/specs/2026-04-27-phase-5-centralised-membership.md`)
//! replaces that storm with a single shared-WAL transfer: the new voter
//! receives the per-org WAL up to the conf-change index once, then
//! replays it locally — fanning out per-vault apply tasks in parallel
//! against the same WAL bytes.
//!
//! The per-org shared WAL already tags every frame with its
//! [`ConsensusStateId`](inferadb_ledger_consensus::types::ConsensusStateId)
//! (see `crates/consensus/src/wal/segmented.rs`); per-vault and per-org
//! groups share the same WAL within an organization (root rule 17, the
//! shared `ConsensusEngine` per `(Region, OrganizationId)` tuple). That
//! sharing is what makes a single-pass WAL scan sufficient to recover
//! every vault group on the new voter.
//!
//! ## What this module ships (M4)
//!
//! [`replay_shared_wal_for_org`] — the cancellation-aware primitive that:
//!
//! 1. Calls [`recover_from_wal`](inferadb_ledger_consensus::recovery::recover_from_wal) once across
//!    the supplied per-vault `applied_durable` map, producing one [`CommittedBatch`] per shard that
//!    needs replay.
//! 2. Routes each batch to the caller-supplied per-vault apply closure via a
//!    [`tokio::task::JoinSet`], bounded by a [`tokio::sync::Semaphore`] of capacity
//!    [`ParallelReplayConfig::max_concurrent`].
//! 3. Aggregates per-vault outcomes into [`ParallelReplayStats`] and returns the first error if any
//!    closure failed (preserving Raft's fail-fast discipline — a half-applied vault on a new voter
//!    is indistinguishable from data corruption).
//!
//! ## Migration discipline
//!
//! Per the M4 plan, this module **only adds the primitive**. It is not yet
//! wired into the production catch-up path — the new-voter bootstrap path
//! still defers to the legacy per-vault `InstallSnapshot` flow. The
//! `enable_parallel_wal_replay` flag on
//! [`crate::raft_manager::RaftManagerConfig`] gates the future wire-in
//! (default `true`, since the legacy path is the snapshot storm); flipping
//! the flag is a follow-up that requires reshaping
//! [`crate::raft_manager::RaftManager::start_vault_group`]'s bootstrap
//! seam to consume the shared-WAL replay output.
//!
//! ## Determinism
//!
//! Within a single vault, entries replay in strict log-index order
//! (preserved by [`recover_from_wal`]'s per-shard sort). Across vaults,
//! application order is deliberately unconstrained — vault groups under
//! the same org are independent state machines (root rule 17 and the
//! per-vault `RaftLogStore` invariant), so concurrent apply across
//! distinct vaults produces the same final state as serial apply.
//!
//! ## Encryption
//!
//! Snapshot encryption (root golden rule 7) is preserved by reusing the
//! existing
//! [`SnapshotCryptoError`](inferadb_ledger_consensus::snapshot_crypto::SnapshotCryptoError)
//! envelope when the shared-WAL bytes are transferred over the wire. This
//! module operates on a [`WalBackend`] reference that is **already
//! decrypted** — the caller (the future
//! [`crate::raft_manager::RaftManager::start_organization_group`]
//! bootstrap path) is responsible for decrypting the snapshot envelope
//! into a local
//! [`SegmentedWalBackend`](inferadb_ledger_consensus::wal::SegmentedWalBackend)
//! (or an
//! [`EncryptedWalBackend`](inferadb_ledger_consensus::wal::EncryptedWalBackend)
//! wrapping one) before calling into this primitive.

use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::future::BoxFuture;
use inferadb_ledger_consensus::{
    WalBackend, committed::CommittedBatch, recovery::recover_from_wal, types::ConsensusStateId,
};
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Default fan-out for [`replay_shared_wal_for_org`].
///
/// Conservative — matches the `APPLY_POOL` worker count typically used
/// for cross-shard apply parallelism. The bottleneck for replay is per-DB
/// `sync_state`, not CPU; over-parallelising serialises on disk fsync.
pub const DEFAULT_MAX_CONCURRENT_REPLAY: usize = 4;

/// One per-vault apply closure. Receives the [`CommittedBatch`] of the
/// vault's entries from the shared WAL and resolves to `Ok(())` on
/// successful apply, `Err` on any apply failure.
///
/// The closure is `FnOnce` because each vault's batch is dispatched
/// exactly once. `Send + 'static` is required for [`JoinSet::spawn`].
pub type VaultApplyFn = Box<
    dyn FnOnce(CommittedBatch) -> BoxFuture<'static, Result<u64, ParallelReplayError>>
        + Send
        + 'static,
>;

/// Configuration for [`replay_shared_wal_for_org`].
#[derive(Debug, Clone, bon::Builder)]
pub struct ParallelReplayConfig {
    /// Maximum number of vaults whose batches may apply concurrently.
    ///
    /// The fan-out is bounded by a [`Semaphore`] — entries beyond this
    /// cap wait inside the [`JoinSet`] until a permit frees up. Default
    /// [`DEFAULT_MAX_CONCURRENT_REPLAY`].
    #[builder(default = DEFAULT_MAX_CONCURRENT_REPLAY)]
    pub max_concurrent: usize,
}

impl Default for ParallelReplayConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Per-vault stats produced by a successful [`replay_shared_wal_for_org`]
/// run.
#[derive(Debug, Clone)]
pub struct ParallelReplayStats {
    /// Total wall-clock duration of the replay.
    pub duration: std::time::Duration,
    /// Number of vault groups that received at least one entry.
    pub vaults_replayed: usize,
    /// Sum of entries applied across all vaults.
    pub total_entries: u64,
    /// Per-vault entry counts, keyed by [`VaultId`]. Useful for asserting
    /// fairness and for emitting per-vault metrics from the caller.
    pub per_vault_entries: HashMap<VaultId, u64>,
    /// Highest committed index observed in the WAL checkpoint at the
    /// start of the replay. Equal to the snapshot's
    /// `last_included_index` when invoked from the new-voter bootstrap
    /// path.
    pub committed_index: u64,
}

/// Failure modes for [`replay_shared_wal_for_org`].
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParallelReplayError {
    /// Reading the WAL frames or the WAL checkpoint failed.
    #[snafu(display(
        "Shared WAL recovery failed for region {region} organization {organization}: {source}"
    ))]
    WalRecovery {
        /// Region the WAL belongs to.
        region: Region,
        /// Organization the WAL belongs to.
        organization: OrganizationId,
        /// Underlying [`inferadb_ledger_consensus::wal_backend::WalError`].
        source: inferadb_ledger_consensus::wal_backend::WalError,
        /// Captured location.
        #[snafu(implicit)]
        location: snafu::Location,
        /// Captured backtrace.
        backtrace: Backtrace,
    },

    /// The shared WAL contained a batch for a `ConsensusStateId` that the
    /// caller did not supply an apply closure for. This is treated as a
    /// hard error — silently skipping the batch would leave that vault's
    /// state divergent from the org's WAL.
    #[snafu(display(
        "Shared WAL contained a batch for unknown shard {shard:?} (region {region}, \
         organization {organization}); no apply closure was supplied for it"
    ))]
    UnknownShard {
        /// Region the WAL belongs to.
        region: Region,
        /// Organization the WAL belongs to.
        organization: OrganizationId,
        /// The shard the WAL had entries for, but no closure was registered.
        shard: ConsensusStateId,
        /// Captured backtrace.
        backtrace: Backtrace,
    },

    /// One of the per-vault apply closures returned an error. The first
    /// failure short-circuits the run; remaining in-flight tasks are
    /// cancelled via the [`JoinSet`] drop and the
    /// [`CancellationToken`].
    #[snafu(display(
        "Vault {vault} apply failed during shared-WAL replay (region {region}, \
         organization {organization}): {message}"
    ))]
    VaultApply {
        /// Region the WAL belongs to.
        region: Region,
        /// Organization the WAL belongs to.
        organization: OrganizationId,
        /// Vault whose apply closure failed.
        vault: VaultId,
        /// Stringified failure reason — apply closures live behind a
        /// trait-object boundary so we capture the rendered message.
        message: String,
        /// Captured backtrace.
        backtrace: Backtrace,
    },

    /// A spawned apply task panicked or was cancelled before completion.
    /// Distinguished from [`ParallelReplayError::VaultApply`] because the
    /// closure never returned a `Result`.
    #[snafu(display(
        "Vault apply task joined with error during shared-WAL replay (region {region}, \
         organization {organization}): {message}"
    ))]
    JoinError {
        /// Region the WAL belongs to.
        region: Region,
        /// Organization the WAL belongs to.
        organization: OrganizationId,
        /// Stringified [`tokio::task::JoinError`].
        message: String,
        /// Captured backtrace.
        backtrace: Backtrace,
    },

    /// The cancellation token fired before the replay completed. Pending
    /// tasks were cancelled. The caller should treat this as a clean
    /// shutdown signal — the partial replay is safe to discard because
    /// the new voter has not yet served traffic, and the caller will
    /// re-drive the catch-up on next start.
    #[snafu(display(
        "Shared-WAL replay cancelled before completion (region {region}, \
         organization {organization}, vaults completed: {vaults_completed})"
    ))]
    Cancelled {
        /// Region the WAL belongs to.
        region: Region,
        /// Organization the WAL belongs to.
        organization: OrganizationId,
        /// How many vault apply tasks completed before cancellation.
        vaults_completed: usize,
        /// Captured backtrace.
        backtrace: Backtrace,
    },
}

/// Replays the shared per-organization WAL for every vault group the
/// caller supplies an apply closure for.
///
/// See the module-level docs for the design and the migration discipline.
///
/// # Arguments
///
/// * `wal` — the **decrypted** per-org WAL backend. Must already contain the entries up to and
///   including the conf-change index that admitted the new voter; the caller is responsible for
///   landing the WAL bytes (typically by replicating the org-level Raft log up to the conf-change
///   index) before calling this primitive.
/// * `region` / `organization_id` — for log fields and error context.
/// * `applied_durable` — per-shard `applied_durable` baseline. Entries at or before the per-shard
///   value are skipped (already applied). For a fresh voter this map is `0` for every vault.
/// * `apply_fns` — one closure per vault (keyed by both [`VaultId`] for diagnostics and
///   [`ConsensusStateId`] for routing). Each closure receives the [`CommittedBatch`] of its vault's
///   entries and resolves to the count of entries applied.
/// * `config` — fan-out and concurrency bounds.
/// * `cancel` — cancellation token. When fired before completion the function returns
///   [`ParallelReplayError::Cancelled`] and any in-flight apply tasks are aborted via [`JoinSet`]
///   drop semantics (they observe the cancellation through their own future).
///
/// # Determinism
///
/// Within a vault, entries apply in strict log-index order. Across
/// vaults, apply order is unconstrained but the final per-vault states
/// are independent (root rule 17), so the cross-vault interleaving does
/// not affect the resulting state machine.
///
/// # Errors
///
/// Returns the first error encountered:
/// * [`ParallelReplayError::WalRecovery`] for [`recover_from_wal`] failures.
/// * [`ParallelReplayError::UnknownShard`] when the WAL contains a batch for a shard the caller did
///   not register.
/// * [`ParallelReplayError::VaultApply`] when an apply closure returns an error.
/// * [`ParallelReplayError::JoinError`] when an apply task panics.
/// * [`ParallelReplayError::Cancelled`] when `cancel` fires.
#[allow(clippy::too_many_arguments)]
pub async fn replay_shared_wal_for_org<W: WalBackend>(
    wal: &W,
    region: Region,
    organization_id: OrganizationId,
    applied_durable: HashMap<ConsensusStateId, u64>,
    apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)>,
    config: ParallelReplayConfig,
    cancel: CancellationToken,
) -> Result<ParallelReplayStats, ParallelReplayError> {
    let start = Instant::now();

    // ── 1. Single WAL scan -------------------------------------------------
    //
    // `recover_from_wal` is shared with the per-org `replay_crash_gap`
    // path — same primitive, different consumer. It returns one
    // `CommittedBatch` per shard that has entries in
    // `(applied_durable, committed_index]`.
    let recovery = recover_from_wal(wal, &applied_durable)
        .context(WalRecoverySnafu { region, organization: organization_id })?;

    let committed_index = recovery.committed_index;

    info!(
        region = region.as_str(),
        organization_id = organization_id.value(),
        committed_index,
        replay_count = recovery.replay_count,
        batches = recovery.entries_to_replay.len(),
        "replay_shared_wal_for_org: recovered batches from shared WAL",
    );

    // Fast-path: nothing to apply. Either the WAL is empty (fresh
    // bootstrap with no committed entries) or every vault is already
    // up-to-date.
    if recovery.replay_count == 0 || recovery.entries_to_replay.is_empty() {
        return Ok(ParallelReplayStats {
            duration: start.elapsed(),
            vaults_replayed: 0,
            total_entries: 0,
            per_vault_entries: HashMap::new(),
            committed_index,
        });
    }

    // ── 2. Validate routing ------------------------------------------------
    //
    // Every batch must map to a registered apply closure. A batch
    // without one would leave that vault divergent — caught here as a
    // hard error so the caller realises its closure map is incomplete
    // before any apply runs.
    //
    // Per-org control-plane batches (the `(region, OrganizationId(0))`
    // shard, when this primitive is called against the per-org WAL)
    // are NOT a vault and are filtered upstream by the caller — the
    // map only contains vault `ConsensusStateId`s. Anything else here
    // is a bug.
    let mut apply_fns = apply_fns;
    let mut routed: Vec<(ConsensusStateId, VaultId, VaultApplyFn, CommittedBatch)> =
        Vec::with_capacity(recovery.entries_to_replay.len());

    for batch in recovery.entries_to_replay {
        if batch.entries.is_empty() {
            continue;
        }
        match apply_fns.remove(&batch.shard) {
            Some((vault_id, apply_fn)) => routed.push((batch.shard, vault_id, apply_fn, batch)),
            None => {
                return UnknownShardSnafu {
                    region,
                    organization: organization_id,
                    shard: batch.shard,
                }
                .fail();
            },
        }
    }

    // Empty after filtering empty batches — no per-vault work to do.
    if routed.is_empty() {
        return Ok(ParallelReplayStats {
            duration: start.elapsed(),
            vaults_replayed: 0,
            total_entries: 0,
            per_vault_entries: HashMap::new(),
            committed_index,
        });
    }

    // ── 3. Bounded fan-out -------------------------------------------------
    //
    // `JoinSet` owns the spawned tasks; dropping it aborts every
    // in-flight task. We also pass the cancellation token into each
    // closure (forwarded via `tokio::select!` below) so the closure can
    // unwind cooperatively.
    let max_concurrent = config.max_concurrent.max(1);
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut join_set: JoinSet<Result<(VaultId, u64), ParallelReplayError>> = JoinSet::new();

    debug!(
        region = region.as_str(),
        organization_id = organization_id.value(),
        max_concurrent,
        vault_count = routed.len(),
        "replay_shared_wal_for_org: fanning out per-vault apply",
    );

    for (_shard_id, vault_id, apply_fn, batch) in routed {
        let permit_sem = Arc::clone(&semaphore);
        let task_cancel = cancel.clone();
        let task_region = region;
        let task_org = organization_id;

        join_set.spawn(async move {
            // Race the permit acquire against cancellation so we don't
            // park indefinitely on a permit that will never come.
            let permit = tokio::select! {
                biased;
                () = task_cancel.cancelled() => {
                    return CancelledSnafu {
                        region: task_region,
                        organization: task_org,
                        vaults_completed: 0_usize,
                    }
                    .fail();
                }
                permit = permit_sem.acquire_owned() => match permit {
                    Ok(p) => p,
                    Err(_) => {
                        // Semaphore closed — treat as cancellation.
                        return CancelledSnafu {
                            region: task_region,
                            organization: task_org,
                            vaults_completed: 0_usize,
                        }
                        .fail();
                    }
                },
            };

            // Apply this vault's batch. Race the future against the
            // cancellation token so a parent-shutdown cancels the
            // closure cooperatively.
            let apply_future = apply_fn(batch);
            let result = tokio::select! {
                biased;
                () = task_cancel.cancelled() => {
                    drop(permit);
                    return CancelledSnafu {
                        region: task_region,
                        organization: task_org,
                        vaults_completed: 0_usize,
                    }
                    .fail();
                }
                r = apply_future => r,
            };

            drop(permit);

            match result {
                Ok(applied) => Ok((vault_id, applied)),
                Err(e) => Err(e),
            }
        });
    }

    // ── 4. Aggregate -------------------------------------------------------
    //
    // Drain the JoinSet. First failure short-circuits and aborts the
    // remaining tasks via `join_set` drop. Cancellation observed on the
    // outer token also short-circuits.
    let mut per_vault_entries: HashMap<VaultId, u64> = HashMap::new();
    let mut total_entries: u64 = 0;
    let mut vaults_completed: usize = 0;

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                // Drop the JoinSet to abort remaining tasks.
                join_set.shutdown().await;
                return CancelledSnafu {
                    region,
                    organization: organization_id,
                    vaults_completed,
                }
                .fail();
            }
            res = join_set.join_next() => {
                match res {
                    None => break, // All tasks completed.
                    Some(Ok(Ok((vault_id, applied)))) => {
                        per_vault_entries.insert(vault_id, applied);
                        total_entries = total_entries.saturating_add(applied);
                        vaults_completed += 1;
                    }
                    Some(Ok(Err(e))) => {
                        // Apply closure (or per-task cancellation)
                        // failure — short-circuit and abort the rest.
                        join_set.shutdown().await;
                        return Err(e);
                    }
                    Some(Err(join_err)) => {
                        join_set.shutdown().await;
                        // Preserve panic vs. cancel distinction in the
                        // message for operator triage.
                        let kind = if join_err.is_cancelled() {
                            "cancelled"
                        } else {
                            "panicked"
                        };
                        warn!(
                            region = region.as_str(),
                            organization_id = organization_id.value(),
                            kind,
                            error = %join_err,
                            "replay_shared_wal_for_org: vault apply task did not complete",
                        );
                        return JoinSnafu {
                            region,
                            organization: organization_id,
                            message: format!("{kind}: {join_err}"),
                        }
                        .fail();
                    }
                }
            }
        }
    }

    let stats = ParallelReplayStats {
        duration: start.elapsed(),
        vaults_replayed: vaults_completed,
        total_entries,
        per_vault_entries,
        committed_index,
    };

    info!(
        region = region.as_str(),
        organization_id = organization_id.value(),
        vaults_replayed = stats.vaults_replayed,
        total_entries = stats.total_entries,
        committed_index = stats.committed_index,
        elapsed_ms = stats.duration.as_millis(),
        "replay_shared_wal_for_org: complete",
    );

    Ok(stats)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::Duration,
    };

    use inferadb_ledger_consensus::{
        wal::InMemoryWalBackend,
        wal_backend::{CheckpointFrame, WalFrame},
    };

    use super::*;

    /// Small helper — synthesise a vault `ConsensusStateId` from a vault
    /// id. Mirrors how `start_vault_group` derives the shard id, but
    /// inlined so the test does not need access to the seahash key.
    fn vault_shard(vault: i64) -> ConsensusStateId {
        ConsensusStateId(vault as u64 + 1000) // arbitrary deterministic mapping
    }

    fn make_frame(shard: ConsensusStateId, index: u64, term: u64, payload: &[u8]) -> WalFrame {
        WalFrame { shard_id: shard, index, term, data: Arc::from(payload) }
    }

    fn checkpoint(committed_index: u64, term: u64) -> CheckpointFrame {
        CheckpointFrame { committed_index, term, voted_for: None }
    }

    /// Build an in-memory shared WAL containing entries for `vault_count`
    /// vaults, `entries_per_vault` each, then write a checkpoint covering
    /// all of them.
    fn build_shared_wal(vault_count: i64, entries_per_vault: u64) -> InMemoryWalBackend {
        let mut wal = InMemoryWalBackend::new();
        let mut next_index = 1u64;
        for vault in 1..=vault_count {
            let shard = vault_shard(vault);
            let mut frames = Vec::with_capacity(entries_per_vault as usize);
            for _ in 0..entries_per_vault {
                frames.push(make_frame(shard, next_index, 1, &next_index.to_le_bytes()));
                next_index += 1;
            }
            wal.append(&frames).unwrap();
        }
        // committed_index = total entries written.
        let total = next_index - 1;
        wal.write_checkpoint(&checkpoint(total, 1)).unwrap();
        wal.sync().unwrap();
        wal
    }

    /// Counting closure factory — returns an apply closure that
    /// records the count of entries it received and the order it
    /// completed in.
    fn counting_apply_fn(
        counter: Arc<AtomicU64>,
        completion_index: Arc<AtomicUsize>,
        record_completion_at: Arc<parking_lot::Mutex<Vec<usize>>>,
        delay: Option<Duration>,
    ) -> VaultApplyFn {
        Box::new(move |batch: CommittedBatch| {
            let counter = counter.clone();
            let completion_index = completion_index.clone();
            let record_completion_at = record_completion_at.clone();
            Box::pin(async move {
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                let n = batch.entries.len() as u64;
                counter.fetch_add(n, Ordering::SeqCst);
                let order = completion_index.fetch_add(1, Ordering::SeqCst);
                record_completion_at.lock().push(order);
                Ok(n)
            })
        })
    }

    #[tokio::test]
    async fn empty_wal_returns_zero_stats() {
        let wal = InMemoryWalBackend::new();
        let stats = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(42),
            HashMap::new(),
            HashMap::new(),
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(stats.vaults_replayed, 0);
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.committed_index, 0);
        assert!(stats.per_vault_entries.is_empty());
    }

    #[tokio::test]
    async fn fans_out_per_vault_apply() {
        // 3 vaults, 5 entries each.
        let wal = build_shared_wal(3, 5);

        let counter1 = Arc::new(AtomicU64::new(0));
        let counter2 = Arc::new(AtomicU64::new(0));
        let counter3 = Arc::new(AtomicU64::new(0));
        let completion_index = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        apply_fns.insert(
            vault_shard(1),
            (
                VaultId::new(1),
                counting_apply_fn(counter1.clone(), completion_index.clone(), order.clone(), None),
            ),
        );
        apply_fns.insert(
            vault_shard(2),
            (
                VaultId::new(2),
                counting_apply_fn(counter2.clone(), completion_index.clone(), order.clone(), None),
            ),
        );
        apply_fns.insert(
            vault_shard(3),
            (
                VaultId::new(3),
                counting_apply_fn(counter3.clone(), completion_index.clone(), order.clone(), None),
            ),
        );

        let stats = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(stats.vaults_replayed, 3);
        assert_eq!(stats.total_entries, 15);
        assert_eq!(stats.committed_index, 15);
        assert_eq!(stats.per_vault_entries.get(&VaultId::new(1)), Some(&5));
        assert_eq!(stats.per_vault_entries.get(&VaultId::new(2)), Some(&5));
        assert_eq!(stats.per_vault_entries.get(&VaultId::new(3)), Some(&5));

        assert_eq!(counter1.load(Ordering::SeqCst), 5);
        assert_eq!(counter2.load(Ordering::SeqCst), 5);
        assert_eq!(counter3.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn unknown_shard_in_wal_returns_error() {
        // 2 vaults in the WAL, but only 1 closure registered.
        let wal = build_shared_wal(2, 3);

        let counter = Arc::new(AtomicU64::new(0));
        let completion_index = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        apply_fns.insert(
            vault_shard(1),
            (VaultId::new(1), counting_apply_fn(counter, completion_index, order, None)),
        );

        let err = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ParallelReplayError::UnknownShard { .. }));
    }

    #[tokio::test]
    async fn applied_durable_skips_already_applied_entries() {
        // 2 vaults, 5 entries each. Mark vault 1 as applied through index 3
        // (the WAL has indices 1..=5 for vault 1, indices 6..=10 for vault 2).
        let wal = build_shared_wal(2, 5);

        let counter1 = Arc::new(AtomicU64::new(0));
        let counter2 = Arc::new(AtomicU64::new(0));
        let completion_index = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        apply_fns.insert(
            vault_shard(1),
            (
                VaultId::new(1),
                counting_apply_fn(counter1.clone(), completion_index.clone(), order.clone(), None),
            ),
        );
        apply_fns.insert(
            vault_shard(2),
            (
                VaultId::new(2),
                counting_apply_fn(counter2.clone(), completion_index.clone(), order.clone(), None),
            ),
        );

        let mut applied_durable = HashMap::new();
        applied_durable.insert(vault_shard(1), 3); // skip indices 1..=3 for vault 1

        let stats = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            applied_durable,
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // Vault 1 should have replayed only indices 4 and 5 (2 entries).
        // Vault 2 should have replayed all 5.
        assert_eq!(counter1.load(Ordering::SeqCst), 2);
        assert_eq!(counter2.load(Ordering::SeqCst), 5);
        assert_eq!(stats.total_entries, 7);
    }

    #[tokio::test]
    async fn cancellation_short_circuits() {
        // 4 vaults, 3 entries each, with a slow apply closure to give the
        // cancellation a chance to fire mid-flight.
        let wal = build_shared_wal(4, 3);

        let cancel = CancellationToken::new();
        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        for v in 1..=4i64 {
            let counter = Arc::new(AtomicU64::new(0));
            let completion_index = Arc::new(AtomicUsize::new(0));
            let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
            apply_fns.insert(
                vault_shard(v),
                (
                    VaultId::new(v),
                    counting_apply_fn(
                        counter,
                        completion_index,
                        order,
                        Some(Duration::from_millis(500)),
                    ),
                ),
            );
        }

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let err = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::builder().max_concurrent(1).build(),
            cancel,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, ParallelReplayError::Cancelled { .. }),
            "expected Cancelled, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn vault_apply_failure_short_circuits() {
        let wal = build_shared_wal(2, 2);

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        // Vault 1: succeeds.
        let counter1 = Arc::new(AtomicU64::new(0));
        let completion_index = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        apply_fns.insert(
            vault_shard(1),
            (VaultId::new(1), counting_apply_fn(counter1, completion_index, order, None)),
        );
        // Vault 2: fails immediately.
        let failing_apply: VaultApplyFn = Box::new(|_batch: CommittedBatch| {
            Box::pin(async move {
                VaultApplySnafu {
                    region: Region::US_EAST_VA,
                    organization: OrganizationId::new(7),
                    vault: VaultId::new(2),
                    message: "synthetic apply failure".to_string(),
                }
                .fail()
            })
        });
        apply_fns.insert(vault_shard(2), (VaultId::new(2), failing_apply));

        let err = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

        match err {
            ParallelReplayError::VaultApply { vault, .. } => assert_eq!(vault, VaultId::new(2)),
            other => panic!("expected VaultApply failure, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn fan_out_respects_max_concurrent_cap() {
        // 8 vaults, slow apply, max_concurrent=2. Track the maximum number
        // of in-flight apply tasks; assert it never exceeds 2.
        let wal = build_shared_wal(8, 1);

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        for v in 1..=8i64 {
            let in_flight = in_flight.clone();
            let max_observed = max_observed.clone();
            let f: VaultApplyFn = Box::new(move |batch: CommittedBatch| {
                let in_flight = in_flight.clone();
                let max_observed = max_observed.clone();
                Box::pin(async move {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    let prev_max = max_observed.load(Ordering::SeqCst);
                    if now > prev_max {
                        max_observed.store(now, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(batch.entries.len() as u64)
                })
            });
            apply_fns.insert(vault_shard(v), (VaultId::new(v), f));
        }

        let stats = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::builder().max_concurrent(2).build(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(stats.vaults_replayed, 8);
        assert!(
            max_observed.load(Ordering::SeqCst) <= 2,
            "expected at most 2 in-flight; observed {}",
            max_observed.load(Ordering::SeqCst),
        );
    }

    /// End-to-end check that the primitive composes with
    /// [`EncryptedWalBackend`] (root rule 7, "Snapshots are encrypted
    /// end-to-end"). The encrypted backend transparently decrypts
    /// frames in `read_frames`, so the replay primitive does not have
    /// to know about the envelope — but we exercise the composition to
    /// catch a regression where the encryption layer changes its
    /// payload framing in a way that breaks `recover_from_wal`'s shard
    /// routing.
    #[tokio::test]
    async fn replays_against_encrypted_wal_backend() {
        use inferadb_ledger_consensus::{crypto::InMemoryKeyProvider, wal::EncryptedWalBackend};

        // Register a DEK for each vault we'll use, plus the
        // checkpoint sentinel shard id (`u64::MAX`) — checkpoint
        // frames flow through the same encrypted append path as
        // normal frames.
        let kp = Arc::new(InMemoryKeyProvider::new());
        for v in 1..=3i64 {
            let shard = vault_shard(v);
            kp.set_key(shard.0, 0, [v as u8 + 0x10; 32]);
        }
        kp.set_key(inferadb_ledger_consensus::wal_backend::CHECKPOINT_SHARD_ID.0, 0, [0xCC; 32]);

        // Construct an encrypted WAL on top of the in-memory backend.
        let inner = InMemoryWalBackend::new();
        let mut encrypted = EncryptedWalBackend::new(inner, Arc::clone(&kp));

        // Append entries for 3 vaults, then write a covering checkpoint.
        let mut next_index = 1u64;
        for vault in 1..=3i64 {
            let shard = vault_shard(vault);
            let mut frames = Vec::new();
            for _ in 0..4 {
                frames.push(make_frame(shard, next_index, 1, &next_index.to_le_bytes()));
                next_index += 1;
            }
            encrypted.append(&frames).unwrap();
        }
        encrypted.write_checkpoint(&checkpoint(next_index - 1, 1)).unwrap();
        encrypted.sync().unwrap();

        // Counters to verify each closure receives the right entry count.
        let counters: Vec<Arc<AtomicU64>> = (0..3).map(|_| Arc::new(AtomicU64::new(0))).collect();
        let completion_index = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        for v in 1..=3i64 {
            apply_fns.insert(
                vault_shard(v),
                (
                    VaultId::new(v),
                    counting_apply_fn(
                        Arc::clone(&counters[(v - 1) as usize]),
                        completion_index.clone(),
                        order.clone(),
                        None,
                    ),
                ),
            );
        }

        let stats = replay_shared_wal_for_org(
            &encrypted,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(stats.vaults_replayed, 3);
        assert_eq!(stats.total_entries, 12);
        for c in &counters {
            assert_eq!(c.load(Ordering::SeqCst), 4);
        }
    }

    /// Stress test — 100 vaults, 10 entries each, parallel apply.
    /// The apply closure does no real work so this benchmarks the
    /// fan-out plumbing itself (semaphore, JoinSet, channel-free
    /// dispatch). On loopback the convergence target is sub-30s; this
    /// in-memory variant runs in under a second.
    #[tokio::test]
    async fn stress_100_vault_replay_under_target() {
        let vault_count: i64 = 100;
        let entries_per_vault: u64 = 10;
        let wal = build_shared_wal(vault_count, entries_per_vault);

        let apply_count = Arc::new(AtomicU64::new(0));

        let mut apply_fns: HashMap<ConsensusStateId, (VaultId, VaultApplyFn)> = HashMap::new();
        for v in 1..=vault_count {
            let apply_count = apply_count.clone();
            let f: VaultApplyFn = Box::new(move |batch: CommittedBatch| {
                let apply_count = apply_count.clone();
                Box::pin(async move {
                    let n = batch.entries.len() as u64;
                    apply_count.fetch_add(n, Ordering::SeqCst);
                    Ok(n)
                })
            });
            apply_fns.insert(vault_shard(v), (VaultId::new(v), f));
        }

        let started = Instant::now();
        let stats = replay_shared_wal_for_org(
            &wal,
            Region::US_EAST_VA,
            OrganizationId::new(7),
            HashMap::new(),
            apply_fns,
            ParallelReplayConfig::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();

        assert_eq!(stats.vaults_replayed, vault_count as usize);
        assert_eq!(stats.total_entries, (vault_count as u64) * entries_per_vault,);
        assert_eq!(apply_count.load(Ordering::SeqCst), stats.total_entries);
        // The integration-level target is < 30s on loopback; this is the
        // primitive itself with zero apply work, so we set a tight bound
        // to catch performance regressions in the fan-out plumbing.
        assert!(
            elapsed < Duration::from_secs(5),
            "100-vault primitive took {}ms (>5s)",
            elapsed.as_millis(),
        );
    }
}
