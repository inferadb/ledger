//! Out-of-band commands routed alongside committed batches into the apply
//! pipeline.
//!
//! Stage 4 of the snapshot install path. The apply worker (org-level via
//! [`ApplyWorker::run`](crate::apply_worker::ApplyWorker::run) and per-vault
//! via the inlined commit pump in
//! [`RaftManager::start_vault_group`](crate::raft_manager::RaftManager::start_vault_group))
//! drains a typed [`ApplyCommand`] channel via `tokio::select!` alongside
//! its `mpsc::Receiver<CommittedBatch>`. The ordering guarantee is strict:
//! a command observed in a select round runs to completion before the next
//! batch is processed, preventing any race between an in-flight batch
//! apply and a snapshot install.
//!
//! ## Why a separate channel?
//!
//! The reactor's commit channel carries only `CommittedBatch` values
//! (`inferadb_ledger_consensus::committed::CommittedBatch`), keyed by
//! shard. Repurposing it to carry control messages would require
//! widening `CommittedBatch` (cascading through the
//! [`CommitDispatcher`](crate::commit_dispatcher) fan-out and every
//! downstream consumer) and would leak Stage 4 concepts into the consensus
//! crate. The dedicated control channel keeps the consensus-crate boundary
//! clean: `RaftManagerSnapshotInstaller` (Stage 4) emits the command into a
//! per-shard `ApplyCommand` channel that lives entirely in the raft crate.
//!
//! ## Receiver contract
//!
//! - Acquire the command, run it to completion, then return to the `tokio::select!` loop.
//!   Concurrent batch apply during command execution is not safe — the snapshot install replaces
//!   the state-machine wholesale, and an interleaved batch apply would corrupt `applied_state` /
//!   `region_chain`.
//! - Send the result to the `completion` oneshot. The originating
//!   installer awaits this oneshot; on success it advances the shard's
//!   `last_applied` via
//!   [`ConsensusEngine::notify_snapshot_installed`](inferadb_ledger_consensus::ConsensusEngine::notify_snapshot_installed).
//! - On a closed `completion` channel (e.g. the installer task has been cancelled), drop the
//!   command silently — the leader's heartbeat replicator re-emits `Action::SendSnapshot` on the
//!   next cycle.

use inferadb_ledger_store::FileBackend;
use tokio::sync::oneshot;

use crate::log_storage::{SnapshotMeta, StoreError};

/// Channel sender for [`ApplyCommand`] values, paired with an
/// `mpsc::Receiver<ApplyCommand>` drained by the apply worker.
pub type ApplyCommandSender = tokio::sync::mpsc::Sender<ApplyCommand>;

/// Channel receiver for [`ApplyCommand`] values. The apply worker holds
/// this and drains it via `tokio::select!` alongside its committed-batch
/// channel.
pub type ApplyCommandReceiver = tokio::sync::mpsc::Receiver<ApplyCommand>;

/// Capacity of the per-shard apply-command channel.
///
/// Snapshot installs are rare events (only fire when a follower lags far
/// enough behind the leader to need a state transfer). A capacity of `4`
/// is plenty: at most one install runs at a time, and the channel buffers
/// one or two more in case the leader retransmits before the installer
/// drains. Beyond that, drop-and-let-Raft-retry kicks in (the channel
/// returns `TrySendError::Full` and the installer logs + drops; the next
/// heartbeat re-emits `Action::SendSnapshot` and the cycle resumes).
pub const APPLY_COMMAND_CHANNEL_CAPACITY: usize = 4;

/// Out-of-band command handed to the apply worker via the control channel.
///
/// Stage 4 introduces a single variant — `InstallSnapshot` — but the enum
/// shape leaves room for future control-plane operations (e.g. a
/// `Quiesce` command that flushes pending applies and yields a barrier
/// before backup, or a `ReopenStateLayer` for a future hot-resize path).
#[derive(Debug)]
pub enum ApplyCommand {
    /// Install a leader-streamed snapshot into the apply worker's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    ///
    /// The plaintext file is the LSNP-v2 payload recovered after the
    /// snapshot installer (`RaftManagerSnapshotInstaller`) decrypted the
    /// staged file. The apply worker calls
    /// [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot)
    /// and forwards the result on `completion`.
    InstallSnapshot {
        /// Snapshot metadata (last log id, membership, snapshot id) — fed
        /// directly to `install_snapshot`.
        meta: SnapshotMeta,
        /// LSNP-v2 plaintext file. The apply worker passes ownership to
        /// `install_snapshot`, which streams the bytes through its
        /// decompression + table-restore pipeline.
        plaintext: Box<tokio::fs::File>,
        /// One-shot to acknowledge install success / failure back to the
        /// originating installer. The installer awaits this and either
        /// advances `last_applied` via `notify_snapshot_installed` (success)
        /// or logs + records a metric and drops the staged file (failure).
        completion: oneshot::Sender<Result<(), StoreError>>,
        /// Marker for the underlying storage backend type — carried so
        /// the apply worker's typed `RaftLogStore<FileBackend>` install
        /// path stays unambiguous when this enum gains additional
        /// variants.
        _backend: std::marker::PhantomData<FileBackend>,
    },
}
