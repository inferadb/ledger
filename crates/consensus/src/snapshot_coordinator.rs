//! Snapshot coordination trait used by the reactor when a shard requests a
//! snapshot via [`Action::TriggerSnapshot`](crate::action::Action::TriggerSnapshot).
//!
//! This is the consensus-crate side of the Stage 2 snapshot persistence API.
//! The trait is defined here (with a no-op default implementation) so the
//! consensus crate does not depend on the raft crate; the raft crate
//! installs a real implementation that builds the snapshot file via
//! [`LedgerSnapshotBuilder`](../../../raft/src/log_storage/raft_impl.rs),
//! persists it via the `SnapshotPersister` (encryption envelope + atomic
//! write + retention), and then calls
//! [`ConsensusEngine::notify_snapshot_completed`](crate::ConsensusEngine::notify_snapshot_completed)
//! to advance the shard's `last_snapshot_index` so future threshold checks
//! use the correct baseline.
//!
//! ## Implementor contract
//!
//! [`SnapshotCoordinator::on_trigger_snapshot`] is called from the reactor's
//! event loop. The reactor's event loop is paused for the duration of the
//! call, so implementors **must** dispatch any I/O asynchronously
//! (e.g., spawn a tokio task) and return immediately. Blocking inside this
//! method stalls every other shard managed by the reactor — same hazard as
//! [`ShardWakeNotifier`](crate::wake::ShardWakeNotifier).
//!
//! ## Action vs. coverage index
//!
//! The reactor passes `last_included_index` / `last_included_term` from the
//! [`Action::TriggerSnapshot`](crate::action::Action::TriggerSnapshot)
//! payload, which is the shard's `commit_index` at the moment of trigger.
//! The actual snapshot the coordinator builds may cover up to a *lower*
//! index than `last_included_index` (the on-disk applied state lags
//! `commit_index` under lazy commit). The coordinator MUST resolve the
//! actual covered index from the snapshot it produces and pass that to
//! [`notify_snapshot_completed`](crate::ConsensusEngine::notify_snapshot_completed),
//! not the trigger-time index. Passing the trigger-time index when the
//! snapshot covers less would incorrectly suppress the next threshold
//! check.

use crate::types::ConsensusStateId;

/// Coordinator invoked by the reactor when an
/// [`Action::TriggerSnapshot`](crate::action::Action::TriggerSnapshot) is
/// processed.
///
/// See module docs for the implementor contract and the action-vs-coverage
/// index distinction.
pub trait SnapshotCoordinator: Send + Sync + 'static {
    /// Called by the reactor when an `Action::TriggerSnapshot` is processed
    /// for a shard.
    ///
    /// `last_included_index` and `last_included_term` are the shard's
    /// commit-time coordinates from the trigger action. The implementor
    /// MUST dispatch any snapshot-building I/O asynchronously (the
    /// reactor's event loop is on hold until this returns) and resolve the
    /// actual covered index from the produced snapshot before calling
    /// [`notify_snapshot_completed`](crate::ConsensusEngine::notify_snapshot_completed).
    fn on_trigger_snapshot(
        &self,
        shard_id: ConsensusStateId,
        last_included_index: u64,
        last_included_term: u64,
    );
}

/// Default no-op implementation used when no snapshot coordinator is supplied.
///
/// Used by [`ConsensusEngine::start`](crate::ConsensusEngine::start) and
/// [`ConsensusEngine::start_with_wake_notifier`](crate::ConsensusEngine::start_with_wake_notifier)
/// so existing call sites that do not yet wire up Stage 2 snapshot
/// persistence continue to compile without code changes. With this
/// installed, [`Action::TriggerSnapshot`](crate::action::Action::TriggerSnapshot)
/// is observed but no snapshot is persisted — the same behaviour as the
/// pre-Stage-2 reactor.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSnapshotCoordinator;

impl SnapshotCoordinator for NoopSnapshotCoordinator {
    fn on_trigger_snapshot(
        &self,
        _shard_id: ConsensusStateId,
        _last_included_index: u64,
        _last_included_term: u64,
    ) {
        // No-op. The reactor logs at debug level at the call site; there is
        // no further snapshot action to take when no coordinator is wired.
    }
}
