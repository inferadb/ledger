//! Snapshot sender trait used by the reactor when a shard requests an
//! [`Action::SendSnapshot`](crate::action::Action::SendSnapshot).
//!
//! This is the consensus-crate side of the Stage 3 snapshot wire-transfer
//! API. The trait is defined here (with a no-op default implementation) so
//! the consensus crate does not depend on the raft crate; the raft crate
//! installs a real implementation that:
//!
//! 1. Resolves the shard ID to its `(region, organization_id, vault_id?)` scope tuple.
//! 2. Reads the at-rest encrypted snapshot bytes from the local
//!    [`SnapshotPersister`](../../../raft/src/snapshot_persister.rs).
//! 3. Streams the bytes to the follower via the internal Raft transport's `InstallSnapshotStream`
//!    RPC.
//! 4. Records leader-side metrics on success / failure.
//!
//! ## Implementor contract
//!
//! [`SnapshotSender::send_snapshot`] is called from the reactor's event
//! loop. The reactor's event loop is paused for the duration of the call,
//! so implementors **must** dispatch any I/O asynchronously
//! (e.g., spawn a tokio task) and return immediately. Blocking inside this
//! method stalls every other shard managed by the reactor — same hazard as
//! [`ShardWakeNotifier`](crate::wake::ShardWakeNotifier) and
//! [`SnapshotCoordinator`](crate::snapshot_coordinator::SnapshotCoordinator).
//!
//! ## Drop-and-let-Raft-retry
//!
//! When no sender is registered (the [`NoopSnapshotSender`] default), the
//! reactor logs the [`Action::SendSnapshot`](crate::action::Action::SendSnapshot)
//! at debug level and drops it.
//! Raft tolerates this naturally: the leader's heartbeat replicator detects
//! the still-lagging follower on the next AppendEntries cycle and re-emits
//! the action. The same drop-and-retry semantics apply when the sender's
//! async dispatch fails (e.g., the follower is unreachable, the staging
//! file write fails, the CRC validation rejects the stream): the leader
//! will retry on the next heartbeat tick.
//!
//! ## Why not an explicit transport handle?
//!
//! Earlier drafts threaded a transport handle through the reactor and
//! invoked it directly. This conflated two concerns: the consensus crate
//! must not depend on the raft crate (where the encrypted on-disk snapshot
//! bytes live), and the reactor must not perform I/O (root rule 10). The
//! callback split keeps the reactor I/O-free and the consensus crate
//! transport-agnostic — same shape as `ShardWakeNotifier` and
//! `SnapshotCoordinator`.

use crate::types::{ConsensusStateId, NodeId};

/// Notification trait invoked by the reactor when an
/// [`Action::SendSnapshot`](crate::action::Action::SendSnapshot) is
/// processed.
///
/// See module docs for the implementor contract and the drop-and-retry
/// semantics when the sender's async dispatch fails.
pub trait SnapshotSender: Send + Sync + 'static {
    /// Called by the reactor when an `Action::SendSnapshot` is processed
    /// for a shard.
    ///
    /// `shard_id` identifies the shard the snapshot covers (the implementor
    /// resolves this to a `(region, organization_id, vault_id?)` scope and
    /// finds the corresponding persisted snapshot file). `follower_id` is
    /// the target peer that needs the snapshot; the implementor resolves
    /// the peer's network address from the shared peer-address map.
    /// `snapshot_index` is the snapshot's last-included log index, used as
    /// the file-index suffix when locating the persisted file on disk.
    ///
    /// The implementor MUST dispatch any I/O asynchronously and return
    /// immediately — the reactor's event loop is on hold until this
    /// returns. Failures are logged + recorded as metrics; the leader's
    /// heartbeat replicator re-emits `Action::SendSnapshot` on the next
    /// cycle, so transient failures self-heal without explicit retry
    /// state in the reactor.
    fn send_snapshot(&self, shard_id: ConsensusStateId, follower_id: NodeId, snapshot_index: u64);
}

/// Default no-op implementation used when no snapshot sender is supplied.
///
/// Used by [`ConsensusEngine::start`](crate::ConsensusEngine::start),
/// [`ConsensusEngine::start_with_wake_notifier`](crate::ConsensusEngine::start_with_wake_notifier),
/// and
/// [`ConsensusEngine::start_with_coordinators`](crate::ConsensusEngine::start_with_coordinators)
/// so existing call sites that do not yet wire up Stage 3 snapshot
/// streaming continue to compile without code changes. With this installed,
/// [`Action::SendSnapshot`](crate::action::Action::SendSnapshot) is
/// observed but no snapshot is streamed — the same drop-and-retry behaviour
/// the reactor exhibits when an async dispatch fails. The leader's next
/// heartbeat retry will re-emit the action; if a real sender is wired by
/// then, the snapshot ships.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSnapshotSender;

impl SnapshotSender for NoopSnapshotSender {
    fn send_snapshot(
        &self,
        _shard_id: ConsensusStateId,
        _follower_id: NodeId,
        _snapshot_index: u64,
    ) {
        // No-op. The reactor logs at debug level at the call site; there is
        // no further send action to take when no sender is wired. The
        // leader's heartbeat replicator will re-emit `Action::SendSnapshot`
        // on the next cycle.
    }
}
