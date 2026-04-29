//! Actions returned by the ConsensusState to the reactor.
//!
//! The ConsensusState never performs I/O directly. It returns Actions that
//! the reactor batches and executes.

use std::time::Instant;

use crate::{
    message::Message,
    types::{ConsensusStateId, Entry, Membership, NodeId, TimerKind},
};

/// An action the reactor should execute on behalf of a shard.
#[derive(Debug)]
pub enum Action {
    /// Persist the current term and votedFor to stable storage.
    ///
    /// The Raft paper (Figure 2) requires `currentTerm` and `votedFor` to be
    /// updated on stable storage before responding to RPCs. The reactor must
    /// process this action (write checkpoint + fsync) **before** flushing any
    /// [`Send`](Action::Send) actions from the same batch.
    PersistTermState {
        /// ConsensusState whose term state changed.
        shard: ConsensusStateId,
        /// The current term to persist.
        term: u64,
        /// The candidate that received our vote in this term, or `None`.
        voted_for: Option<NodeId>,
    },
    /// Send a message to a peer node.
    Send {
        /// Destination node.
        to: NodeId,
        /// ConsensusState this message belongs to.
        shard: ConsensusStateId,
        /// The Raft message to send.
        msg: Message,
    },
    /// Persist entries to the WAL.
    PersistEntries {
        /// ConsensusState owning the entries.
        shard: ConsensusStateId,
        /// Entries to persist.
        entries: Vec<Entry>,
    },
    /// Entries have been committed up to this index.
    Committed {
        /// ConsensusState with committed entries.
        shard: ConsensusStateId,
        /// Commit index (inclusive).
        up_to: u64,
    },
    /// Schedule a timer (election or heartbeat).
    ScheduleTimer {
        /// ConsensusState the timer belongs to.
        shard: ConsensusStateId,
        /// Whether this is an election or heartbeat timer.
        kind: TimerKind,
        /// When the timer should fire.
        deadline: Instant,
    },
    /// Renew the leader lease (quorum acknowledged).
    RenewLease {
        /// ConsensusState whose lease is renewed.
        shard: ConsensusStateId,
    },
    /// Membership changed (new membership committed).
    MembershipChanged {
        /// ConsensusState with the new membership.
        shard: ConsensusStateId,
        /// The new membership configuration.
        membership: Membership,
    },
    /// Log exceeded snapshot threshold — reactor should trigger snapshot creation.
    TriggerSnapshot {
        /// ConsensusState that needs a snapshot.
        shard: ConsensusStateId,
        /// Index of the last entry to include in the snapshot.
        last_included_index: u64,
        /// Term of the last entry to include in the snapshot.
        last_included_term: u64,
    },
    /// The leader needs to send a snapshot to a lagging follower.
    ///
    /// The reactor delegates actual data transfer to a `SnapshotSender`
    /// callback (registered by the raft crate). When no callback is
    /// registered, the reactor falls back to an `InstallSnapshot` message.
    SendSnapshot {
        /// Target peer that needs the snapshot.
        to: NodeId,
        /// ConsensusState that needs the snapshot.
        shard: ConsensusStateId,
        /// Current term of the leader emitting this action.
        term: u64,
        /// Leader node ID.
        leader_id: NodeId,
        /// Last index included in the snapshot.
        last_included_index: u64,
        /// Last term included in the snapshot.
        last_included_term: u64,
    },
    /// The follower has accepted an `InstallSnapshot` message and the
    /// reactor should install the staged snapshot file written by Stage 3's
    /// `snapshot_receiver`.
    ///
    /// The reactor delegates the actual install to a `SnapshotInstaller`
    /// callback (registered by the raft crate). When no callback is
    /// registered, the action is dropped — the leader's heartbeat
    /// replicator re-emits `Action::SendSnapshot` on the next cycle, which
    /// re-stages the file via Stage 3 and re-emits this action.
    ///
    /// Stage 4 of the snapshot install path.
    InstallSnapshot {
        /// ConsensusState that needs the install.
        shard: ConsensusStateId,
        /// Term of the originating `Message::InstallSnapshot` (the leader's
        /// term at the moment of streaming). Carried for diagnostics +
        /// metrics; the install does not gate on this term match because
        /// the staged file's LSNP header is the authoritative source for
        /// scope + index + term verification.
        leader_term: u64,
        /// Last log index covered by the snapshot — used by the installer
        /// to find the matching staged file via
        /// [`SnapshotPersister::list_staged`](../../../raft/src/snapshot_persister.rs).
        last_included_index: u64,
        /// Last log term covered by the snapshot. Stored as snapshot
        /// metadata; the installer passes it back to
        /// [`ConsensusEngine::notify_snapshot_installed`](crate::ConsensusEngine::notify_snapshot_installed)
        /// so the shard's `last_snapshot_index` / `last_snapshot_term`
        /// advance.
        last_included_term: u64,
    },
    /// The local node was removed from this shard's voter and learner sets.
    /// The reactor should schedule cleanup (stop timers, remove shard).
    ShardRemoved {
        /// ConsensusState that should be cleaned up.
        shard: ConsensusStateId,
    },
}
