//! Actions returned by the Shard to the reactor.
//!
//! The Shard never performs I/O directly. It returns Actions that
//! the reactor batches and executes.

use std::time::Instant;

use crate::{
    message::Message,
    types::{Entry, Membership, NodeId, ShardId, TimerKind},
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
        /// Shard whose term state changed.
        shard: ShardId,
        /// The current term to persist.
        term: u64,
        /// The candidate that received our vote in this term, or `None`.
        voted_for: Option<NodeId>,
    },
    /// Send a message to a peer node.
    Send {
        /// Destination node.
        to: NodeId,
        /// Shard this message belongs to.
        shard: ShardId,
        /// The Raft message to send.
        msg: Message,
    },
    /// Persist entries to the WAL.
    PersistEntries {
        /// Shard owning the entries.
        shard: ShardId,
        /// Entries to persist.
        entries: Vec<Entry>,
    },
    /// Entries have been committed up to this index.
    Committed {
        /// Shard with committed entries.
        shard: ShardId,
        /// Commit index (inclusive).
        up_to: u64,
    },
    /// Schedule a timer (election or heartbeat).
    ScheduleTimer {
        /// Shard the timer belongs to.
        shard: ShardId,
        /// Whether this is an election or heartbeat timer.
        kind: TimerKind,
        /// When the timer should fire.
        deadline: Instant,
    },
    /// Renew the leader lease (quorum acknowledged).
    RenewLease {
        /// Shard whose lease is renewed.
        shard: ShardId,
    },
    /// Membership changed (new membership committed).
    MembershipChanged {
        /// Shard with the new membership.
        shard: ShardId,
        /// The new membership configuration.
        membership: Membership,
    },
    /// Log exceeded snapshot threshold — reactor should trigger snapshot creation.
    TriggerSnapshot {
        /// Shard that needs a snapshot.
        shard: ShardId,
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
        /// Shard that needs the snapshot.
        shard: ShardId,
        /// Current term of the leader emitting this action.
        term: u64,
        /// Leader node ID.
        leader_id: NodeId,
        /// Last index included in the snapshot.
        last_included_index: u64,
        /// Last term included in the snapshot.
        last_included_term: u64,
    },
    /// The local node was removed from this shard's voter and learner sets.
    /// The reactor should schedule cleanup (stop timers, remove shard).
    ShardRemoved {
        /// Shard that should be cleaned up.
        shard: ShardId,
    },
}
