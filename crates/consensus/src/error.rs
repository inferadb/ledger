//! Consensus engine error types.

use snafu::Snafu;

/// Errors from consensus operations.
#[derive(Debug, Snafu)]
pub enum ConsensusError {
    /// This node is not the leader for this shard.
    #[snafu(display("Not the leader"))]
    NotLeader,

    /// The proposal queue is full (back-pressure).
    #[snafu(display("Proposal queue full"))]
    ProposalQueueFull,

    /// The shard is unavailable.
    #[snafu(display("Shard {shard:?} unavailable"))]
    ShardUnavailable {
        /// The shard that is unavailable.
        shard: crate::types::ShardId,
    },

    /// A membership change is already in progress.
    #[snafu(display("Membership change already in progress"))]
    MembershipChangePending,

    /// The cluster is already initialized.
    #[snafu(display("Cluster already initialized"))]
    AlreadyInitialized,

    /// The requested membership change is invalid.
    #[snafu(display("Invalid membership change: {reason}"))]
    InvalidMembershipChange {
        /// Why the membership change was rejected.
        reason: String,
    },

    /// Leader has not yet committed an entry in its current term (Raft §4.1).
    #[snafu(display("Leader has not yet committed an entry in its current term"))]
    LeaderNotReady,

    /// The membership change references a stale configuration epoch.
    #[snafu(display("Stale epoch: expected {expected}, actual {actual}"))]
    StaleEpoch {
        /// The epoch the caller expected.
        expected: u64,
        /// The shard's current configuration epoch.
        actual: u64,
    },
}
