//! Consensus engine error types.

use snafu::Snafu;

/// Errors from consensus operations.
#[derive(Debug, Snafu)]
pub enum ConsensusError {
    /// This node is not the leader for this shard.
    #[snafu(display("Not the leader"))]
    NotLeader,

    /// The reactor inbox is full — transient backpressure.
    #[snafu(display("Reactor inbox full"))]
    InboxFull,

    /// The reactor has shut down (channel closed or oneshot dropped).
    #[snafu(display("Reactor shut down"))]
    ReactorShutdown,

    /// WAL append or sync failed — persistent I/O error.
    #[snafu(display("WAL write error"))]
    WalWriteError,

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

impl ConsensusError {
    /// Returns the appropriate gRPC status code for this error.
    ///
    /// Uses raw `i32` codes to avoid depending on `tonic` in the consensus crate.
    /// Values from the gRPC status code specification:
    /// `INVALID_ARGUMENT=3`, `ALREADY_EXISTS=6`, `RESOURCE_EXHAUSTED=8`,
    /// `FAILED_PRECONDITION=9`, `INTERNAL=13`, `UNAVAILABLE=14`.
    pub fn grpc_code(&self) -> i32 {
        match self {
            Self::NotLeader | Self::ReactorShutdown | Self::LeaderNotReady => 14, // UNAVAILABLE
            Self::InboxFull => 8, // RESOURCE_EXHAUSTED
            Self::ShardUnavailable { .. } | Self::WalWriteError => 13, // INTERNAL
            Self::MembershipChangePending | Self::StaleEpoch { .. } => 9, // FAILED_PRECONDITION
            Self::AlreadyInitialized => 6, // ALREADY_EXISTS
            Self::InvalidMembershipChange { .. } => 3, // INVALID_ARGUMENT
        }
    }

    /// Whether the caller should retry this operation.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NotLeader | Self::InboxFull | Self::ReactorShutdown | Self::LeaderNotReady)
    }
}
