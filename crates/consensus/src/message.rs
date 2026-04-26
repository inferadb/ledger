//! Raft protocol messages exchanged between nodes.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::types::{Entry, NodeId};

/// Messages exchanged between Raft nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    /// Pre-vote request (checks if election would succeed without term increment).
    PreVoteRequest {
        /// Current term of the pre-candidate.
        term: u64,
        /// Node requesting pre-votes.
        candidate_id: NodeId,
        /// Index of the candidate's last log entry.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Pre-vote response.
    PreVoteResponse {
        /// Current term of the responding node.
        term: u64,
        /// Whether the pre-vote was granted.
        vote_granted: bool,
    },
    /// Request a vote during a real election.
    VoteRequest {
        /// Current term of the candidate.
        term: u64,
        /// Node requesting votes.
        candidate_id: NodeId,
        /// Index of the candidate's last log entry.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Response to a vote request.
    VoteResponse {
        /// Current term of the responding node.
        term: u64,
        /// Whether the vote was granted.
        vote_granted: bool,
    },
    /// Append entries (or heartbeat if entries is empty).
    AppendEntries {
        /// Leader's current term.
        term: u64,
        /// Leader sending the entries.
        leader_id: NodeId,
        /// Index of the log entry immediately preceding the new entries.
        prev_log_index: u64,
        /// Term of the log entry at `prev_log_index`.
        prev_log_term: u64,
        /// Shared log slice — one Arc clone per peer instead of N entry clones.
        entries: Arc<[Entry]>,
        /// Leader's commit index.
        leader_commit: u64,
        /// Closed timestamp in nanoseconds — the point before which the leader
        /// guarantees no future writes will be assigned. Followers use this to
        /// serve stale reads locally.
        closed_ts_nanos: u64,
    },
    /// Response to an append entries request.
    AppendEntriesResponse {
        /// Current term of the responding node.
        term: u64,
        /// Whether the append succeeded.
        success: bool,
        /// Highest log index matched on the follower.
        match_index: u64,
    },
    /// Snapshot chunk for follower catch-up.
    InstallSnapshot {
        /// Leader's current term.
        term: u64,
        /// Leader sending the snapshot.
        leader_id: NodeId,
        /// Index of the last entry included in the snapshot.
        last_included_index: u64,
        /// Term of the last entry included in the snapshot.
        last_included_term: u64,
        /// Byte offset within the snapshot data.
        offset: u64,
        /// Raw snapshot data for this chunk.
        data: Vec<u8>,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// Response to an InstallSnapshot request.
    InstallSnapshotResponse {
        /// Current term of the responding node.
        term: u64,
    },
    /// Leader tells a follower to immediately start an election (leadership transfer).
    TimeoutNow,
}

impl Message {
    /// Returns true if this message is election-critical and not on a
    /// retransmission timer. Such messages must be preserved across
    /// transport reconnects — Raft will not re-emit them until the next
    /// election timeout fires (~500ms), and dropping them creates a
    /// liveness gap where elections cannot converge.
    ///
    /// Heartbeats and `AppendEntries` are NOT election-critical because
    /// the heartbeat timer re-emits them periodically; dropping them
    /// across a reconnect window is benign. `InstallSnapshot` is also
    /// driven by the leader's replication state and re-issued on the
    /// next heartbeat tick when a follower still lags. `TimeoutNow` is
    /// a leadership-transfer trigger — if it is dropped the source
    /// leader simply retains leadership, which is a degraded mode but
    /// not a liveness gap, so it is treated as non-critical to keep the
    /// preserved set tight to the election round-trip.
    pub fn is_election_critical(&self) -> bool {
        match self {
            Message::PreVoteRequest { .. }
            | Message::PreVoteResponse { .. }
            | Message::VoteRequest { .. }
            | Message::VoteResponse { .. } => true,
            Message::AppendEntries { .. }
            | Message::AppendEntriesResponse { .. }
            | Message::InstallSnapshot { .. }
            | Message::InstallSnapshotResponse { .. }
            | Message::TimeoutNow => false,
        }
    }
}
