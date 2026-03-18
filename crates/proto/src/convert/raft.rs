//! Raft message conversions (`openraft::Vote` <-> `proto::RaftVote`).

use inferadb_ledger_types::LedgerNodeId;
use openraft::Vote;

use crate::proto;

/// Converts a domain [`Vote`] reference to its protobuf representation.
impl From<&Vote<LedgerNodeId>> for proto::RaftVote {
    fn from(vote: &Vote<LedgerNodeId>) -> Self {
        proto::RaftVote {
            term: vote.leader_id.term,
            node_id: vote.leader_id.voted_for().unwrap_or(0),
            committed: vote.committed,
        }
    }
}

/// Converts an owned domain [`Vote`] to its protobuf representation.
impl From<Vote<LedgerNodeId>> for proto::RaftVote {
    fn from(vote: Vote<LedgerNodeId>) -> Self {
        (&vote).into()
    }
}

/// Converts a protobuf [`RaftVote`](proto::RaftVote) reference to the domain [`Vote`].
impl From<&proto::RaftVote> for Vote<LedgerNodeId> {
    fn from(proto: &proto::RaftVote) -> Self {
        if proto.committed {
            Vote::new_committed(proto.term, proto.node_id)
        } else {
            Vote::new(proto.term, proto.node_id)
        }
    }
}

/// Converts an owned protobuf [`RaftVote`](proto::RaftVote) to the domain [`Vote`].
impl From<proto::RaftVote> for Vote<LedgerNodeId> {
    fn from(proto: proto::RaftVote) -> Self {
        (&proto).into()
    }
}
