//! Observable leadership and cluster state.

use std::collections::BTreeSet;

use crate::types::{NodeId, NodeState, ConsensusStateId};

/// Observable shard state snapshot.
///
/// Delivered via a `tokio::sync::watch` channel whenever the shard's
/// leadership or term changes. Consumers can `borrow()` the latest state
/// without blocking the reactor.
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_consensus::leadership::ShardState;
/// use inferadb_ledger_consensus::types::NodeState;
///
/// let state = ShardState::default();
/// assert_eq!(state.state, NodeState::Follower);
/// assert!(state.leader.is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardState {
    /// The shard this state belongs to.
    pub shard: ConsensusStateId,
    /// Current Raft term.
    pub term: u64,
    /// This node's role in the shard.
    pub state: NodeState,
    /// Known leader node ID, if any.
    pub leader: Option<NodeId>,
    /// Highest log index known to be committed.
    pub commit_index: u64,
    /// Current voting membership.
    pub voters: BTreeSet<NodeId>,
    /// Non-voting learners (receive replication but don't vote).
    pub learners: BTreeSet<NodeId>,
    /// Monotonic configuration epoch, incremented on every committed membership change.
    pub conf_epoch: u64,
    /// Whether a membership change is currently in-flight (proposed but not committed).
    pub pending_membership: bool,
    /// Last log index in the shard's in-memory log.
    pub last_log_index: u64,
}

impl Default for ShardState {
    fn default() -> Self {
        Self {
            shard: ConsensusStateId(0),
            term: 0,
            state: NodeState::Follower,
            leader: None,
            commit_index: 0,
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
            conf_epoch: 0,
            pending_membership: false,
            last_log_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeId, NodeState, ConsensusStateId};

    #[test]
    fn default_starts_as_follower_with_zeroed_fields() {
        let state = ShardState::default();
        assert_eq!(state.shard, ConsensusStateId(0));
        assert_eq!(state.term, 0);
        assert_eq!(state.state, NodeState::Follower);
        assert!(state.leader.is_none());
        assert_eq!(state.commit_index, 0);
        assert!(state.voters.is_empty());
        assert!(state.learners.is_empty());
        assert_eq!(state.last_log_index, 0);
    }

    #[test]
    fn clone_preserves_all_fields() {
        let state = ShardState {
            shard: ConsensusStateId(3),
            term: 7,
            state: NodeState::Leader,
            leader: Some(NodeId(1)),
            commit_index: 42,
            voters: [NodeId(1), NodeId(2)].into_iter().collect(),
            learners: [NodeId(3)].into_iter().collect(),
            conf_epoch: 2,
            pending_membership: false,
            last_log_index: 50,
        };

        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    #[test]
    fn inequality_per_field() {
        let base = ShardState {
            shard: ConsensusStateId(1),
            term: 5,
            state: NodeState::Follower,
            leader: Some(NodeId(2)),
            commit_index: 10,
            voters: [NodeId(1), NodeId(2)].into_iter().collect(),
            learners: BTreeSet::new(),
            conf_epoch: 0,
            pending_membership: false,
            last_log_index: 10,
        };

        // Each single-field change produces inequality.
        let diff_shard = ShardState { shard: ConsensusStateId(99), ..base.clone() };
        assert_ne!(base, diff_shard);

        let diff_term = ShardState { term: 999, ..base.clone() };
        assert_ne!(base, diff_term);

        let diff_state = ShardState { state: NodeState::Candidate, ..base.clone() };
        assert_ne!(base, diff_state);

        let diff_leader = ShardState { leader: None, ..base.clone() };
        assert_ne!(base, diff_leader);

        let diff_commit = ShardState { commit_index: 0, ..base.clone() };
        assert_ne!(base, diff_commit);

        let diff_voters = ShardState { voters: BTreeSet::new(), ..base.clone() };
        assert_ne!(base, diff_voters);

        let diff_learners =
            ShardState { learners: [NodeId(99)].into_iter().collect(), ..base.clone() };
        assert_ne!(base, diff_learners);

        let diff_last_log = ShardState { last_log_index: 999, ..base.clone() };
        assert_ne!(base, diff_last_log);
    }
}
