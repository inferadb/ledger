//! Core types for the consensus engine.

use std::{collections::BTreeSet, sync::Arc};

use serde::{Deserialize, Serialize};

/// Unique identifier for a shard (consensus group).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize
)]
#[rkyv(compare(PartialEq, PartialOrd), derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord))]
pub struct ShardId(pub u64);

/// Unique identifier for a node in the cluster.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize
)]
#[rkyv(compare(PartialEq, PartialOrd), derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord))]
pub struct NodeId(pub u64);

/// A single log entry in the Raft log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    /// Term when this entry was created.
    pub term: u64,
    /// 1-based index of this entry.
    pub index: u64,
    /// Opaque payload (shared via Arc to avoid deep copies on replication).
    pub data: Arc<[u8]>,
    /// Entry kind — normal data or membership change.
    pub kind: EntryKind,
}

/// Archivable representation of a log entry for zero-copy access.
///
/// Unlike [`Entry`], this type owns its data as a `Vec<u8>` so that rkyv
/// can derive directly on it. Convert from `Entry` via `From` and use
/// the [`zero_copy`](crate::zero_copy) module helpers to serialize and
/// access fields without full deserialization.
#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub struct ArchivableEntry {
    /// Term when this entry was created.
    pub term: u64,
    /// 1-based index of this entry.
    pub index: u64,
    /// Opaque payload.
    pub data: Vec<u8>,
    /// Entry kind — normal data or membership change.
    pub kind: EntryKind,
}

impl From<&Entry> for ArchivableEntry {
    fn from(entry: &Entry) -> Self {
        Self {
            term: entry.term,
            index: entry.index,
            data: entry.data.to_vec(),
            kind: entry.kind.clone(),
        }
    }
}

impl From<Entry> for ArchivableEntry {
    fn from(entry: Entry) -> Self {
        Self { term: entry.term, index: entry.index, data: entry.data.to_vec(), kind: entry.kind }
    }
}

impl From<ArchivableEntry> for Entry {
    fn from(entry: ArchivableEntry) -> Self {
        Self { term: entry.term, index: entry.index, data: Arc::from(entry.data), kind: entry.kind }
    }
}

/// Distinguishes normal entries from special entries.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize
)]
#[rkyv(derive(Debug))]
pub enum EntryKind {
    /// Normal application data.
    Normal,
    /// Membership change — new membership takes effect at commit.
    Membership(Membership),
}

/// Cluster membership configuration.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize
)]
#[rkyv(derive(Debug))]
pub struct Membership {
    /// Voting members.
    pub voters: BTreeSet<NodeId>,
    /// Non-voting learners (receive replication but don't vote).
    pub learners: BTreeSet<NodeId>,
}

impl Membership {
    /// Creates a new membership with the given voters and no learners.
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Self { voters: voters.into_iter().collect(), learners: BTreeSet::new() }
    }

    /// Returns the quorum size for the current voter set.
    #[inline]
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Returns the total number of voters.
    #[inline]
    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    /// Whether a node is a voter.
    #[inline]
    pub fn is_voter(&self, node: NodeId) -> bool {
        self.voters.contains(&node)
    }

    /// Whether a node is a learner.
    #[inline]
    pub fn is_learner(&self, node: NodeId) -> bool {
        self.learners.contains(&node)
    }

    /// Whether a node is a member (voter or learner).
    #[inline]
    pub fn is_member(&self, node: NodeId) -> bool {
        self.is_voter(node) || self.is_learner(node)
    }

    /// Adds a node as a voter.
    pub fn add_voter(&mut self, node: NodeId) {
        self.voters.insert(node);
    }

    /// Removes a node from the voter set.
    pub fn remove_voter(&mut self, node: NodeId) {
        self.voters.remove(&node);
    }

    /// Adds a node as a learner.
    pub fn add_learner(&mut self, node: NodeId) {
        self.learners.insert(node);
    }

    /// Removes a node from the learner set.
    pub fn remove_learner(&mut self, node: NodeId) {
        self.learners.remove(&node);
    }

    /// Promotes a learner to voter.
    pub fn promote_learner(&mut self, node: NodeId) {
        self.learners.remove(&node);
        self.voters.insert(node);
    }
}

/// Membership change operations (single-step only).
#[derive(Clone, Debug)]
pub enum MembershipChange {
    /// Add a non-voting learner.
    AddLearner {
        /// The node to add as a learner.
        node_id: NodeId,
        /// Whether this learner can be auto-promoted to voter.
        promotable: bool,
        /// If set, the change is rejected unless this matches the shard's current
        /// configuration epoch. Prevents stale callers from racing membership
        /// changes against one another.
        expected_conf_epoch: Option<u64>,
    },
    /// Promote a learner to voter.
    PromoteVoter {
        /// The learner node to promote.
        node_id: NodeId,
        /// If set, the change is rejected unless this matches the shard's current
        /// configuration epoch.
        expected_conf_epoch: Option<u64>,
    },
    /// Remove a node entirely (voter or learner).
    RemoveNode {
        /// The node to remove.
        node_id: NodeId,
        /// If set, the change is rejected unless this matches the shard's current
        /// configuration epoch.
        expected_conf_epoch: Option<u64>,
    },
}

/// Per-peer replication state tracked by the leader.
#[derive(Clone, Debug)]
pub struct PeerState {
    /// Peer's node ID.
    pub id: NodeId,
    /// Next log index to send to this peer.
    pub next_index: u64,
    /// Highest log index known to be replicated on this peer.
    pub match_index: u64,
    /// Number of in-flight (unacknowledged) AppendEntries batches.
    pub in_flight: u32,
    /// Whether this peer is a learner.
    pub is_learner: bool,
    /// Whether this learner can be auto-promoted.
    pub promotable: bool,
}

impl PeerState {
    /// Creates a new peer state for a voter.
    pub fn voter(id: NodeId, next_index: u64) -> Self {
        Self { id, next_index, match_index: 0, in_flight: 0, is_learner: false, promotable: false }
    }

    /// Creates a new peer state for a learner.
    pub fn learner(id: NodeId, next_index: u64, promotable: bool) -> Self {
        Self { id, next_index, match_index: 0, in_flight: 0, is_learner: true, promotable }
    }
}

/// The role of a node in a Raft shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    /// Follower awaiting heartbeats.
    Follower,
    /// Pre-candidate checking if election would succeed.
    PreCandidate,
    /// Candidate running a real election.
    Candidate,
    /// Leader replicating entries.
    Leader,
    /// Shard experienced a panic and is non-functional.
    Failed,
    /// Node has been removed from this shard's membership.
    Shutdown,
}

/// Timer kinds for the reactor's timer wheel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimerKind {
    /// Election timeout.
    Election,
    /// Heartbeat timeout (leader).
    Heartbeat,
    /// Shard cleanup after removal from membership (grace period).
    Cleanup,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Membership quorum: table-driven for edge cases --

    #[test]
    fn membership_quorum_table() {
        let cases: &[(usize, usize)] = &[
            (0, 1), // 0 voters: 0/2+1 = 1
            (1, 1), // 1 voter: 1/2+1 = 1
            (2, 2), // 2 voters (even): 2/2+1 = 2
            (3, 2), // 3 voters: 3/2+1 = 2
            (4, 3), // 4 voters (even): 4/2+1 = 3
            (5, 3), // 5 voters: 5/2+1 = 3
            (7, 4), // 7 voters: 7/2+1 = 4
        ];
        for &(voter_count, expected_quorum) in cases {
            let voters: Vec<NodeId> = (1..=voter_count as u64).map(NodeId).collect();
            let m = Membership::new(voters);
            assert_eq!(
                m.quorum(),
                expected_quorum,
                "quorum for {voter_count} voters should be {expected_quorum}"
            );
        }
    }

    // -- Membership role queries --

    #[test]
    fn membership_role_queries() {
        let mut m = Membership::new([NodeId(1), NodeId(2)]);
        m.learners.insert(NodeId(3));

        assert!(m.is_voter(NodeId(1)));
        assert!(m.is_voter(NodeId(2)));
        assert!(!m.is_voter(NodeId(3)));
        assert!(!m.is_voter(NodeId(99)));

        assert!(m.is_learner(NodeId(3)));
        assert!(!m.is_learner(NodeId(1)));

        assert!(m.is_member(NodeId(1)));
        assert!(m.is_member(NodeId(3)));
        assert!(!m.is_member(NodeId(99)));
    }

    #[test]
    fn membership_voter_count() {
        let m = Membership::new([NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(m.voter_count(), 3);

        let empty = Membership::new(Vec::<NodeId>::new());
        assert_eq!(empty.voter_count(), 0);
    }

    // -- Membership mutation methods --

    #[test]
    fn membership_add_and_remove_voter() {
        let mut m = Membership::new([NodeId(1)]);
        m.add_voter(NodeId(2));
        assert!(m.is_voter(NodeId(2)));
        assert_eq!(m.voter_count(), 2);

        m.remove_voter(NodeId(1));
        assert!(!m.is_voter(NodeId(1)));
        assert_eq!(m.voter_count(), 1);
    }

    #[test]
    fn membership_add_and_remove_learner() {
        let mut m = Membership::new([NodeId(1)]);
        m.add_learner(NodeId(10));
        assert!(m.is_learner(NodeId(10)));
        assert!(m.is_member(NodeId(10)));

        m.remove_learner(NodeId(10));
        assert!(!m.is_learner(NodeId(10)));
        assert!(!m.is_member(NodeId(10)));
    }

    #[test]
    fn membership_promote_learner_to_voter() {
        let mut m = Membership::new([NodeId(1)]);
        m.add_learner(NodeId(2));
        assert!(m.is_learner(NodeId(2)));
        assert!(!m.is_voter(NodeId(2)));

        m.promote_learner(NodeId(2));
        assert!(m.is_voter(NodeId(2)));
        assert!(!m.is_learner(NodeId(2)));
        assert_eq!(m.voter_count(), 2);
    }

    #[test]
    fn membership_add_duplicate_voter_is_idempotent() {
        let mut m = Membership::new([NodeId(1)]);
        m.add_voter(NodeId(1));
        assert_eq!(m.voter_count(), 1);
    }

    #[test]
    fn membership_remove_nonexistent_voter_is_noop() {
        let mut m = Membership::new([NodeId(1)]);
        m.remove_voter(NodeId(99));
        assert_eq!(m.voter_count(), 1);
    }

    // -- PeerState construction --

    #[test]
    fn peer_state_voter_defaults() {
        let p = PeerState::voter(NodeId(5), 10);
        assert_eq!(p.id, NodeId(5));
        assert_eq!(p.next_index, 10);
        assert_eq!(p.match_index, 0);
        assert_eq!(p.in_flight, 0);
        assert!(!p.is_learner);
        assert!(!p.promotable);
    }

    #[test]
    fn peer_state_learner_promotable() {
        let p = PeerState::learner(NodeId(7), 1, true);
        assert!(p.is_learner);
        assert!(p.promotable);
        assert_eq!(p.next_index, 1);
    }

    #[test]
    fn peer_state_learner_not_promotable() {
        let p = PeerState::learner(NodeId(8), 5, false);
        assert!(p.is_learner);
        assert!(!p.promotable);
    }

    // -- Entry <-> ArchivableEntry round-trip --

    #[test]
    fn entry_to_archivable_round_trip() {
        let entry = Entry {
            term: 3,
            index: 42,
            data: Arc::from(b"payload" as &[u8]),
            kind: EntryKind::Normal,
        };

        let archivable = ArchivableEntry::from(&entry);
        assert_eq!(archivable.term, 3);
        assert_eq!(archivable.index, 42);
        assert_eq!(archivable.data, b"payload");
        assert_eq!(archivable.kind, EntryKind::Normal);

        let restored = Entry::from(archivable);
        assert_eq!(restored.term, entry.term);
        assert_eq!(restored.index, entry.index);
        assert_eq!(&*restored.data, &*entry.data);
    }

    #[test]
    fn entry_to_archivable_owned_conversion() {
        let entry =
            Entry { term: 1, index: 1, data: Arc::from(b"test" as &[u8]), kind: EntryKind::Normal };
        let archivable = ArchivableEntry::from(entry);
        assert_eq!(archivable.data, b"test");
    }

    #[test]
    fn archivable_entry_with_membership_kind() {
        let m = Membership::new([NodeId(1), NodeId(2)]);
        let entry = Entry {
            term: 2,
            index: 5,
            data: Arc::from(b"" as &[u8]),
            kind: EntryKind::Membership(m),
        };
        let archivable = ArchivableEntry::from(&entry);
        assert!(matches!(archivable.kind, EntryKind::Membership(ref m) if m.voter_count() == 2));
    }

    // -- NodeState variants --

    #[test]
    fn node_state_equality() {
        assert_eq!(NodeState::Follower, NodeState::Follower);
        assert_ne!(NodeState::Follower, NodeState::Leader);
        assert_ne!(NodeState::Candidate, NodeState::PreCandidate);
        assert_ne!(NodeState::Leader, NodeState::Failed);
    }

    // -- ShardId / NodeId --

    #[test]
    fn shard_id_ordering() {
        assert!(ShardId(1) < ShardId(2));
        assert_eq!(ShardId(5), ShardId(5));
    }

    #[test]
    fn node_id_ordering() {
        assert!(NodeId(1) < NodeId(2));
        assert_eq!(NodeId(5), NodeId(5));
    }
}
