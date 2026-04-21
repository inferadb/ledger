//! Types for committed entry batches delivered to the apply worker.

use std::sync::Arc;

use crate::types::{EntryKind, ConsensusStateId};

/// A single committed log entry carried to the apply worker.
#[derive(Debug, Clone)]
pub struct CommittedEntry {
    /// 1-based log index.
    pub index: u64,
    /// Term when this entry was created.
    pub term: u64,
    /// Opaque payload.
    pub data: Arc<[u8]>,
    /// Entry kind — normal data or membership change.
    pub kind: EntryKind,
}

/// A batch of committed entries for a single shard.
///
/// Delivered by the reactor to the apply worker after WAL sync. Entries are
/// ordered by ascending index and span the range `(last_applied, up_to]`
/// relative to the previous batch for this shard.
#[derive(Debug, Clone)]
pub struct CommittedBatch {
    /// ConsensusState that produced these entries.
    pub shard: ConsensusStateId,
    /// Committed entries in ascending index order.
    pub entries: Vec<CommittedEntry>,
    /// The leader node that committed these entries, if known.
    ///
    /// Set by the reactor from the shard's current leader state. Used by the
    /// apply worker to conditionally renew the leader lease (only the leader
    /// should maintain a valid lease).
    pub leader_node: Option<u64>,
}

impl CommittedBatch {
    /// Returns the highest index in this batch, or `None` if empty.
    pub fn last_index(&self) -> Option<u64> {
        self.entries.last().map(|e| e.index)
    }

    /// Returns `true` if this batch carries no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::types::{EntryKind, Membership, NodeId};

    fn make_entry(index: u64, term: u64) -> CommittedEntry {
        CommittedEntry { index, term, data: Arc::from(b"data" as &[u8]), kind: EntryKind::Normal }
    }

    // -- CommittedBatch: is_empty / last_index --

    #[test]
    fn empty_batch_has_no_last_index() {
        let batch = CommittedBatch { shard: ConsensusStateId(1), entries: vec![], leader_node: None };
        assert!(batch.is_empty());
        assert_eq!(batch.last_index(), None);
    }

    #[test]
    fn single_entry_batch_returns_that_index() {
        let batch = CommittedBatch {
            shard: ConsensusStateId(5),
            entries: vec![make_entry(1, 1)],
            leader_node: None,
        };
        assert!(!batch.is_empty());
        assert_eq!(batch.last_index(), Some(1));
    }

    #[test]
    fn multi_entry_batch_returns_last_index() {
        let batch = CommittedBatch {
            shard: ConsensusStateId(1),
            entries: vec![make_entry(3, 1), make_entry(4, 1), make_entry(5, 2)],
            leader_node: None,
        };
        assert!(!batch.is_empty());
        assert_eq!(batch.last_index(), Some(5));
    }

    // -- CommittedEntry construction and fields --

    #[test]
    fn committed_entry_fields() {
        let entry = CommittedEntry {
            index: 42,
            term: 7,
            data: Arc::from(b"payload" as &[u8]),
            kind: EntryKind::Normal,
        };
        assert_eq!(entry.index, 42);
        assert_eq!(entry.term, 7);
        assert_eq!(&*entry.data, b"payload");
        assert!(matches!(entry.kind, EntryKind::Normal));
    }

    #[test]
    fn committed_entry_with_membership_kind() {
        let m = Membership::new([NodeId(1), NodeId(2)]);
        let entry = CommittedEntry {
            index: 7,
            term: 2,
            data: Arc::from(b"" as &[u8]),
            kind: EntryKind::Membership(m),
        };
        assert!(matches!(entry.kind, EntryKind::Membership(ref m) if m.voter_count() == 2));
    }

    // -- CommittedBatch: leader_node field --

    #[test]
    fn batch_leader_node_none() {
        let batch = CommittedBatch { shard: ConsensusStateId(1), entries: vec![], leader_node: None };
        assert!(batch.leader_node.is_none());
    }

    #[test]
    fn batch_leader_node_set() {
        let batch = CommittedBatch {
            shard: ConsensusStateId(1),
            entries: vec![make_entry(1, 1)],
            leader_node: Some(42),
        };
        assert_eq!(batch.leader_node, Some(42));
        assert_eq!(batch.shard, ConsensusStateId(1));
    }

    // -- CommittedBatch: shard identity preserved --

    #[test]
    fn batch_preserves_shard_id() {
        let batch = CommittedBatch {
            shard: ConsensusStateId(999),
            entries: vec![make_entry(1, 1)],
            leader_node: None,
        };
        assert_eq!(batch.shard, ConsensusStateId(999));
    }
}
