//! WAL-based crash recovery.
//!
//! On startup, replays committed-but-unapplied WAL entries to bring
//! the state machine up to date with the consensus log.

use std::collections::HashMap;

use crate::{
    committed::{CommittedBatch, CommittedEntry},
    types::{EntryKind, ConsensusStateId},
    wal_backend::{CHECKPOINT_SHARD_ID, WalBackend, WalError},
};

/// Result of WAL recovery scan.
#[derive(Debug)]
pub struct RecoveryResult {
    /// Last committed index found in WAL checkpoints.
    pub committed_index: u64,
    /// Term at the checkpoint.
    pub term: u64,
    /// Entries that need replay (committed but not yet applied), grouped by shard.
    pub entries_to_replay: Vec<CommittedBatch>,
    /// Total number of entries to replay across all shards.
    pub replay_count: usize,
}

/// Scans the WAL and returns entries needing replay.
///
/// Compares the WAL's last checkpoint (committed_index) against the
/// state machine's last_applied index to determine the replay window.
///
/// # Arguments
/// - `wal` — WAL backend to scan
/// - `last_applied` — state machine's last applied index per shard
///
/// # Returns
/// Entries in `(last_applied, committed_index]` that need replay.
pub fn recover_from_wal<W: WalBackend>(
    wal: &W,
    last_applied: &HashMap<ConsensusStateId, u64>,
) -> Result<RecoveryResult, WalError> {
    let checkpoint = wal.last_checkpoint()?;
    let (committed_index, term) = match checkpoint {
        Some(cp) => (cp.committed_index, cp.term),
        None => {
            return Ok(RecoveryResult {
                committed_index: 0,
                term: 0,
                entries_to_replay: vec![],
                replay_count: 0,
            });
        },
    };

    // Read all frames and collect entries in the replay window.
    let frames = wal.read_frames(0)?;
    let mut shard_entries: HashMap<ConsensusStateId, Vec<CommittedEntry>> = HashMap::new();

    for frame in &frames {
        if frame.shard_id == CHECKPOINT_SHARD_ID {
            continue;
        }
        // Skip entries at or before the shard's last_applied.
        let shard_applied = last_applied.get(&frame.shard_id).copied().unwrap_or(0);
        if frame.index <= shard_applied {
            continue;
        }
        // Skip entries past committed_index (uncommitted).
        if frame.index > committed_index {
            continue;
        }
        shard_entries.entry(frame.shard_id).or_default().push(CommittedEntry {
            index: frame.index,
            term: frame.term,
            data: std::sync::Arc::clone(&frame.data),
            // Recovery assumes Normal; membership is re-derived from state.
            kind: EntryKind::Normal,
        });
    }

    // Sort entries per shard by index for ordered replay.
    for entries in shard_entries.values_mut() {
        entries.sort_by_key(|e| e.index);
    }

    let replay_count: usize = shard_entries.values().map(|v| v.len()).sum();
    let batches: Vec<CommittedBatch> = shard_entries
        .into_iter()
        .map(|(shard, entries)| CommittedBatch { shard, entries, leader_node: None })
        .collect();

    Ok(RecoveryResult { committed_index, term, entries_to_replay: batches, replay_count })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::{
        wal::InMemoryWalBackend,
        wal_backend::{CheckpointFrame, WalFrame},
    };

    fn make_frame(shard: u64, index: u64, term: u64) -> WalFrame {
        WalFrame {
            shard_id: ConsensusStateId(shard),
            index,
            term,
            data: std::sync::Arc::from(format!("entry-{index}").into_bytes().as_slice()),
        }
    }

    fn checkpoint(committed_index: u64, term: u64) -> CheckpointFrame {
        CheckpointFrame { committed_index, term, voted_for: None }
    }

    #[test]
    fn no_checkpoint_means_no_replay() {
        let wal = InMemoryWalBackend::new();
        let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
        assert_eq!(result.committed_index, 0);
        assert_eq!(result.term, 0);
        assert!(result.entries_to_replay.is_empty());
        assert_eq!(result.replay_count, 0);
    }

    #[test]
    fn entries_after_last_applied_are_replayed() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[make_frame(1, 1, 1), make_frame(1, 2, 1), make_frame(1, 3, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
        wal.sync().unwrap();

        let mut applied = HashMap::new();
        applied.insert(ConsensusStateId(1), 1);

        let result = recover_from_wal(&wal, &applied).expect("recover");
        assert_eq!(result.committed_index, 3);
        assert_eq!(result.replay_count, 2);
    }

    #[test]
    fn entries_past_committed_index_are_excluded() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[make_frame(1, 1, 1), make_frame(1, 2, 1), make_frame(1, 3, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
        wal.sync().unwrap();

        let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
        assert_eq!(result.replay_count, 2);
    }

    #[test]
    fn multi_shard_recovery_counts_per_shard() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[
            make_frame(1, 1, 1),
            make_frame(2, 1, 1),
            make_frame(1, 2, 1),
            make_frame(2, 2, 1),
        ])
        .unwrap();
        wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
        wal.sync().unwrap();

        let mut applied = HashMap::new();
        applied.insert(ConsensusStateId(1), 1); // shard 1: needs index 2
        // shard 2: needs index 1 and 2

        let result = recover_from_wal(&wal, &applied).expect("recover");
        assert_eq!(result.replay_count, 3);
        assert_eq!(result.entries_to_replay.len(), 2); // 2 shards
    }

    #[test]
    fn fully_applied_means_no_replay() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[make_frame(1, 1, 1), make_frame(1, 2, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
        wal.sync().unwrap();

        let mut applied = HashMap::new();
        applied.insert(ConsensusStateId(1), 2);

        let result = recover_from_wal(&wal, &applied).expect("recover");
        assert_eq!(result.replay_count, 0);
    }

    #[test]
    fn replay_entries_are_sorted_by_index_within_shard() {
        let mut wal = InMemoryWalBackend::new();
        // Append out of order within shard.
        wal.append(&[make_frame(1, 3, 1), make_frame(1, 1, 1), make_frame(1, 2, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
        wal.sync().unwrap();

        let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
        assert_eq!(result.replay_count, 3);
        let batch = &result.entries_to_replay[0];
        let indices: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn checkpoint_frames_are_not_included_in_replay() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[make_frame(1, 1, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
        wal.append(&[make_frame(1, 2, 1)]).unwrap();
        wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
        wal.sync().unwrap();

        let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
        // Only real data frames, not checkpoint frames.
        assert_eq!(result.replay_count, 2);
        for batch in &result.entries_to_replay {
            assert_ne!(batch.shard, CHECKPOINT_SHARD_ID);
        }
    }

    #[test]
    fn empty_wal_with_no_frames_returns_zero_state() {
        let wal = InMemoryWalBackend::new();
        let mut applied = HashMap::new();
        applied.insert(ConsensusStateId(1), 5);

        let result = recover_from_wal(&wal, &applied).expect("recover");
        assert_eq!(result.committed_index, 0);
        assert_eq!(result.replay_count, 0);
    }

    #[test]
    fn term_is_preserved_from_checkpoint() {
        let mut wal = InMemoryWalBackend::new();
        wal.append(&[make_frame(1, 1, 3)]).unwrap();
        wal.write_checkpoint(&checkpoint(1, 3)).unwrap();
        wal.sync().unwrap();

        let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
        assert_eq!(result.term, 3);
    }
}
