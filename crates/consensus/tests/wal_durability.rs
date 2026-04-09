//! WAL durability and crash recovery integration tests.
//!
//! These tests exercise the recovery path end-to-end: WAL append → sync →
//! checkpoint → `recover_from_wal`. Unit tests for `InMemoryWalBackend` and
//! `recover_from_wal` live in their respective modules; these integration tests
//! cover cross-component scenarios, edge cases, and ordering guarantees.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::collections::HashMap;

use inferadb_ledger_consensus::{
    recovery::recover_from_wal,
    types::ShardId,
    wal::InMemoryWalBackend,
    wal_backend::{CHECKPOINT_SHARD_ID, CheckpointFrame, WalBackend, WalFrame},
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn frame(shard: u64, index: u64, term: u64, data: &[u8]) -> WalFrame {
    WalFrame { shard_id: ShardId(shard), index, term, data: std::sync::Arc::from(data) }
}

fn checkpoint(committed: u64, term: u64) -> CheckpointFrame {
    CheckpointFrame { committed_index: committed, term }
}

fn applied(pairs: &[(u64, u64)]) -> HashMap<ShardId, u64> {
    pairs.iter().map(|&(shard, idx)| (ShardId(shard), idx)).collect()
}

// ---------------------------------------------------------------------------
// Basic recovery: empty and no-checkpoint
// ---------------------------------------------------------------------------

#[test]
fn recover_empty_wal_returns_zero_state() {
    let wal = InMemoryWalBackend::new();
    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 0);
    assert_eq!(result.term, 0);
    assert!(result.entries_to_replay.is_empty());
    assert_eq!(result.replay_count, 0);
}

#[test]
fn recover_no_checkpoint_with_frames_returns_zero_state() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"data"), frame(1, 2, 1, b"data")]).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 0);
    assert_eq!(result.replay_count, 0, "frames without checkpoint must not replay");
}

// ---------------------------------------------------------------------------
// Partial replay: entries_to_replay window
// ---------------------------------------------------------------------------

#[test]
fn recover_replays_entries_after_last_applied() {
    let mut wal = InMemoryWalBackend::new();
    for i in 1..=10 {
        wal.append(&[frame(1, i, 1, format!("e{i}").as_bytes())]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(10, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &applied(&[(1, 7)])).expect("recover");

    assert_eq!(result.replay_count, 3);
    let batch = result.entries_to_replay.iter().find(|b| b.shard == ShardId(1)).unwrap();
    let indices: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
    assert_eq!(indices, vec![8, 9, 10]);
}

#[test]
fn recover_excludes_uncommitted_entries_past_checkpoint() {
    let mut wal = InMemoryWalBackend::new();
    for i in 1..=5 {
        wal.append(&[frame(1, i, 1, &[i as u8])]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 3);
    assert_eq!(result.replay_count, 3);
    let batch = &result.entries_to_replay[0];
    assert!(batch.entries.iter().all(|e| e.index <= 3));
}

#[test]
fn recover_fully_applied_produces_empty_replay() {
    let mut wal = InMemoryWalBackend::new();
    for i in 1..=5 {
        wal.append(&[frame(1, i, 1, &[i as u8])]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(5, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &applied(&[(1, 5)])).expect("recover");

    assert_eq!(result.committed_index, 5);
    assert_eq!(result.replay_count, 0);
    assert!(result.entries_to_replay.is_empty());
}

// ---------------------------------------------------------------------------
// Checkpoint semantics
// ---------------------------------------------------------------------------

#[test]
fn recover_uses_last_checkpoint_when_multiple_exist() {
    let mut wal = InMemoryWalBackend::new();
    for i in 1..=10 {
        wal.append(&[frame(1, i, 1, format!("e{i}").as_bytes())]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.write_checkpoint(&checkpoint(7, 1)).unwrap();
    wal.write_checkpoint(&checkpoint(10, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 10);
    assert_eq!(result.replay_count, 10);
}

#[test]
fn recover_preserves_term_from_checkpoint() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 3, b"entry")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 3)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.term, 3);
}

#[test]
fn recover_checkpoint_zero_committed_replays_nothing() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"entry")]).unwrap();
    wal.write_checkpoint(&checkpoint(0, 0)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 0);
    assert_eq!(result.replay_count, 0);
}

#[test]
fn recover_checkpoint_without_matching_entries_replays_nothing() {
    let mut wal = InMemoryWalBackend::new();
    // Checkpoint references committed_index=5 but WAL has no data frames.
    wal.write_checkpoint(&checkpoint(5, 2)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 5);
    assert_eq!(result.term, 2);
    assert_eq!(result.replay_count, 0);
}

// ---------------------------------------------------------------------------
// Multi-shard recovery
// ---------------------------------------------------------------------------

#[test]
fn recover_multi_shard_replays_per_shard_applied_index() {
    let mut wal = InMemoryWalBackend::new();
    // Interleave entries from 3 shards.
    wal.append(&[
        frame(1, 1, 1, b"s1-1"),
        frame(2, 1, 1, b"s2-1"),
        frame(3, 1, 1, b"s3-1"),
        frame(1, 2, 1, b"s1-2"),
        frame(2, 2, 1, b"s2-2"),
        frame(3, 2, 1, b"s3-2"),
        frame(1, 3, 1, b"s1-3"),
    ])
    .unwrap();
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.sync().unwrap();

    // Shard 1 applied through 2, shard 2 applied through 1, shard 3 fully applied.
    let result = recover_from_wal(&wal, &applied(&[(1, 2), (2, 1), (3, 2)])).expect("recover");

    // Shard 1: index 3. Shard 2: index 2. Shard 3: nothing. Total = 2.
    assert_eq!(result.replay_count, 2);

    let shard1 = result.entries_to_replay.iter().find(|b| b.shard == ShardId(1));
    let shard2 = result.entries_to_replay.iter().find(|b| b.shard == ShardId(2));
    let shard3 = result.entries_to_replay.iter().find(|b| b.shard == ShardId(3));

    assert_eq!(shard1.unwrap().entries.len(), 1);
    assert_eq!(shard1.unwrap().entries[0].index, 3);
    assert_eq!(shard2.unwrap().entries.len(), 1);
    assert_eq!(shard2.unwrap().entries[0].index, 2);
    assert!(shard3.is_none());
}

#[test]
fn recover_multi_shard_preserves_entry_ordering_within_shards() {
    let mut wal = InMemoryWalBackend::new();
    // Interleave shards 1 and 2 with non-sequential appends.
    wal.append(&[frame(2, 3, 1, b"s2-3")]).unwrap();
    wal.append(&[frame(1, 2, 1, b"s1-2")]).unwrap();
    wal.append(&[frame(2, 1, 1, b"s2-1")]).unwrap();
    wal.append(&[frame(1, 1, 1, b"s1-1")]).unwrap();
    wal.append(&[frame(2, 2, 1, b"s2-2")]).unwrap();
    wal.append(&[frame(1, 3, 1, b"s1-3")]).unwrap();
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.replay_count, 6);
    for batch in &result.entries_to_replay {
        let indices: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
        let mut sorted = indices.clone();
        sorted.sort();
        assert_eq!(indices, sorted, "shard {:?} entries not sorted", batch.shard);
    }
}

#[test]
fn recover_unknown_shard_in_applied_map_is_ignored() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"data")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
    wal.sync().unwrap();

    // Shard 99 in applied but not in WAL — should not cause errors.
    let result = recover_from_wal(&wal, &applied(&[(99, 10)])).expect("recover");

    assert_eq!(result.replay_count, 1);
}

// ---------------------------------------------------------------------------
// Crypto-shredding interaction
// ---------------------------------------------------------------------------

#[test]
fn recover_after_shred_includes_zeroed_frames_in_replay() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"secret"), frame(2, 1, 1, b"public")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
    wal.sync().unwrap();

    let zeroed = wal.shred_frames(ShardId(1)).unwrap();
    assert_eq!(zeroed, 1);

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    // Recovery still counts zeroed frames — the apply layer handles skip logic.
    assert_eq!(result.replay_count, 2);

    // Verify shard 1 data is zeroed, shard 2 intact.
    let frames = wal.read_frames(0).unwrap();
    let s1 = frames.iter().find(|f| f.shard_id == ShardId(1)).unwrap();
    assert!(s1.data.iter().all(|&b| b == 0));
    let s2 = frames.iter().find(|f| f.shard_id == ShardId(2)).unwrap();
    assert_eq!(&*s2.data, b"public");
}

// ---------------------------------------------------------------------------
// Truncation then recovery
// ---------------------------------------------------------------------------

#[test]
fn recover_after_truncate_before_excludes_truncated_entries() {
    let mut wal = InMemoryWalBackend::new();
    // Append 5 entries, checkpoint, sync.
    // Durable positions: [0]=idx1, [1]=idx2, [2]=idx3, [3]=idx4, [4]=idx5, [5]=checkpoint
    for i in 1..=5 {
        wal.append(&[frame(1, i, 1, &[i as u8])]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(5, 1)).unwrap();
    wal.sync().unwrap();

    // Truncate first 3 frames — removes entries 1, 2, 3.
    // Remaining: [0]=idx4, [1]=idx5, [2]=checkpoint(5,1)
    wal.truncate_before(3).unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    // Checkpoint survives at position 2 (committed_index=5).
    // Only entries with index <= 5 are replayed: indices 4 and 5.
    assert_eq!(result.committed_index, 5);
    assert_eq!(result.replay_count, 2);
    let batch = &result.entries_to_replay[0];
    let indices: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
    assert_eq!(indices, vec![4, 5]);
}

#[test]
fn recover_after_truncate_preserves_checkpoint_when_still_present() {
    let mut wal = InMemoryWalBackend::new();
    for i in 1..=5 {
        wal.append(&[frame(1, i, 1, &[i as u8])]).unwrap();
    }
    wal.write_checkpoint(&checkpoint(5, 1)).unwrap();
    wal.sync().unwrap();

    // Truncate only the first entry — checkpoint at position 5 survives.
    wal.truncate_before(1).unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    // Entries 2..=5 remain, checkpoint says committed_index=5, so indices 2-5 replay.
    assert_eq!(result.committed_index, 5);
    assert_eq!(result.replay_count, 4);
}

#[test]
fn recover_after_truncate_removes_checkpoint_returns_zero_state() {
    let mut wal = InMemoryWalBackend::new();
    // Durable: [0]=idx1, [1]=checkpoint
    wal.append(&[frame(1, 1, 1, b"entry")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
    wal.sync().unwrap();

    // Truncate everything.
    wal.truncate_before(2).unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 0);
    assert_eq!(result.replay_count, 0);
}

// ---------------------------------------------------------------------------
// Sync error then recovery
// ---------------------------------------------------------------------------

#[test]
fn recover_after_sync_error_retry_succeeds() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"entry-1"), frame(1, 2, 1, b"entry-2")]).unwrap();
    wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
    wal.inject_sync_error("disk full");

    // First sync fails — frames stay pending.
    assert!(wal.sync().is_err());
    assert!(wal.read_frames(0).unwrap().is_empty());

    // Retry succeeds — all frames become durable.
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
    assert_eq!(result.committed_index, 2);
    assert_eq!(result.replay_count, 2);
}

#[test]
fn recover_after_multiple_sync_errors_then_success() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"data")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();

    // Fail 3 times in sequence.
    for msg in &["error-1", "error-2", "error-3"] {
        wal.inject_sync_error(*msg);
        assert!(wal.sync().is_err());
    }

    // All frames still pending.
    assert!(wal.read_frames(0).unwrap().is_empty());
    assert_eq!(wal.pending_count(), 2); // 1 data frame + 1 checkpoint frame

    // Finally succeeds.
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");
    assert_eq!(result.committed_index, 1);
    assert_eq!(result.replay_count, 1);
}

// ---------------------------------------------------------------------------
// Corrupted checkpoint frame
// ---------------------------------------------------------------------------

#[test]
fn recover_skips_corrupted_checkpoint_frame() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"entry")]).unwrap();

    // Write a valid checkpoint first.
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();

    // Write a corrupted checkpoint: data too short for decode.
    let corrupted = WalFrame {
        shard_id: CHECKPOINT_SHARD_ID,
        index: 0,
        term: 0,
        data: std::sync::Arc::from(&[0xDE, 0xAD][..]),
    };
    wal.append(&[corrupted]).unwrap();
    wal.sync().unwrap();

    // `last_checkpoint` scans in reverse, hits the corrupt frame first (decode
    // returns None), then finds the valid one.
    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 1);
    assert_eq!(result.term, 1);
    assert_eq!(result.replay_count, 1);
}

#[test]
fn recover_returns_zero_state_when_all_checkpoints_corrupted() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"entry")]).unwrap();

    // Only corrupted checkpoint frames.
    for truncated_len in [0, 4, 8, 15] {
        let corrupted = WalFrame {
            shard_id: CHECKPOINT_SHARD_ID,
            index: 0,
            term: 0,
            data: std::sync::Arc::from(vec![0xAB; truncated_len].as_slice()),
        };
        wal.append(&[corrupted]).unwrap();
    }
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 0);
    assert_eq!(result.replay_count, 0);
}

// ---------------------------------------------------------------------------
// Replay data integrity
// ---------------------------------------------------------------------------

#[test]
fn recover_replay_entries_contain_correct_data() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 2, b"alpha"), frame(1, 2, 2, b"beta"), frame(1, 3, 3, b"gamma")])
        .unwrap();
    wal.write_checkpoint(&checkpoint(3, 3)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &applied(&[(1, 1)])).expect("recover");

    assert_eq!(result.replay_count, 2);
    let batch = result.entries_to_replay.iter().find(|b| b.shard == ShardId(1)).unwrap();
    assert_eq!(&*batch.entries[0].data, b"beta");
    assert_eq!(batch.entries[0].term, 2);
    assert_eq!(batch.entries[0].index, 2);
    assert_eq!(&*batch.entries[1].data, b"gamma");
    assert_eq!(batch.entries[1].term, 3);
    assert_eq!(batch.entries[1].index, 3);
}

#[test]
fn recover_checkpoint_frames_never_appear_in_replay_batches() {
    let mut wal = InMemoryWalBackend::new();
    // Interleave data and checkpoint frames.
    wal.append(&[frame(1, 1, 1, b"a")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
    wal.append(&[frame(1, 2, 1, b"b")]).unwrap();
    wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
    wal.append(&[frame(1, 3, 1, b"c")]).unwrap();
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.sync().unwrap();

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.replay_count, 3);
    for batch in &result.entries_to_replay {
        assert_ne!(batch.shard, CHECKPOINT_SHARD_ID);
        for entry in &batch.entries {
            assert!(!entry.data.is_empty());
        }
    }
}

// ---------------------------------------------------------------------------
// Large-scale recovery
// ---------------------------------------------------------------------------

#[test]
fn recover_handles_large_number_of_entries() {
    let mut wal = InMemoryWalBackend::new();
    let entry_count: u64 = 10_000;
    let batch_size = 100;

    for batch_start in (1..=entry_count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size as u64 - 1).min(entry_count);
        let frames: Vec<WalFrame> =
            (batch_start..=batch_end).map(|i| frame(1, i, 1, format!("e{i}").as_bytes())).collect();
        wal.append(&frames).unwrap();
    }
    wal.write_checkpoint(&checkpoint(entry_count, 1)).unwrap();
    wal.sync().unwrap();

    // Apply first half.
    let half = entry_count / 2;
    let result = recover_from_wal(&wal, &applied(&[(1, half)])).expect("recover");

    assert_eq!(result.committed_index, entry_count);
    assert_eq!(result.replay_count, (entry_count - half) as usize);

    // Verify ordering.
    let batch = &result.entries_to_replay[0];
    for (i, entry) in batch.entries.iter().enumerate() {
        assert_eq!(entry.index, half + 1 + i as u64);
    }
}

#[test]
fn recover_large_multi_shard_maintains_per_shard_order() {
    let mut wal = InMemoryWalBackend::new();
    let shards = 5u64;
    let entries_per_shard = 200u64;

    // Interleave shards round-robin.
    for idx in 1..=entries_per_shard {
        let frames: Vec<WalFrame> =
            (1..=shards).map(|s| frame(s, idx, 1, &[s as u8, idx as u8])).collect();
        wal.append(&frames).unwrap();
    }
    wal.write_checkpoint(&checkpoint(entries_per_shard, 1)).unwrap();
    wal.sync().unwrap();

    // Each shard applied halfway.
    let mid = entries_per_shard / 2;
    let applied_map = applied(&(1..=shards).map(|s| (s, mid)).collect::<Vec<_>>());
    let result = recover_from_wal(&wal, &applied_map).expect("recover");

    let expected_per_shard = (entries_per_shard - mid) as usize;
    assert_eq!(result.replay_count, expected_per_shard * shards as usize);
    assert_eq!(result.entries_to_replay.len(), shards as usize);

    for batch in &result.entries_to_replay {
        assert_eq!(batch.entries.len(), expected_per_shard);
        let indices: Vec<u64> = batch.entries.iter().map(|e| e.index).collect();
        let expected: Vec<u64> = (mid + 1..=entries_per_shard).collect();
        assert_eq!(indices, expected, "shard {:?} order mismatch", batch.shard);
    }
}

// ---------------------------------------------------------------------------
// Edge case: last_applied beyond committed_index
// ---------------------------------------------------------------------------

#[test]
fn recover_last_applied_beyond_committed_replays_nothing_for_that_shard() {
    let mut wal = InMemoryWalBackend::new();
    wal.append(&[frame(1, 1, 1, b"a"), frame(1, 2, 1, b"b"), frame(1, 3, 1, b"c")]).unwrap();
    wal.write_checkpoint(&checkpoint(3, 1)).unwrap();
    wal.sync().unwrap();

    // Shard claims to have applied index 5, which is beyond committed_index 3.
    let result = recover_from_wal(&wal, &applied(&[(1, 5)])).expect("recover");

    assert_eq!(result.replay_count, 0);
}

// ---------------------------------------------------------------------------
// Unsync data not visible to recovery
// ---------------------------------------------------------------------------

#[test]
fn recover_ignores_unsynced_frames_and_checkpoint() {
    let mut wal = InMemoryWalBackend::new();
    // Synced data.
    wal.append(&[frame(1, 1, 1, b"synced")]).unwrap();
    wal.write_checkpoint(&checkpoint(1, 1)).unwrap();
    wal.sync().unwrap();

    // Unsynced data — simulates crash before fsync.
    wal.append(&[frame(1, 2, 1, b"unsynced")]).unwrap();
    wal.write_checkpoint(&checkpoint(2, 1)).unwrap();
    // No sync — these are lost.

    let result = recover_from_wal(&wal, &HashMap::new()).expect("recover");

    assert_eq!(result.committed_index, 1, "should use synced checkpoint");
    assert_eq!(result.replay_count, 1, "should only see synced entry");
}
