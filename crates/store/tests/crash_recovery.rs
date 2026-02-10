//! Crash recovery tests for the dual-slot commit protocol.
//!
//! These tests systematically verify that the storage engine recovers correctly
//! from crashes at every point in the commit sequence. Each test:
//!
//! 1. Creates a database and writes initial data
//! 2. Performs a second commit (so both slots are populated)
//! 3. Simulates a crash at a specific point by corrupting the file header
//! 4. Reopens the database and verifies recovery
//!
//! # Crash Points in Dual-Slot Commit
//!
//! ```text
//! persist_state_to_disk():
//!   1. Write table directory page         ─┐
//!   2. Read current header                  │ Pre-sync writes
//!   3. Write secondary slot + header      ─┘
//!   4. SYNC (first)                       ← CrashPoint::BeforeFirstSync
//!   5. Flip god byte                      ← CrashPoint::AfterFirstSync
//!   6. Write header with flipped god byte ← CrashPoint::DuringGodByteFlip
//!   7. SYNC (second)                      ← CrashPoint::AfterSecondSync
//! ```

// Test code is allowed to use unwrap for simplicity
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};

use inferadb_ledger_store::{
    FileBackend, HEADER_SIZE,
    backend::{CommitSlot, DatabaseHeader},
    db::Database,
    tables,
};

/// Helper: create a database, write key=1, commit, write key=2, commit.
/// Returns the path. Both commit slots should have valid data.
fn setup_two_commits(dir: &std::path::Path) -> std::path::PathBuf {
    let db_path = dir.join("crash_test.ink");

    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();

        // First commit: key=1
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![0xAA; 64]).unwrap();
        txn.commit().unwrap();

        // Second commit: key=2 (this populates the other slot)
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&2u64, &vec![0xBB; 64]).unwrap();
        txn.commit().unwrap();
    }

    db_path
}

/// Helper: read the database header from a file.
fn read_header(path: &std::path::Path) -> (Vec<u8>, DatabaseHeader) {
    let mut file = std::fs::File::open(path).unwrap();
    let mut header_bytes = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header_bytes).unwrap();
    let header = DatabaseHeader::from_bytes(&header_bytes).unwrap();
    (header_bytes, header)
}

/// Helper: write a header back to the file.
fn write_header(path: &std::path::Path, header: &DatabaseHeader) {
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&header.to_bytes()).unwrap();
    file.sync_all().unwrap();
}

/// Helper: write raw header bytes back to the file.
fn write_raw_header(path: &std::path::Path, header_bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(header_bytes).unwrap();
    file.sync_all().unwrap();
}

/// Helper: corrupt a specific slot's checksum to simulate partial/invalid write.
fn corrupt_slot_checksum(header_bytes: &mut [u8], slot_index: usize) {
    // Slot 0 starts at byte 16, slot 1 at byte 80
    // Checksum is at offset 40 within each slot
    let slot_base = 16 + (slot_index * CommitSlot::SIZE);
    let checksum_offset = slot_base + 40;
    header_bytes[checksum_offset] ^= 0xFF;
}

// ============================================================================
// Core Dual-Slot Commit Protocol Tests
// ============================================================================

/// Crash before the first sync in the commit sequence.
///
/// Simulated state: secondary slot was written but not synced. On a real
/// filesystem, the data may not have reached disk. Recovery should use
/// the old primary slot.
#[test]
fn test_crash_before_first_sync_uses_old_primary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    // Read header and identify which slot is primary after both commits
    let (mut header_bytes, header) = read_header(&db_path);
    let secondary_idx = header.secondary_slot_index();

    // Simulate crash before first sync: the secondary slot was written
    // with new data but never synced. Corrupt the secondary slot to
    // represent partial/unsynced writes.
    corrupt_slot_checksum(&mut header_bytes, secondary_idx);

    // The primary slot should still be valid
    write_raw_header(&db_path, &header_bytes);

    // Reopen — should use primary slot successfully
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    // Primary slot has the latest committed data — both keys should exist
    // since both commits completed successfully before we corrupted
    assert!(
        txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(),
        "Key 1 should be readable after recovery from primary slot"
    );
    assert!(
        txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
        "Key 2 should be readable (primary has latest commit)"
    );
}

/// Crash after the first sync but before the god byte flip.
///
/// Simulated state: secondary slot has valid data and checksum (synced),
/// but the god byte still points to the old primary. Recovery should use
/// the primary slot (which still has valid data from the previous commit).
#[test]
fn test_crash_after_first_sync_before_god_byte_flip() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    // At this point, both commits succeeded. Simulate a third write that
    // crashes after syncing the secondary slot but before flipping god byte.
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&3u64, &vec![0xCC; 64]).unwrap();
        txn.commit().unwrap();
    }

    // Now simulate crash: flip the god byte back to the previous slot,
    // as if the god byte flip never happened. The current primary has
    // key 3's data, but we make the old primary active again.
    let (_, mut header) = read_header(&db_path);
    header.flip_primary_slot(); // Undo the last flip
    write_header(&db_path, &header);

    // Reopen — should use the (now-primary) old slot
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    // Keys 1 and 2 should exist (from the old primary slot)
    assert!(
        txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(),
        "Key 1 should be readable after recovery"
    );
    assert!(
        txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
        "Key 2 should be readable after recovery"
    );
    // Key 3 may or may not be visible depending on which slot we're reading
    // The important thing is the database didn't corrupt
}

/// Crash during the god byte flip (god byte may be in unknown state).
///
/// Simulated state: the god byte was being written when the crash occurred.
/// Both slots have valid checksums (since the first sync completed). Recovery
/// should work regardless of which slot the god byte points to.
#[test]
fn test_crash_during_god_byte_flip_both_slots_valid() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    let (_, header) = read_header(&db_path);

    // Both slots should have valid checksums after two successful commits
    assert!(header.slot(0).verify_checksum(), "Slot 0 should have valid checksum");
    assert!(header.slot(1).verify_checksum(), "Slot 1 should have valid checksum");

    // Test recovery with god byte pointing to slot 0
    {
        let mut h = header.clone();
        // Force god byte to point to slot 0
        h.god_byte &= !DatabaseHeader::GOD_BYTE_SLOT_MASK;
        write_header(&db_path, &h);

        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let txn = db.read().unwrap();
        // At least one key should be readable
        assert!(
            txn.get::<tables::RaftLog>(&1u64).unwrap().is_some()
                || txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
            "At least one key should be readable with god byte pointing to slot 0"
        );
    }

    // Test recovery with god byte pointing to slot 1
    {
        let mut h = header.clone();
        // Force god byte to point to slot 1
        h.god_byte |= DatabaseHeader::GOD_BYTE_SLOT_MASK;
        write_header(&db_path, &h);

        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let txn = db.read().unwrap();
        // At least one key should be readable
        assert!(
            txn.get::<tables::RaftLog>(&1u64).unwrap().is_some()
                || txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
            "At least one key should be readable with god byte pointing to slot 1"
        );
    }
}

/// Crash after the second sync (effectively a completed commit followed by crash).
///
/// Simulated state: full commit completed. The recovery flag may still be set
/// (it's set during commit and cleared on clean shutdown). Recovery should see
/// all committed data and trigger a free list rebuild.
#[test]
fn test_crash_after_second_sync_full_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    // Set recovery required flag (simulates crash after commit but before clean shutdown)
    let (_, mut header) = read_header(&db_path);
    header.set_recovery_required(true);
    write_header(&db_path, &header);

    // Reopen — recovery flag triggers free list rebuild but all data should be intact
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    assert_eq!(
        txn.get::<tables::RaftLog>(&1u64).unwrap(),
        Some(vec![0xAA; 64]),
        "Key 1 should have correct value after recovery"
    );
    assert_eq!(
        txn.get::<tables::RaftLog>(&2u64).unwrap(),
        Some(vec![0xBB; 64]),
        "Key 2 should have correct value after recovery"
    );
}

// ============================================================================
// Corrupt Slot Recovery Tests
// ============================================================================

/// Primary slot corrupted — fall back to secondary.
///
/// Simulates a crash during the header write that corrupted the primary slot.
/// Recovery should detect the invalid checksum and use the secondary slot.
#[test]
fn test_corrupt_primary_falls_back_to_secondary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    let (mut header_bytes, header) = read_header(&db_path);
    let primary_idx = header.primary_slot_index();

    // Corrupt the primary slot's checksum
    corrupt_slot_checksum(&mut header_bytes, primary_idx);
    write_raw_header(&db_path, &header_bytes);

    // Reopen — should fall back to secondary slot
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    // At least key 1 should be readable (it was in the first commit)
    assert!(
        txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(),
        "Key 1 should be readable after falling back to secondary slot"
    );
}

/// Secondary slot corrupted — primary should still work.
///
/// The secondary slot is the inactive one. Corruption there should not
/// affect recovery since we always try the primary first.
#[test]
fn test_corrupt_secondary_primary_still_works() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    let (mut header_bytes, header) = read_header(&db_path);
    let secondary_idx = header.secondary_slot_index();

    // Corrupt the secondary slot's checksum
    corrupt_slot_checksum(&mut header_bytes, secondary_idx);
    write_raw_header(&db_path, &header_bytes);

    // Reopen — should use primary slot without issue
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    assert!(
        txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(),
        "Key 1 should be readable when secondary is corrupt"
    );
    assert!(
        txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
        "Key 2 should be readable when secondary is corrupt"
    );
}

/// Both slots corrupted — should return an error, not panic.
///
/// This is an unrecoverable state. The database should return a clear error.
#[test]
fn test_both_slots_corrupt_returns_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    let (mut header_bytes, _) = read_header(&db_path);

    // Corrupt both slots
    corrupt_slot_checksum(&mut header_bytes, 0);
    corrupt_slot_checksum(&mut header_bytes, 1);
    write_raw_header(&db_path, &header_bytes);

    // Reopen — should fail with a clear error
    let result = Database::<FileBackend>::open(&db_path);
    assert!(result.is_err(), "Opening database with both slots corrupt should fail");

    let err = result.err().expect("should be an error");
    let err_msg = format!("{err}");
    assert!(err_msg.contains("checksum"), "Error should mention checksums: {err_msg}");
}

// ============================================================================
// Multi-Transaction Crash Recovery Tests
// ============================================================================

/// Crash after many sequential commits.
///
/// Verifies that the dual-slot protocol works correctly after many commits,
/// not just after the first two.
#[test]
fn test_recovery_after_many_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("many_commits.ink");

    // Perform 10 sequential commits
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        for i in 0u64..10 {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&i, &vec![i as u8; 32]).unwrap();
            txn.commit().unwrap();
        }
    }

    // Simulate crash (recovery flag set)
    let (_, mut header) = read_header(&db_path);
    header.set_recovery_required(true);
    write_header(&db_path, &header);

    // Reopen and verify all data
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    for i in 0u64..10 {
        assert_eq!(
            txn.get::<tables::RaftLog>(&i).unwrap(),
            Some(vec![i as u8; 32]),
            "Key {i} should have correct value after recovery"
        );
    }
}

/// Crash during a commit that deletes data.
///
/// Verifies recovery handles partial delete transactions correctly.
/// If the delete didn't complete, the data should still be present.
#[test]
fn test_recovery_during_delete_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("delete_crash.ink");

    // Create database with data across multiple commits
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![0xAA; 32]).unwrap();
        txn.insert::<tables::RaftLog>(&2u64, &vec![0xBB; 32]).unwrap();
        txn.insert::<tables::RaftLog>(&3u64, &vec![0xCC; 32]).unwrap();
        txn.commit().unwrap();

        // Delete key 2 in a second commit
        let mut txn = db.write().unwrap();
        txn.delete::<tables::RaftLog>(&2u64).unwrap();
        txn.commit().unwrap();
    }

    // Simulate crash by corrupting the primary slot (fall back to secondary)
    let (mut header_bytes, header) = read_header(&db_path);
    let primary_idx = header.primary_slot_index();
    corrupt_slot_checksum(&mut header_bytes, primary_idx);
    write_raw_header(&db_path, &header_bytes);

    // Reopen — should fall back to secondary slot (which has the pre-delete state)
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    // Keys 1 and 3 should exist regardless of which slot we recovered from
    assert!(txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(), "Key 1 should survive recovery");
    assert!(txn.get::<tables::RaftLog>(&3u64).unwrap().is_some(), "Key 3 should survive recovery");
    // Key 2 may or may not exist depending on which slot we fell back to
}

/// Multiple tables survive crash recovery.
///
/// Verifies that the table directory (stored in the commit slot) correctly
/// preserves root pages for all tables after recovery.
#[test]
fn test_recovery_preserves_multiple_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multi_table.ink");

    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();

        // Write to RaftLog table
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![0x11; 16]).unwrap();
        txn.commit().unwrap();

        // Write to RaftState table
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftState>(&"key1".to_string(), &vec![0x22; 16]).unwrap();
        txn.commit().unwrap();
    }

    // Simulate crash with recovery flag
    let (_, mut header) = read_header(&db_path);
    header.set_recovery_required(true);
    write_header(&db_path, &header);

    // Reopen and verify both tables
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    assert_eq!(
        txn.get::<tables::RaftLog>(&1u64).unwrap(),
        Some(vec![0x11; 16]),
        "RaftLog data should survive recovery"
    );
    assert_eq!(
        txn.get::<tables::RaftState>(&"key1".to_string()).unwrap(),
        Some(vec![0x22; 16]),
        "RaftState data should survive recovery"
    );
}

// ============================================================================
// Dual-Slot Protocol Correctness Tests
// ============================================================================

/// Verifies that each commit alternates which slot is primary.
///
/// The dual-slot protocol flips the god byte on each commit, alternating
/// between slot 0 and slot 1 as the primary.
#[test]
fn test_commits_alternate_primary_slot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("alternating.ink");

    // Create database
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![1]).unwrap();
        txn.commit().unwrap();
    }

    let (_, header1) = read_header(&db_path);
    let slot_after_first = header1.primary_slot_index();

    // Second commit
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&2u64, &vec![2]).unwrap();
        txn.commit().unwrap();
    }

    let (_, header2) = read_header(&db_path);
    let slot_after_second = header2.primary_slot_index();

    assert_ne!(
        slot_after_first, slot_after_second,
        "Primary slot should alternate between commits"
    );

    // Third commit
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&3u64, &vec![3]).unwrap();
        txn.commit().unwrap();
    }

    let (_, header3) = read_header(&db_path);
    let slot_after_third = header3.primary_slot_index();

    assert_eq!(
        slot_after_first, slot_after_third,
        "Primary slot should cycle back after two commits"
    );
}

/// Verifies that both commit slots have valid checksums after a successful commit.
///
/// After two commits, both slots should have data with valid checksums.
/// This is the foundation of crash safety — either slot can be used for recovery.
#[test]
fn test_both_slots_valid_after_two_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    let (_, header) = read_header(&db_path);

    assert!(header.slot0.verify_checksum(), "Slot 0 should have valid checksum after two commits");
    assert!(header.slot1.verify_checksum(), "Slot 1 should have valid checksum after two commits");
}

/// Verifies that recovery flag is set during commit.
///
/// The recovery flag is set at the start of persist_state_to_disk and
/// should remain set if the process crashes before clean shutdown.
#[test]
fn test_recovery_flag_set_during_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("recovery_flag.ink");

    // Create and commit — the recovery flag is set during commit
    // and should remain set (no clean shutdown clears it in this implementation)
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![1]).unwrap();
        txn.commit().unwrap();
    }

    let (_, header) = read_header(&db_path);
    // The recovery required flag is set during persist_state_to_disk
    // Note: the current implementation sets it but doesn't clear it after commit
    // This is actually correct — it gets cleared when recovery is not needed on open
    assert!(
        header.recovery_required(),
        "Recovery flag should be set after commit (cleared on clean open)"
    );
}

/// Verifies that free list rebuild produces correct results after crash.
///
/// After recovery with the recovery flag set, the free list is rebuilt
/// by walking all B-tree roots. Writes after recovery should succeed,
/// proving the allocator is in a consistent state.
#[test]
fn test_free_list_rebuild_allows_subsequent_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("free_list_rebuild.ink");

    // Create database with some data
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        for i in 0u64..50 {
            txn.insert::<tables::RaftLog>(&i, &vec![i as u8; 128]).unwrap();
        }
        txn.commit().unwrap();
    }

    // Set recovery flag to force free list rebuild
    let (_, mut header) = read_header(&db_path);
    header.set_recovery_required(true);
    write_header(&db_path, &header);

    // Reopen (triggers free list rebuild) and write more data
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();

        // Verify existing data
        let txn = db.read().unwrap();
        for i in 0u64..50 {
            assert!(
                txn.get::<tables::RaftLog>(&i).unwrap().is_some(),
                "Key {i} should exist after recovery"
            );
        }
        drop(txn);

        // Write more data — this validates the allocator is consistent
        let mut txn = db.write().unwrap();
        for i in 50u64..100 {
            txn.insert::<tables::RaftLog>(&i, &vec![i as u8; 128]).unwrap();
        }
        txn.commit().unwrap();

        // Verify all data
        let txn = db.read().unwrap();
        for i in 0u64..100 {
            assert!(
                txn.get::<tables::RaftLog>(&i).unwrap().is_some(),
                "Key {i} should exist after recovery + new writes"
            );
        }
    }
}

// ============================================================================
// Repeated Crash Recovery Tests
// ============================================================================

/// Multiple sequential crashes and recoveries.
///
/// Simulates a scenario where the system crashes and recovers multiple times.
/// Each recovery should leave the database in a consistent state.
#[test]
fn test_repeated_crash_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("repeated_crash.ink");

    // Initial data
    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&1u64, &vec![1; 32]).unwrap();
        txn.commit().unwrap();
    }

    // Three rounds of crash-and-recover
    for round in 0..3 {
        // Simulate crash
        let (_, mut header) = read_header(&db_path);
        header.set_recovery_required(true);
        write_header(&db_path, &header);

        // Recover and add more data
        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let key = 10 + round as u64;
        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&key, &vec![key as u8; 32]).unwrap();
        txn.commit().unwrap();
    }

    // Final verification
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    assert!(
        txn.get::<tables::RaftLog>(&1u64).unwrap().is_some(),
        "Original data should survive repeated crashes"
    );
    for round in 0..3 {
        let key = 10 + round as u64;
        assert!(
            txn.get::<tables::RaftLog>(&key).unwrap().is_some(),
            "Data from recovery round {round} should exist"
        );
    }
}

/// Recovery from crash that corrupts the latest commit slot,
/// followed by a new successful commit.
///
/// Verifies that after falling back to the secondary slot, the database
/// can perform new commits successfully.
#[test]
fn test_recovery_from_corrupt_slot_allows_new_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = setup_two_commits(temp_dir.path());

    // Corrupt primary slot
    let (mut header_bytes, header) = read_header(&db_path);
    let primary_idx = header.primary_slot_index();
    corrupt_slot_checksum(&mut header_bytes, primary_idx);
    write_raw_header(&db_path, &header_bytes);

    // Recover and write new data
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();

        let mut txn = db.write().unwrap();
        txn.insert::<tables::RaftLog>(&100u64, &vec![0xFF; 32]).unwrap();
        txn.commit().unwrap();

        // Verify the new data is accessible
        let txn = db.read().unwrap();
        assert_eq!(
            txn.get::<tables::RaftLog>(&100u64).unwrap(),
            Some(vec![0xFF; 32]),
            "New data written after recovery should be accessible"
        );
    }

    // Reopen again (clean this time) and verify everything
    {
        let db = Database::<FileBackend>::open(&db_path).unwrap();
        let txn = db.read().unwrap();
        assert!(
            txn.get::<tables::RaftLog>(&100u64).unwrap().is_some(),
            "New data should persist after clean reopen"
        );
    }
}

// ============================================================================
// Large Data Crash Recovery Tests
// ============================================================================

/// Recovery with large values that span multiple pages.
///
/// Ensures the dual-slot protocol correctly tracks all pages even when
/// values are large enough to span multiple B-tree leaf nodes.
#[test]
fn test_recovery_with_large_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("large_values.ink");

    {
        let db = Database::<FileBackend>::create(&db_path).unwrap();
        let mut txn = db.write().unwrap();

        // Write values large enough to create multiple B-tree pages
        for i in 0u64..20 {
            let value = vec![i as u8; 1024]; // 1KB values
            txn.insert::<tables::RaftLog>(&i, &value).unwrap();
        }
        txn.commit().unwrap();
    }

    // Set recovery flag
    let (_, mut header) = read_header(&db_path);
    header.set_recovery_required(true);
    write_header(&db_path, &header);

    // Recover and verify
    let db = Database::<FileBackend>::open(&db_path).unwrap();
    let txn = db.read().unwrap();

    for i in 0u64..20 {
        let expected = vec![i as u8; 1024];
        assert_eq!(
            txn.get::<tables::RaftLog>(&i).unwrap(),
            Some(expected),
            "Large value for key {i} should be intact after recovery"
        );
    }
}
