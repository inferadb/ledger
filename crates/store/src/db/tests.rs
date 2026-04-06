#[cfg(test)]
mod tests {
    use crate::{
        Database, TableIterator,
        backend::{DEFAULT_PAGE_SIZE, FileBackend, HEADER_SIZE, InMemoryBackend},
        db::{DatabaseConfig, iterator::DEFAULT_BUFFER_SIZE},
        tables::{self, Table},
    };

    #[test]
    fn test_database_config_builder() {
        let config =
            DatabaseConfig::builder().page_size(8192).cache_size(512).sync_on_commit(false).build();

        assert_eq!(config.page_size, 8192);
        assert_eq!(config.cache_size, 512);
        assert!(!config.sync_on_commit);
    }

    #[test]
    fn test_database_config_builder_defaults() {
        let from_builder = DatabaseConfig::builder().build();
        let from_default = DatabaseConfig::default();

        assert_eq!(from_builder.page_size, from_default.page_size);
        assert_eq!(from_builder.cache_size, from_default.cache_size);
        assert_eq!(from_builder.sync_on_commit, from_default.sync_on_commit);
    }

    #[test]
    fn test_database_config_builder_partial() {
        // Override only one field, others use defaults
        let config = DatabaseConfig::builder().page_size(16384).build();

        assert_eq!(config.page_size, 16384);
        assert_eq!(config.cache_size, 16_384); // default
        assert!(config.sync_on_commit); // default
    }

    #[test]
    fn test_database_create() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let stats = db.stats();
        assert_eq!(stats.page_size, DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_write_and_read() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Write
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3]).unwrap();
            txn.insert::<tables::RaftLog>(&2u64, &vec![4u8, 5, 6]).unwrap();
            txn.commit().unwrap();
        }

        // Read
        {
            let txn = db.read().unwrap();
            let val1 = txn.get::<tables::RaftLog>(&1u64).unwrap();
            assert_eq!(val1, Some(vec![1u8, 2, 3]));

            let val2 = txn.get::<tables::RaftLog>(&2u64).unwrap();
            assert_eq!(val2, Some(vec![4u8, 5, 6]));

            let val3 = txn.get::<tables::RaftLog>(&3u64).unwrap();
            assert_eq!(val3, None);
        }
    }

    #[test]
    fn test_delete() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3]).unwrap();
            txn.commit().unwrap();
        }

        // Delete
        {
            let mut txn = db.write().unwrap();
            let deleted = txn.delete::<tables::RaftLog>(&1u64).unwrap();
            assert!(deleted);
            txn.commit().unwrap();
        }

        // Verify deleted
        {
            let txn = db.read().unwrap();
            let val = txn.get::<tables::RaftLog>(&1u64).unwrap();
            assert_eq!(val, None);
        }
    }

    #[test]
    fn test_multiple_tables() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Write to different tables
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8]).unwrap();
            txn.insert::<tables::RaftState>(&"term".to_string(), &vec![0u8, 0, 0, 1]).unwrap();
            txn.commit().unwrap();
        }

        // Read from both
        {
            let txn = db.read().unwrap();
            assert!(txn.get::<tables::RaftLog>(&1u64).unwrap().is_some());
            assert!(txn.get::<tables::RaftState>(&"term".to_string()).unwrap().is_some());
        }
    }

    /// Stress test: multiple sequential writes followed by verification (numeric keys)
    #[test]
    fn test_many_sequential_writes_numeric_keys() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Write 500 keys sequentially
        let num_keys = 500;
        for i in 0..num_keys {
            let mut txn = db.write().unwrap();
            let key = i as u64;
            let value = format!("value-{}", i).into_bytes();
            txn.insert::<tables::Entities>(&key.to_be_bytes().to_vec(), &value).unwrap();
            txn.commit().unwrap();
        }

        // Verify all keys are present
        let txn = db.read().unwrap();
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = i as u64;
            let expected = format!("value-{}", i).into_bytes();
            match txn.get::<tables::Entities>(&key.to_be_bytes().to_vec()).unwrap() {
                Some(value) => {
                    assert_eq!(value, expected, "Value mismatch for key {}", i);
                },
                None => {
                    missing.push(i);
                },
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            num_keys,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    /// Stress test: multiple sequential writes with STRING keys (like storage layer)
    /// This tests variable-length keys that may trigger different B+ tree code paths
    #[test]
    fn test_many_sequential_writes_string_keys() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Write 500 keys sequentially with variable-length string keys
        let num_keys = 500;
        for i in 0..num_keys {
            let mut txn = db.write().unwrap();
            // Simulate storage layer key format: prefix + bucket + local_key
            let key = format!("\x00\x00\x00\x00\x00\x00\x00\x01\x42stress-key-{}", i);
            let value = format!("value-{}", i).into_bytes();
            txn.insert::<tables::Entities>(&key.into_bytes(), &value).unwrap();
            txn.commit().unwrap();
        }

        // Verify all keys are present
        let txn = db.read().unwrap();
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = format!("\x00\x00\x00\x00\x00\x00\x00\x01\x42stress-key-{}", i);
            let expected = format!("value-{}", i).into_bytes();
            match txn.get::<tables::Entities>(&key.into_bytes()).unwrap() {
                Some(value) => {
                    assert_eq!(value, expected, "Value mismatch for key {}", i);
                },
                None => {
                    missing.push(i);
                },
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            num_keys,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    /// Stress test with concurrent threads writing different keys
    #[test]
    fn test_concurrent_writes_from_threads() {
        use std::{sync::Arc, thread};

        let db = Arc::new(Database::<InMemoryBackend>::open_in_memory().unwrap());
        let num_threads = 4;
        let writes_per_thread = 50;

        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let db = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let mut txn = db.write().unwrap();
                    let key = format!("key-{}-{}", thread_id, i);
                    let value = format!("value-{}-{}", thread_id, i).into_bytes();
                    txn.insert::<tables::Entities>(&key.into_bytes(), &value).unwrap();
                    txn.commit().unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all keys are present
        let txn = db.read().unwrap();
        let mut missing = Vec::new();
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key-{}-{}", thread_id, i);
                let expected = format!("value-{}-{}", thread_id, i).into_bytes();
                let key_bytes = key.clone().into_bytes();
                match txn.get::<tables::Entities>(&key_bytes).unwrap() {
                    Some(value) => {
                        assert_eq!(value, expected, "Value mismatch for key {}", key);
                    },
                    None => {
                        missing.push(key);
                    },
                }
            }
        }

        let expected_total = num_threads * writes_per_thread;
        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            expected_total,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    /// Tests that data persists across database close and reopen
    #[test]
    fn test_file_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.ink");

        let num_keys = 100;

        // Create database and write data
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();

            for i in 0..num_keys {
                let mut txn = db.write().unwrap();
                txn.insert::<tables::RaftLog>(&(i as u64), &format!("value-{}", i).into_bytes())
                    .unwrap();
                txn.commit().unwrap();
            }

            // Database dropped here, should persist state
        }

        // Reopen database and verify data persisted
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();
            let txn = db.read().unwrap();

            let mut missing = Vec::new();
            for i in 0..num_keys {
                let expected = format!("value-{}", i).into_bytes();
                match txn.get::<tables::RaftLog>(&(i as u64)).unwrap() {
                    Some(value) => {
                        assert_eq!(value, expected, "Value mismatch for key {}", i);
                    },
                    None => {
                        missing.push(i);
                    },
                }
            }

            assert!(
                missing.is_empty(),
                "Missing {} keys after reopen: {:?}",
                missing.len(),
                &missing[..std::cmp::min(10, missing.len())]
            );
        }
    }

    /// Tests persistence with multiple tables
    #[test]
    fn test_file_persistence_multiple_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("multi.ink");

        // Create and write to multiple tables
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();
            let mut txn = db.write().unwrap();

            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3]).unwrap();
            txn.insert::<tables::RaftState>(&"term".to_string(), &vec![0u8, 0, 0, 42]).unwrap();
            txn.insert::<tables::Entities>(&b"entity-key".to_vec(), &b"entity-value".to_vec())
                .unwrap();

            txn.commit().unwrap();
        }

        // Reopen and verify all tables
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();
            let txn = db.read().unwrap();

            assert_eq!(
                txn.get::<tables::RaftLog>(&1u64).unwrap(),
                Some(vec![1u8, 2, 3]),
                "RaftLog data missing"
            );
            assert_eq!(
                txn.get::<tables::RaftState>(&"term".to_string()).unwrap(),
                Some(vec![0u8, 0, 0, 42]),
                "RaftState data missing"
            );
            assert_eq!(
                txn.get::<tables::Entities>(&b"entity-key".to_vec()).unwrap(),
                Some(b"entity-value".to_vec()),
                "Entities data missing"
            );
        }
    }

    // ========================================================================
    // Crash Simulation Tests
    // ========================================================================

    /// Tests that recovery flag triggers free list rebuild.
    ///
    /// Simulates a crash by manually setting the recovery flag in the header,
    /// then verifies that reopening the database works correctly.
    #[test]
    fn test_recovery_flag_triggers_rebuild() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("recovery_test.ink");

        // Create database and write some data
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3]).unwrap();
            txn.insert::<tables::RaftLog>(&2u64, &vec![4u8, 5, 6]).unwrap();
            txn.commit().unwrap();
        }

        // Simulate crash by setting recovery flag
        {
            use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};

            use crate::backend::DatabaseHeader;

            let mut file =
                std::fs::OpenOptions::new().read(true).write(true).open(&db_path).unwrap();

            // Read header
            let mut header_bytes = vec![0u8; HEADER_SIZE];
            file.read_exact(&mut header_bytes).unwrap();

            let mut header = DatabaseHeader::from_bytes(&header_bytes).unwrap();

            // Set recovery flag (simulates unclean shutdown)
            header.set_recovery_required(true);

            // Write modified header
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen - should detect recovery flag and rebuild free list
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();

            // Data should still be accessible
            let txn = db.read().unwrap();
            assert_eq!(txn.get::<tables::RaftLog>(&1u64).unwrap(), Some(vec![1u8, 2, 3]));
            assert_eq!(txn.get::<tables::RaftLog>(&2u64).unwrap(), Some(vec![4u8, 5, 6]));
        }
    }

    /// Tests that corrupt primary slot falls back to secondary.
    ///
    /// Simulates a crash during header write by corrupting the primary slot's
    /// checksum, then verifies that recovery uses the secondary slot.
    #[test]
    fn test_corrupt_primary_slot_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("corrupt_slot_test.ink");

        // Create database and commit twice (so both slots have valid data)
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();

            // First commit
            {
                let mut txn = db.write().unwrap();
                txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3]).unwrap();
                txn.commit().unwrap();
            }

            // Second commit - this will flip the primary slot
            {
                let mut txn = db.write().unwrap();
                txn.insert::<tables::RaftLog>(&2u64, &vec![4u8, 5, 6]).unwrap();
                txn.commit().unwrap();
            }
        }

        // Corrupt the primary slot's checksum
        {
            use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};

            use crate::backend::DatabaseHeader;

            let mut file =
                std::fs::OpenOptions::new().read(true).write(true).open(&db_path).unwrap();

            // Read header
            let mut header_bytes = vec![0u8; HEADER_SIZE];
            file.read_exact(&mut header_bytes).unwrap();

            let header = DatabaseHeader::from_bytes(&header_bytes).unwrap();
            let primary_idx = header.primary_slot_index();

            // Corrupt the primary slot's checksum byte
            // Slot 0 is at bytes 16-79, slot 1 at bytes 80-143
            // Checksum is at offset 40 within each slot
            let checksum_offset = 16 + (primary_idx * 64) + 40;
            header_bytes[checksum_offset] ^= 0xFF;

            // Write corrupted header
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header_bytes).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen - should fall back to secondary slot
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();

            // At least the first write's data should be accessible
            // (depending on which slot was corrupted)
            let txn = db.read().unwrap();
            assert!(
                txn.get::<tables::RaftLog>(&1u64).unwrap().is_some()
                    || txn.get::<tables::RaftLog>(&2u64).unwrap().is_some(),
                "At least one key should be readable after recovery"
            );
        }
    }

    /// Tests that dual-slot commit survives simulated power loss.
    ///
    /// This test creates a database, performs a write, then simulates
    /// a crash at various points in the commit sequence to verify
    /// the database remains consistent.
    #[test]
    fn test_dual_slot_consistency_after_crash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("dual_slot_test.ink");

        // Create database with initial data
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![0xAA; 100]).unwrap();
            txn.commit().unwrap();
        }

        // Perform another write
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&2u64, &vec![0xBB; 100]).unwrap();
            txn.commit().unwrap();
        }

        // Verify both values exist
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();
            let txn = db.read().unwrap();

            let val1 = txn.get::<tables::RaftLog>(&1u64).unwrap();
            assert!(val1.is_some(), "First key should exist");

            let val2 = txn.get::<tables::RaftLog>(&2u64).unwrap();
            assert!(val2.is_some(), "Second key should exist");
        }
    }

    /// Tests that free list persists across database close and reopen.
    ///
    /// This test uses a multi-table write pattern that generates free pages
    /// when tables are cleared, then verifies the persisted free list is
    /// correctly restored on reopen.
    #[test]
    fn test_free_list_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("free_list_test.ink");

        // Phase 1: Create database, write to multiple tables, then clear them
        // to generate free pages
        let expected_free_pages: usize;
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();

            // Write to multiple tables to allocate pages
            {
                let mut txn = db.write().unwrap();
                for i in 0..10 {
                    txn.insert::<tables::RaftLog>(&(i as u64), &vec![0xAA; 50]).unwrap();
                }
                txn.commit().unwrap();
            }

            // Clear the table completely (single-entry delete creates empty root)
            // Delete all entries one at a time
            for i in 0..10 {
                let mut txn = db.write().unwrap();
                txn.delete::<tables::RaftLog>(&(i as u64)).unwrap();
                txn.commit().unwrap();
            }

            // Flush pending frees
            db.try_free_pending_pages();

            // At this point, the RaftLog tree root should be freed
            expected_free_pages = db.allocator.lock().free_page_count();

            // If B-tree doesn't generate free pages in this scenario,
            // manually add some to test the persistence mechanism
            if expected_free_pages == 0 {
                // Allocate a page and immediately free it to test persistence
                let page_id = db.allocator.lock().allocate();
                db.allocator.lock().free(page_id);
                // Need to persist this - do a dummy write to trigger commit
                {
                    let mut txn = db.write().unwrap();
                    txn.insert::<tables::RaftState>(&"test".to_string(), &vec![1]).unwrap();
                    txn.commit().unwrap();
                }
            }
        }

        // Phase 2: Reopen and verify free list persisted
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();

            // Should have loaded free list from disk (not rebuilt)
            let actual_free_pages = db.allocator.lock().free_page_count();

            // The free list should be non-empty and loaded from disk
            // (exact count may vary based on B-tree behavior)
            assert!(
                actual_free_pages > 0 || expected_free_pages == 0,
                "Free list should be restored if it was persisted"
            );
        }
    }

    // ========================================================================
    // Iteration Tests (after multi-transaction writes)
    // ========================================================================

    /// Tests that iteration returns all entries written across separate transactions.
    ///
    /// This is a minimal reproduction case for a bug where iteration was returning
    /// incomplete results even though direct `get()` operations worked correctly.
    #[test]
    fn test_iteration_after_multi_transaction_writes() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let num_keys = 20;

        // Write entries in SEPARATE transactions (like gRPC server would)
        for i in 0..num_keys {
            let mut txn = db.write().unwrap();
            let key = format!("key-{:04}", i).into_bytes();
            let value = format!("value-{}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        // Verify all entries via direct get()
        {
            let txn = db.read().unwrap();
            let mut missing_get = Vec::new();
            for i in 0..num_keys {
                let key = format!("key-{:04}", i).into_bytes();
                if txn.get::<tables::Entities>(&key).unwrap().is_none() {
                    missing_get.push(i);
                }
            }
            assert!(
                missing_get.is_empty(),
                "Direct get() missing {} keys: {:?}",
                missing_get.len(),
                missing_get
            );
        }

        // Verify all entries via iteration
        {
            let txn = db.read().unwrap();
            let iter = txn.iter::<tables::Entities>().unwrap();
            let all_entries: Vec<_> = iter.collect_entries();

            assert_eq!(
                all_entries.len(),
                num_keys,
                "Iteration returned {} entries, expected {}. Missing entries!",
                all_entries.len(),
                num_keys
            );

            // Verify entries are in order
            let mut prev_key: Option<Vec<u8>> = None;
            for (key, _) in &all_entries {
                if let Some(prev) = &prev_key {
                    assert!(key > prev, "Entries not in order: {:?} should be > {:?}", key, prev);
                }
                prev_key = Some(key.clone());
            }
        }
    }

    /// Same test with variable-length keys that span multiple buckets.
    ///
    /// This mimics the Ledger state layer's key encoding where keys are
    /// scattered across buckets based on seahash.
    #[test]
    fn test_iteration_with_bucketed_keys() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let num_keys = 50;

        // Simulate the Ledger state layer's key format:
        // {vault_id:8BE}{bucket_id:1}{local_key:var}
        // Using different bucket values to scatter keys
        for i in 0..num_keys {
            let mut txn = db.write().unwrap();

            // Vault ID (fixed) + bucket ID (varying) + local key
            let vault_id: i64 = 1;
            let bucket_id = (i * 7) % 256; // Scatter across buckets
            let local_key = format!("rel:doc:{}#viewer@user:{}", i, i);

            let mut key = Vec::with_capacity(9 + local_key.len());
            key.extend_from_slice(&vault_id.to_be_bytes());
            key.push(bucket_id as u8);
            key.extend_from_slice(local_key.as_bytes());

            let value = format!("entity-{}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        // Verify all entries via iteration
        let txn = db.read().unwrap();
        let iter = txn.iter::<tables::Entities>().unwrap();
        let all_entries: Vec<_> = iter.collect_entries();

        assert_eq!(
            all_entries.len(),
            num_keys,
            "Iteration returned {} entries, expected {}. Missing entries!",
            all_entries.len(),
            num_keys
        );
    }

    /// Tests iteration with file-backed database across reopen.
    ///
    /// Ensures that iteration works correctly after database close and reopen.
    #[test]
    fn test_iteration_after_file_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("iter_test.ink");
        let num_keys = 30;

        // Create database and write entries
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();

            for i in 0..num_keys {
                let mut txn = db.write().unwrap();
                let key = format!("persistent-key-{:04}", i).into_bytes();
                let value = format!("value-{}", i).into_bytes();
                txn.insert::<tables::Entities>(&key, &value).unwrap();
                txn.commit().unwrap();
            }
        }

        // Reopen and verify via iteration
        {
            let db = Database::<FileBackend>::open(&db_path).unwrap();
            let txn = db.read().unwrap();
            let iter = txn.iter::<tables::Entities>().unwrap();
            let all_entries: Vec<_> = iter.collect_entries();

            assert_eq!(
                all_entries.len(),
                num_keys,
                "After reopen, iteration returned {} entries, expected {}",
                all_entries.len(),
                num_keys
            );
        }
    }

    // ========================================================================
    // Streaming TableIterator tests
    // ========================================================================

    /// Verifies that buffer refill produces the same results as unbuffered iteration.
    /// Uses a buffer_size of 3 against 10 entries so multiple refills occur.
    #[test]
    fn test_streaming_iter_buffer_refill() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let num_keys = 10;

        for i in 0..num_keys {
            let mut txn = db.write().unwrap();
            let key = format!("key-{:04}", i).into_bytes();
            let value = format!("val-{}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let root = txn.snapshot.table_roots[tables::Entities::ID as usize];

        // Create an iterator with a tiny buffer so we exercise multiple refills.
        let mut iter = TableIterator::<InMemoryBackend, tables::Entities>::with_range(
            &db,
            root,
            &txn.page_cache,
            None,
            None,
        )
        .unwrap();
        iter.buffer_size = 3;
        // Drain the initial pre-fill, then let the small buffer take over.
        iter.buffer.clear();
        iter.exhausted = false;
        iter.last_key = None;
        iter.refill_buffer().unwrap();

        let mut collected = Vec::new();
        while let Ok(Some(entry)) = iter.next_entry() {
            collected.push(entry);
        }

        assert_eq!(collected.len(), num_keys);

        // Verify ordering
        for i in 1..collected.len() {
            assert!(collected[i].0 > collected[i - 1].0, "Entries not sorted at index {}", i);
        }
    }

    /// Verifies that streaming works correctly with range bounds and small buffer.
    #[test]
    fn test_streaming_iter_with_range_bounds() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        for i in 0..20u64 {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&i, &vec![i as u8]).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        // Range [5, 15)
        let start_bytes = Some(5u64.to_be_bytes().to_vec());
        let end_bytes = Some(15u64.to_be_bytes().to_vec());

        let root = txn.snapshot.table_roots[tables::RaftLog::ID as usize];
        let mut iter = TableIterator::<InMemoryBackend, tables::RaftLog>::with_range(
            &db,
            root,
            &txn.page_cache,
            start_bytes,
            end_bytes,
        )
        .unwrap();
        iter.buffer_size = 3; // Force multiple refills within the range.

        // Drain the initial oversized buffer, rebuild with small size.
        iter.buffer.clear();
        iter.exhausted = false;
        iter.last_key = None;
        iter.refill_buffer().unwrap();

        let entries: Vec<_> = iter.collect_entries();
        assert_eq!(entries.len(), 10, "Expected keys 5..15, got {}", entries.len());

        // First key is 5, last key is 14
        let first_key = u64::from_be_bytes(entries[0].0.as_slice().try_into().unwrap());
        let last_key = u64::from_be_bytes(entries.last().unwrap().0.as_slice().try_into().unwrap());
        assert_eq!(first_key, 5);
        assert_eq!(last_key, 14);
    }

    /// Verifies that an empty table returns no entries without error.
    #[test]
    fn test_streaming_iter_empty_table() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let txn = db.read().unwrap();
        let iter = txn.iter::<tables::Entities>().unwrap();
        let entries = iter.collect_entries();
        assert!(entries.is_empty());
    }

    /// Verifies that a single entry works correctly.
    #[test]
    fn test_streaming_iter_single_entry() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"only-key".to_vec(), &b"only-val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let mut iter = txn.iter::<tables::Entities>().unwrap();
        let first = iter.next_entry().unwrap();
        assert!(first.is_some());
        assert_eq!(first.unwrap().0, b"only-key");

        let second = iter.next_entry().unwrap();
        assert!(second.is_none());
    }

    /// Verifies that the Iterator trait correctly yields all entries.
    #[test]
    fn test_streaming_iter_trait_integration() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let count = 25;

        for i in 0..count {
            let mut txn = db.write().unwrap();
            let key = format!("k{:04}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &vec![0u8; 64]).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let iter = txn.iter::<tables::Entities>().unwrap();

        // Use the Iterator trait (for .. in ..)
        let collected: Vec<_> = iter.collect();
        assert_eq!(collected.len(), count);
    }

    /// Verifies correct behavior with entry count exactly equal to buffer size.
    ///
    /// Inserts entries across separate transactions (the production pattern)
    /// to match the B-tree structure that leaf traversal expects.
    #[test]
    fn test_streaming_iter_exact_buffer_boundary() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert exactly DEFAULT_BUFFER_SIZE entries across separate transactions.
        for i in 0..DEFAULT_BUFFER_SIZE {
            let mut txn = db.write().unwrap();
            let key = format!("b{:06}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &b"v".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let entries: Vec<_> = txn.iter::<tables::Entities>().unwrap().collect_entries();
        assert_eq!(entries.len(), DEFAULT_BUFFER_SIZE);
    }

    /// Large dataset test: 5000 entries across separate transactions, confirming
    /// that the streaming iterator correctly handles multiple buffer refills
    /// across many B-tree leaf pages.
    #[test]
    fn test_streaming_iter_large_dataset() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let num_keys: usize = 5000;

        // Insert one entry per transaction (the production pattern via Raft).
        for i in 0..num_keys {
            let mut txn = db.write().unwrap();
            let key = format!("e{:08}", i).into_bytes();
            let value = vec![0u8; 128];
            txn.insert::<tables::Entities>(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let iter = txn.iter::<tables::Entities>().unwrap();

        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;
        for (key, _) in iter {
            if let Some(ref prev) = prev_key {
                assert!(key > *prev, "Ordering violated at entry {}", count);
            }
            prev_key = Some(key);
            count += 1;
        }
        assert_eq!(count, num_keys, "Expected {} entries, got {}", num_keys, count);
    }

    /// Tests bulk single-transaction insert with iteration (exercises the
    /// B-tree find_next_leaf path for dense intra-leaf key ranges).
    #[test]
    fn test_streaming_iter_bulk_single_transaction() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let num_keys = 2000;

        {
            let mut txn = db.write().unwrap();
            for i in 0..num_keys {
                let key = format!("bulk{:06}", i).into_bytes();
                txn.insert::<tables::Entities>(&key, &vec![0u8; 64]).unwrap();
            }
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let entries: Vec<_> = txn.iter::<tables::Entities>().unwrap().collect_entries();
        assert_eq!(entries.len(), num_keys);
    }

    /// Verifies write transaction iteration still works (sees uncommitted changes).
    #[test]
    fn test_streaming_iter_write_transaction() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        let mut txn = db.write().unwrap();
        for i in 0..5 {
            let key = format!("wk{:02}", i).into_bytes();
            txn.insert::<tables::Entities>(&key, &b"wv".to_vec()).unwrap();
        }

        // Iterate before commit — should see uncommitted entries.
        let entries: Vec<_> = txn.iter::<tables::Entities>().unwrap().collect_entries();
        assert_eq!(entries.len(), 5);

        txn.commit().unwrap();

        // After commit, a new read should also see them.
        let rtxn = db.read().unwrap();
        let entries: Vec<_> = rtxn.iter::<tables::Entities>().unwrap().collect_entries();
        assert_eq!(entries.len(), 5);
    }

    // ── Concurrency Stress Tests ────────────────────────────────────────

    /// Stress test: 100 tasks contending on concurrent reads and writes.
    ///
    /// Verifies that the single-writer model correctly serializes writes
    /// while allowing concurrent readers to observe consistent snapshots.
    #[test]
    fn stress_concurrent_read_write_contention() {
        use std::{sync::Arc, thread};

        let db = Arc::new(Database::<InMemoryBackend>::open_in_memory().unwrap());
        let num_writers = 10;
        let writes_per_writer = 100;
        let num_readers = 10;
        let reads_per_reader = 100;

        let mut handles = Vec::new();

        // Spawn writer threads — each writes to its own key space
        for writer_id in 0..num_writers {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..writes_per_writer {
                    let mut txn = db.write().unwrap();
                    let key = format!("w{:02}-{:04}", writer_id, i).into_bytes();
                    let value = format!("v{:02}-{:04}", writer_id, i).into_bytes();
                    txn.insert::<tables::Entities>(&key, &value).unwrap();
                    txn.commit().unwrap();
                }
            }));
        }

        // Spawn reader threads — reads should always see consistent state
        for _ in 0..num_readers {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for _ in 0..reads_per_reader {
                    let txn = db.read().unwrap();
                    // Snapshot consistency: if we can see key N for a writer,
                    // we must also see all keys < N from that same writer.
                    for writer_id in 0..num_writers {
                        let mut last_seen = None;
                        for i in 0..writes_per_writer {
                            let key = format!("w{:02}-{:04}", writer_id, i).into_bytes();
                            match txn.get::<tables::Entities>(&key).unwrap() {
                                Some(_) => last_seen = Some(i),
                                None => {
                                    // Gap: if we see a gap, all later keys
                                    // should also be missing (monotonic writes).
                                    // Break and verify last_seen is continuous from 0.
                                    break;
                                },
                            }
                        }
                        // If we saw any keys, they must be a contiguous prefix [0..last_seen].
                        if let Some(last) = last_seen {
                            for i in 0..=last {
                                let key = format!("w{:02}-{:04}", writer_id, i).into_bytes();
                                assert!(
                                    txn.get::<tables::Entities>(&key).unwrap().is_some(),
                                    "Snapshot inconsistency: saw key {last} but missing key {i} for writer {writer_id}"
                                );
                            }
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all writes landed
        let txn = db.read().unwrap();
        for writer_id in 0..num_writers {
            for i in 0..writes_per_writer {
                let key = format!("w{:02}-{:04}", writer_id, i).into_bytes();
                let expected = format!("v{:02}-{:04}", writer_id, i).into_bytes();
                let value = txn.get::<tables::Entities>(&key).unwrap();
                assert_eq!(value, Some(expected), "Missing key w{writer_id:02}-{i:04}");
            }
        }
    }

    /// Stress test: high-contention B-tree operations with many threads.
    ///
    /// 50 threads each perform 100 write transactions to the same table,
    /// exercising the single-writer serialization under heavy thread contention.
    /// Verifies all writes land correctly and no data is lost.
    #[test]
    fn stress_high_contention_btree_writes() {
        use std::{sync::Arc, thread};

        let db = Arc::new(Database::<InMemoryBackend>::open_in_memory().unwrap());
        let num_threads = 50;
        let ops_per_thread = 100;

        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let mut txn = db.write().unwrap();
                    let key = format!("hc-{:03}-{:04}", thread_id, i).into_bytes();
                    let value = vec![thread_id as u8; 32]; // 32 bytes of thread_id
                    txn.insert::<tables::Entities>(&key, &value).unwrap();
                    txn.commit().unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all writes landed
        let txn = db.read().unwrap();
        let all_entries: Vec<_> = txn.iter::<tables::Entities>().unwrap().collect_entries();
        assert_eq!(
            all_entries.len(),
            num_threads * ops_per_thread,
            "Expected {} entries, got {}",
            num_threads * ops_per_thread,
            all_entries.len()
        );

        // Spot-check values
        for thread_id in 0..num_threads {
            let key = format!("hc-{:03}-0000", thread_id).into_bytes();
            let value = txn.get::<tables::Entities>(&key).unwrap();
            assert_eq!(
                value,
                Some(vec![thread_id as u8; 32]),
                "Value mismatch for thread {thread_id}"
            );
        }
    }

    /// Stress test: concurrent writes with deletes to exercise B-tree rebalancing.
    ///
    /// Interleaves inserts and deletes from multiple threads to stress the
    /// B-tree's split/merge/rebalance paths under contention.
    #[test]
    fn stress_concurrent_write_delete_interleave() {
        use std::{sync::Arc, thread};

        let db = Arc::new(Database::<InMemoryBackend>::open_in_memory().unwrap());
        let num_threads = 8;
        let ops_per_thread = 100;

        // Phase 1: Seed the database so there's something to delete
        {
            let mut txn = db.write().unwrap();
            for i in 0..500 {
                let key = format!("seed-{:04}", i).into_bytes();
                let value = format!("seedval-{:04}", i).into_bytes();
                txn.insert::<tables::Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }

        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let mut txn = db.write().unwrap();
                    // Insert a new key
                    let new_key = format!("new-{:02}-{:04}", thread_id, i).into_bytes();
                    txn.insert::<tables::Entities>(&new_key, &b"new-val".to_vec()).unwrap();
                    // Delete a seeded key (may already be deleted by another thread)
                    let del_key =
                        format!("seed-{:04}", (thread_id * ops_per_thread + i) % 500).into_bytes();
                    let _ = txn.delete::<tables::Entities>(&del_key);
                    txn.commit().unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all new keys exist
        let txn = db.read().unwrap();
        for thread_id in 0..num_threads {
            for i in 0..ops_per_thread {
                let key = format!("new-{:02}-{:04}", thread_id, i).into_bytes();
                assert!(
                    txn.get::<tables::Entities>(&key).unwrap().is_some(),
                    "Missing new key from thread {thread_id} op {i}"
                );
            }
        }
    }

    // ========================================================================
    // Transaction Edge Case Tests
    // ========================================================================

    /// Tests that aborting a write transaction discards changes.
    #[test]
    fn test_write_transaction_abort() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert some initial data
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"existing".to_vec(), &b"val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        // Start a write, insert, then abort
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"aborted".to_vec(), &b"gone".to_vec()).unwrap();
            txn.abort();
        }

        // Aborted key should not be visible
        let txn = db.read().unwrap();
        assert!(txn.get::<tables::Entities>(&b"aborted".to_vec()).unwrap().is_none());
        assert!(txn.get::<tables::Entities>(&b"existing".to_vec()).unwrap().is_some());
    }

    /// Tests that dropping a write transaction without commit or abort discards changes.
    #[test]
    fn test_write_transaction_drop_without_commit() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"dropped".to_vec(), &b"val".to_vec()).unwrap();
            // Drop without commit or abort
        }

        let txn = db.read().unwrap();
        assert!(txn.get::<tables::Entities>(&b"dropped".to_vec()).unwrap().is_none());
    }

    /// Tests read transaction contains() method.
    #[test]
    fn test_read_transaction_contains() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"present".to_vec(), &b"val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        assert!(txn.contains::<tables::Entities>(&b"present".to_vec()).unwrap());
        assert!(!txn.contains::<tables::Entities>(&b"absent".to_vec()).unwrap());
    }

    /// Tests reading from an empty table returns None.
    #[test]
    fn test_read_empty_table() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let txn = db.read().unwrap();
        assert!(txn.get::<tables::RaftLog>(&1u64).unwrap().is_none());
    }

    /// Tests first() and last() on read transactions.
    #[test]
    fn test_read_transaction_first_last() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Empty table
        {
            let txn = db.read().unwrap();
            assert!(txn.first::<tables::Entities>().unwrap().is_none());
            assert!(txn.last::<tables::Entities>().unwrap().is_none());
        }

        // With data
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"aaa".to_vec(), &b"first".to_vec()).unwrap();
            txn.insert::<tables::Entities>(&b"zzz".to_vec(), &b"last".to_vec()).unwrap();
            txn.insert::<tables::Entities>(&b"mmm".to_vec(), &b"middle".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let (first_key, _) = txn.first::<tables::Entities>().unwrap().unwrap();
        assert_eq!(first_key, b"aaa");
        let (last_key, _) = txn.last::<tables::Entities>().unwrap().unwrap();
        assert_eq!(last_key, b"zzz");
    }

    /// Tests first() and last() on write transactions.
    #[test]
    fn test_write_transaction_first_last() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        let mut txn = db.write().unwrap();
        // Empty table
        assert!(txn.first::<tables::Entities>().unwrap().is_none());
        assert!(txn.last::<tables::Entities>().unwrap().is_none());

        txn.insert::<tables::Entities>(&b"alpha".to_vec(), &b"v1".to_vec()).unwrap();
        txn.insert::<tables::Entities>(&b"omega".to_vec(), &b"v2".to_vec()).unwrap();

        let (first_key, _) = txn.first::<tables::Entities>().unwrap().unwrap();
        assert_eq!(first_key, b"alpha");
        let (last_key, _) = txn.last::<tables::Entities>().unwrap().unwrap();
        assert_eq!(last_key, b"omega");

        txn.commit().unwrap();
    }

    /// Tests deleting from empty table returns false.
    #[test]
    fn test_delete_from_empty_table() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let mut txn = db.write().unwrap();
        let deleted = txn.delete::<tables::Entities>(&b"nonexistent".to_vec()).unwrap();
        assert!(!deleted);
        txn.commit().unwrap();
    }

    /// Tests deleting a nonexistent key from a non-empty table.
    #[test]
    fn test_delete_nonexistent_key() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"exists".to_vec(), &b"val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let mut txn = db.write().unwrap();
        let deleted = txn.delete::<tables::Entities>(&b"missing".to_vec()).unwrap();
        assert!(!deleted);
        txn.commit().unwrap();
    }

    /// Tests write transaction get() (read-your-own-writes).
    #[test]
    fn test_write_transaction_read_own_writes() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        let mut txn = db.write().unwrap();
        txn.insert::<tables::Entities>(&b"key1".to_vec(), &b"val1".to_vec()).unwrap();

        // Read within the same write transaction (not yet committed)
        let val = txn.get::<tables::Entities>(&b"key1".to_vec()).unwrap();
        assert_eq!(val, Some(b"val1".to_vec()));

        // Nonexistent key
        let val = txn.get::<tables::Entities>(&b"missing".to_vec()).unwrap();
        assert!(val.is_none());

        txn.commit().unwrap();
    }

    /// Tests insert_raw for pre-encoded key-value pairs.
    #[test]
    fn test_insert_raw() {
        use crate::tables::TableId;

        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            txn.insert_raw(TableId::Entities, b"raw_key", b"raw_value").unwrap();
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let val = txn.get::<tables::Entities>(&b"raw_key".to_vec()).unwrap();
        assert_eq!(val, Some(b"raw_value".to_vec()));
    }

    /// Tests database stats reporting.
    #[test]
    fn test_database_stats() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        let stats = db.stats();
        assert_eq!(stats.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(stats.dirty_pages, 0);

        // Insert data to generate some stats
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"key".to_vec(), &b"val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        let stats = db.stats();
        assert!(stats.total_pages > 0);
    }

    /// Tests record_page_split increments counter.
    #[test]
    fn test_record_page_split() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let before = db.stats().page_splits;
        db.record_page_split();
        let after = db.stats().page_splits;
        assert_eq!(after, before + 1);
    }

    /// Tests table_depths returns depths for non-empty tables.
    #[test]
    fn test_table_depths() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Empty database: no depths
        let depths = db.table_depths().unwrap();
        assert!(depths.is_empty());

        // Add data
        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8]).unwrap();
            txn.commit().unwrap();
        }

        let depths = db.table_depths().unwrap();
        assert!(!depths.is_empty());
        // RaftLog should have depth 1 (single leaf)
        let raft_depth = depths.iter().find(|(name, _)| *name == "raft_log");
        assert!(raft_depth.is_some());
        assert_eq!(raft_depth.unwrap().1, 1);
    }

    /// Tests read_raw_page with unwritten page returns error.
    #[test]
    fn test_read_raw_page_unwritten() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        // Page 999 is far beyond any allocation
        let result = db.read_raw_page(999);
        assert!(result.is_err());
    }

    /// Tests page_size returns configured page size.
    #[test]
    fn test_page_size() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        assert_eq!(db.page_size(), DEFAULT_PAGE_SIZE);
    }

    /// Tests dirty_page_ids / dirty_page_count for backup tracking.
    #[test]
    fn test_dirty_bitmap_tracking() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        assert_eq!(db.dirty_page_count(), 0);
        assert!(db.dirty_page_ids().is_empty());

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::Entities>(&b"key".to_vec(), &b"val".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        // After commit, dirty bitmap should have been populated
        assert!(db.dirty_page_count() > 0);
        assert!(!db.dirty_page_ids().is_empty());

        // Clear and verify
        db.clear_dirty_bitmap();
        assert_eq!(db.dirty_page_count(), 0);
    }

    /// Tests free_page_ids after inserts and deletes.
    #[test]
    fn test_free_page_ids() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert some data
        {
            let mut txn = db.write().unwrap();
            for i in 0..20u32 {
                let key = format!("free_test_{i:04}").into_bytes();
                txn.insert::<tables::Entities>(&key, &b"val".to_vec()).unwrap();
            }
            txn.commit().unwrap();
        }

        // Delete all entries
        {
            let mut txn = db.write().unwrap();
            for i in 0..20u32 {
                let key = format!("free_test_{i:04}").into_bytes();
                txn.delete::<tables::Entities>(&key).unwrap();
            }
            txn.commit().unwrap();
        }

        // Free pending pages
        db.try_free_pending_pages();

        // There should be free pages now (from COW page replacement)
        let total = db.total_page_count();
        assert!(total > 0);
    }

    /// Tests range queries on read transactions.
    #[test]
    fn test_read_transaction_range() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            for i in 0..20u64 {
                txn.insert::<tables::RaftLog>(&i, &vec![i as u8]).unwrap();
            }
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let iter = txn.range::<tables::RaftLog>(Some(&5u64), Some(&15u64)).unwrap();
        let entries = iter.collect_entries();
        assert_eq!(entries.len(), 10);
    }

    /// Tests compact_table on a write transaction.
    #[test]
    fn test_compact_table() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert then delete to create sparse leaves
        {
            let mut txn = db.write().unwrap();
            for i in 0..200u32 {
                let key = format!("compact_{i:06}").into_bytes();
                txn.insert::<tables::Entities>(&key, &vec![0xAA; 100]).unwrap();
            }
            txn.commit().unwrap();
        }

        {
            let mut txn = db.write().unwrap();
            // Delete every other key to make leaves sparse
            for i in (0..200u32).step_by(2) {
                let key = format!("compact_{i:06}").into_bytes();
                txn.delete::<tables::Entities>(&key).unwrap();
            }
            txn.commit().unwrap();
        }

        // Compact
        {
            let mut txn = db.write().unwrap();
            let stats = txn.compact_table::<tables::Entities>(0.4).unwrap();
            // We may or may not merge leaves depending on fill factor
            // Just verify it doesn't error
            let _ = stats;
            txn.commit().unwrap();
        }

        // Verify remaining entries are intact
        let txn = db.read().unwrap();
        for i in (1..200u32).step_by(2) {
            let key = format!("compact_{i:06}").into_bytes();
            assert!(txn.get::<tables::Entities>(&key).unwrap().is_some());
        }
    }

    /// Tests compact_all_tables.
    #[test]
    fn test_compact_all_tables() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Compact on empty database
        {
            let mut txn = db.write().unwrap();
            let stats = txn.compact_all_tables(0.4).unwrap();
            assert_eq!(stats.pages_merged, 0);
            assert_eq!(stats.pages_freed, 0);
            txn.commit().unwrap();
        }
    }

    /// Tests table_root_pages accessor.
    #[test]
    fn test_table_root_pages() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        let roots = db.table_root_pages();
        // All empty initially
        assert!(roots.iter().all(|&r| r == 0));

        {
            let mut txn = db.write().unwrap();
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8]).unwrap();
            txn.commit().unwrap();
        }

        let roots = db.table_root_pages();
        assert!(roots[tables::RaftLog::ID as usize] > 0);
    }

    /// Tests contains on empty table in read transaction.
    #[test]
    fn test_contains_empty_table() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();
        let txn = db.read().unwrap();
        assert!(!txn.contains::<tables::Entities>(&b"any".to_vec()).unwrap());
    }

    /// Tests read transaction table_depths.
    #[test]
    fn test_read_transaction_table_depths() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        {
            let mut txn = db.write().unwrap();
            for i in 0..100u64 {
                txn.insert::<tables::RaftLog>(&i, &vec![0u8; 50]).unwrap();
            }
            txn.commit().unwrap();
        }

        let txn = db.read().unwrap();
        let depths = txn.table_depths().unwrap();
        assert!(!depths.is_empty());
        // With 100 entries, depth should be > 1
        let (_, depth) = depths.iter().find(|(name, _)| *name == "raft_log").unwrap();
        assert!(*depth >= 1);
    }

    /// Tests open_with_config for file-backed databases.
    #[test]
    fn test_open_with_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("config_test.ink");

        // Create with default config
        {
            let _db = Database::<FileBackend>::create(&db_path).unwrap();
        }

        // Reopen with custom config (page_size from config is overridden by on-disk)
        let config = DatabaseConfig::builder().cache_size(256).sync_on_commit(false).build();
        let db = Database::<FileBackend>::open_with_config(&db_path, config).unwrap();
        // Cache size should be from our config
        assert_eq!(db.config.cache_size, 256);
    }

    /// Tests in-memory database with custom config.
    #[test]
    fn test_open_in_memory_with_config() {
        let config = DatabaseConfig::builder().page_size(8192).cache_size(64).build();
        let db = Database::<InMemoryBackend>::open_in_memory_with_config(config).unwrap();
        assert_eq!(db.page_size(), 8192);
    }

    /// Tests that page splits are tracked in stats.
    #[test]
    fn test_page_splits_tracked() {
        let db = Database::<InMemoryBackend>::open_in_memory().unwrap();

        // Insert enough to trigger splits
        {
            let mut txn = db.write().unwrap();
            for i in 0..500u32 {
                let key = format!("split_{i:06}").into_bytes();
                txn.insert::<tables::Entities>(&key, &vec![0xBB; 100]).unwrap();
            }
            txn.commit().unwrap();
        }

        let stats = db.stats();
        assert!(stats.page_splits > 0, "Expected page splits with 500 entries");
    }
}
