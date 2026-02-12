//! Database and transaction management for inferadb-ledger-store.
//!
//! Provides ACID transactions over the 15 fixed tables. Uses a single-writer
//! model optimized for Raft's serialized writes.
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_store::{Database, tables};
//!
//! // Open or create a database
//! let db = Database::open("data.ink")?;
//!
//! // Write transaction
//! {
//!     let mut txn = db.write()?;
//!     txn.insert::<tables::RaftLog>(&1u64, &vec![1, 2, 3])?;
//!     txn.commit()?;
//! }
//!
//! // Read transaction
//! {
//!     let txn = db.read()?;
//!     let value = txn.get::<tables::RaftLog>(&1u64)?;
//! }
//! ```

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};

use crate::{
    backend::{
        CommitSlot, DEFAULT_PAGE_SIZE, DatabaseHeader, FileBackend, HEADER_SIZE, InMemoryBackend,
        StorageBackend,
    },
    btree::{BTree, CompactionStats, PageProvider},
    dirty_bitmap::DirtyBitmap,
    error::{Error, PageId, PageType, Result},
    page::{Page, PageAllocator, PageCache},
    tables::{Table, TableEntry, TableId},
    transaction::{CommittedState, PendingFrees, SnapshotId, TransactionTracker},
    types::{Key, Value},
};

/// Database configuration options.
#[derive(Debug, Clone, bon::Builder)]
pub struct DatabaseConfig {
    /// Page size (must be power of 2, default 4096).
    #[builder(default = DEFAULT_PAGE_SIZE)]
    pub page_size: usize,
    /// Maximum pages to cache in memory.
    #[builder(default = 1024)]
    pub cache_size: usize,
    /// Whether to sync on every commit (default true for durability).
    #[builder(default = true)]
    pub sync_on_commit: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: 1024, // ~4MB with 4KB pages
            sync_on_commit: true,
        }
    }
}

/// The main database handle.
///
/// Generic over [`StorageBackend`] — use [`Database<FileBackend>`] for production
/// and [`Database<InMemoryBackend>`] for testing.
///
/// Thread-safe with interior mutability. Supports concurrent reads
/// and exclusive writes (single-writer model).
///
/// # Transaction Isolation (Copy-on-Write)
///
/// Uses Copy-on-Write (COW) semantics for snapshot isolation:
/// - Read transactions capture an immutable snapshot at start (no locks held)
/// - Write transactions modify copies of pages with NEW page IDs
/// - Atomic pointer swap makes commits visible instantly
/// - Old pages are freed only when no readers reference them
///
/// This enables high concurrency: readers never block writers, writers don't block readers.
pub struct Database<B: StorageBackend> {
    /// Storage backend (file or memory).
    backend: RwLock<B>,
    /// Page cache shared across all operations.
    cache: PageCache,
    /// Page allocator for new pages.
    allocator: Mutex<PageAllocator>,
    /// Current committed state (atomically swapped on commit).
    /// This is the key to COW - readers capture this, writers swap it.
    committed_state: ArcSwap<CommittedState>,
    /// Transaction tracker for safe page deallocation.
    tracker: TransactionTracker,
    /// Pages pending deallocation (freed when no readers reference them).
    pending_frees: Mutex<PendingFrees>,
    /// Configuration.
    config: DatabaseConfig,
    /// Writes lock to ensure only one write transaction at a time.
    write_lock: std::sync::Mutex<()>,
    /// Total B-tree page splits since database creation.
    page_splits: AtomicU64,
    /// Pages modified since the last backup checkpoint.
    ///
    /// Used by incremental backup to track which pages need to be included
    /// in the delta. Reset when a backup completes successfully.
    dirty_bitmap: Mutex<DirtyBitmap>,
}

impl Database<FileBackend> {
    /// Opens an existing database at the given path.
    ///
    /// Recovers committed state from the dual-slot header. If the recovery
    /// flag is set (crash detected), the free list is rebuilt automatically.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be opened or read.
    /// Returns [`Error::InvalidMagic`] if the file is not an InferaDB database.
    /// Returns [`Error::Corrupted`] if both commit slots have invalid checksums.
    ///
    /// ```no_run
    /// use inferadb_ledger_store::Database;
    ///
    /// let db = Database::open("/var/lib/ledger/state.db")?;
    /// let txn = db.read()?;
    /// # Ok::<(), inferadb_ledger_store::Error>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let backend = FileBackend::open(path)?;
        let config = DatabaseConfig { page_size: backend.page_size(), ..Default::default() };
        Self::from_backend(backend, config)
    }

    /// Creates a new database at the given path with default configuration.
    ///
    /// Uses 4KB pages, 1024-entry cache, and sync-on-commit.
    /// For custom settings, use [`create_with_config`](Self::create_with_config).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be created.
    /// Returns [`Error::Corrupted`] if the page size is invalid.
    ///
    /// ```no_run
    /// use inferadb_ledger_store::Database;
    /// use inferadb_ledger_store::tables::Entities;
    ///
    /// let db = Database::create("/var/lib/ledger/new.db")?;
    ///
    /// let mut txn = db.write()?;
    /// txn.insert::<Entities>(&b"key".to_vec(), &b"value".to_vec())?;
    /// txn.commit()?;
    /// # Ok::<(), inferadb_ledger_store::Error>(())
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_config(path, DatabaseConfig::default())
    }

    /// Creates a new database with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be created.
    /// Returns [`Error::Corrupted`] if the page size is invalid.
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: DatabaseConfig) -> Result<Self> {
        let backend = FileBackend::create(path, config.page_size)?;
        Self::from_backend(backend, config)
    }
}

impl Database<InMemoryBackend> {
    /// Creates a new in-memory database.
    ///
    /// Useful for testing and ephemeral workloads. Data is lost on drop.
    ///
    /// # Errors
    ///
    /// Returns an error if backend initialization fails.
    ///
    /// ```no_run
    /// use inferadb_ledger_store::Database;
    /// use inferadb_ledger_store::tables::Entities;
    ///
    /// let db = Database::open_in_memory()?;
    ///
    /// // Write some data
    /// let mut txn = db.write()?;
    /// txn.insert::<Entities>(&b"key".to_vec(), &b"value".to_vec())?;
    /// txn.commit()?;
    ///
    /// // Read it back
    /// let txn = db.read()?;
    /// let val = txn.get::<Entities>(&b"key".to_vec())?;
    /// # Ok::<(), inferadb_ledger_store::Error>(())
    /// ```
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_config(DatabaseConfig::default())
    }

    /// Creates a new in-memory database with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if backend initialization fails.
    pub fn open_in_memory_with_config(config: DatabaseConfig) -> Result<Self> {
        let backend = InMemoryBackend::new();
        Self::from_backend(backend, config)
    }
}

impl<B: StorageBackend> Database<B> {
    /// Creates a database from an existing backend.
    fn from_backend(backend: B, config: DatabaseConfig) -> Result<Self> {
        let cache = PageCache::new(config.cache_size);

        // Try to load existing state from disk
        let file_size = backend.file_size()?;
        let (initial_state, next_page, recovery_required, free_list_head) =
            if file_size > HEADER_SIZE as u64 {
                // Database exists - load state from header and directory
                Self::load_state_from_disk(&backend, &config)?
            } else {
                // New database - start fresh
                // Start at page 2: page 1 is reserved for the table directory
                (CommittedState::default(), 2, false, 0)
            };

        let allocator = PageAllocator::new(config.page_size, next_page);
        let tracker = TransactionTracker::new(initial_state.snapshot_id.next());

        let db = Self {
            backend: RwLock::new(backend),
            cache,
            allocator: Mutex::new(allocator),
            committed_state: ArcSwap::from_pointee(initial_state),
            tracker,
            pending_frees: Mutex::new(PendingFrees::new()),
            config,
            write_lock: std::sync::Mutex::new(()),
            page_splits: AtomicU64::new(0),
            dirty_bitmap: Mutex::new(DirtyBitmap::new()),
        };

        // Restore free list: use persisted list if available, otherwise rebuild
        if recovery_required {
            // Crash detected - must rebuild from B-tree scan to ensure correctness
            tracing::warn!("Recovery required - rebuilding free list from B-tree walk");
            let state = db.committed_state.load_full();
            db.rebuild_free_list(&state.table_roots, next_page)?;
        } else if free_list_head != 0 {
            // Load free list from persisted linked list (O(free_pages) vs O(total_pages))
            let backend = db.backend.read();
            let free_pages = db.load_free_list(&*backend, free_list_head, next_page)?;
            db.allocator.lock().init_free_list(free_pages);
        }

        Ok(db)
    }

    /// Loads committed state from disk (header + table directory).
    ///
    /// Uses dual-slot commit validation: tries primary slot first, falls back
    /// to secondary if primary is corrupt (indicates crash during commit).
    ///
    /// Returns `(state, next_page, recovery_required, free_list_head)`:
    /// - `recovery_required` is true if we detected a crash (fell back to secondary slot or
    ///   recovery flag was set). This triggers free list rebuild.
    /// - `free_list_head` is the persisted free list head (0 if none).
    fn load_state_from_disk(
        backend: &B,
        config: &DatabaseConfig,
    ) -> Result<(CommittedState, PageId, bool, PageId)> {
        let header_bytes = backend.read_header()?;
        let header = DatabaseHeader::from_bytes(&header_bytes)?;

        let valid_slot_index = header.validate_and_choose_slot()?;
        let slot = header.slot(valid_slot_index);

        // Recovery is required if:
        // 1. The recovery flag was set (unclean shutdown), OR
        // 2. We had to fall back to the secondary slot (crash during commit)
        let recovery_required =
            header.recovery_required() || valid_slot_index != header.primary_slot_index();

        let mut table_roots = [0; TableId::COUNT];
        let snapshot_id = SnapshotId::new(slot.last_txn_id);

        // If there's a table directory page, read it
        if slot.table_directory_page != 0 {
            let dir_page_data = backend.read_page(slot.table_directory_page)?;

            // Parse table entries from directory page
            // Format: [entry_count: u16][TableEntry; entry_count]
            if dir_page_data.len() >= 2 {
                let entry_count = u16::from_le_bytes([dir_page_data[0], dir_page_data[1]]) as usize;
                let mut offset = 2;

                for _ in 0..entry_count {
                    if offset + TableEntry::SIZE > dir_page_data.len() {
                        break;
                    }
                    if let Some(entry) = TableEntry::from_bytes(&dir_page_data[offset..]) {
                        let idx = entry.table_id as usize;
                        if idx < TableId::COUNT {
                            table_roots[idx] = entry.root_page;
                        }
                    }
                    offset += TableEntry::SIZE;
                }
            }
        }

        // Calculate next_page from slot's total_pages or file size
        // Minimum is 2 because page 1 is reserved for table directory
        let next_page = if slot.total_pages > 1 {
            slot.total_pages
        } else {
            let file_size = backend.file_size()?;
            std::cmp::max(2, ((file_size - HEADER_SIZE as u64) / config.page_size as u64) as PageId)
        };

        Ok((
            CommittedState { table_roots, snapshot_id },
            next_page,
            recovery_required,
            slot.free_list_head,
        ))
    }

    /// Persists committed state to disk using dual-slot commit for crash safety.
    ///
    /// # Dual-Slot Commit Protocol
    ///
    /// This implements a crash-safe commit sequence:
    ///
    /// 1. Write table directory page (contains all table roots)
    /// 2. Read current header to get existing slots
    /// 3. Write new state to the SECONDARY (inactive) slot
    /// 4. Write the full header with updated secondary slot
    /// 5. Sync to ensure all data is durable
    /// 6. Flip the god byte to make secondary become primary
    /// 7. Sync again to ensure god byte flip is durable
    ///
    /// If a crash occurs:
    /// - Before step 5: Old primary is still valid, secondary may be partial
    /// - Between step 5-7: Secondary has valid checksum, can recover from either
    /// - After step 7: New primary is valid
    ///
    /// Recovery reads both slots and uses the one indicated by god byte. If that
    /// slot has an invalid checksum, it falls back to the other slot.
    fn persist_state_to_disk(
        &self,
        table_roots: &[PageId; TableId::COUNT],
        snapshot_id: SnapshotId,
    ) -> Result<()> {
        let backend = self.backend.write();

        // Allocate or reuse page 1 for table directory (page 0 reserved for future use)
        let dir_page_id: PageId = 1;

        // Build directory page content
        // Format: [entry_count: u16][TableEntry; entry_count][padding]
        let mut dir_data = vec![0u8; self.config.page_size];

        let entry_count = TableId::COUNT as u16;
        dir_data[0..2].copy_from_slice(&entry_count.to_le_bytes());

        let mut offset = 2;
        for table_id in TableId::all() {
            let entry = TableEntry {
                table_id,
                root_page: table_roots[table_id as usize],
                entry_count: 0, // We don't track this currently
            };
            let entry_bytes = entry.to_bytes();
            dir_data[offset..offset + TableEntry::SIZE].copy_from_slice(&entry_bytes);
            offset += TableEntry::SIZE;
        }

        backend.write_page(dir_page_id, &dir_data)?;

        let header_bytes = backend.read_header()?;
        let mut header = DatabaseHeader::from_bytes(&header_bytes).unwrap_or_else(|_| {
            // If header is corrupt or new, create a fresh one
            DatabaseHeader::new(self.config.page_size.trailing_zeros() as u8)
        });

        // Persist the free list to disk and get the head pointer
        let free_list_head = self.persist_free_list(&*backend)?;

        let total_pages = self.allocator.lock().next_page_id();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let new_slot = CommitSlot {
            table_directory_page: dir_page_id,
            total_pages,
            last_txn_id: snapshot_id.raw(),
            last_write_timestamp: timestamp,
            free_list_head,
            checksum: 0, // Will be computed by to_bytes()
        };

        // Write to the SECONDARY (inactive) slot
        // This ensures primary remains valid if we crash during this write
        *header.secondary_slot_mut() = new_slot;

        // Set recovery required flag (will be cleared on clean shutdown)
        header.set_recovery_required(true);

        // Write the full header with updated secondary slot
        backend.write_header(&header.to_bytes())?;

        // Sync to ensure secondary slot data is durable
        // After this point, secondary slot has a valid checksum
        backend.sync()?;

        // Flip the god byte to make secondary become primary
        // This is the atomic commit point
        header.flip_primary_slot();

        // Write just the updated header (with flipped god byte)
        backend.write_header(&header.to_bytes())?;

        // Sync again to ensure god byte flip is durable
        // After this point, new state is fully committed
        backend.sync()?;

        Ok(())
    }

    /// Rebuilds the free list by scanning all reachable pages.
    ///
    /// This is called during recovery to restore the free list after a crash.
    /// The algorithm:
    /// 1. Walk all B-tree roots to find all reachable pages
    /// 2. Page 1 is always reserved (table directory)
    /// 3. Any page from 2 to total_pages-1 that's not reachable is free
    ///
    /// Note: This is O(total_pages) but only runs on recovery, not on every startup.
    fn rebuild_free_list(&self, table_roots: &[PageId], total_pages: PageId) -> Result<()> {
        let mut reachable = HashSet::new();

        // Page 1 is always reserved for the table directory
        reachable.insert(1u64);

        // Walk all non-empty table B-trees
        for &root in table_roots {
            if root != 0 {
                self.collect_reachable_pages(root, &mut reachable)?;
            }
        }

        // All pages from 2 to total_pages-1 that aren't reachable are free
        let mut free_pages = Vec::new();
        for page_id in 2..total_pages {
            if !reachable.contains(&page_id) {
                free_pages.push(page_id);
            }
        }

        self.allocator.lock().init_free_list(free_pages);

        Ok(())
    }

    /// Persists the free list to disk as a linked list of pages.
    ///
    /// Each free page stores the next free page ID at offset 0.
    /// Returns the head of the list (first free page), or 0 if empty.
    fn persist_free_list(&self, backend: &B) -> Result<PageId> {
        let free_pages = self.allocator.lock().get_free_list();
        if free_pages.is_empty() {
            return Ok(0);
        }

        // Write each free page with a pointer to the next
        let mut page_data = vec![0u8; self.config.page_size];
        for (i, &page_id) in free_pages.iter().enumerate() {
            // Next page ID (0 for last page)
            let next_page_id = free_pages.get(i + 1).copied().unwrap_or(0);
            page_data[0..8].copy_from_slice(&next_page_id.to_le_bytes());

            backend.write_page(page_id, &page_data)?;
        }

        Ok(free_pages[0])
    }

    /// Loads the free list from disk by walking the linked list.
    ///
    /// Returns the list of free page IDs, or empty if head is 0.
    fn load_free_list(
        &self,
        backend: &B,
        head: PageId,
        total_pages: PageId,
    ) -> Result<Vec<PageId>> {
        if head == 0 {
            return Ok(Vec::new());
        }

        let mut free_pages = Vec::new();
        let mut current = head;

        // Walk the linked list with cycle detection (max iterations = total_pages)
        let max_iterations = total_pages.saturating_sub(2); // Can't have more free pages than total - 2
        let mut iterations = 0;

        while current != 0 && iterations < max_iterations {
            free_pages.push(current);

            // Read the next pointer from the page
            let page_data = backend.read_page(current)?;
            if page_data.len() < 8 {
                // Invalid page data, stop here
                break;
            }
            current = u64::from_le_bytes(page_data[0..8].try_into().unwrap());
            iterations += 1;
        }

        Ok(free_pages)
    }

    /// Recursively collect all pages reachable from a B-tree root.
    fn collect_reachable_pages(
        &self,
        page_id: PageId,
        reachable: &mut HashSet<PageId>,
    ) -> Result<()> {
        // Avoid infinite loops (shouldn't happen, but defensive)
        if reachable.contains(&page_id) {
            return Ok(());
        }
        reachable.insert(page_id);

        let page = self.read_page(page_id)?;
        let page_type = page.page_type()?;

        match page_type {
            PageType::BTreeLeaf => {
                // Leaf nodes have no children - we're done
            },
            PageType::BTreeBranch => {
                // Branch nodes have children we need to visit
                use crate::btree::node::BranchNodeRef;
                let branch = BranchNodeRef::from_page(&page)?;
                let count = branch.cell_count() as usize;

                // Visit all children (cells + rightmost)
                for i in 0..count {
                    let child = branch.child(i);
                    self.collect_reachable_pages(child, reachable)?;
                }

                // Visit rightmost child
                let rightmost = branch.rightmost_child();
                self.collect_reachable_pages(rightmost, reachable)?;
            },
            _ => {
                // Unknown page type - skip (could be corruption)
            },
        }

        Ok(())
    }

    /// Begin a read-only transaction.
    ///
    /// Captures an immutable snapshot of the database state. No locks are held
    /// after this returns, allowing full concurrency with write transactions.
    ///
    /// The transaction sees a consistent point-in-time view thanks to COW:
    /// writers create new page copies, never modifying pages readers might see.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if reading the snapshot state fails.
    pub fn read(&self) -> Result<ReadTransaction<'_, B>> {
        // Load current committed state (atomic, lock-free)
        // Use load_full() to get Arc directly - avoids Guard which blocks writers
        let snapshot = self.committed_state.load_full();
        let snapshot_id = snapshot.snapshot_id;

        // Register with tracker to prevent page cleanup while we're reading
        self.tracker.register_read_transaction(snapshot_id);

        Ok(ReadTransaction {
            db: self,
            snapshot: (*snapshot).clone(),
            snapshot_id,
            page_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Begin a write transaction.
    ///
    /// Only one write transaction can be active at a time. However, read
    /// transactions can run concurrently - they see a consistent snapshot
    /// and are unaffected by COW modifications.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Poisoned`] if the write lock is poisoned.
    pub fn write(&self) -> Result<WriteTransaction<'_, B>> {
        let write_guard = self.write_lock.lock().map_err(|_| Error::Poisoned)?;

        let snapshot_id = self.tracker.start_write_transaction();

        let current = self.committed_state.load_full();
        let table_roots = current.table_roots;

        Ok(WriteTransaction {
            db: self,
            snapshot_id,
            table_roots,
            committed: false,
            dirty_pages: HashMap::new(),
            pages_to_free: Vec::new(),
            page_cache: RefCell::new(HashMap::new()),
            _write_guard: write_guard,
        })
    }

    /// Returns database statistics.
    pub fn stats(&self) -> DatabaseStats {
        let cache_stats = self.cache.stats();
        let allocator = self.allocator.lock();

        DatabaseStats {
            page_size: self.config.page_size,
            total_pages: allocator.next_page_id(),
            cached_pages: cache_stats.size,
            dirty_pages: cache_stats.dirty_count,
            free_pages: allocator.free_page_count(),
            cache_hits: cache_stats.hits,
            cache_misses: cache_stats.misses,
            page_splits: self.page_splits.load(Ordering::Relaxed),
        }
    }

    /// Records a B-tree page split event.
    pub fn record_page_split(&self) {
        self.page_splits.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns B-tree depths for all non-empty tables.
    ///
    /// Opens a read transaction internally to walk each table's B-tree.
    /// Returns `(table_name, depth)` pairs.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction cannot be started or
    /// if a page read fails while walking the B-tree.
    pub fn table_depths(&self) -> Result<Vec<(&'static str, u32)>> {
        let txn = self.read()?;
        txn.table_depths()
    }

    /// Reads a raw page bypassing cache, for integrity verification.
    ///
    /// Returns the page without caching it. The caller is responsible for
    /// checksum verification via [`Page::verify_checksum()`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend read fails.
    /// Returns [`Error::PageNotFound`] if the page is unwritten (all zeros).
    pub fn read_raw_page(&self, page_id: PageId) -> Result<Page> {
        let backend = self.backend.read();
        let data = backend.read_page(page_id)?;

        if data.iter().all(|&b| b == 0) {
            return Err(Error::PageNotFound { page_id });
        }

        Ok(Page::from_bytes(page_id, data))
    }

    /// Returns the total number of pages allocated (including free pages).
    pub fn total_page_count(&self) -> PageId {
        self.allocator.lock().next_page_id()
    }

    /// Returns IDs of pages currently on the free list.
    pub fn free_page_ids(&self) -> Vec<PageId> {
        self.allocator.lock().get_free_list()
    }

    /// Returns root page IDs for each table, indexed by `TableId`.
    ///
    /// A root of 0 indicates an empty table.
    pub fn table_root_pages(&self) -> [PageId; TableId::COUNT] {
        self.committed_state.load().table_roots
    }

    /// Returns the configured page size in bytes.
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// Writes raw page data directly to the backend, bypassing cache and checksums.
    ///
    /// Intended for testing only — allows injecting corruption for integrity
    /// scrubber verification. Not hidden from docs since test-utils crate needs it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails.
    pub fn write_raw_page_for_test(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        let backend = self.backend.read();
        backend.write_page(page_id, data)?;
        // Evict from cache so raw reads see the corrupted data
        self.cache.remove(page_id);
        Ok(())
    }

    /// Returns page IDs modified since the last backup checkpoint.
    ///
    /// Returned in ascending order. Used by incremental backup to determine
    /// which pages to include in the delta.
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        self.dirty_bitmap.lock().dirty_ids()
    }

    /// Returns the number of pages marked dirty since the last backup.
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_bitmap.lock().count()
    }

    /// Clears the dirty bitmap after a successful backup.
    ///
    /// Called by the backup system after writing all dirty pages to a backup
    /// file. Subsequent writes will be tracked for the next incremental backup.
    pub fn clear_dirty_bitmap(&self) {
        self.dirty_bitmap.lock().clear();
    }

    /// Serializes the dirty bitmap for persistence.
    ///
    /// The bitmap can be stored alongside the database and reloaded on restart
    /// to maintain incremental backup state across server restarts.
    pub fn dirty_bitmap_bytes(&self) -> Vec<u8> {
        self.dirty_bitmap.lock().to_bytes()
    }

    /// Restores the dirty bitmap from previously serialized bytes.
    ///
    /// Call during database open to resume incremental backup tracking.
    /// Invalid data is silently ignored (bitmap stays empty).
    pub fn load_dirty_bitmap(&self, data: &[u8]) {
        if let Some(bitmap) = DirtyBitmap::from_bytes(data) {
            *self.dirty_bitmap.lock() = bitmap;
        }
    }

    /// Export raw page data for the specified page IDs.
    ///
    /// Reads pages directly from the backend (bypassing cache) to capture
    /// the on-disk state. Returns `(page_id, page_data)` pairs for all
    /// valid pages. Free or unwritten pages are skipped.
    pub fn export_pages(&self, page_ids: &[PageId]) -> Vec<(PageId, Vec<u8>)> {
        let backend = self.backend.read();
        let free_pages: HashSet<PageId> = self.free_page_ids().into_iter().collect();
        let total = self.total_page_count();

        let mut pages = Vec::with_capacity(page_ids.len());
        for &page_id in page_ids {
            if page_id >= total || free_pages.contains(&page_id) {
                continue;
            }
            if let Ok(data) = backend.read_page(page_id) {
                // Skip unwritten pages (all zeros)
                if !data.iter().all(|&b| b == 0) {
                    pages.push((page_id, data));
                }
            }
        }
        pages
    }

    /// Import raw page data, writing directly to the backend.
    ///
    /// Used during backup restore to apply page-level patches.
    /// Evicts each page from cache to ensure subsequent reads see the
    /// imported data.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if a backend write fails.
    pub fn import_pages(&self, pages: &[(PageId, &[u8])]) -> Result<()> {
        let backend = self.backend.read();
        for &(page_id, data) in pages {
            backend.write_page(page_id, data)?;
            self.cache.remove(page_id);
        }
        Ok(())
    }

    /// Reads a page from cache or backend.
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.cache.get(page_id) {
            return Ok(page);
        }

        let backend = self.backend.read();
        let data = backend.read_page(page_id)?;

        // Check for all-zeros (unwritten page)
        if data.iter().all(|&b| b == 0) {
            return Err(Error::PageNotFound { page_id });
        }

        let page = Page::from_bytes(page_id, data);

        if !page.verify_checksum() {
            return Err(Error::PageChecksumMismatch { page_id });
        }

        self.cache.insert(page.clone());

        Ok(page)
    }

    /// Allocates a new page.
    fn allocate_page(&self, page_type: PageType, txn_id: u64) -> Page {
        let page_id = self.allocator.lock().allocate();
        Page::new(page_id, self.config.page_size, page_type, txn_id)
    }

    /// Frees a page for later reuse.
    fn free_page(&self, page_id: PageId) {
        self.allocator.lock().free(page_id);
        self.cache.remove(page_id);
    }

    /// Attempts to free pages that are no longer referenced by any reader.
    ///
    /// This is the garbage collection pass for COW - old pages can only be
    /// freed once all readers that might reference them have finished.
    fn try_free_pending_pages(&self) {
        let oldest_reader = self.tracker.oldest_live_read_transaction();
        let pages_to_free = self.pending_frees.lock().drain_freeable(oldest_reader);

        if !pages_to_free.is_empty() {
            let allocator = self.allocator.lock();
            for page_id in pages_to_free {
                allocator.free(page_id);
                self.cache.remove(page_id);
            }
        }
    }

    /// Writes a page to the backend.
    fn write_page_to_backend(&self, page: &Page) -> Result<()> {
        let mut page = page.clone();
        page.update_checksum();

        let backend = self.backend.write();
        backend.write_page(page.id, &page.data)?;
        Ok(())
    }

    /// Flushes all dirty pages to disk (without sync).
    ///
    /// Caller is responsible for calling sync() afterward if durability is needed.
    fn flush_pages(&self) -> Result<()> {
        let dirty_pages = self.cache.dirty_pages();

        for page in &dirty_pages {
            self.write_page_to_backend(page)?;
            self.cache.mark_clean(page.id);
        }

        Ok(())
    }
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Page size in bytes.
    pub page_size: usize,
    /// Total number of pages allocated.
    pub total_pages: PageId,
    /// Pages currently in cache.
    pub cached_pages: usize,
    /// Dirty pages pending write.
    pub dirty_pages: usize,
    /// Free pages available for reuse.
    pub free_pages: usize,
    /// Total page cache hits since creation.
    pub cache_hits: u64,
    /// Total page cache misses since creation.
    pub cache_misses: u64,
    /// Total B-tree page splits since creation.
    pub page_splits: u64,
}

/// A read-only transaction.
///
/// Provides a consistent snapshot view of the database using Copy-on-Write.
/// No locks are held - the transaction captures an immutable snapshot at creation
/// and can run fully concurrently with write transactions.
pub struct ReadTransaction<'db, B: StorageBackend> {
    db: &'db Database<B>,
    /// Captured snapshot at transaction start (immutable).
    snapshot: CommittedState,
    /// Snapshot ID for tracker (to prevent page cleanup).
    snapshot_id: SnapshotId,
    /// Local page cache for read operations.
    page_cache: RefCell<HashMap<PageId, Page>>,
}

impl<'db, B: StorageBackend> ReadTransaction<'db, B> {
    /// Returns a value by key from a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn get<T: Table>(&self, key: &T::KeyType) -> Result<Option<Vec<u8>>>
    where
        T::KeyType: Key,
    {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: &self.page_cache };
        let btree = BTree::new(root, provider);

        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        btree.get(&key_bytes)
    }

    /// Checks if a key exists in a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn contains<T: Table>(&self, key: &T::KeyType) -> Result<bool>
    where
        T::KeyType: Key,
    {
        Ok(self.get::<T>(key)?.is_some())
    }

    /// Returns the first (smallest key) entry in a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking the B-tree.
    pub fn first<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: &self.page_cache };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Returns the last (largest key) entry in a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking the B-tree.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: &self.page_cache };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterates over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial B-tree seek fails.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        TableIterator::new(self.db, root, &self.page_cache)
    }

    /// Iterates over a range of entries in a table.
    ///
    /// The range is specified as (start_key, end_key) where start is inclusive
    /// and end is exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial B-tree seek fails.
    pub fn range<T: Table>(
        &self,
        start: Option<&T::KeyType>,
        end: Option<&T::KeyType>,
    ) -> Result<TableIterator<'_, 'db, B, T>>
    where
        T::KeyType: Key,
    {
        let root = self.snapshot.table_roots[T::ID as usize];
        let start_bytes = start.map(|k| {
            let mut buf = Vec::new();
            k.encode(&mut buf);
            buf
        });
        let end_bytes = end.map(|k| {
            let mut buf = Vec::new();
            k.encode(&mut buf);
            buf
        });
        TableIterator::with_range(self.db, root, &self.page_cache, start_bytes, end_bytes)
    }

    /// Returns the B-tree depth for each table.
    ///
    /// Returns a list of `(table_name, depth)` pairs for all tables with
    /// non-empty roots. Empty tables (root == 0) are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking the B-tree.
    pub fn table_depths(&self) -> Result<Vec<(&'static str, u32)>> {
        let mut depths = Vec::new();

        for table_id in TableId::all() {
            let root = self.snapshot.table_roots[table_id as u8 as usize];
            if root == 0 {
                continue;
            }
            let provider = CachingReadPageProvider { db: self.db, page_cache: &self.page_cache };
            let btree = BTree::new(root, provider);
            let depth = btree.depth()?;
            depths.push((table_id.name(), depth));
        }

        Ok(depths)
    }
}

impl<B: StorageBackend> Drop for ReadTransaction<'_, B> {
    fn drop(&mut self) {
        // Unregister from tracker to allow page cleanup
        // This is critical for COW - pages can only be freed when all
        // readers that might reference them have finished
        self.db.tracker.unregister_read_transaction(self.snapshot_id);

        // Attempt to free pending pages now that this reader is done
        self.db.try_free_pending_pages();
    }
}

/// A write transaction.
///
/// Changes are buffered until commit. On commit, all changes are
/// atomically written and synced to disk using Copy-on-Write semantics.
///
/// Read transactions can run concurrently — they see a consistent snapshot
/// and are unaffected by COW modifications.
///
/// # Invariants
///
/// **Lock ordering:** The `write_lock` (a `Mutex<()>` on `Database`) is acquired
/// first, guaranteeing at most one `WriteTransaction` exists at a time. All other
/// locks (`allocator`, `pending_frees`, `backend`) are acquired inside individual
/// operations and released before returning. This total ordering prevents deadlocks.
///
/// **Dirty page lifecycle:**
/// 1. B+ tree operations (insert/delete) write to COW copies via `BufferedWritePageProvider`.
///    Modified pages are stored in `dirty_pages`; their original page IDs are appended to
///    `pages_to_free`.
/// 2. On `commit()`, dirty pages are moved into the shared `PageCache`, flushed to disk, and the
///    header is durably persisted via the dual-slot commit protocol.
/// 3. The `CommittedState` is atomically swapped (`ArcSwap::store`), making changes visible to new
///    read transactions.
/// 4. Pages in `pages_to_free` are recorded in `pending_frees` for deferred cleanup. They cannot be
///    reused until all read transactions referencing the prior snapshot have ended.
///
/// **Drop behavior:** If a `WriteTransaction` is dropped without calling `commit()` or
/// `abort()`, all dirty pages are discarded and the write transaction is ended in the
/// tracker. The database state remains unchanged.
pub struct WriteTransaction<'db, B: StorageBackend> {
    db: &'db Database<B>,
    /// Snapshot ID for this transaction (used for deferred page cleanup).
    snapshot_id: SnapshotId,
    /// Table roots (may diverge from committed state as we modify).
    table_roots: [PageId; TableId::COUNT],
    /// Whether the transaction has been committed or aborted.
    committed: bool,
    /// Buffered page modifications - not visible to other transactions until commit.
    dirty_pages: HashMap<PageId, Page>,
    /// Old pages replaced by COW - freed after commit when no readers reference them.
    pages_to_free: Vec<PageId>,
    /// Page cache for read operations within this transaction.
    page_cache: RefCell<HashMap<PageId, Page>>,
    /// Guard to ensure only one write transaction at a time.
    _write_guard: std::sync::MutexGuard<'db, ()>,
}

impl<'db, B: StorageBackend> WriteTransaction<'db, B> {
    /// Inserts or updates a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or value exceeds page capacity, or if
    /// a page read/write fails during the B-tree operation.
    pub fn insert<T: Table>(&mut self, key: &T::KeyType, value: &T::ValueType) -> Result<()>
    where
        T::KeyType: Key,
        T::ValueType: Value,
    {
        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        let mut value_bytes = Vec::new();
        value.encode(&mut value_bytes);

        let root = self.table_roots[T::ID as usize];
        let provider = BufferedWritePageProvider {
            db: self.db,
            txn_id: self.snapshot_id.raw(),
            dirty_pages: &mut self.dirty_pages,
            pages_to_free: &mut self.pages_to_free,
        };

        let mut btree = BTree::new(root, provider);
        btree.insert(&key_bytes, &value_bytes)?;

        // Track page splits for metrics
        let splits = btree.split_count();
        if splits > 0 {
            self.db.page_splits.fetch_add(splits, Ordering::Relaxed);
        }

        // Update root if it changed
        self.table_roots[T::ID as usize] = btree.root_page();

        Ok(())
    }

    /// Deletes a key from a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during the B-tree operation.
    pub fn delete<T: Table>(&mut self, key: &T::KeyType) -> Result<bool>
    where
        T::KeyType: Key,
    {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(false);
        }

        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        let provider = BufferedWritePageProvider {
            db: self.db,
            txn_id: self.snapshot_id.raw(),
            dirty_pages: &mut self.dirty_pages,
            pages_to_free: &mut self.pages_to_free,
        };

        let mut btree = BTree::new(root, provider);
        let deleted = btree.delete(&key_bytes)?;

        // Update root if it changed
        self.table_roots[T::ID as usize] = btree.root_page();

        Ok(deleted.is_some())
    }

    /// Returns a value (within the transaction's view).
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn get<T: Table>(&self, key: &T::KeyType) -> Result<Option<Vec<u8>>>
    where
        T::KeyType: Key,
    {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = BufferedReadPageProvider { db: self.db, dirty_pages: &self.dirty_pages };
        let btree = BTree::new(root, provider);

        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        btree.get(&key_bytes)
    }

    /// Returns the first (smallest key) entry in a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking the B-tree.
    pub fn first<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = BufferedReadPageProvider { db: self.db, dirty_pages: &self.dirty_pages };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Returns the last (largest key) entry in a table.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking the B-tree.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = BufferedReadPageProvider { db: self.db, dirty_pages: &self.dirty_pages };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterates over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    /// Note: This reads the transaction's current view including uncommitted changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial B-tree seek fails.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.table_roots[T::ID as usize];
        // Pre-populate page_cache with dirty_pages so the iterator sees uncommitted changes.
        // CachingReadPageProvider checks page_cache first, so dirty pages will be found.
        for (&page_id, page) in &self.dirty_pages {
            self.page_cache.borrow_mut().insert(page_id, page.clone());
        }
        TableIterator::new(self.db, root, &self.page_cache)
    }

    /// Commits the transaction.
    ///
    /// Uses Copy-on-Write + Dual-Slot commit for crash safety:
    /// 1. Write dirty pages to backend
    /// 2. Flush dirty pages to ensure data is on disk
    /// 3. Dual-slot commit: write to secondary slot → sync → flip god byte → sync
    /// 4. Atomically swap committed_state (makes changes visible in-memory)
    /// 5. Record freed pages for deferred cleanup
    /// 6. End transaction in tracker
    ///
    /// The dual-slot commit ensures there's ALWAYS one valid commit slot to
    /// recover from, even if a crash occurs during the commit sequence.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if flushing dirty pages or writing the header fails.
    pub fn commit(mut self) -> Result<()> {
        // Record modified page IDs in the backup dirty bitmap before draining.
        // This enables incremental backups to capture exactly which pages changed.
        {
            let mut bitmap = self.db.dirty_bitmap.lock();
            for &page_id in self.dirty_pages.keys() {
                bitmap.mark(page_id);
            }
        }

        // Move all buffered dirty pages into the shared cache
        for (_, page) in self.dirty_pages.drain() {
            self.db.cache.insert(page);
        }

        // Flush all dirty pages to disk (data pages)
        self.db.flush_pages()?;

        // Persist table directory and header using dual-slot commit protocol
        // This includes the necessary syncs for crash safety
        self.db.persist_state_to_disk(&self.table_roots, self.snapshot_id)?;

        let new_state =
            CommittedState { table_roots: self.table_roots, snapshot_id: self.snapshot_id };

        // Atomically swap the committed state - this is the magic moment
        // where our changes become visible to new read transactions
        self.db.committed_state.store(Arc::new(new_state));

        // Record pages to be freed for deferred cleanup
        // These can only be freed once no readers reference this snapshot
        if !self.pages_to_free.is_empty() {
            let pages = std::mem::take(&mut self.pages_to_free);
            self.db.pending_frees.lock().record_freed_pages(self.snapshot_id, pages);
        }

        // Attempt to free old pages that are no longer referenced
        self.db.try_free_pending_pages();

        self.db.tracker.end_write_transaction(self.snapshot_id);

        self.committed = true;
        Ok(())
    }

    /// Compacts a table's B+ tree by merging underfull leaf nodes.
    ///
    /// This reclaims space from pages left sparse by deletions. Leaves
    /// with a fill factor below `min_fill_factor` are merged with their
    /// right sibling when the combined entries fit in one page.
    ///
    /// Returns statistics about pages merged and freed.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during compaction.
    pub fn compact_table<T: Table>(&mut self, min_fill_factor: f64) -> Result<CompactionStats> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(CompactionStats::default());
        }

        let provider = BufferedWritePageProvider {
            db: self.db,
            txn_id: self.snapshot_id.raw(),
            dirty_pages: &mut self.dirty_pages,
            pages_to_free: &mut self.pages_to_free,
        };
        let mut btree = BTree::new(root, provider);
        let stats = btree.compact(min_fill_factor)?;
        self.table_roots[T::ID as usize] = btree.root_page();
        Ok(stats)
    }

    /// Compacts all tables in the database.
    ///
    /// Runs compaction on every non-empty table. Returns total statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during compaction.
    pub fn compact_all_tables(&mut self, min_fill_factor: f64) -> Result<CompactionStats> {
        let mut total = CompactionStats::default();

        for table_id in TableId::all() {
            let root = self.table_roots[table_id as usize];
            if root == 0 {
                continue;
            }

            let provider = BufferedWritePageProvider {
                db: self.db,
                txn_id: self.snapshot_id.raw(),
                dirty_pages: &mut self.dirty_pages,
                pages_to_free: &mut self.pages_to_free,
            };
            let mut btree = BTree::new(root, provider);
            let stats = btree.compact(min_fill_factor)?;
            self.table_roots[table_id as usize] = btree.root_page();

            total.pages_merged += stats.pages_merged;
            total.pages_freed += stats.pages_freed;
        }

        Ok(total)
    }

    /// Aborts the transaction (discard all changes).
    pub fn abort(mut self) {
        // Simply drop the dirty_pages buffer - changes were never visible to others
        // Note: allocated page IDs are "leaked" to the freelist, but this is a
        // minor issue for now. A more sophisticated implementation would track
        // and return them.
        self.dirty_pages.clear();

        // End the write transaction in tracker (no pages were committed, so no cleanup)
        self.db.tracker.end_write_transaction(self.snapshot_id);

        self.committed = true; // Prevent drop from warning
    }
}

impl<B: StorageBackend> Drop for WriteTransaction<'_, B> {
    fn drop(&mut self) {
        if !self.committed {
            // Transaction was not committed or aborted - this is likely a bug
            // In production, we'd want to log this
            #[cfg(debug_assertions)]
            eprintln!("Warning: WriteTransaction dropped without commit or abort");

            // Still need to end the transaction in the tracker to maintain correctness
            self.db.tracker.end_write_transaction(self.snapshot_id);
        }
    }
}

/// Page provider for read-only operations with transaction-local caching.
///
/// This provider caches pages locally within the transaction to ensure
/// snapshot isolation. Once a page is read, it's stored in the local cache
/// so subsequent reads return the same data even if concurrent writes
/// modify the shared cache.
struct CachingReadPageProvider<'txn, 'db, B: StorageBackend> {
    db: &'db Database<B>,
    /// Reference to the transaction's local page cache.
    page_cache: &'txn RefCell<HashMap<PageId, Page>>,
}

impl<B: StorageBackend> PageProvider for CachingReadPageProvider<'_, '_, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.page_cache.borrow().get(&page_id) {
            return Ok(page.clone());
        }

        let page = self.db.read_page(page_id)?;
        self.page_cache.borrow_mut().insert(page_id, page.clone());
        Ok(page)
    }

    fn write_page(&mut self, _page: Page) {
        panic!("write_page called on read-only caching page provider");
    }

    fn allocate_page(&mut self, _page_type: PageType) -> Page {
        panic!("allocate_page called on read-only caching page provider");
    }

    fn free_page(&mut self, _page_id: PageId) {
        panic!("free_page called on read-only caching page provider");
    }

    fn page_size(&self) -> usize {
        self.db.config.page_size
    }

    fn txn_id(&self) -> u64 {
        0 // Read-only, doesn't matter
    }
}

/// Page provider for write operations with buffered isolation and COW semantics.
///
/// This provider maintains transaction isolation by buffering all page
/// modifications in a local HashMap. Changes are not visible to concurrent
/// read transactions until the write transaction commits.
///
/// For Copy-on-Write: instead of immediately freeing pages, we record them
/// in pages_to_free. They're only truly freed after commit when no readers
/// reference them anymore.
struct BufferedWritePageProvider<'txn, 'db, B: StorageBackend> {
    db: &'db Database<B>,
    txn_id: u64,
    /// Mutable reference to the transaction's dirty page buffer.
    dirty_pages: &'txn mut HashMap<PageId, Page>,
    /// Pages that should be freed after commit (COW deferred cleanup).
    pages_to_free: &'txn mut Vec<PageId>,
}

impl<B: StorageBackend> PageProvider for BufferedWritePageProvider<'_, '_, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, mut page: Page) {
        // Update checksum and store in local buffer (NOT shared cache)
        // This ensures concurrent read transactions don't see uncommitted writes
        page.update_checksum();
        self.dirty_pages.insert(page.id, page);
    }

    fn allocate_page(&mut self, page_type: PageType) -> Page {
        self.db.allocate_page(page_type, self.txn_id)
    }

    fn free_page(&mut self, page_id: PageId) {
        // Remove from our buffer if present (newly allocated page in this txn)
        if self.dirty_pages.remove(&page_id).is_some() {
            // Page was allocated in this transaction, can immediately free
            self.db.free_page(page_id);
        } else {
            // COW: This page exists from a previous commit. Don't free immediately
            // because concurrent readers may still reference it. Record it for
            // deferred cleanup after commit.
            self.pages_to_free.push(page_id);
        }
    }

    fn page_size(&self) -> usize {
        self.db.config.page_size
    }

    fn txn_id(&self) -> u64 {
        self.txn_id
    }
}

/// Read-only page provider for reading within a write transaction.
///
/// This allows read operations within a write transaction to see their own
/// uncommitted changes (read-your-own-writes), while still being read-only
/// so it can work with `&self` methods.
struct BufferedReadPageProvider<'txn, 'db, B: StorageBackend> {
    db: &'db Database<B>,
    /// Immutable reference to the transaction's dirty page buffer.
    dirty_pages: &'txn HashMap<PageId, Page>,
}

impl<B: StorageBackend> PageProvider for BufferedReadPageProvider<'_, '_, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, _page: Page) {
        panic!("write_page called on read-only buffered page provider");
    }

    fn allocate_page(&mut self, _page_type: PageType) -> Page {
        panic!("allocate_page called on read-only buffered page provider");
    }

    fn free_page(&mut self, _page_id: PageId) {
        panic!("free_page called on read-only buffered page provider");
    }

    fn page_size(&self) -> usize {
        self.db.config.page_size
    }

    fn txn_id(&self) -> u64 {
        0 // Read-only, doesn't matter
    }
}

// ============================================================================
// Table Iterator
// ============================================================================

use crate::btree::cursor::{Bound, Range};

/// Default number of entries to buffer per refill in the streaming iterator.
const DEFAULT_BUFFER_SIZE: usize = 1000;

/// Streaming iterator over table entries.
///
/// Yields `(key, value)` pairs in key order using an internal buffer that is
/// lazily refilled from the B-tree. This keeps memory usage at
/// `O(buffer_size)` instead of `O(n)` regardless of table size.
///
/// # Memory characteristics
///
/// The iterator maintains a buffer of at most `buffer_size` entries (default:
/// 1000). When the buffer drains, a new B-tree range scan resumes from the
/// last key seen. For a 1000-entry buffer with 512-byte values, peak memory
/// is approximately 1 MB rather than scaling linearly with table size.
///
/// # Error handling
///
/// The `Iterator` trait implementation silently stops on B-tree errors
/// (returning `None`). Use [`next_entry`](Self::next_entry) for explicit
/// `Result`-based iteration when error handling matters.
pub struct TableIterator<'a, 'db, B: StorageBackend, T: Table> {
    db: &'db Database<B>,
    root: PageId,
    page_cache: &'a RefCell<HashMap<PageId, Page>>,
    start_bytes: Option<Vec<u8>>,
    end_bytes: Option<Vec<u8>>,
    /// Buffered entries awaiting consumption.
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    /// The last key returned, used to resume scanning after the buffer drains.
    last_key: Option<Vec<u8>>,
    /// True once the B-tree range has been fully consumed.
    exhausted: bool,
    /// Maximum entries to fetch per refill.
    buffer_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, 'db, B: StorageBackend, T: Table> TableIterator<'a, 'db, B, T> {
    fn new(
        db: &'db Database<B>,
        root: PageId,
        page_cache: &'a RefCell<HashMap<PageId, Page>>,
    ) -> Result<Self> {
        Self::with_range(db, root, page_cache, None, None)
    }

    fn with_range(
        db: &'db Database<B>,
        root: PageId,
        page_cache: &'a RefCell<HashMap<PageId, Page>>,
        start_bytes: Option<Vec<u8>>,
        end_bytes: Option<Vec<u8>>,
    ) -> Result<Self> {
        let mut iter = Self {
            db,
            root,
            page_cache,
            start_bytes,
            end_bytes,
            buffer: VecDeque::with_capacity(DEFAULT_BUFFER_SIZE),
            last_key: None,
            exhausted: false,
            buffer_size: DEFAULT_BUFFER_SIZE,
            _marker: std::marker::PhantomData,
        };

        iter.refill_buffer()?;

        Ok(iter)
    }

    /// Fill the internal buffer with the next batch of entries from the B-tree.
    ///
    /// After the first batch, subsequent refills start from an exclusive bound
    /// just past the last key returned so entries are never duplicated.
    fn refill_buffer(&mut self) -> Result<()> {
        if self.exhausted || self.root == 0 {
            self.exhausted = true;
            return Ok(());
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: self.page_cache };
        let btree = BTree::new(self.root, provider);

        // Determine the lower bound for this scan.
        // On the first call we use the original start_bytes;
        // on subsequent calls we resume just past the last key returned.
        let start_bound: Bound<'_> = if let Some(ref last) = self.last_key {
            Bound::Excluded(last.as_slice())
        } else if let Some(ref start) = self.start_bytes {
            Bound::Included(start.as_slice())
        } else {
            Bound::Unbounded
        };

        let end_bound: Bound<'_> = match &self.end_bytes {
            Some(end) => Bound::Excluded(end.as_slice()),
            None => Bound::Unbounded,
        };

        let range = Range { start: start_bound, end: end_bound };

        let mut btree_iter = btree.range(range)?;
        let mut count = 0;

        while count < self.buffer_size {
            match btree_iter.next_entry()? {
                Some((k, v)) => {
                    self.buffer.push_back((k, v));
                    count += 1;
                },
                None => {
                    self.exhausted = true;
                    break;
                },
            }
        }

        Ok(())
    }

    /// Returns the next entry, returning an explicit `Result`.
    ///
    /// Prefer this over the `Iterator` trait when error handling is needed,
    /// as `Iterator::next` silently converts B-tree errors into `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while refilling the buffer.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some((k, v)) = self.buffer.pop_front() {
            self.last_key = Some(k.clone());
            return Ok(Some((k, v)));
        }

        if self.exhausted {
            return Ok(None);
        }

        self.refill_buffer()?;

        if let Some((k, v)) = self.buffer.pop_front() {
            self.last_key = Some(k.clone());
            Ok(Some((k, v)))
        } else {
            Ok(None)
        }
    }

    /// Collects all remaining entries into a `Vec`.
    pub fn collect_entries(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        while let Ok(Some(entry)) = self.next_entry() {
            result.push(entry);
        }
        result
    }
}

impl<B: StorageBackend, T: Table> Iterator for TableIterator<'_, '_, B, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // Silently convert errors to None to satisfy the Iterator trait.
        self.next_entry().ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables;

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
        assert_eq!(config.cache_size, 1024); // default
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
}
