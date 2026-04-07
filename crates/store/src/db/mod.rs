//! Database and transaction management for inferadb-ledger-store.
//!
//! Provides ACID transactions over the fixed table set. Uses a single-writer
//! model optimized for Raft's serialized writes.
//!
//! # Example
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
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
//! # Ok(())
//! # }
//! ```

mod iterator;
mod page_providers;
#[allow(clippy::module_inception)]
mod tests;
mod transactions;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwap;
pub use iterator::TableIterator;
use parking_lot::{Mutex, RwLock};
pub use transactions::{DatabaseStats, ReadTransaction, WriteTransaction};

use crate::{
    backend::{
        CommitSlot, DEFAULT_PAGE_SIZE, DatabaseHeader, FileBackend, HEADER_SIZE, InMemoryBackend,
        StorageBackend,
    },
    dirty_bitmap::DirtyBitmap,
    error::{Error, PageId, PageType, Result},
    page::{Page, PageAllocator, PageCache},
    tables::{TableEntry, TableId},
    transaction::{CommittedState, PendingFrees, SnapshotId, TransactionTracker},
};

/// Database configuration options.
#[derive(Debug, Clone, bon::Builder)]
pub struct DatabaseConfig {
    /// Page size (must be power of 2, default 4096).
    #[builder(default = DEFAULT_PAGE_SIZE)]
    pub page_size: usize,
    /// Maximum pages to cache in memory (default 16384 = 64MB with 4KB pages).
    ///
    /// Production deployments should tune this to 10-25% of available memory.
    #[builder(default = 16_384)]
    pub cache_size: usize,
    /// Whether to sync on every commit (default true for durability).
    #[builder(default = true)]
    pub sync_on_commit: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: 16_384, // ~64MB with 4KB pages
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
    pub(super) backend: RwLock<B>,
    /// Page cache shared across all operations.
    pub(super) cache: PageCache,
    /// Page allocator for new pages.
    pub(super) allocator: Mutex<PageAllocator>,
    /// Current committed state (atomically swapped on commit).
    /// This is the key to COW - readers capture this, writers swap it.
    pub(super) committed_state: ArcSwap<CommittedState>,
    /// Transaction tracker for safe page deallocation.
    pub(super) tracker: TransactionTracker,
    /// Pages pending deallocation (freed when no readers reference them).
    pub(super) pending_frees: Mutex<PendingFrees>,
    /// Configuration.
    pub(super) config: DatabaseConfig,
    /// Writes lock to ensure only one write transaction at a time.
    pub(super) write_lock: std::sync::Mutex<()>,
    /// Total B-tree page splits since database creation.
    pub(super) page_splits: AtomicU64,
    /// Pages modified since the last backup checkpoint.
    ///
    /// Used by incremental backup to track which pages need to be included
    /// in the delta. Reset when a backup completes successfully.
    pub(super) dirty_bitmap: Mutex<DirtyBitmap>,
    /// Monotonically increasing generation counter, incremented on each commit.
    pub(super) generation: AtomicU64,
    /// Tracks which generation each page was last written in.
    pub(super) page_generations: Mutex<HashMap<PageId, u64>>,
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

    /// Opens an existing database with custom configuration.
    ///
    /// The `page_size` field in `config` is ignored — the on-disk page size
    /// is always used for an existing database.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be opened or read.
    /// Returns [`Error::InvalidMagic`] if the file is not an InferaDB database.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: DatabaseConfig) -> Result<Self> {
        let backend = FileBackend::open(path)?;
        let config = DatabaseConfig { page_size: backend.page_size(), ..config };
        Self::from_backend(backend, config)
    }

    /// Creates a new database at the given path with default configuration.
    ///
    /// Uses 4KB pages, 16384-entry cache (64MB), and sync-on-commit.
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
            generation: AtomicU64::new(0),
            page_generations: Mutex::new(HashMap::new()),
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
    pub(super) fn persist_state_to_disk(
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
            let entry = TableEntry { table_id, root_page: table_roots[table_id as usize] };
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
    /// This method is infallible in practice; the `Result` return preserves
    /// API consistency with other transaction entry points.
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

    /// Returns the current generation counter.
    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Returns page IDs modified since the given generation.
    pub fn pages_modified_since(&self, since_generation: u64) -> Vec<PageId> {
        let page_gens = self.page_generations.lock();
        page_gens
            .iter()
            .filter(|&(_, &page_gen)| page_gen > since_generation)
            .map(|(&pid, _)| pid)
            .collect()
    }

    /// Returns the total number of tracked pages.
    pub fn tracked_page_count(&self) -> usize {
        self.page_generations.lock().len()
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

    /// Re-wraps a batch of pages' crypto sidecar metadata to a target RMK version.
    ///
    /// Delegates to the backend's [`StorageBackend::rewrap_pages`]. Non-encrypted
    /// backends return `(0, None)` immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if an RMK version cannot be resolved or an unwrap/wrap fails.
    pub fn rewrap_pages(
        &self,
        start_page_id: u64,
        batch_size: usize,
        target_version: Option<u32>,
    ) -> Result<(usize, Option<u64>)> {
        let backend = self.backend.read();
        backend.rewrap_pages(start_page_id, batch_size, target_version)
    }

    /// Returns the total page count in the crypto sidecar.
    ///
    /// Non-encrypted backends return 0.
    ///
    /// # Errors
    ///
    /// Returns an error if the sidecar metadata cannot be read.
    pub fn sidecar_page_count(&self) -> Result<u64> {
        let backend = self.backend.read();
        backend.sidecar_page_count()
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
    pub(super) fn read_page(&self, page_id: PageId) -> Result<Page> {
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
    pub(super) fn allocate_page(&self, page_type: PageType, txn_id: u64) -> Page {
        let page_id = self.allocator.lock().allocate();
        Page::new(page_id, self.config.page_size, page_type, txn_id)
    }

    /// Frees a page for later reuse.
    pub(super) fn free_page(&self, page_id: PageId) {
        self.allocator.lock().free(page_id);
        self.cache.remove(page_id);
    }

    /// Attempts to free pages that are no longer referenced by any reader.
    ///
    /// This is the garbage collection pass for COW - old pages can only be
    /// freed once all readers that might reference them have finished.
    pub(super) fn try_free_pending_pages(&self) {
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

    /// Flushes all dirty pages to disk (without sync).
    ///
    /// Updates each page's checksum in the cache before writing, ensuring
    /// consistency between the cached and on-disk versions.
    ///
    /// Caller is responsible for calling sync() afterward if durability is needed.
    pub(super) fn flush_pages(&self) -> Result<()> {
        let dirty_pages = self.cache.dirty_pages();
        let backend = self.backend.write();

        for mut page in dirty_pages {
            // Update checksum in the cache so it matches the on-disk version
            self.cache.update_checksum(page.id);

            // Compute checksum on the clone used for writing
            page.update_checksum();
            backend.write_page(page.id, &page.data)?;
            self.cache.mark_clean(page.id);
        }

        Ok(())
    }
}
