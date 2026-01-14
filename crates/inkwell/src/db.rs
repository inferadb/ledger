//! Database and transaction management for Inkwell.
//!
//! Provides ACID transactions over the 13 fixed tables. Uses a single-writer
//! model optimized for Raft's serialized writes.
//!
//! # Example
//!
//! ```ignore
//! use inkwell::{Database, tables};
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

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};

use crate::backend::{
    DatabaseHeader, FileBackend, InMemoryBackend, StorageBackend, DEFAULT_PAGE_SIZE, HEADER_SIZE,
};
use crate::btree::{BTree, PageProvider};
use crate::error::{Error, PageId, PageType, Result};
use crate::page::{Page, PageAllocator, PageCache};
use crate::tables::{Table, TableEntry, TableId};
use crate::transaction::{CommittedState, PendingFrees, SnapshotId, TransactionTracker};
use crate::types::{Key, Value};

/// Database configuration options.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Page size (must be power of 2, default 4096).
    pub page_size: usize,
    /// Maximum pages to cache in memory.
    pub cache_size: usize,
    /// Whether to sync on every commit (default true for durability).
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
    /// Write lock to ensure only one write transaction at a time.
    write_lock: std::sync::Mutex<()>,
}

impl Database<FileBackend> {
    /// Open an existing database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let backend = FileBackend::open(path)?;
        let config = DatabaseConfig {
            page_size: backend.page_size(),
            ..Default::default()
        };
        Self::from_backend(backend, config)
    }

    /// Create a new database at the given path.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_config(path, DatabaseConfig::default())
    }

    /// Create a new database with custom configuration.
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: DatabaseConfig) -> Result<Self> {
        let backend = FileBackend::create(path, config.page_size)?;
        Self::from_backend(backend, config)
    }
}

impl Database<InMemoryBackend> {
    /// Create a new in-memory database.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_config(DatabaseConfig::default())
    }

    /// Create a new in-memory database with custom configuration.
    pub fn open_in_memory_with_config(config: DatabaseConfig) -> Result<Self> {
        let backend = InMemoryBackend::new();
        Self::from_backend(backend, config)
    }
}

impl<B: StorageBackend> Database<B> {
    /// Create a database from an existing backend.
    fn from_backend(backend: B, config: DatabaseConfig) -> Result<Self> {
        let cache = PageCache::new(config.cache_size);

        // Try to load existing state from disk
        let file_size = backend.file_size()?;
        let (initial_state, next_page) = if file_size > HEADER_SIZE as u64 {
            // Database exists - load state from header and directory
            Self::load_state_from_disk(&backend, &config)?
        } else {
            // New database - start fresh
            // Start at page 2: page 1 is reserved for the table directory
            (CommittedState::default(), 2)
        };

        let allocator = PageAllocator::new(config.page_size, next_page);
        let tracker = TransactionTracker::new(initial_state.snapshot_id.next());

        Ok(Self {
            backend: RwLock::new(backend),
            cache,
            allocator: Mutex::new(allocator),
            committed_state: ArcSwap::from_pointee(initial_state),
            tracker,
            pending_frees: Mutex::new(PendingFrees::new()),
            config,
            write_lock: std::sync::Mutex::new(()),
        })
    }

    /// Load committed state from disk (header + table directory).
    fn load_state_from_disk(
        backend: &B,
        config: &DatabaseConfig,
    ) -> Result<(CommittedState, PageId)> {
        // Read and parse header
        let header_bytes = backend.read_header()?;
        let header = DatabaseHeader::from_bytes(&header_bytes)?;

        let mut table_roots = [0; TableId::COUNT];
        let snapshot_id = SnapshotId::new(header.last_txn_id);

        // If there's a table directory page, read it
        if header.table_directory_page != 0 {
            let dir_page_data = backend.read_page(header.table_directory_page)?;

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

        // Calculate next_page from header's total_pages or file size
        // Minimum is 2 because page 1 is reserved for table directory
        let next_page = if header.total_pages > 1 {
            header.total_pages
        } else {
            let file_size = backend.file_size()?;
            std::cmp::max(
                2,
                ((file_size - HEADER_SIZE as u64) / config.page_size as u64) as PageId,
            )
        };

        Ok((
            CommittedState {
                table_roots,
                snapshot_id,
            },
            next_page,
        ))
    }

    /// Persist committed state to disk (table directory + header).
    ///
    /// This is called during commit to make state durable. The sequence is:
    /// 1. Write table directory page (contains all table roots)
    /// 2. Write updated header (points to directory, stores txn_id)
    /// 3. Sync to ensure durability
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

        // Write entry count
        let entry_count = TableId::COUNT as u16;
        dir_data[0..2].copy_from_slice(&entry_count.to_le_bytes());

        // Write table entries
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

        // Write directory page
        backend.write_page(dir_page_id, &dir_data)?;

        // Build and write header
        let total_pages = self.allocator.lock().next_page_id();
        let header = DatabaseHeader {
            magic: *crate::backend::MAGIC,
            version: crate::backend::FORMAT_VERSION,
            page_size_power: self.config.page_size.trailing_zeros() as u8,
            reserved: 0,
            reserved2: 0,
            total_pages,
            free_list_head: 0, // TODO: implement free list persistence
            table_directory_page: dir_page_id,
            last_txn_id: snapshot_id.raw(),
            last_write_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            checksum: 0, // Will be computed by to_bytes()
        };

        backend.write_header(&header.to_bytes())?;

        // Note: Caller is responsible for calling sync() if durability is needed
        // This allows batching multiple writes before a single sync

        Ok(())
    }

    /// Begin a read-only transaction.
    ///
    /// Captures an immutable snapshot of the database state. No locks are held
    /// after this returns, allowing full concurrency with write transactions.
    ///
    /// The transaction sees a consistent point-in-time view thanks to COW:
    /// writers create new page copies, never modifying pages readers might see.
    pub fn read(&self) -> Result<ReadTransaction<'_, B>> {
        // Load current committed state (atomic, lock-free)
        let snapshot = self.committed_state.load();
        let snapshot_id = snapshot.snapshot_id;

        // Register with tracker to prevent page cleanup while we're reading
        self.tracker.register_read_transaction(snapshot_id);

        Ok(ReadTransaction {
            db: self,
            snapshot: (**snapshot).clone(),
            snapshot_id,
            page_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Begin a write transaction.
    ///
    /// Only one write transaction can be active at a time. However, read
    /// transactions can run concurrently - they see a consistent snapshot
    /// and are unaffected by COW modifications.
    pub fn write(&self) -> Result<WriteTransaction<'_, B>> {
        // Acquire write mutex to ensure only one write at a time
        let write_guard = self.write_lock.lock().map_err(|_| Error::Poisoned)?;

        // Get snapshot ID for this write transaction
        let snapshot_id = self.tracker.start_write_transaction();

        // Start from current committed state
        let current = self.committed_state.load();
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

    /// Get database statistics.
    pub fn stats(&self) -> DatabaseStats {
        let cache_stats = self.cache.stats();
        let allocator = self.allocator.lock();

        DatabaseStats {
            page_size: self.config.page_size,
            total_pages: allocator.next_page_id(),
            cached_pages: cache_stats.size,
            dirty_pages: cache_stats.dirty_count,
            free_pages: allocator.free_page_count(),
        }
    }

    /// Read a page from cache or backend.
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check cache first
        if let Some(page) = self.cache.get(page_id) {
            return Ok(page);
        }

        // Read from backend
        let backend = self.backend.read();
        let data = backend.read_page(page_id)?;

        // Check for all-zeros (unwritten page)
        if data.iter().all(|&b| b == 0) {
            return Err(Error::PageNotFound { page_id });
        }

        let page = Page::from_bytes(page_id, data);

        // Verify checksum
        if !page.verify_checksum() {
            return Err(Error::PageChecksumMismatch { page_id });
        }

        // Cache the page
        self.cache.insert(page.clone());

        Ok(page)
    }

    /// Allocate a new page.
    fn allocate_page(&self, page_type: PageType, txn_id: u64) -> Page {
        let page_id = self.allocator.lock().allocate();
        Page::new(page_id, self.config.page_size, page_type, txn_id)
    }

    /// Free a page for later reuse.
    fn free_page(&self, page_id: PageId) {
        self.allocator.lock().free(page_id);
        self.cache.remove(page_id);
    }

    /// Attempt to free pages that are no longer referenced by any reader.
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

    /// Write a page to the backend.
    fn write_page_to_backend(&self, page: &Page) -> Result<()> {
        let mut page = page.clone();
        page.update_checksum();

        let backend = self.backend.write();
        backend.write_page(page.id, &page.data)?;
        Ok(())
    }

    /// Flush all dirty pages to disk (without sync).
    ///
    /// Caller is responsible for calling sync() afterward if durability is needed.
    fn flush_pages(&self) -> Result<()> {
        // Get all dirty pages from cache
        let dirty_pages = self.cache.dirty_pages();

        for page in &dirty_pages {
            self.write_page_to_backend(page)?;
            self.cache.mark_clean(page.id);
        }

        Ok(())
    }

    /// Sync all buffered writes to durable storage.
    fn sync(&self) -> Result<()> {
        if self.config.sync_on_commit {
            self.backend.write().sync()?;
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
    /// Get a value by key from a table.
    pub fn get<T: Table>(&self, key: &T::KeyType) -> Result<Option<Vec<u8>>>
    where
        T::KeyType: Key,
    {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider {
            db: self.db,
            page_cache: &self.page_cache,
        };
        let btree = BTree::new(root, provider);

        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        btree.get(&key_bytes)
    }

    /// Check if a key exists in a table.
    pub fn contains<T: Table>(&self, key: &T::KeyType) -> Result<bool>
    where
        T::KeyType: Key,
    {
        Ok(self.get::<T>(key)?.is_some())
    }

    /// Get the first (smallest key) entry in a table.
    pub fn first<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider {
            db: self.db,
            page_cache: &self.page_cache,
        };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Get the last (largest key) entry in a table.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider {
            db: self.db,
            page_cache: &self.page_cache,
        };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterate over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.snapshot.table_roots[T::ID as usize];
        TableIterator::new(self.db, root, &self.page_cache)
    }

    /// Iterate over a range of entries in a table.
    ///
    /// The range is specified as (start_key, end_key) where start is inclusive
    /// and end is exclusive.
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
}

impl<'db, B: StorageBackend> Drop for ReadTransaction<'db, B> {
    fn drop(&mut self) {
        // Unregister from tracker to allow page cleanup
        // This is critical for COW - pages can only be freed when all
        // readers that might reference them have finished
        self.db
            .tracker
            .unregister_read_transaction(self.snapshot_id);

        // Attempt to free pending pages now that this reader is done
        self.db.try_free_pending_pages();
    }
}

/// A write transaction.
///
/// Changes are buffered until commit. On commit, all changes are
/// atomically written and synced to disk using Copy-on-Write semantics.
///
/// Read transactions can run concurrently - they see a consistent snapshot
/// and are unaffected by COW modifications.
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
    #[allow(dead_code)]
    _write_guard: std::sync::MutexGuard<'db, ()>,
}

impl<'db, B: StorageBackend> WriteTransaction<'db, B> {
    /// Insert or update a key-value pair.
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

        // Update root if it changed
        self.table_roots[T::ID as usize] = btree.root_page();

        Ok(())
    }

    /// Delete a key from a table.
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

    /// Get a value (within the transaction's view).
    pub fn get<T: Table>(&self, key: &T::KeyType) -> Result<Option<Vec<u8>>>
    where
        T::KeyType: Key,
    {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        // Use buffered provider to see our own uncommitted changes
        let provider = BufferedReadPageProvider {
            db: self.db,
            dirty_pages: &self.dirty_pages,
        };
        let btree = BTree::new(root, provider);

        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes);

        btree.get(&key_bytes)
    }

    /// Get the first (smallest key) entry in a table.
    pub fn first<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        // Use buffered provider to see our own uncommitted changes
        let provider = BufferedReadPageProvider {
            db: self.db,
            dirty_pages: &self.dirty_pages,
        };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Get the last (largest key) entry in a table.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        // Use buffered provider to see our own uncommitted changes
        let provider = BufferedReadPageProvider {
            db: self.db,
            dirty_pages: &self.dirty_pages,
        };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterate over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    /// Note: This reads the transaction's current view including uncommitted changes.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.table_roots[T::ID as usize];
        // Pre-populate page_cache with dirty_pages so the iterator sees uncommitted changes.
        // CachingReadPageProvider checks page_cache first, so dirty pages will be found.
        for (&page_id, page) in &self.dirty_pages {
            self.page_cache.borrow_mut().insert(page_id, page.clone());
        }
        TableIterator::new(self.db, root, &self.page_cache)
    }

    /// Commit the transaction.
    ///
    /// Uses Copy-on-Write semantics:
    /// 1. Write dirty pages to backend
    /// 2. Persist table directory and header to disk
    /// 3. Atomically swap committed_state (makes changes visible in-memory)
    /// 4. Record freed pages for deferred cleanup
    /// 5. End transaction in tracker
    pub fn commit(mut self) -> Result<()> {
        // Move all buffered dirty pages into the shared cache
        for (_, page) in self.dirty_pages.drain() {
            self.db.cache.insert(page);
        }

        // Flush all dirty pages to disk (data pages)
        self.db.flush_pages()?;

        // Persist table directory and header (makes state recoverable on restart)
        self.db
            .persist_state_to_disk(&self.table_roots, self.snapshot_id)?;

        // Single sync after all writes are done - this is the durability barrier
        self.db.sync()?;

        // Create new committed state with our updated table roots
        let new_state = CommittedState {
            table_roots: self.table_roots,
            snapshot_id: self.snapshot_id,
        };

        // Atomically swap the committed state - this is the magic moment
        // where our changes become visible to new read transactions
        self.db.committed_state.store(Arc::new(new_state));

        // Record pages to be freed for deferred cleanup
        // These can only be freed once no readers reference this snapshot
        if !self.pages_to_free.is_empty() {
            let pages = std::mem::take(&mut self.pages_to_free);
            self.db
                .pending_frees
                .lock()
                .record_freed_pages(self.snapshot_id, pages);
        }

        // Attempt to free old pages that are no longer referenced
        self.db.try_free_pending_pages();

        // End the write transaction in tracker
        self.db.tracker.end_write_transaction(self.snapshot_id);

        self.committed = true;
        Ok(())
    }

    /// Abort the transaction (discard all changes).
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

impl<'db, B: StorageBackend> Drop for WriteTransaction<'db, B> {
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

impl<'txn, 'db, B: StorageBackend> PageProvider for CachingReadPageProvider<'txn, 'db, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check local cache first
        if let Some(page) = self.page_cache.borrow().get(&page_id) {
            return Ok(page.clone());
        }

        // Read from shared cache/backend and store in local cache
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

impl<'txn, 'db, B: StorageBackend> PageProvider for BufferedWritePageProvider<'txn, 'db, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        // Fall back to shared cache/backend
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

impl<'txn, 'db, B: StorageBackend> PageProvider for BufferedReadPageProvider<'txn, 'db, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        // Fall back to shared cache/backend
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

/// Iterator over table entries.
///
/// Yields (key, value) pairs in key order.
pub struct TableIterator<'a, 'db, B: StorageBackend, T: Table> {
    db: &'db Database<B>,
    root: PageId,
    page_cache: &'a RefCell<HashMap<PageId, Page>>,
    start_bytes: Option<Vec<u8>>,
    end_bytes: Option<Vec<u8>>,
    current_position: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
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
            current_position: 0,
            entries: Vec::new(),
            exhausted: false,
            _marker: std::marker::PhantomData,
        };

        // Pre-load entries from the B-tree
        // This is a simplified approach - a production implementation would
        // stream entries lazily
        iter.load_entries()?;

        Ok(iter)
    }

    fn load_entries(&mut self) -> Result<()> {
        if self.root == 0 {
            self.exhausted = true;
            return Ok(());
        }

        let provider = CachingReadPageProvider {
            db: self.db,
            page_cache: self.page_cache,
        };
        let btree = BTree::new(self.root, provider);

        // Build range bounds
        let range = match (&self.start_bytes, &self.end_bytes) {
            (None, None) => Range::all(),
            (Some(start), None) => Range {
                start: Bound::Included(start.as_slice()),
                end: Bound::Unbounded,
            },
            (None, Some(end)) => Range {
                start: Bound::Unbounded,
                end: Bound::Excluded(end.as_slice()),
            },
            (Some(start), Some(end)) => Range {
                start: Bound::Included(start.as_slice()),
                end: Bound::Excluded(end.as_slice()),
            },
        };

        // Collect entries
        let mut iter = btree.range(range)?;
        while let Some((k, v)) = iter.next_entry()? {
            self.entries.push((k, v));
        }

        Ok(())
    }

    /// Get the next entry.
    pub fn next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current_position < self.entries.len() {
            let entry = self.entries[self.current_position].clone();
            self.current_position += 1;
            Some(entry)
        } else {
            None
        }
    }

    /// Collect all remaining entries into a Vec.
    pub fn collect_entries(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        while let Some(entry) = self.next_entry() {
            result.push(entry);
        }
        result
    }
}

impl<'a, 'db, B: StorageBackend, T: Table> Iterator for TableIterator<'a, 'db, B, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables;

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
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3])
                .unwrap();
            txn.insert::<tables::RaftLog>(&2u64, &vec![4u8, 5, 6])
                .unwrap();
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
            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3])
                .unwrap();
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
            txn.insert::<tables::RaftState>(&"term".to_string(), &vec![0u8, 0, 0, 1])
                .unwrap();
            txn.commit().unwrap();
        }

        // Read from both
        {
            let txn = db.read().unwrap();
            assert!(txn.get::<tables::RaftLog>(&1u64).unwrap().is_some());
            assert!(txn
                .get::<tables::RaftState>(&"term".to_string())
                .unwrap()
                .is_some());
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
            txn.insert::<tables::Entities>(&key.to_be_bytes().to_vec(), &value)
                .unwrap();
            txn.commit().unwrap();
        }

        // Verify all keys are present
        let txn = db.read().unwrap();
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = i as u64;
            let expected = format!("value-{}", i).into_bytes();
            match txn
                .get::<tables::Entities>(&key.to_be_bytes().to_vec())
                .unwrap()
            {
                Some(value) => {
                    assert_eq!(value, expected, "Value mismatch for key {}", i);
                }
                None => {
                    missing.push(i);
                }
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
            txn.insert::<tables::Entities>(&key.into_bytes(), &value)
                .unwrap();
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
                }
                None => {
                    missing.push(i);
                }
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
        use std::sync::Arc;
        use std::thread;

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
                    txn.insert::<tables::Entities>(&key.into_bytes(), &value)
                        .unwrap();
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
                    }
                    None => {
                        missing.push(key);
                    }
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

    /// Test that data persists across database close and reopen
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
                    }
                    None => {
                        missing.push(i);
                    }
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

    /// Test persistence with multiple tables
    #[test]
    fn test_file_persistence_multiple_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("multi.ink");

        // Create and write to multiple tables
        {
            let db = Database::<FileBackend>::create(&db_path).unwrap();
            let mut txn = db.write().unwrap();

            txn.insert::<tables::RaftLog>(&1u64, &vec![1u8, 2, 3])
                .unwrap();
            txn.insert::<tables::RaftState>(&"term".to_string(), &vec![0u8, 0, 0, 42])
                .unwrap();
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
                txn.get::<tables::Entities>(&b"entity-key".to_vec())
                    .unwrap(),
                Some(b"entity-value".to_vec()),
                "Entities data missing"
            );
        }
    }
}
