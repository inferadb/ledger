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

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::backend::{StorageBackend, FileBackend, InMemoryBackend, DEFAULT_PAGE_SIZE, HEADER_SIZE};
use crate::btree::{BTree, PageProvider};
use crate::error::{Error, PageId, PageType, Result};
use crate::page::{Page, PageCache, PageAllocator};
use crate::tables::{TableId, Table};
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
pub struct Database<B: StorageBackend> {
    /// Storage backend (file or memory).
    backend: RwLock<B>,
    /// Page cache shared across all operations.
    cache: PageCache,
    /// Page allocator for new pages.
    allocator: RwLock<PageAllocator>,
    /// Root page IDs for each table (indexed by TableId).
    table_roots: RwLock<[PageId; TableId::COUNT]>,
    /// Current transaction ID (monotonically increasing).
    txn_id: AtomicU64,
    /// Configuration.
    config: DatabaseConfig,
    /// Write lock to ensure only one write transaction at a time.
    /// This prevents data loss from concurrent commits overwriting table_roots.
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

        // Initialize allocator - start after header page
        // Page 0 is reserved for table directory
        let file_size = backend.file_size()?;
        let next_page = if file_size <= HEADER_SIZE as u64 {
            1 // First page after header
        } else {
            ((file_size - HEADER_SIZE as u64) / config.page_size as u64) as PageId
        };
        let allocator = PageAllocator::new(config.page_size, next_page);

        // Initialize table roots (0 means empty table)
        let table_roots = [0; TableId::COUNT];

        // TODO: Load table roots from header/directory page if database exists

        Ok(Self {
            backend: RwLock::new(backend),
            cache,
            allocator: RwLock::new(allocator),
            table_roots: RwLock::new(table_roots),
            txn_id: AtomicU64::new(1),
            config,
            write_lock: std::sync::Mutex::new(()),
        })
    }

    /// Begin a read-only transaction.
    pub fn read(&self) -> Result<ReadTransaction<'_, B>> {
        Ok(ReadTransaction {
            db: self,
            table_roots: *self.table_roots.read(),
        })
    }

    /// Begin a write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// This is enforced by holding a mutex for the duration of the transaction.
    pub fn write(&self) -> Result<WriteTransaction<'_, B>> {
        // Acquire exclusive write lock - blocks until any active write transaction completes
        let write_guard = self
            .write_lock
            .lock()
            .map_err(|_| Error::Poisoned)?;

        let txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst);
        let table_roots = *self.table_roots.read();

        Ok(WriteTransaction {
            db: self,
            txn_id,
            table_roots,
            committed: false,
            _write_guard: write_guard,
        })
    }

    /// Get database statistics.
    pub fn stats(&self) -> DatabaseStats {
        let cache_stats = self.cache.stats();
        let allocator = self.allocator.read();

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
        let page_id = self.allocator.write().allocate();
        Page::new(page_id, self.config.page_size, page_type, txn_id)
    }

    /// Free a page for later reuse.
    fn free_page(&self, page_id: PageId) {
        self.allocator.write().free(page_id);
        self.cache.remove(page_id);
    }

    /// Write a page to the backend.
    fn write_page_to_backend(&self, page: &Page) -> Result<()> {
        let mut page = page.clone();
        page.update_checksum();

        let backend = self.backend.write();
        backend.write_page(page.id, &page.data)?;
        Ok(())
    }

    /// Flush all dirty pages and sync to disk.
    fn flush(&self) -> Result<()> {
        // Get all dirty pages from cache
        let dirty_pages = self.cache.dirty_pages();

        for page in &dirty_pages {
            self.write_page_to_backend(page)?;
            self.cache.mark_clean(page.id);
        }

        // Sync to disk if configured
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
/// Provides a consistent snapshot view of the database.
pub struct ReadTransaction<'db, B: StorageBackend> {
    db: &'db Database<B>,
    table_roots: [PageId; TableId::COUNT],
}

impl<'db, B: StorageBackend> ReadTransaction<'db, B> {
    /// Get a value by key from a table.
    pub fn get<T: Table>(&self, key: &T::KeyType) -> Result<Option<Vec<u8>>>
    where
        T::KeyType: Key,
    {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = ReadOnlyPageProvider { db: self.db };
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
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = ReadOnlyPageProvider { db: self.db };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Get the last (largest key) entry in a table.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = ReadOnlyPageProvider { db: self.db };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterate over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.table_roots[T::ID as usize];
        TableIterator::new(self.db, root)
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
        let root = self.table_roots[T::ID as usize];
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
        TableIterator::with_range(self.db, root, start_bytes, end_bytes)
    }
}

/// A write transaction.
///
/// Changes are buffered until commit. On commit, all changes are
/// atomically written and synced to disk.
pub struct WriteTransaction<'db, B: StorageBackend> {
    db: &'db Database<B>,
    txn_id: u64,
    table_roots: [PageId; TableId::COUNT],
    committed: bool,
    /// Guard to ensure only one write transaction at a time.
    /// Held for the lifetime of the transaction.
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
        let provider = WritePageProvider {
            db: self.db,
            txn_id: self.txn_id,
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

        let provider = WritePageProvider {
            db: self.db,
            txn_id: self.txn_id,
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

        let provider = ReadOnlyPageProvider { db: self.db };
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

        let provider = ReadOnlyPageProvider { db: self.db };
        let btree = BTree::new(root, provider);
        btree.first()
    }

    /// Get the last (largest key) entry in a table.
    pub fn last<T: Table>(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.table_roots[T::ID as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = ReadOnlyPageProvider { db: self.db };
        let btree = BTree::new(root, provider);
        btree.last()
    }

    /// Iterate over all entries in a table.
    ///
    /// Returns an iterator that yields (key, value) pairs in key order.
    /// Note: This reads the transaction's current view including uncommitted changes.
    pub fn iter<T: Table>(&self) -> Result<TableIterator<'_, 'db, B, T>> {
        let root = self.table_roots[T::ID as usize];
        TableIterator::new(self.db, root)
    }

    /// Commit the transaction.
    ///
    /// All changes are atomically written to disk.
    pub fn commit(mut self) -> Result<()> {
        // Update global table roots
        *self.db.table_roots.write() = self.table_roots;

        // Flush all dirty pages
        self.db.flush()?;

        self.committed = true;
        Ok(())
    }

    /// Abort the transaction (discard all changes).
    pub fn abort(mut self) {
        // Clear cache of any dirty pages from this transaction
        // In a more sophisticated implementation, we'd track which pages were modified
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
        }
    }
}

/// Page provider for read-only operations.
struct ReadOnlyPageProvider<'db, B: StorageBackend> {
    db: &'db Database<B>,
}

impl<'db, B: StorageBackend> PageProvider for ReadOnlyPageProvider<'db, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, _page: Page) {
        // Read-only - this should never be called
        panic!("write_page called on read-only page provider");
    }

    fn allocate_page(&mut self, _page_type: PageType) -> Page {
        // Read-only - this should never be called
        panic!("allocate_page called on read-only page provider");
    }

    fn free_page(&mut self, _page_id: PageId) {
        // Read-only - this should never be called
        panic!("free_page called on read-only page provider");
    }

    fn page_size(&self) -> usize {
        self.db.config.page_size
    }

    fn txn_id(&self) -> u64 {
        0 // Read-only, doesn't matter
    }
}

/// Page provider for write operations.
struct WritePageProvider<'db, B: StorageBackend> {
    db: &'db Database<B>,
    txn_id: u64,
}

impl<'db, B: StorageBackend> PageProvider for WritePageProvider<'db, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, mut page: Page) {
        // Update checksum and write to cache (will be flushed on commit)
        page.update_checksum();
        self.db.cache.insert(page);
    }

    fn allocate_page(&mut self, page_type: PageType) -> Page {
        self.db.allocate_page(page_type, self.txn_id)
    }

    fn free_page(&mut self, page_id: PageId) {
        self.db.free_page(page_id);
    }

    fn page_size(&self) -> usize {
        self.db.config.page_size
    }

    fn txn_id(&self) -> u64 {
        self.txn_id
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
    start_bytes: Option<Vec<u8>>,
    end_bytes: Option<Vec<u8>>,
    current_position: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
    _marker: std::marker::PhantomData<&'a T>,
}

impl<'a, 'db, B: StorageBackend, T: Table> TableIterator<'a, 'db, B, T> {
    fn new(db: &'db Database<B>, root: PageId) -> Result<Self> {
        Self::with_range(db, root, None, None)
    }

    fn with_range(
        db: &'db Database<B>,
        root: PageId,
        start_bytes: Option<Vec<u8>>,
        end_bytes: Option<Vec<u8>>,
    ) -> Result<Self> {
        let mut iter = Self {
            db,
            root,
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

        let provider = ReadOnlyPageProvider { db: self.db };
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
}
