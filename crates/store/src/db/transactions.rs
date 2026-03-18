//! Read and write transaction types.
//!
//! [`ReadTransaction`] provides snapshot-isolated reads using Copy-on-Write.
//! [`WriteTransaction`] buffers changes and commits them atomically via the
//! dual-slot commit protocol.

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use super::{
    Database, TableIterator,
    page_providers::{
        BufferedReadPageProvider, BufferedWritePageProvider, CachingReadPageProvider,
    },
};
use crate::{
    backend::StorageBackend,
    btree::{BTree, CompactionStats},
    error::{PageId, Result},
    page::Page,
    tables::{Table, TableId},
    transaction::{CommittedState, SnapshotId},
    types::{Key, Value},
};

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
    pub(super) db: &'db Database<B>,
    /// Captured snapshot at transaction start (immutable).
    pub(super) snapshot: CommittedState,
    /// Snapshot ID for tracker (to prevent page cleanup).
    pub(super) snapshot_id: SnapshotId,
    /// Local page cache for read operations.
    pub(super) page_cache: RefCell<HashMap<PageId, Page>>,
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
    pub(super) db: &'db Database<B>,
    /// Snapshot ID for this transaction (used for deferred page cleanup).
    pub(super) snapshot_id: SnapshotId,
    /// Table roots (may diverge from committed state as we modify).
    pub(super) table_roots: [PageId; TableId::COUNT],
    /// Whether the transaction has been committed or aborted.
    pub(super) committed: bool,
    /// Buffered page modifications - not visible to other transactions until commit.
    pub(super) dirty_pages: HashMap<PageId, Page>,
    /// Old pages replaced by COW - freed after commit when no readers reference them.
    pub(super) pages_to_free: Vec<PageId>,
    /// Page cache for read operations within this transaction.
    pub(super) page_cache: RefCell<HashMap<PageId, Page>>,
    /// Guard to ensure only one write transaction at a time.
    pub(super) _write_guard: std::sync::MutexGuard<'db, ()>,
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

        self.table_roots[T::ID as usize] = btree.root_page();

        Ok(())
    }

    /// Inserts a pre-encoded key-value pair into a table by ID.
    ///
    /// Unlike [`insert`](Self::insert), this method accepts raw bytes that are
    /// **already** in the B-tree encoding format (e.g. from [`TableIterator`]).
    /// No additional encoding is applied. This is used by snapshot installation
    /// to stream entries directly from the snapshot file into the B-tree.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during the B-tree operation.
    pub fn insert_raw(&mut self, table_id: TableId, key: &[u8], value: &[u8]) -> Result<()> {
        let root = self.table_roots[table_id as usize];
        let provider = BufferedWritePageProvider {
            db: self.db,
            txn_id: self.snapshot_id.raw(),
            dirty_pages: &mut self.dirty_pages,
            pages_to_free: &mut self.pages_to_free,
        };

        let mut btree = BTree::new(root, provider);
        btree.insert(key, value)?;

        let splits = btree.split_count();
        if splits > 0 {
            self.db.page_splits.fetch_add(splits, Ordering::Relaxed);
        }

        self.table_roots[table_id as usize] = btree.root_page();

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
    /// Returns [`super::Error::Io`] if flushing dirty pages or writing the header fails.
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
