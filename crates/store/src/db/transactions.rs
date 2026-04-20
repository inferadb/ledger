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

    /// Gets a value by raw key bytes, bypassing `Key::encode` overhead.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn get_raw(&self, table_id: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root = self.snapshot.table_roots[table_id as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = CachingReadPageProvider { db: self.db, page_cache: &self.page_cache };
        let btree = BTree::new(root, provider);
        btree.get(key)
    }

    /// Checks key existence by raw bytes, bypassing `Key::encode` overhead.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn contains_raw(&self, table_id: TableId, key: &[u8]) -> Result<bool> {
        Ok(self.get_raw(table_id, key)?.is_some())
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

    /// Gets a value by raw key bytes from a write transaction.
    ///
    /// Sees uncommitted writes within this transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during the B-tree lookup.
    pub fn get_raw(&self, table_id: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root = self.table_roots[table_id as usize];
        if root == 0 {
            return Ok(None);
        }

        let provider = BufferedReadPageProvider { db: self.db, dirty_pages: &self.dirty_pages };
        let btree = BTree::new(root, provider);
        btree.get(key)
    }

    /// Deletes a key by raw bytes, bypassing `Key::encode` overhead.
    ///
    /// Returns `true` if the key existed and was removed, `false` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during the B-tree operation.
    pub fn delete_raw(&mut self, table_id: TableId, key: &[u8]) -> Result<bool> {
        let root = self.table_roots[table_id as usize];
        if root == 0 {
            return Ok(false);
        }

        let provider = BufferedWritePageProvider {
            db: self.db,
            txn_id: self.snapshot_id.raw(),
            dirty_pages: &mut self.dirty_pages,
            pages_to_free: &mut self.pages_to_free,
        };

        let mut btree = BTree::new(root, provider);
        let existed = btree.delete(key)?.is_some();
        self.table_roots[table_id as usize] = btree.root_page();
        Ok(existed)
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dirty_pages = self.dirty_pages.len())
    )]
    pub fn commit(mut self) -> Result<()> {
        // Collect dirty page IDs before pages are moved to the cache.
        let dirty_page_ids: Vec<PageId> = self.dirty_pages.keys().copied().collect();

        // Record modified page IDs in the backup dirty bitmap before draining.
        // This enables incremental backups to capture exactly which pages changed.
        {
            let mut bitmap = self.db.dirty_bitmap.lock();
            for &page_id in &dirty_page_ids {
                bitmap.mark(page_id);
            }
        }

        // Move all buffered dirty pages into the shared cache
        for (_, page) in self.dirty_pages.drain() {
            self.db.cache.insert(page);
        }

        // Increment generation and record which pages were modified.
        let current_gen = self.db.generation.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut page_gens = self.db.page_generations.lock();
            for &page_id in &dirty_page_ids {
                page_gens.insert(page_id, current_gen);
            }
        }

        self.db.flush_pages()?;

        // Persist table directory and header using dual-slot commit protocol
        // This includes the necessary syncs for crash safety
        self.db.persist_state_to_disk(&self.table_roots, self.snapshot_id)?;

        // Advertise that the god byte now references this snapshot, so a
        // concurrent `Database::sync_state` can coalesce against it instead
        // of redundantly re-running `flush_pages` + `persist_state_to_disk`.
        // Release pairs with the `Acquire` load in `sync_state`'s coalesce
        // check (see `Database::sync_state` in `db/mod.rs`). Mirrors the
        // store done at the end of `sync_state` on the successful path.
        self.db.last_synced_snapshot_id.store(self.snapshot_id.raw(), Ordering::Release);

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

    /// Commits the transaction to the in-memory committed state WITHOUT
    /// running the dual-slot on-disk persist.
    ///
    /// After this returns:
    /// - Dirty pages have been moved into the shared page cache, making them visible to future
    ///   [`ReadTransaction`] instances in this process.
    /// - `committed_state` has been atomically swapped to include this txn's table roots.
    ///   In-process reads observe the new state immediately.
    /// - On-disk backend state (pages + dual-slot state pointer) has NOT been touched. The on-disk
    ///   state pointer still references the previous synced snapshot.
    ///
    /// Durability is deferred to the next `Database::sync_state` call.
    /// `sync_state` runs periodically via `StateCheckpointer`, and is forced
    /// synchronously before snapshot creation, before backup creation, and
    /// during graceful shutdown.
    ///
    /// Intended only for callers whose writes are reconstructible from
    /// the WAL on crash recovery — see the "API surface and
    /// commit-durability classification" table in
    /// `docs/superpowers/specs/2026-04-19-sprint-1b2-apply-batching-design.md`.
    /// Callers that need strict on-disk durability on return (e.g.
    /// `save_vote`) MUST use [`WriteTransaction::commit`] instead.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns `Result<()>` for signature symmetry with
    /// [`WriteTransaction::commit`] and for forward compatibility if future
    /// bookkeeping steps become fallible.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dirty_pages = self.dirty_pages.len())
    )]
    pub fn commit_in_memory(mut self) -> Result<()> {
        // Collect dirty page IDs before pages are moved to the cache.
        let dirty_page_ids: Vec<PageId> = self.dirty_pages.keys().copied().collect();

        // Record modified page IDs in the backup dirty bitmap before draining.
        // This enables incremental backups to capture exactly which pages changed.
        {
            let mut bitmap = self.db.dirty_bitmap.lock();
            for &page_id in &dirty_page_ids {
                bitmap.mark(page_id);
            }
        }

        // Move all buffered dirty pages into the shared cache
        for (_, page) in self.dirty_pages.drain() {
            self.db.cache.insert(page);
        }

        // Increment generation and record which pages were modified.
        let current_gen = self.db.generation.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut page_gens = self.db.page_generations.lock();
            for &page_id in &dirty_page_ids {
                page_gens.insert(page_id, current_gen);
            }
        }

        // NOTE: `flush_pages` and `persist_state_to_disk` are intentionally
        // skipped here — that is the entire point of this variant. Pages
        // stay in the in-process cache until the next `sync_state` flushes
        // them, and the dual-slot state pointer on disk continues to
        // reference the previous synced snapshot.

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

        // NOTE: do NOT call try_free_pending_pages() here. Unlike `commit`, this
        // method does not advance the on-disk god byte, so the durable state
        // pointer still references the pre-commit root graph. Releasing freed
        // page IDs to the allocator now would let a subsequent durable `commit`
        // reuse them and overwrite pages that recovery would walk from the old
        // root. The drain runs in `Database::sync_state` AFTER
        // `persist_state_to_disk` advances the god byte. See the apply-batching
        // design doc's "Invariants that MUST be preserved" section.

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
