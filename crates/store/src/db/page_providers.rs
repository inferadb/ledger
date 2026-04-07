//! Page providers for B-tree operations within transactions.
//!
//! These types bridge the transaction layer with the B-tree layer by
//! implementing [`PageProvider`]. Each variant controls read/write visibility
//! to enforce transaction isolation.

use std::{cell::RefCell, collections::HashMap};

use super::Database;
use crate::{
    backend::StorageBackend,
    btree::PageProvider,
    error::{PageId, PageType, Result},
    page::Page,
};

/// Page provider for read-only operations with transaction-local caching.
///
/// This provider caches pages locally within the transaction to ensure
/// snapshot isolation. Once a page is read, it's stored in the local cache
/// so subsequent reads return the same data even if concurrent writes
/// modify the shared cache.
pub(super) struct CachingReadPageProvider<'txn, 'db, B: StorageBackend> {
    pub(super) db: &'db Database<B>,
    /// Reference to the transaction's local page cache.
    pub(super) page_cache: &'txn RefCell<HashMap<PageId, Page>>,
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

    fn write_page(&mut self, _page: &Page) {
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
pub(super) struct BufferedWritePageProvider<'txn, 'db, B: StorageBackend> {
    pub(super) db: &'db Database<B>,
    pub(super) txn_id: u64,
    /// Mutable reference to the transaction's dirty page buffer.
    pub(super) dirty_pages: &'txn mut HashMap<PageId, Page>,
    /// Pages that should be freed after commit (COW deferred cleanup).
    pub(super) pages_to_free: &'txn mut Vec<PageId>,
}

impl<B: StorageBackend> PageProvider for BufferedWritePageProvider<'_, '_, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, page: &Page) {
        // Update checksum and store in local buffer (NOT shared cache)
        // This ensures concurrent read transactions don't see uncommitted writes
        let mut owned = page.clone();
        owned.update_checksum();
        self.dirty_pages.insert(owned.id, owned);
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
pub(super) struct BufferedReadPageProvider<'txn, 'db, B: StorageBackend> {
    pub(super) db: &'db Database<B>,
    /// Immutable reference to the transaction's dirty page buffer.
    pub(super) dirty_pages: &'txn HashMap<PageId, Page>,
}

impl<B: StorageBackend> PageProvider for BufferedReadPageProvider<'_, '_, B> {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        // Check our local buffer first (read-your-own-writes)
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, _page: &Page) {
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
