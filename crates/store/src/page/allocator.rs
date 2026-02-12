//! Simple free-list page allocator.
//!
//! The store uses a simple free list rather than a buddy allocator because:
//! - All pages are the same size (no variable allocation orders)
//! - Single-writer model simplifies state management
//! - Free list can be rebuilt from B-tree scan on recovery

use parking_lot::Mutex;

use crate::error::PageId;

/// Simple free-list based page allocator.
///
/// Pages are allocated from a free list. When the free list is empty,
/// new pages are allocated by extending the file.
pub struct PageAllocator {
    /// Free pages available for reuse.
    free_pages: Mutex<Vec<PageId>>,
    /// Next page ID to allocate if free list is empty.
    next_page: Mutex<PageId>,
    /// Page size in bytes.
    page_size: usize,
}

impl PageAllocator {
    /// Creates a new allocator.
    pub fn new(page_size: usize, initial_next_page: PageId) -> Self {
        Self {
            free_pages: Mutex::new(Vec::new()),
            next_page: Mutex::new(initial_next_page),
            page_size,
        }
    }

    /// Allocates a new page.
    ///
    /// Returns a page ID. Prefers reusing freed pages over allocating new ones.
    pub fn allocate(&self) -> PageId {
        // Try to reuse a freed page first
        if let Some(page_id) = self.free_pages.lock().pop() {
            return page_id;
        }

        // Allocate a new page by incrementing the counter
        let mut next = self.next_page.lock();
        let page_id = *next;
        *next += 1;
        page_id
    }

    /// Frees a page for later reuse.
    ///
    /// The page will be added to the free list and may be returned
    /// by a future `allocate()` call.
    pub fn free(&self, page_id: PageId) {
        self.free_pages.lock().push(page_id);
    }

    /// Frees multiple pages at once.
    pub fn free_batch(&self, page_ids: &[PageId]) {
        let mut free_pages = self.free_pages.lock();
        free_pages.extend_from_slice(page_ids);
    }

    /// Returns the next page ID that would be allocated (for file size calculation).
    pub fn next_page_id(&self) -> PageId {
        *self.next_page.lock()
    }

    /// Sets the next page ID (used during recovery).
    pub fn set_next_page(&self, page_id: PageId) {
        *self.next_page.lock() = page_id;
    }

    /// Returns the number of free pages in the free list.
    pub fn free_page_count(&self) -> usize {
        self.free_pages.lock().len()
    }

    /// Returns the total number of page slots (the next page ID that would be allocated).
    pub fn total_pages(&self) -> PageId {
        self.next_page_id()
    }

    /// Clears the free list (used during recovery before rebuilding).
    pub fn clear_free_list(&self) {
        self.free_pages.lock().clear();
    }

    /// Initializes free list from a list of free page IDs (used during recovery).
    pub fn init_free_list(&self, free_pages: Vec<PageId>) {
        *self.free_pages.lock() = free_pages;
    }

    /// Returns a copy of the current free list (for debugging/testing).
    pub fn get_free_list(&self) -> Vec<PageId> {
        self.free_pages.lock().clone()
    }

    /// Returns page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Calculates required file size in bytes for current allocation state.
    pub fn required_file_size(&self, header_size: usize) -> u64 {
        let next_page = self.next_page_id();
        header_size as u64 + (next_page * self.page_size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_new_pages() {
        let allocator = PageAllocator::new(4096, 0);

        assert_eq!(allocator.allocate(), 0);
        assert_eq!(allocator.allocate(), 1);
        assert_eq!(allocator.allocate(), 2);
        assert_eq!(allocator.next_page_id(), 3);
    }

    #[test]
    fn test_free_and_reuse() {
        let allocator = PageAllocator::new(4096, 0);

        // Allocate some pages
        let _p0 = allocator.allocate();
        let p1 = allocator.allocate();
        let _p2 = allocator.allocate();

        // Free one
        allocator.free(p1);

        // Next allocation should reuse freed page
        let p3 = allocator.allocate();
        assert_eq!(p3, p1);

        // Now should allocate new
        let p4 = allocator.allocate();
        assert_eq!(p4, 3);
    }

    #[test]
    fn test_free_batch() {
        let allocator = PageAllocator::new(4096, 0);

        // Allocate pages 0-4
        for _ in 0..5 {
            allocator.allocate();
        }

        // Free pages 1, 2, 3 as a batch
        allocator.free_batch(&[1, 2, 3]);
        assert_eq!(allocator.free_page_count(), 3);

        // Allocate should reuse (LIFO order)
        assert_eq!(allocator.allocate(), 3);
        assert_eq!(allocator.allocate(), 2);
        assert_eq!(allocator.allocate(), 1);
        assert_eq!(allocator.allocate(), 5); // New page
    }

    #[test]
    fn test_required_file_size() {
        let allocator = PageAllocator::new(4096, 0);

        // Header only
        assert_eq!(allocator.required_file_size(512), 512);

        // Allocate 3 pages
        allocator.allocate();
        allocator.allocate();
        allocator.allocate();

        // 512 header + 3 * 4096 pages
        assert_eq!(allocator.required_file_size(512), 512 + 3 * 4096);
    }
}
