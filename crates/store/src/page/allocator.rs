//! Free-list page allocator.
//!
//! The store uses a free list rather than a buddy allocator because:
//! - All pages are the same size (no variable allocation orders)
//! - Single-writer model simplifies state management
//! - Free list can be rebuilt from B-tree scan on recovery

use std::collections::HashSet;

use parking_lot::Mutex;

use crate::error::PageId;

/// Internal state guarded by a single mutex.
struct AllocatorState {
    /// Free pages available for reuse, stored in LIFO order.
    free_pages: Vec<PageId>,
    /// Mirror set for O(1) duplicate detection. Always kept in sync with `free_pages`.
    free_set: HashSet<PageId>,
    /// Next page ID to allocate if free list is empty.
    next_page: PageId,
}

/// Free-list based page allocator.
///
/// Pages are allocated from a free list. When the free list is empty,
/// new pages are allocated by extending the file.
pub struct PageAllocator {
    state: Mutex<AllocatorState>,
    /// Page size in bytes.
    page_size: usize,
}

impl PageAllocator {
    /// Creates a new allocator.
    pub fn new(page_size: usize, initial_next_page: PageId) -> Self {
        Self {
            state: Mutex::new(AllocatorState {
                free_pages: Vec::new(),
                free_set: HashSet::new(),
                next_page: initial_next_page,
            }),
            page_size,
        }
    }

    /// Allocates a new page.
    ///
    /// Returns a page ID. Prefers reusing freed pages over allocating new ones.
    pub fn allocate(&self) -> PageId {
        let mut state = self.state.lock();
        if let Some(page_id) = state.free_pages.pop() {
            state.free_set.remove(&page_id);
            return page_id;
        }
        let page_id = state.next_page;
        state.next_page += 1;
        page_id
    }

    /// Frees a page for later reuse.
    ///
    /// The page will be added to the free list and may be returned
    /// by a future `allocate()` call.
    ///
    /// If `page_id` is already in the free list (double-free), the call is
    /// silently ignored and a warning is emitted. This prevents duplicate
    /// entries from causing two allocations to return the same page.
    pub fn free(&self, page_id: PageId) {
        let mut state = self.state.lock();
        if !state.free_set.insert(page_id) {
            tracing::warn!(page_id, "double-free detected: page already in free list; ignoring");
            return;
        }
        state.free_pages.push(page_id);
    }

    /// Frees multiple pages at once.
    ///
    /// Pages already in the free list are silently skipped (see [`free`](Self::free)).
    pub fn free_batch(&self, page_ids: &[PageId]) {
        let mut state = self.state.lock();
        for &page_id in page_ids {
            if !state.free_set.insert(page_id) {
                tracing::warn!(
                    page_id,
                    "double-free detected in batch: page already in free list; ignoring"
                );
                continue;
            }
            state.free_pages.push(page_id);
        }
    }

    /// Returns the next page ID that would be allocated (for file size calculation).
    pub fn next_page_id(&self) -> PageId {
        self.state.lock().next_page
    }

    /// Sets the next page ID (used during recovery).
    pub fn set_next_page(&self, page_id: PageId) {
        self.state.lock().next_page = page_id;
    }

    /// Returns the number of free pages in the free list.
    pub fn free_page_count(&self) -> usize {
        self.state.lock().free_pages.len()
    }

    /// Returns the high-water mark page ID (number of page slots ever allocated).
    pub fn total_pages(&self) -> PageId {
        self.next_page_id()
    }

    /// Clears the free list (used during recovery before rebuilding).
    pub fn clear_free_list(&self) {
        let mut state = self.state.lock();
        state.free_pages.clear();
        state.free_set.clear();
    }

    /// Initializes free list from a list of free page IDs (used during recovery).
    ///
    /// Duplicates in `free_pages` are removed; only the last occurrence of each
    /// page ID is retained (LIFO order is preserved for non-duplicate entries).
    pub fn init_free_list(&self, free_pages: Vec<PageId>) {
        let mut state = self.state.lock();
        state.free_set.clear();
        state.free_pages.clear();
        for page_id in free_pages {
            if state.free_set.insert(page_id) {
                state.free_pages.push(page_id);
            }
        }
    }

    /// Returns a copy of the current free list (for debugging/testing).
    pub fn get_free_list(&self) -> Vec<PageId> {
        self.state.lock().free_pages.clone()
    }

    /// Returns the page size in bytes.
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
    use proptest::prelude::*;

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

    /// A double-free must not cause two separate allocations to return the same page.
    #[test]
    fn test_double_free_does_not_duplicate_in_free_list() {
        let allocator = PageAllocator::new(4096, 0);

        let p0 = allocator.allocate();
        let p1 = allocator.allocate();
        let _p2 = allocator.allocate();

        allocator.free(p0);
        allocator.free(p1);

        // Double-free p0: should be silently ignored.
        allocator.free(p0);

        // Free list should still have exactly 2 entries (p0 and p1).
        assert_eq!(allocator.free_page_count(), 2);

        let a = allocator.allocate();
        let b = allocator.allocate();

        // The two allocations must return distinct page IDs.
        assert_ne!(a, b, "double-free caused two allocations to return the same page");

        // Both returned pages must be among the originally freed ones.
        assert!(
            (a == p0 || a == p1) && (b == p0 || b == p1),
            "unexpected page IDs returned: a={a}, b={b}"
        );
    }

    /// Freeing the same page twice via free_batch must not duplicate it.
    #[test]
    fn test_double_free_via_batch_does_not_duplicate() {
        let allocator = PageAllocator::new(4096, 0);

        allocator.allocate(); // p0
        allocator.allocate(); // p1

        // Batch containing a duplicate.
        allocator.free_batch(&[0, 1, 0]);

        // Only two unique pages should be in the free list.
        assert_eq!(allocator.free_page_count(), 2);

        let a = allocator.allocate();
        let b = allocator.allocate();
        assert_ne!(a, b, "duplicate in free_batch caused aliased allocation");
    }

    /// `init_free_list` with duplicates must deduplicate on load.
    #[test]
    fn test_init_free_list_deduplicates() {
        let allocator = PageAllocator::new(4096, 10);

        allocator.init_free_list(vec![1, 2, 3, 2, 1]);

        // Three unique IDs despite five entries.
        assert_eq!(allocator.free_page_count(), 3);

        let mut allocated = std::collections::HashSet::new();
        for _ in 0..3 {
            let p = allocator.allocate();
            assert!(allocated.insert(p), "init_free_list duplicate caused aliased allocation: {p}");
        }
    }

    /// After a free-set-tracked free, `clear_free_list` resets both the vec and the set,
    /// so subsequent frees of the same page succeed normally.
    #[test]
    fn test_clear_free_list_resets_dedup_state() {
        let allocator = PageAllocator::new(4096, 0);

        allocator.allocate(); // p0
        allocator.free(0);
        assert_eq!(allocator.free_page_count(), 1);

        allocator.clear_free_list();
        assert_eq!(allocator.free_page_count(), 0);

        // After clearing, freeing the same page again must succeed (not be treated as duplicate).
        allocator.free(0);
        assert_eq!(allocator.free_page_count(), 1);
    }

    /// Defines an operation on the allocator for property testing.
    #[derive(Debug, Clone)]
    enum AllocOp {
        Allocate,
        Free(u8), // index into `live` pages; ignored if live is empty
    }

    fn arb_alloc_ops() -> impl Strategy<Value = Vec<AllocOp>> {
        proptest::collection::vec(
            prop_oneof![
                Just(AllocOp::Allocate),
                (0u8..32u8).prop_map(AllocOp::Free),
            ],
            1..200,
        )
    }

    proptest! {
        /// No page is ever returned by two concurrent live allocations.
        ///
        /// Invariant: the set of currently-allocated (live) pages never contains
        /// duplicates, even when the same page is freed multiple times before
        /// being reallocated.
        #[test]
        fn prop_no_aliased_allocations(ops in arb_alloc_ops()) {
            let allocator = PageAllocator::new(4096, 0);
            let mut live: Vec<PageId> = Vec::new();

            for op in ops {
                match op {
                    AllocOp::Allocate => {
                        let p = allocator.allocate();
                        // The newly allocated page must not already be live.
                        prop_assert!(
                            !live.contains(&p),
                            "page {p} was allocated while still live; live={live:?}"
                        );
                        live.push(p);
                    }
                    AllocOp::Free(idx) => {
                        if live.is_empty() {
                            continue;
                        }
                        let idx = (idx as usize) % live.len();
                        let p = live.swap_remove(idx);
                        allocator.free(p);
                        // Double-free the same page; the allocator must silently ignore it.
                        allocator.free(p);
                    }
                }
            }
        }
    }
}
