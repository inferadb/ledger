//! Page cache for the store engine.
//!
//! The cache stores recently accessed pages to reduce I/O.
//! Uses clock eviction (approximate LRU) for low overhead.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;

use super::Page;
use crate::error::PageId;

/// Page cache with clock eviction.
///
/// Caches recently accessed pages to reduce disk I/O.
/// Uses clock algorithm (approximation of LRU) for eviction.
/// Tracks hit/miss counters for observability.
pub struct PageCache {
    /// Cached pages.
    pages: RwLock<HashMap<PageId, CacheEntry>>,
    /// Maximum number of pages to cache.
    capacity: usize,
    /// Clock hand for eviction.
    clock_hand: RwLock<usize>,
    /// Order of pages for clock algorithm.
    page_order: RwLock<Vec<PageId>>,
    /// Total cache hits since creation.
    hits: AtomicU64,
    /// Total cache misses since creation.
    misses: AtomicU64,
}

/// Cache entry with access tracking.
struct CacheEntry {
    /// The cached page.
    page: Page,
    /// Whether page was accessed since last clock sweep (second chance).
    accessed: bool,
}

impl PageCache {
    /// Creates a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: RwLock::new(HashMap::with_capacity(capacity)),
            capacity,
            clock_hand: RwLock::new(0),
            page_order: RwLock::new(Vec::with_capacity(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Returns a clone of the cached page, or `None` if not present.
    pub fn get(&self, page_id: PageId) -> Option<Page> {
        let mut pages = self.pages.write();
        if let Some(entry) = pages.get_mut(&page_id) {
            entry.accessed = true;
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.page.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Inserts a page into the cache.
    ///
    /// May evict an existing clean page if the cache is full.
    /// If all pages are dirty, allows temporary growth beyond capacity
    /// to prevent data loss (dirty pages must be flushed, not evicted).
    pub fn insert(&self, page: Page) {
        let page_id = page.id;
        let mut pages = self.pages.write();

        // Check if already in cache
        if let Some(entry) = pages.get_mut(&page_id) {
            entry.page = page;
            entry.accessed = true;
            return;
        }

        // Try to evict if at capacity
        if pages.len() >= self.capacity {
            drop(pages); // Release lock before eviction
            let evicted = self.evict_one();
            pages = self.pages.write();

            // If eviction failed (all pages dirty), allow temporary growth.
            // This is safer than losing data - the transaction commit will
            // flush dirty pages and restore capacity.
            if !evicted && pages.len() >= self.capacity {
                // Allow growth but log in debug builds
                #[cfg(debug_assertions)]
                if pages.len() > self.capacity * 2 {
                    eprintln!(
                        "Warning: Cache grew to {} pages (capacity {}), consider flushing",
                        pages.len(),
                        self.capacity
                    );
                }
            }
        }

        // Insert new entry
        let mut page_order = self.page_order.write();
        page_order.push(page_id);

        pages.insert(page_id, CacheEntry { page, accessed: true });
    }

    /// Removes a page from the cache.
    pub fn remove(&self, page_id: PageId) -> Option<Page> {
        let mut pages = self.pages.write();
        let mut page_order = self.page_order.write();

        if let Some(entry) = pages.remove(&page_id) {
            page_order.retain(|&id| id != page_id);
            Some(entry.page)
        } else {
            None
        }
    }

    /// Evicts one page using the clock algorithm.
    ///
    /// IMPORTANT: Never evicts dirty pages to prevent data loss. If all pages
    /// are dirty, returns false and the caller must flush before retrying.
    fn evict_one(&self) -> bool {
        let mut pages = self.pages.write();
        let mut page_order = self.page_order.write();
        let mut clock_hand = self.clock_hand.write();

        if page_order.is_empty() {
            return true; // Nothing to evict, but that's fine
        }

        // Clock algorithm: find first page with accessed=false AND not dirty
        // If all pages have accessed=true, clear flags and retry
        // Never evict dirty pages - they must be flushed first
        let mut iterations = 0;
        let max_iterations = page_order.len() * 2; // At most 2 sweeps

        loop {
            if *clock_hand >= page_order.len() {
                *clock_hand = 0;
            }

            let page_id = page_order[*clock_hand];

            if let Some(entry) = pages.get_mut(&page_id) {
                // Never evict dirty pages - this would cause data loss
                if entry.page.dirty {
                    *clock_hand += 1;
                    iterations += 1;
                } else if !entry.accessed {
                    // Evict this clean page
                    pages.remove(&page_id);
                    page_order.remove(*clock_hand);
                    if *clock_hand >= page_order.len() && !page_order.is_empty() {
                        *clock_hand = 0;
                    }
                    return true;
                } else {
                    // Give second chance
                    entry.accessed = false;
                    *clock_hand += 1;
                    iterations += 1;
                }
            } else {
                *clock_hand += 1;
                iterations += 1;
            }

            // Safety: prevent infinite loops
            // If we've swept twice and found no clean pages, all pages are dirty
            if iterations >= max_iterations {
                return false; // Cannot evict - all pages dirty, need flush
            }
        }
    }

    /// Clears all cached pages.
    pub fn clear(&self) {
        self.pages.write().clear();
        self.page_order.write().clear();
        *self.clock_hand.write() = 0;
    }

    /// Returns the number of cached pages.
    pub fn len(&self) -> usize {
        self.pages.read().len()
    }

    /// Checks if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.read().is_empty()
    }

    /// Checks if a page is in the cache.
    pub fn contains(&self, page_id: PageId) -> bool {
        self.pages.read().contains_key(&page_id)
    }

    /// Returns all dirty pages (for flushing).
    pub fn dirty_pages(&self) -> Vec<Page> {
        self.pages.read().values().filter(|e| e.page.dirty).map(|e| e.page.clone()).collect()
    }

    /// Marks a page as clean (after flushing).
    pub fn mark_clean(&self, page_id: PageId) {
        if let Some(entry) = self.pages.write().get_mut(&page_id) {
            entry.page.dirty = false;
        }
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        let pages = self.pages.read();
        CacheStats {
            size: pages.len(),
            capacity: self.capacity,
            dirty_count: pages.values().filter(|e| e.page.dirty).count(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of cached pages.
    pub size: usize,
    /// Maximum capacity.
    pub capacity: usize,
    /// Number of dirty pages.
    pub dirty_count: usize,
    /// Total cache hits since creation.
    pub hits: u64,
    /// Total cache misses since creation.
    pub misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PageType;

    /// Creates a dirty page (newly allocated, not yet flushed).
    fn make_dirty_page(id: PageId) -> Page {
        Page::new(id, 4096, PageType::BTreeLeaf, 1)
    }

    /// Creates a clean page (simulates page loaded from disk).
    fn make_clean_page(id: PageId) -> Page {
        Page::from_bytes(id, vec![0u8; 4096])
    }

    #[test]
    fn test_cache_basic() {
        let cache = PageCache::new(10);

        // Insert and get
        cache.insert(make_dirty_page(0));
        assert!(cache.contains(0));
        assert!(cache.get(0).is_some());

        // Remove
        cache.remove(0);
        assert!(!cache.contains(0));
        assert!(cache.get(0).is_none());
    }

    #[test]
    fn test_cache_eviction_clean_pages() {
        let cache = PageCache::new(3);

        // Fill cache with clean pages (can be evicted)
        cache.insert(make_clean_page(0));
        cache.insert(make_clean_page(1));
        cache.insert(make_clean_page(2));
        assert_eq!(cache.len(), 3);

        // Insert one more - should evict a clean page
        cache.insert(make_clean_page(3));
        assert_eq!(cache.len(), 3);
        assert!(cache.contains(3));
    }

    #[test]
    fn test_cache_no_eviction_of_dirty_pages() {
        let cache = PageCache::new(3);

        // Fill cache with dirty pages (cannot be evicted)
        cache.insert(make_dirty_page(0));
        cache.insert(make_dirty_page(1));
        cache.insert(make_dirty_page(2));
        assert_eq!(cache.len(), 3);

        // Insert one more - cannot evict dirty pages, cache grows
        cache.insert(make_dirty_page(3));
        assert_eq!(cache.len(), 4); // Cache exceeds capacity to preserve dirty data
        assert!(cache.contains(3));
        assert!(cache.contains(0)); // All original pages still present
        assert!(cache.contains(1));
        assert!(cache.contains(2));
    }

    #[test]
    fn test_cache_lru_behavior() {
        let cache = PageCache::new(3);

        // Fill cache with clean pages
        cache.insert(make_clean_page(0));
        cache.insert(make_clean_page(1));
        cache.insert(make_clean_page(2));

        // Access page 0 to mark it as recently used
        cache.get(0);

        // Insert new page - clock algorithm will evict one clean page
        cache.insert(make_clean_page(3));

        // Cache should still have exactly 3 pages (one was evicted)
        assert_eq!(cache.len(), 3);
        // Page 3 should be there (just inserted)
        assert!(cache.contains(3));
        // At least two of the original pages should remain
        // (Clock algorithm is an approximation of LRU, not exact LRU)
        let remaining = [0, 1, 2].iter().filter(|&&id| cache.contains(id)).count();
        assert_eq!(remaining, 2);
    }

    #[test]
    fn test_cache_dirty_tracking() {
        let cache = PageCache::new(10);

        let page = make_dirty_page(0);
        cache.insert(page);

        let dirty = cache.dirty_pages();
        assert_eq!(dirty.len(), 1);

        cache.mark_clean(0);

        let dirty = cache.dirty_pages();
        assert_eq!(dirty.len(), 0);
    }

    #[test]
    fn test_cache_mixed_dirty_clean() {
        let cache = PageCache::new(3);

        // Insert mix of dirty and clean pages
        cache.insert(make_dirty_page(0)); // dirty - won't be evicted
        cache.insert(make_clean_page(1)); // clean - can be evicted
        cache.insert(make_clean_page(2)); // clean - can be evicted
        assert_eq!(cache.len(), 3);

        // Insert another clean page - should evict one of the clean pages
        cache.insert(make_clean_page(3));
        assert_eq!(cache.len(), 3);
        assert!(cache.contains(0)); // Dirty page preserved
        assert!(cache.contains(3)); // New page added
        // One of pages 1 or 2 was evicted
        let clean_remaining = [1, 2].iter().filter(|&&id| cache.contains(id)).count();
        assert_eq!(clean_remaining, 1);
    }

    // ── Concurrency Stress Tests ────────────────────────────────────────

    /// Stress test: concurrent page cache access with eviction under contention.
    ///
    /// 10 threads inserting and reading pages simultaneously to verify the clock
    /// eviction algorithm doesn't corrupt state under concurrent access.
    #[test]
    fn stress_concurrent_cache_access_with_eviction() {
        use std::{sync::Arc, thread};

        let cache = Arc::new(PageCache::new(50)); // Small capacity to force eviction
        let num_threads = 10;
        let ops_per_thread = 200;

        let mut handles = Vec::new();

        // Mix of inserters and readers
        for thread_id in 0..num_threads {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let page_id = (thread_id * ops_per_thread + i) as PageId;
                    // Insert a clean page (evictable)
                    cache.insert(make_clean_page(page_id));

                    // Try to read back a recently inserted page
                    if i > 0 {
                        let recent_id = (thread_id * ops_per_thread + i - 1) as PageId;
                        // May or may not still be cached (eviction is allowed)
                        let _ = cache.get(recent_id);
                    }

                    // Occasionally remove a page
                    if i % 5 == 0 && i > 10 {
                        let old_id = (thread_id * ops_per_thread + i - 10) as PageId;
                        let _ = cache.remove(old_id);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify cache is in a consistent state
        let stats = cache.stats();
        assert!(stats.size <= stats.capacity + num_threads, "Cache grew far beyond capacity");
        // Total operations should have generated some hits and misses
        assert!(stats.hits + stats.misses > 0, "Expected some cache activity");
    }
}
