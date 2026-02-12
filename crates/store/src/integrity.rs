//! Integrity scrubber for detecting silent data corruption.
//!
//! Provides page-level checksum verification and B-tree structural invariant
//! checks. Designed to be called periodically by a background job to detect
//! bit rot and data corruption before it causes application-level failures.

use std::collections::HashSet;

use crate::{
    Database,
    backend::StorageBackend,
    btree::node::{BranchNodeRef, LeafNodeRef},
    error::{PageId, PageType},
    page::Page,
    tables::TableId,
};

/// Result of an integrity scrub cycle.
#[derive(Debug, Clone, Default)]
pub struct ScrubResult {
    /// Number of pages checked.
    pub pages_checked: u64,
    /// Number of checksum errors detected.
    pub checksum_errors: u64,
    /// Number of structural errors detected (key ordering, invalid page types).
    pub structural_errors: u64,
    /// Detailed error descriptions.
    pub errors: Vec<ScrubError>,
}

/// A single integrity error detected during scrubbing.
#[derive(Debug, Clone)]
pub struct ScrubError {
    /// The page where the error was detected.
    pub page_id: PageId,
    /// The table this page belongs to (if known).
    pub table_name: Option<&'static str>,
    /// Human-readable description of the error.
    pub description: String,
}

/// Integrity scrubber for a store database.
///
/// Verifies page checksums and B-tree structural invariants to detect
/// silent data corruption. Operates on raw pages, bypassing the cache
/// to verify what is actually on disk.
pub struct IntegrityScrubber<'a, B: StorageBackend> {
    db: &'a Database<B>,
}

impl<'a, B: StorageBackend> IntegrityScrubber<'a, B> {
    /// Creates a scrubber for the given database.
    pub fn new(db: &'a Database<B>) -> Self {
        Self { db }
    }

    /// Verifies checksums for the specified page IDs.
    ///
    /// Reads each page raw (bypassing cache) and verifies its stored checksum
    /// against a freshly computed one. Pages that are on the free list or
    /// unallocated are skipped.
    pub fn verify_page_checksums(&self, page_ids: &[PageId]) -> ScrubResult {
        let free_pages: HashSet<PageId> = self.db.free_page_ids().into_iter().collect();
        let total_pages = self.db.total_page_count();
        let mut result = ScrubResult::default();

        for &page_id in page_ids {
            // Skip free pages and out-of-range pages
            if free_pages.contains(&page_id) || page_id >= total_pages {
                continue;
            }

            result.pages_checked += 1;

            match self.db.read_raw_page(page_id) {
                Ok(page) => {
                    // Only verify checksums for B-tree pages.
                    // Infrastructure pages (table directory, free list) use
                    // different formats without standard page-header checksums.
                    let has_checksum = matches!(
                        page.page_type(),
                        Ok(PageType::BTreeLeaf)
                            | Ok(PageType::BTreeBranch)
                            | Ok(PageType::Overflow)
                    );

                    if has_checksum && !page.verify_checksum() {
                        result.checksum_errors += 1;
                        result.errors.push(ScrubError {
                            page_id,
                            table_name: None,
                            description: format!("Checksum mismatch on page {page_id}"),
                        });
                    }
                },
                Err(crate::error::Error::PageNotFound { .. }) => {
                    // Unwritten page — skip
                },
                Err(e) => {
                    result.checksum_errors += 1;
                    result.errors.push(ScrubError {
                        page_id,
                        table_name: None,
                        description: format!("Failed to read page {page_id}: {e}"),
                    });
                },
            }
        }

        result
    }

    /// Verifies B-tree structural invariants for all non-empty tables.
    ///
    /// Checks:
    /// - Leaf key ordering (keys must be strictly ascending)
    /// - Branch key ordering (separator keys must be strictly ascending)
    /// - Page type consistency (leaf pages are leaves, branch pages are branches)
    pub fn verify_btree_invariants(&self) -> ScrubResult {
        let table_roots = self.db.table_root_pages();
        let mut result = ScrubResult::default();

        for table_id in TableId::all() {
            let root_page_id = table_roots[table_id as usize];
            if root_page_id == 0 {
                continue;
            }

            self.verify_subtree(root_page_id, table_id.name(), &mut result);
        }

        result
    }

    /// Recursively verify a subtree rooted at the given page.
    fn verify_subtree(&self, page_id: PageId, table_name: &'static str, result: &mut ScrubResult) {
        let page = match self.db.read_raw_page(page_id) {
            Ok(p) => p,
            Err(e) => {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id,
                    table_name: Some(table_name),
                    description: format!("Cannot read page {page_id} in table {table_name}: {e}"),
                });
                return;
            },
        };

        result.pages_checked += 1;

        match page.page_type() {
            Ok(PageType::BTreeLeaf) => {
                self.verify_leaf_ordering(&page, table_name, result);
            },
            Ok(PageType::BTreeBranch) => {
                self.verify_branch_ordering(&page, page_id, table_name, result);
            },
            Ok(other) => {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id,
                    table_name: Some(table_name),
                    description: format!(
                        "Unexpected page type {other:?} in B-tree for table {table_name}"
                    ),
                });
            },
            Err(e) => {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id,
                    table_name: Some(table_name),
                    description: format!(
                        "Invalid page header on page {page_id} in table {table_name}: {e}"
                    ),
                });
            },
        }
    }

    /// Verifies that keys in a leaf node are strictly ascending.
    fn verify_leaf_ordering(
        &self,
        page: &Page,
        table_name: &'static str,
        result: &mut ScrubResult,
    ) {
        let leaf = match LeafNodeRef::from_page(page) {
            Ok(l) => l,
            Err(e) => {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id: page.id,
                    table_name: Some(table_name),
                    description: format!("Cannot parse leaf node on page {}: {e}", page.id),
                });
                return;
            },
        };

        let count = leaf.cell_count() as usize;
        for i in 1..count {
            let prev = leaf.key(i - 1);
            let curr = leaf.key(i);
            if prev >= curr {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id: page.id,
                    table_name: Some(table_name),
                    description: format!(
                        "Key ordering violation in leaf page {}: key[{}] >= key[{}]",
                        page.id,
                        i - 1,
                        i
                    ),
                });
                // Report one error per leaf, don't flood
                return;
            }
        }
    }

    /// Verifies branch node key ordering and recurses into children.
    fn verify_branch_ordering(
        &self,
        page: &Page,
        page_id: PageId,
        table_name: &'static str,
        result: &mut ScrubResult,
    ) {
        let branch = match BranchNodeRef::from_page(page) {
            Ok(b) => b,
            Err(e) => {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id,
                    table_name: Some(table_name),
                    description: format!("Cannot parse branch node on page {page_id}: {e}"),
                });
                return;
            },
        };

        let count = branch.cell_count() as usize;

        // Verify separator key ordering
        for i in 1..count {
            let prev = branch.key(i - 1);
            let curr = branch.key(i);
            if prev >= curr {
                result.structural_errors += 1;
                result.errors.push(ScrubError {
                    page_id,
                    table_name: Some(table_name),
                    description: format!(
                        "Separator key ordering violation in branch page {page_id}: key[{}] >= key[{}]",
                        i - 1, i
                    ),
                });
                return;
            }
        }

        // Recurse into children
        for i in 0..count {
            let child_id = branch.child(i);
            self.verify_subtree(child_id, table_name, result);
        }
        // Rightmost child
        let rightmost = branch.rightmost_child();
        if rightmost != 0 {
            self.verify_subtree(rightmost, table_name, result);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::{Database, tables::Entities};

    #[test]
    fn test_scrub_empty_database() {
        let db = Database::open_in_memory().unwrap();
        let scrubber = IntegrityScrubber::new(&db);

        // No allocated pages beyond the initial header pages
        let result = scrubber.verify_page_checksums(&[]);
        assert_eq!(result.pages_checked, 0);
        assert_eq!(result.checksum_errors, 0);
    }

    #[test]
    fn test_scrub_valid_pages() {
        let db = Database::open_in_memory().unwrap();

        // Insert some data to create pages
        {
            let mut txn = db.write().unwrap();
            for i in 0..10u32 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i}").into_bytes();
                txn.insert::<Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }

        let scrubber = IntegrityScrubber::new(&db);
        let total = db.total_page_count();
        let page_ids: Vec<PageId> = (0..total).collect();
        let result = scrubber.verify_page_checksums(&page_ids);

        assert!(result.pages_checked > 0);
        assert_eq!(result.checksum_errors, 0, "errors: {:?}", result.errors);
    }

    #[test]
    fn test_scrub_detects_corrupted_checksum() {
        let db = Database::open_in_memory().unwrap();

        // Insert data
        {
            let mut txn = db.write().unwrap();
            txn.insert::<Entities>(&b"key".to_vec(), &b"value".to_vec()).unwrap();
            txn.commit().unwrap();
        }

        // Find a valid leaf page
        let roots = db.table_root_pages();
        let root = roots[TableId::Entities as usize];
        assert!(root > 0, "Entities table should have a root page");

        // Corrupt the page data in the backend
        {
            let mut page = db.read_raw_page(root).unwrap();
            // Flip a byte in the content area (after header)
            let corrupt_offset = 20; // well into content
            page.data[corrupt_offset] ^= 0xFF;
            // Write back the corrupted page bypassing checksums
            db.write_raw_page_for_test(root, &page.data).unwrap();
        }

        let scrubber = IntegrityScrubber::new(&db);
        let result = scrubber.verify_page_checksums(&[root]);

        assert_eq!(result.pages_checked, 1);
        assert_eq!(result.checksum_errors, 1);
        assert!(result.errors[0].description.contains("Checksum mismatch"));
    }

    #[test]
    fn test_btree_invariants_valid() {
        let db = Database::open_in_memory().unwrap();

        // Insert enough data to create a multi-level tree
        {
            let mut txn = db.write().unwrap();
            for i in 0..100u32 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i}").into_bytes();
                txn.insert::<Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }

        let scrubber = IntegrityScrubber::new(&db);
        let result = scrubber.verify_btree_invariants();

        assert!(result.pages_checked > 0);
        assert_eq!(result.structural_errors, 0, "errors: {:?}", result.errors);
    }

    #[test]
    fn test_btree_invariants_empty_database() {
        let db = Database::open_in_memory().unwrap();
        let scrubber = IntegrityScrubber::new(&db);
        let result = scrubber.verify_btree_invariants();

        assert_eq!(result.pages_checked, 0);
        assert_eq!(result.structural_errors, 0);
    }

    #[test]
    fn test_scrub_skips_free_pages() {
        let db = Database::open_in_memory().unwrap();

        // Insert and delete to create free pages
        {
            let mut txn = db.write().unwrap();
            for i in 0..20u32 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i}").into_bytes();
                txn.insert::<Entities>(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }
        {
            let mut txn = db.write().unwrap();
            for i in 0..10u32 {
                let key = format!("key_{i:04}").into_bytes();
                txn.delete::<Entities>(&key).unwrap();
            }
            txn.commit().unwrap();
        }

        let free_pages = db.free_page_ids();
        let scrubber = IntegrityScrubber::new(&db);

        // Scrub only free pages — should skip them all
        let result = scrubber.verify_page_checksums(&free_pages);
        assert_eq!(result.pages_checked, 0);
    }

    #[test]
    fn test_scrub_skips_out_of_range_pages() {
        let db = Database::open_in_memory().unwrap();
        let scrubber = IntegrityScrubber::new(&db);

        let result = scrubber.verify_page_checksums(&[999_999]);
        assert_eq!(result.pages_checked, 0);
    }

    #[test]
    fn test_scrub_result_default() {
        let result = ScrubResult::default();
        assert_eq!(result.pages_checked, 0);
        assert_eq!(result.checksum_errors, 0);
        assert_eq!(result.structural_errors, 0);
        assert!(result.errors.is_empty());
    }
}
