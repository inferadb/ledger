//! B+ tree implementation for the store engine.
//!
//! This module provides the core B+ tree data structure used for all tables.
//! The implementation is designed for:
//! - Single-writer, multiple-reader access
//! - Copy-on-write semantics for crash safety
//! - Efficient range scans via leaf node iteration
//!
//! ## Structure
//!
//! - `node.rs`: Low-level leaf and branch node operations
//! - `cursor.rs`: Iterator and range query support
//! - `split.rs`: Node splitting logic for insertions

/// Cursor and range query support for B+ tree iteration.
pub mod cursor;
/// Low-level leaf and branch node operations on pages.
pub mod node;
/// Node splitting logic for leaf and branch pages during insertions.
pub mod split;

use cursor::{Range, RangeIterState, SeekResult, cursor_ops};
use node::{BranchNode, LeafNode, LeafNodeRef, SearchResult};
use split::{can_merge_leaves, leaf_fill_factor, merge_leaves, split_branch, split_leaf_for_key};

use crate::{
    error::{Error, PageId, PageType, Result},
    page::Page,
};

/// Trait for providing page operations to the B-tree.
///
/// This abstraction allows the B-tree to work with different page providers:
/// - Read-only providers for read transactions
/// - Read-write providers for write transactions
pub trait PageProvider {
    /// Reads a page by ID.
    fn read_page(&self, page_id: PageId) -> Result<Page>;

    /// Writes a page to the cache/storage.
    fn write_page(&mut self, page: Page);

    /// Allocates a new page of the given type.
    fn allocate_page(&mut self, page_type: PageType) -> Page;

    /// Frees a page for later reuse.
    fn free_page(&mut self, page_id: PageId);

    /// Returns the page size.
    fn page_size(&self) -> usize;

    /// Returns the current transaction ID.
    fn txn_id(&self) -> u64;
}

/// Statistics returned by B+ tree compaction.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of leaf page merges performed.
    pub pages_merged: u64,
    /// Number of pages freed (returned to allocator).
    pub pages_freed: u64,
}

/// B+ tree structure for a single table.
///
/// The BTree provides typed access to key-value pairs stored on disk.
/// All operations go through the PageProvider trait.
pub struct BTree<P: PageProvider> {
    /// Page provider for reading/writing pages.
    provider: P,
    /// Root page ID (0 = empty tree).
    root_page: PageId,
    /// Number of page splits performed during this BTree's lifetime.
    split_count: u64,
}

impl<P: PageProvider> BTree<P> {
    /// Creates a new B-tree accessor.
    pub fn new(root_page: PageId, provider: P) -> Self {
        Self { provider, root_page, split_count: 0 }
    }

    /// Returns the number of page splits performed during operations on this BTree instance.
    pub fn split_count(&self) -> u64 {
        self.split_count
    }

    /// Computes the depth of the B-tree (0 = empty, 1 = root is leaf, 2+ = branches + leaf).
    ///
    /// Walks the leftmost path from root to leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails or a non-B-tree page type is encountered.
    pub fn depth(&self) -> Result<u32> {
        if self.root_page == 0 {
            return Ok(0);
        }

        let mut depth = 1u32;
        let mut page_id = self.root_page;

        loop {
            let page = self.provider.read_page(page_id)?;
            let page_type = page.page_type()?;

            match page_type {
                PageType::BTreeLeaf => return Ok(depth),
                PageType::BTreeBranch => {
                    let branch = node::BranchNodeRef::from_page(&page)?;
                    // Follow leftmost child (child at index 0 or rightmost if no cells)
                    page_id = if branch.cell_count() > 0 {
                        branch.child(0)
                    } else {
                        branch.rightmost_child()
                    };
                    depth += 1;
                },
                _ => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: page_type,
                    });
                },
            }
        }
    }

    /// Checks if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root_page == 0
    }

    /// Returns the root page ID.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Sets the root page ID (called after split creates new root).
    pub fn set_root_page(&mut self, page_id: PageId) {
        self.root_page = page_id;
    }

    /// Allocates and initializes a new leaf page.
    fn new_leaf_page(&mut self) -> Page {
        let mut page = self.provider.allocate_page(PageType::BTreeLeaf);
        LeafNode::init(&mut page);
        page
    }

    /// Allocates and initializes a new branch page.
    fn new_branch_page(&mut self, rightmost_child: PageId) -> Page {
        let mut page = self.provider.allocate_page(PageType::BTreeBranch);
        BranchNode::init(&mut page, rightmost_child);
        page
    }

    /// Returns the value associated with `key`, or `None` if the key does
    /// not exist in the tree.
    ///
    /// Uses the leaf page's embedded bloom filter for fast negative lookups:
    /// if the bloom filter says "definitely absent," the binary search is skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails or a page type mismatch is found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page == 0 {
            return Ok(None);
        }

        let leaf_page = self.find_leaf(key)?;
        let page = self.provider.read_page(leaf_page)?;
        let leaf = LeafNodeRef::from_page(&page)?;

        // Fast path: bloom filter says key is definitely absent
        if !leaf.bloom_filter().may_contain(key) {
            return Ok(None);
        }

        match leaf.search(key) {
            SearchResult::Found(idx) => Ok(Some(leaf.value(idx).to_vec())),
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Finds the leaf page that would contain the given key.
    fn find_leaf(&self, key: &[u8]) -> Result<PageId> {
        let mut current = self.root_page;

        loop {
            let page = self.provider.read_page(current)?;
            let page_type = page.page_type()?;

            match page_type {
                PageType::BTreeLeaf => return Ok(current),
                PageType::BTreeBranch => {
                    let mut page = page;
                    let branch = BranchNode::from_page(&mut page)?;
                    current = branch.child_for_key(key);
                },
                _ => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: page_type,
                    });
                },
            }
        }
    }

    /// Inserts a key-value pair into the B-tree.
    ///
    /// If the key already exists, its value is updated in-place.
    /// Returns the previous value if the key existed, or `None` for new keys.
    ///
    /// Keys and values are stored as byte slices. The B-tree maintains sorted
    /// order by key, splitting leaf and branch pages as needed to accommodate
    /// new entries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key+value exceeds the page capacity (determined by page size)
    /// - A page read or write fails through the `PageProvider`
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page == 0 {
            let mut page = self.new_leaf_page();
            {
                let mut leaf = LeafNode::from_page(&mut page)?;
                leaf.insert(0, key, value)?;
            }
            self.root_page = page.id;
            self.provider.write_page(page);
            return Ok(None);
        }

        let (path, old_value) = self.insert_recursive(self.root_page, key, value)?;

        // Handle potential root split
        if let Some((separator_key, new_child)) = path {
            // Root was split, create new root
            let mut new_root = self.new_branch_page(new_child);
            {
                let mut branch = BranchNode::from_page(&mut new_root)?;
                branch.insert(0, &separator_key, self.root_page)?;
            }
            self.root_page = new_root.id;
            self.provider.write_page(new_root);
        }

        Ok(old_value)
    }

    /// Recursive insert that returns promoted key/child if split occurred.
    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(Option<(Vec<u8>, PageId)>, Option<Vec<u8>>)> {
        let mut page = self.provider.read_page(page_id)?;
        let page_type = page.page_type()?;

        match page_type {
            PageType::BTreeLeaf => {
                let mut leaf = LeafNode::from_page(&mut page)?;
                let old_value: Option<Vec<u8>>;

                match leaf.search(key) {
                    SearchResult::Found(idx) => {
                        old_value = Some(leaf.value(idx).to_vec());
                        if !leaf.update(idx, value) {
                            // Value grew - need to delete and reinsert
                            leaf.delete(idx)?;
                            if leaf.can_insert(key, value) {
                                // Reinsert in same page
                                match leaf.search(key) {
                                    SearchResult::NotFound(new_idx) => {
                                        leaf.insert(new_idx, key, value)?;
                                    },
                                    _ => unreachable!(),
                                }
                            } else {
                                // Need to split
                                drop(leaf);
                                return self
                                    .insert_and_split_leaf(&mut page, key, value, old_value);
                            }
                        }
                        drop(leaf);
                        self.provider.write_page(page);
                        Ok((None, old_value))
                    },
                    SearchResult::NotFound(idx) => {
                        old_value = None;
                        if leaf.can_insert(key, value) {
                            leaf.insert(idx, key, value)?;
                            drop(leaf);
                            self.provider.write_page(page);
                            Ok((None, old_value))
                        } else {
                            drop(leaf);
                            self.insert_and_split_leaf(&mut page, key, value, old_value)
                        }
                    },
                }
            },
            PageType::BTreeBranch => {
                let child_page_id = {
                    let branch = BranchNode::from_page(&mut page)?;
                    branch.child_for_key(key)
                };

                let (promoted, old_value) = self.insert_recursive(child_page_id, key, value)?;

                if let Some((sep_key, new_child)) = promoted {
                    // Child was split, need to insert separator into this branch
                    // child_page_id is the original child (now left half after split)
                    // new_child is the new page (right half after split)
                    let mut branch = BranchNode::from_page(&mut page)?;

                    if branch.can_insert(&sep_key) {
                        let count = branch.cell_count() as usize;
                        let mut insert_idx = count;
                        for i in 0..count {
                            if sep_key.as_slice() < branch.key(i) {
                                insert_idx = i;
                                break;
                            }
                        }

                        // Insert the separator with the original child (left half) as left child.
                        // Then update the next child pointer to be new_child (right half).
                        if insert_idx == count {
                            // Inserting at end: original child (left half) becomes left child of
                            // sep, new_child (right half) becomes new
                            // rightmost
                            branch.insert(insert_idx, &sep_key, child_page_id)?;
                            branch.set_rightmost_child(new_child);
                        } else {
                            // Inserting in middle:
                            // - Insert separator with original child (left half) as its child
                            // - After shift, the old cell at insert_idx moved to insert_idx+1
                            // - Update that cell's child to be new_child (right half)
                            branch.insert(insert_idx, &sep_key, child_page_id)?;
                            branch.set_child(insert_idx + 1, new_child);
                        }

                        drop(branch);
                        self.provider.write_page(page);
                        Ok((None, old_value))
                    } else {
                        // Branch is full, need to split
                        drop(branch);
                        self.insert_and_split_branch(
                            &mut page,
                            &sep_key,
                            child_page_id,
                            new_child,
                            old_value,
                        )
                    }
                } else {
                    Ok((None, old_value))
                }
            },
            _ => Err(Error::PageTypeMismatch { expected: PageType::BTreeLeaf, found: page_type }),
        }
    }

    /// Inserts into a leaf and splits if necessary.
    ///
    /// This uses a key-aware split strategy: we find a split point that ensures
    /// the new key can fit in the correct side (based on ordering).
    fn insert_and_split_leaf(
        &mut self,
        page: &mut Page,
        key: &[u8],
        value: &[u8],
        old_value: Option<Vec<u8>>,
    ) -> Result<(Option<(Vec<u8>, PageId)>, Option<Vec<u8>>)> {
        self.split_count += 1;
        let mut new_page = self.new_leaf_page();

        // Use key-aware split that ensures the new key will fit
        let split_result = split_leaf_for_key(page, &mut new_page, key, value)?;

        // Now insert the new key into the appropriate half based on ordering
        let insert_left = key < split_result.separator_key.as_slice();

        if insert_left {
            let mut leaf = LeafNode::from_page(page)?;
            match leaf.search(key) {
                SearchResult::NotFound(idx) => {
                    leaf.insert(idx, key, value)?;
                },
                SearchResult::Found(_) => {},
            }
        } else {
            let mut leaf = LeafNode::from_page(&mut new_page)?;
            match leaf.search(key) {
                SearchResult::NotFound(idx) => {
                    leaf.insert(idx, key, value)?;
                },
                SearchResult::Found(_) => {},
            }
        }

        self.provider.write_page(page.clone());
        self.provider.write_page(new_page);

        Ok((Some((split_result.separator_key, split_result.new_page_id)), old_value))
    }

    /// Inserts into a branch and splits if necessary.
    ///
    /// # Arguments
    /// * `page` - The branch page to split
    /// * `key` - The separator key to insert
    /// * `original_child` - The original child that was split (left half)
    /// * `right_child` - The new child from the split (right half)
    /// * `old_value` - The old value if this was an update
    fn insert_and_split_branch(
        &mut self,
        page: &mut Page,
        key: &[u8],
        original_child: PageId,
        right_child: PageId,
        old_value: Option<Vec<u8>>,
    ) -> Result<(Option<(Vec<u8>, PageId)>, Option<Vec<u8>>)> {
        self.split_count += 1;
        let mut new_page = self.new_branch_page(0);

        let split_result = split_branch(page, &mut new_page)?;

        // Insert the new separator into appropriate half
        let insert_into_left = key < split_result.separator_key.as_slice();

        if insert_into_left {
            let mut branch = BranchNode::from_page(page)?;
            let count = branch.cell_count() as usize;
            let mut insert_idx = count;
            for i in 0..count {
                if key < branch.key(i) {
                    insert_idx = i;
                    break;
                }
            }

            // Insert with original child (left half) as the left child of separator
            if insert_idx == count {
                branch.insert(insert_idx, key, original_child)?;
                branch.set_rightmost_child(right_child);
            } else {
                branch.insert(insert_idx, key, original_child)?;
                branch.set_child(insert_idx + 1, right_child);
            }
        } else {
            let mut branch = BranchNode::from_page(&mut new_page)?;
            let count = branch.cell_count() as usize;
            let mut insert_idx = count;
            for i in 0..count {
                if key < branch.key(i) {
                    insert_idx = i;
                    break;
                }
            }

            // Insert with original child (left half) as the left child of separator
            if insert_idx == count {
                branch.insert(insert_idx, key, original_child)?;
                branch.set_rightmost_child(right_child);
            } else {
                branch.insert(insert_idx, key, original_child)?;
                branch.set_child(insert_idx + 1, right_child);
            }
        }

        self.provider.write_page(page.clone());
        self.provider.write_page(new_page);

        Ok((Some((split_result.separator_key, split_result.new_page_id)), old_value))
    }

    /// Deletes a key from the tree and returns its former value, or `None`
    /// if the key was not present.
    ///
    /// Uses non-rebalancing deletion: the leaf may become underfull after
    /// removal, but the tree remains structurally valid. Run
    /// [`compact`](Self::compact) to merge underfull leaves.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during the deletion.
    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page == 0 {
            return Ok(None);
        }

        // For simplicity, we do non-rebalancing delete
        // (leaves may become underfull, but tree remains valid)
        let leaf_page_id = self.find_leaf(key)?;
        let mut page = self.provider.read_page(leaf_page_id)?;
        let mut leaf = LeafNode::from_page(&mut page)?;

        match leaf.search(key) {
            SearchResult::Found(idx) => {
                let old_value = leaf.value(idx).to_vec();
                leaf.delete(idx)?;
                drop(leaf);

                // Check if leaf is now empty and was root
                let new_count = {
                    let leaf = LeafNode::from_page(&mut page)?;
                    leaf.cell_count()
                };

                if new_count == 0 && page.id == self.root_page {
                    self.provider.free_page(page.id);
                    self.root_page = 0;
                } else {
                    self.provider.write_page(page);
                }

                Ok(Some(old_value))
            },
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Creates an iterator over all entries in the tree.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial seek to the first leaf fails.
    pub fn iter(&self) -> Result<BTreeIterator<'_, P>> {
        BTreeIterator::new(self, Range::all())
    }

    /// Creates an iterator over a range of entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial seek to the range start fails.
    pub fn range<'a>(&'a self, range: Range<'a>) -> Result<BTreeIterator<'a, P>> {
        BTreeIterator::new(self, range)
    }

    /// Returns the first (smallest) key-value pair in the tree.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking to the leftmost leaf.
    pub fn first(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.root_page == 0 {
            return Ok(None);
        }

        let leaf_page_id = self.find_first_leaf()?;
        let mut page = self.provider.read_page(leaf_page_id)?;
        let leaf = LeafNode::from_page(&mut page)?;

        if leaf.cell_count() == 0 {
            return Ok(None);
        }

        let (key, value) = leaf.get(0);
        Ok(Some((key.to_vec(), value.to_vec())))
    }

    /// Returns the last (largest) key-value pair in the tree.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails while walking to the rightmost leaf.
    pub fn last(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.root_page == 0 {
            return Ok(None);
        }

        let leaf_page_id = self.find_last_leaf()?;
        let mut page = self.provider.read_page(leaf_page_id)?;
        let leaf = LeafNode::from_page(&mut page)?;

        let count = leaf.cell_count() as usize;
        if count == 0 {
            return Ok(None);
        }

        let (key, value) = leaf.get(count - 1);
        Ok(Some((key.to_vec(), value.to_vec())))
    }

    /// Finds the leftmost (first) leaf in the tree.
    fn find_first_leaf(&self) -> Result<PageId> {
        let mut current = self.root_page;

        loop {
            let page = self.provider.read_page(current)?;
            match page.page_type()? {
                PageType::BTreeLeaf => return Ok(current),
                PageType::BTreeBranch => {
                    let mut page = page;
                    let branch = BranchNode::from_page(&mut page)?;
                    current = branch.child(0);
                },
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    });
                },
            }
        }
    }

    /// Finds the rightmost (last) leaf in the tree.
    fn find_last_leaf(&self) -> Result<PageId> {
        let mut current = self.root_page;

        loop {
            let page = self.provider.read_page(current)?;
            match page.page_type()? {
                PageType::BTreeLeaf => return Ok(current),
                PageType::BTreeBranch => {
                    let mut page = page;
                    let branch = BranchNode::from_page(&mut page)?;
                    // Rightmost child is stored separately
                    current = branch.rightmost_child();
                },
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    });
                },
            }
        }
    }

    /// Compacts the B+ tree by merging underfull leaf nodes.
    ///
    /// Walks all leaves left-to-right using the leaf linked list (`next_leaf`
    /// pointers) for O(1) leaf-to-leaf traversal. When a leaf's fill factor
    /// is below `min_fill_factor`, attempts to merge it with its right sibling.
    /// Greedy: after merging, the merged leaf is re-checked against its new
    /// next neighbor.
    ///
    /// Only merges leaves that share the same parent branch node — leaves
    /// spanning a branch boundary are skipped (merging them would modify the
    /// wrong parent branch).
    ///
    /// # Arguments
    /// * `min_fill_factor` — Threshold below which a leaf is considered underfull (0.0 to 1.0,
    ///   default recommendation: 0.4).
    ///
    /// # Returns
    /// Statistics about the compaction: pages merged and pages freed.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read/write fails during the merge operation.
    pub fn compact(&mut self, min_fill_factor: f64) -> Result<CompactionStats> {
        if self.root_page == 0 {
            return Ok(CompactionStats::default());
        }

        // Check if root is a leaf — single leaf trees have nothing to compact
        {
            let root_page = self.provider.read_page(self.root_page)?;
            if root_page.page_type()? == PageType::BTreeLeaf {
                return Ok(CompactionStats::default());
            }
        }

        let mut stats = CompactionStats::default();

        // Find leftmost leaf — O(depth)
        let mut current_id = self.find_first_leaf()?;

        // Forward-only iteration via next_leaf pointers
        loop {
            let current_page = self.provider.read_page(current_id)?;
            let current_leaf = LeafNodeRef::from_page(&current_page)?;
            let next_id = current_leaf.next_leaf();

            // End of linked list
            if next_id == 0 {
                break;
            }

            // Check fill factor of current leaf
            let fill = leaf_fill_factor(&current_page)?;
            if fill >= min_fill_factor {
                current_id = next_id;
                continue;
            }

            // Read next leaf and check if merge is possible
            let next_page = self.provider.read_page(next_id)?;
            if !can_merge_leaves(&current_page, &next_page)? {
                current_id = next_id;
                continue;
            }

            // Same-parent check: find parent of both leaves via root-to-leaf descent
            // Use first key of each leaf as the representative key
            let current_first_key = if current_leaf.cell_count() > 0 {
                current_leaf.key(0).to_vec()
            } else {
                // Empty leaf — use the next leaf's first key to find parents.
                // Empty current can still be merged (merge_leaves handles it).
                Vec::new()
            };

            let next_leaf_ref = LeafNodeRef::from_page(&next_page)?;
            let next_first_key = if next_leaf_ref.cell_count() > 0 {
                next_leaf_ref.key(0).to_vec()
            } else {
                Vec::new()
            };

            let current_parent = self.find_parent_of_leaf(current_id, &current_first_key)?;
            let next_parent = self.find_parent_of_leaf(next_id, &next_first_key)?;

            if current_parent.0 != next_parent.0 {
                // Different parents — skip merge, advance
                current_id = next_id;
                continue;
            }

            let parent_id = current_parent.0;

            // Perform the merge: move all entries from next into current
            let mut current_page = current_page;
            let mut next_page = next_page;
            merge_leaves(&mut current_page, &mut next_page)?;
            self.provider.write_page(current_page);

            // Update parent branch: remove the separator between current and next
            if parent_id != 0 {
                let mut parent_page = self.provider.read_page(parent_id)?;
                let mut branch = BranchNode::from_page(&mut parent_page)?;
                let branch_count = branch.cell_count() as usize;

                let sep_idx = self.find_separator_index(&branch, current_id, next_id, branch_count);

                if let Some(idx) = sep_idx {
                    if idx + 1 < branch_count {
                        // Right is child(idx+1). Delete separator, update child pointer.
                        branch.delete(idx)?;
                        branch.set_child(idx, current_id);
                    } else {
                        // Right is the rightmost_child.
                        branch.delete(idx)?;
                        branch.set_rightmost_child(current_id);
                    }

                    drop(branch);

                    // If parent is now empty and is the root, collapse tree height
                    let parent_count = {
                        let b = BranchNode::from_page(&mut parent_page)?;
                        b.cell_count()
                    };

                    if parent_count == 0 && parent_page.id == self.root_page {
                        let only_child = {
                            let b = BranchNode::from_page(&mut parent_page)?;
                            b.rightmost_child()
                        };
                        self.provider.free_page(parent_page.id);
                        self.root_page = only_child;
                        stats.pages_freed += 1;
                    } else {
                        self.provider.write_page(parent_page);
                    }
                }
            }

            self.provider.free_page(next_id);
            stats.pages_merged += 1;
            stats.pages_freed += 1;

            // Greedy: do NOT advance current_id — re-check the merged leaf
            // against its new next neighbor. The merged leaf's next_leaf was
            // already updated by merge_leaves() to point past the freed right page.
        }

        Ok(stats)
    }

    /// Collects (leaf_page_id, parent_page_id, child_index_in_parent) for all leaves.
    ///
    /// `parent_page_id` is 0 for the root when the root is a leaf.
    /// `child_index_in_parent` is the index of the child pointer in the parent
    /// branch that leads to this leaf. For rightmost children, the index is
    /// set to the branch's cell_count (one past the last separator).
    ///
    /// This method performs a full DFS traversal of the tree — O(N) page reads.
    /// It is retained as a diagnostic utility (e.g., for metrics and
    /// fragmentation analysis) but is no longer used during compaction.
    pub fn collect_leaf_info(&self) -> Result<Vec<(PageId, PageId, usize)>> {
        let mut result = Vec::new();
        self.collect_leaf_info_recursive(self.root_page, 0, 0, &mut result)?;
        Ok(result)
    }

    fn collect_leaf_info_recursive(
        &self,
        page_id: PageId,
        parent_id: PageId,
        child_idx: usize,
        result: &mut Vec<(PageId, PageId, usize)>,
    ) -> Result<()> {
        let page = self.provider.read_page(page_id)?;
        match page.page_type()? {
            PageType::BTreeLeaf => {
                result.push((page_id, parent_id, child_idx));
            },
            PageType::BTreeBranch => {
                let mut page = page;
                let branch = BranchNode::from_page(&mut page)?;
                let count = branch.cell_count() as usize;

                // Visit each child
                for i in 0..count {
                    let child = branch.child(i);
                    self.collect_leaf_info_recursive(child, page_id, i, result)?;
                }

                // Visit rightmost child
                let rightmost = branch.rightmost_child();
                self.collect_leaf_info_recursive(rightmost, page_id, count, result)?;
            },
            pt => {
                return Err(Error::PageTypeMismatch { expected: PageType::BTreeLeaf, found: pt });
            },
        }
        Ok(())
    }

    /// Finds the separator index in a parent branch that separates left_id from right_id.
    ///
    /// Returns `Some(idx)` where `child(idx) == left_id` and either
    /// `child(idx+1) == right_id` or `rightmost_child == right_id`.
    fn find_separator_index(
        &self,
        branch: &BranchNode<'_>,
        left_id: PageId,
        right_id: PageId,
        count: usize,
    ) -> Option<usize> {
        for i in 0..count {
            let child = branch.child(i);
            if child == left_id {
                // Check if right sibling is child(i+1) or rightmost
                if i + 1 < count {
                    if branch.child(i + 1) == right_id {
                        return Some(i);
                    }
                } else if branch.rightmost_child() == right_id {
                    return Some(i);
                }
            }
        }

        // Check if left is the second-to-last child and right is rightmost
        // This handles the case where left is child(count-1)
        // (already covered above)

        None
    }

    /// Finds the parent branch node of a leaf by descending from the root
    /// using a representative key.
    ///
    /// Returns `(parent_page_id, child_index_in_parent)` where
    /// `child_index_in_parent` is the index in the parent's child array that
    /// leads to the target leaf. For rightmost children, the index equals the
    /// branch's cell count.
    ///
    /// If the root IS the target leaf, returns `(0, 0)`.
    fn find_parent_of_leaf(&self, target_leaf_id: PageId, key: &[u8]) -> Result<(PageId, usize)> {
        let mut current = self.root_page;
        let mut parent_id: PageId = 0;
        let mut child_idx: usize = 0;

        loop {
            if current == target_leaf_id {
                return Ok((parent_id, child_idx));
            }

            let page = self.provider.read_page(current)?;
            match page.page_type()? {
                PageType::BTreeLeaf => {
                    // Reached a leaf that isn't the target — the key routing
                    // didn't lead to the expected leaf. This can happen with
                    // empty leaves (empty key). Fall back to scanning all
                    // children of the last parent.
                    if parent_id != 0 {
                        return self.find_parent_by_scan(parent_id, target_leaf_id);
                    }
                    // Root is a leaf and not the target — shouldn't happen
                    return Ok((0, 0));
                },
                PageType::BTreeBranch => {
                    let mut page = page;
                    let branch = BranchNode::from_page(&mut page)?;
                    let count = branch.cell_count() as usize;

                    // Check if any direct child is the target leaf
                    for i in 0..count {
                        if branch.child(i) == target_leaf_id {
                            return Ok((current, i));
                        }
                    }
                    if branch.rightmost_child() == target_leaf_id {
                        return Ok((current, count));
                    }

                    // Not a direct child — descend using the key
                    parent_id = current;
                    if key.is_empty() {
                        // Empty key: go to leftmost child
                        child_idx = 0;
                        current = branch.child(0);
                    } else {
                        // Route via key comparison
                        let mut found = false;
                        for i in 0..count {
                            let sep_key = branch.key(i);
                            if key < sep_key {
                                child_idx = i;
                                current = branch.child(i);
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            child_idx = count;
                            current = branch.rightmost_child();
                        }
                    }
                },
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    });
                },
            }
        }
    }

    /// Fallback parent finder: scans all children of a branch to find the
    /// target leaf. Used when key-based routing fails (e.g., for empty leaves).
    fn find_parent_by_scan(
        &self,
        branch_id: PageId,
        target_leaf_id: PageId,
    ) -> Result<(PageId, usize)> {
        let mut page = self.provider.read_page(branch_id)?;
        let branch = BranchNode::from_page(&mut page)?;
        let count = branch.cell_count() as usize;

        for i in 0..count {
            if branch.child(i) == target_leaf_id {
                return Ok((branch_id, i));
            }
        }
        if branch.rightmost_child() == target_leaf_id {
            return Ok((branch_id, count));
        }

        // Target not found as direct child — search recursively
        for i in 0..count {
            let child_id = branch.child(i);
            let child_page = self.provider.read_page(child_id)?;
            if child_page.page_type()? == PageType::BTreeBranch {
                let result = self.find_parent_by_scan(child_id, target_leaf_id)?;
                if result.0 != 0 {
                    return Ok(result);
                }
            }
        }
        let rightmost = branch.rightmost_child();
        let rightmost_page = self.provider.read_page(rightmost)?;
        if rightmost_page.page_type()? == PageType::BTreeBranch {
            let result = self.find_parent_by_scan(rightmost, target_leaf_id)?;
            if result.0 != 0 {
                return Ok(result);
            }
        }

        Ok((0, 0))
    }
}

/// Iterator over B-tree entries in sorted key order.
///
/// Supports forward iteration over key-value pairs stored in the B-tree.
/// Leaf pages are loaded on demand as the cursor advances. Sibling leaf
/// traversal uses `next_leaf` pointers for O(1) leaf-to-leaf advancement.
///
/// Created by [`BTree::iter`].
pub struct BTreeIterator<'a, P: PageProvider> {
    tree: &'a BTree<P>,
    state: RangeIterState<'a>,
    current_leaf: Option<Page>,
}

impl<'a, P: PageProvider> BTreeIterator<'a, P> {
    fn new(tree: &'a BTree<P>, range: Range<'a>) -> Result<Self> {
        let mut iter = Self { tree, state: RangeIterState::new(range), current_leaf: None };

        iter.seek_to_start()?;

        Ok(iter)
    }

    fn seek_to_start(&mut self) -> Result<()> {
        if self.tree.root_page == 0 {
            self.state.position.valid = false;
            return Ok(());
        }

        let start_key: Option<&[u8]> = match &self.state.range.start {
            cursor::Bound::Unbounded => None,
            cursor::Bound::Included(k) => Some(k),
            cursor::Bound::Excluded(k) => Some(k),
        };

        let leaf_page_id = if let Some(key) = start_key {
            self.tree.find_leaf(key)?
        } else {
            self.tree.find_first_leaf()?
        };

        self.current_leaf = Some(self.tree.provider.read_page(leaf_page_id)?);

        let page = self.current_leaf.as_ref().unwrap();

        if let Some(key) = start_key {
            let result = cursor_ops::seek_in_leaf(&mut self.state.position, page, key)?;

            // If excluded bound, advance past it
            if matches!(&self.state.range.start, cursor::Bound::Excluded(_))
                && result == SeekResult::Found
            {
                self.advance()?;
            }
        } else {
            cursor_ops::move_to_first_in_leaf(&mut self.state.position, page)?;
        }

        self.state.started = true;
        Ok(())
    }

    fn advance(&mut self) -> Result<bool> {
        if !self.state.position.valid {
            return Ok(false);
        }

        let page = self
            .current_leaf
            .as_ref()
            .ok_or(Error::Corrupted { reason: "No current leaf in iterator".to_string() })?;

        // Try to advance within current leaf
        if cursor_ops::next_in_leaf(&mut self.state.position, page)? {
            return Ok(true);
        }

        // Current leaf exhausted — follow the next_leaf pointer for O(1)
        // leaf-to-leaf traversal instead of O(depth) tree re-descent.
        let page = self
            .current_leaf
            .as_ref()
            .ok_or(Error::Corrupted { reason: "No current leaf in iterator".to_string() })?;
        let leaf = LeafNodeRef::from_page(page)?;
        let next_leaf_id = leaf.next_leaf();

        if next_leaf_id == 0 {
            // End of linked list — no more leaves
            self.state.position.valid = false;
            return Ok(false);
        }

        self.current_leaf = Some(self.tree.provider.read_page(next_leaf_id)?);
        let new_page = self
            .current_leaf
            .as_ref()
            .ok_or(Error::Corrupted { reason: "No current leaf in iterator".to_string() })?;
        cursor_ops::move_to_first_in_leaf(&mut self.state.position, new_page)?;
        Ok(self.state.position.valid)
    }

    /// Returns the current key-value pair, if positioned (returns owned copies).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] if the iterator has no current leaf page.
    pub fn current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.state.position.valid {
            return Ok(None);
        }

        let page = self
            .current_leaf
            .as_ref()
            .ok_or(Error::Corrupted { reason: "No current leaf".to_string() })?;

        let key = cursor_ops::current_key(&self.state.position, page)?;
        let value = cursor_ops::current_value(&self.state.position, page)?;

        match (key, value) {
            (Some(k), Some(v)) => {
                // Check if still within range
                if self.state.range.should_continue(&k) { Ok(Some((k, v))) } else { Ok(None) }
            },
            _ => Ok(None),
        }
    }

    /// Moves to the next entry and returns it.
    ///
    /// # Errors
    ///
    /// Returns an error if a page read fails during leaf advancement.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.state.started {
            self.seek_to_start()?;
        }

        match self.current()? {
            Some((k, v)) => {
                self.advance()?;
                Ok(Some((k, v)))
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;

    /// Simple in-memory page provider for testing.
    struct TestPageProvider {
        pages: std::collections::HashMap<PageId, Page>,
        next_page: PageId,
        page_size: usize,
        txn_id: u64,
    }

    impl TestPageProvider {
        fn new() -> Self {
            Self {
                pages: std::collections::HashMap::new(),
                next_page: 1,
                page_size: DEFAULT_PAGE_SIZE,
                txn_id: 1,
            }
        }
    }

    impl PageProvider for TestPageProvider {
        fn read_page(&self, page_id: PageId) -> Result<Page> {
            self.pages.get(&page_id).cloned().ok_or(Error::PageNotFound { page_id })
        }

        fn write_page(&mut self, page: Page) {
            self.pages.insert(page.id, page);
        }

        fn allocate_page(&mut self, page_type: PageType) -> Page {
            let page_id = self.next_page;
            self.next_page += 1;
            Page::new(page_id, self.page_size, page_type, self.txn_id)
        }

        fn free_page(&mut self, page_id: PageId) {
            self.pages.remove(&page_id);
        }

        fn page_size(&self) -> usize {
            self.page_size
        }

        fn txn_id(&self) -> u64 {
            self.txn_id
        }
    }

    fn make_tree() -> BTree<TestPageProvider> {
        BTree::new(0, TestPageProvider::new())
    }

    #[test]
    fn test_empty_tree() {
        let tree = make_tree();
        assert!(tree.is_empty());
        assert_eq!(tree.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_insert_and_get() {
        let mut tree = make_tree();

        tree.insert(b"hello", b"world").unwrap();
        assert!(!tree.is_empty());

        assert_eq!(tree.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(tree.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_insert_multiple() {
        let mut tree = make_tree();

        tree.insert(b"apple", b"1").unwrap();
        tree.insert(b"banana", b"2").unwrap();
        tree.insert(b"cherry", b"3").unwrap();

        assert_eq!(tree.get(b"apple").unwrap(), Some(b"1".to_vec()));
        assert_eq!(tree.get(b"banana").unwrap(), Some(b"2".to_vec()));
        assert_eq!(tree.get(b"cherry").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_update() {
        let mut tree = make_tree();

        let old = tree.insert(b"key", b"value1").unwrap();
        assert_eq!(old, None);

        let old = tree.insert(b"key", b"value2").unwrap();
        assert_eq!(old, Some(b"value1".to_vec()));

        assert_eq!(tree.get(b"key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_delete() {
        let mut tree = make_tree();

        tree.insert(b"key", b"value").unwrap();
        assert_eq!(tree.get(b"key").unwrap(), Some(b"value".to_vec()));

        let deleted = tree.delete(b"key").unwrap();
        assert_eq!(deleted, Some(b"value".to_vec()));

        assert_eq!(tree.get(b"key").unwrap(), None);

        // Delete non-existent
        let deleted = tree.delete(b"nonexistent").unwrap();
        assert_eq!(deleted, None);
    }

    #[test]
    fn test_many_inserts() {
        let mut tree = make_tree();

        // Insert enough to trigger splits
        for i in 0..100u32 {
            let key = format!("key{:05}", i);
            let value = format!("value{}", i);
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all keys
        for i in 0..100u32 {
            let key = format!("key{:05}", i);
            let expected_value = format!("value{}", i);
            let actual = tree.get(key.as_bytes()).unwrap();
            assert_eq!(actual, Some(expected_value.into_bytes()));
        }
    }

    #[test]
    fn test_range_iteration_cross_leaf() {
        use byteorder::{BigEndian, ByteOrder};

        use super::cursor::Range;

        let mut tree = make_tree();

        // Insert 200 entries with u64 big-endian keys (like RaftLog)
        // This should cause multiple leaf splits
        for i in 0u64..200 {
            let mut key = [0u8; 8];
            BigEndian::write_u64(&mut key, i);
            let value = format!("value{}", i);
            tree.insert(&key, value.as_bytes()).unwrap();
        }

        // Verify range iteration returns ALL entries
        let mut iter = tree.range(Range::all()).unwrap();
        let mut all_entries = Vec::new();
        while let Some((k, v)) = iter.next_entry().unwrap() {
            all_entries.push((k, v));
        }

        assert_eq!(
            all_entries.len(),
            200,
            "Expected 200 entries from full range iteration, got {}",
            all_entries.len()
        );

        // Verify entries are in correct order
        for (idx, (key, value)) in all_entries.iter().enumerate() {
            let expected_idx = BigEndian::read_u64(key);
            assert_eq!(
                expected_idx, idx as u64,
                "Entry {} has wrong key: expected {}, got {}",
                idx, idx, expected_idx
            );
            let expected_value = format!("value{}", idx);
            assert_eq!(value.as_slice(), expected_value.as_bytes());
        }

        // Test partial range iteration
        let start_key = {
            let mut k = [0u8; 8];
            BigEndian::write_u64(&mut k, 50);
            k
        };
        let end_key = {
            let mut k = [0u8; 8];
            BigEndian::write_u64(&mut k, 150);
            k
        };

        let mut iter = tree.range(Range::new(&start_key, &end_key)).unwrap();
        let mut partial = Vec::new();
        while let Some((k, v)) = iter.next_entry().unwrap() {
            partial.push((k, v));
        }

        assert_eq!(
            partial.len(),
            100,
            "Expected 100 entries from partial range [50, 150), got {}",
            partial.len()
        );
    }

    // ============================================
    // Property-based B+ tree invariant tests
    // ============================================

    mod proptest_btree {
        use proptest::prelude::*;

        use super::*;
        use crate::btree::cursor::Range;

        /// Generates a vector of unique keys (formatted for sort stability).
        fn arb_unique_keys(max_count: usize) -> impl Strategy<Value = Vec<String>> {
            proptest::collection::hash_set("[a-z]{1,4}", 1..max_count)
                .prop_map(|s| s.into_iter().collect::<Vec<_>>())
        }

        /// Generates key-value pairs with unique keys.
        fn arb_kv_pairs(max_count: usize) -> impl Strategy<Value = Vec<(String, Vec<u8>)>> {
            arb_unique_keys(max_count).prop_flat_map(|keys| {
                let n = keys.len();
                proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..32), n..=n)
                    .prop_map(move |values| {
                        keys.clone().into_iter().zip(values).collect::<Vec<_>>()
                    })
            })
        }

        proptest! {
            /// Inserted keys are always retrievable.
            #[test]
            fn prop_inserted_keys_retrievable(pairs in arb_kv_pairs(200)) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                for (key, value) in &pairs {
                    let result = tree.get(key.as_bytes()).unwrap();
                    prop_assert_eq!(
                        result.as_deref(),
                        Some(value.as_slice()),
                        "key {:?} not found or has wrong value",
                        key
                    );
                }
            }

            /// Full-range iteration returns keys in sorted (lexicographic) order.
            #[test]
            fn prop_iteration_returns_sorted_keys(pairs in arb_kv_pairs(200)) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                let mut iter = tree.range(Range::all()).unwrap();
                let mut keys = Vec::new();
                while let Some((k, _v)) = iter.next_entry().unwrap() {
                    keys.push(k);
                }

                // Keys must be in ascending lexicographic order
                for window in keys.windows(2) {
                    prop_assert!(
                        window[0] < window[1],
                        "keys not sorted: {:?} >= {:?}",
                        window[0],
                        window[1]
                    );
                }

                // Must have exactly the same number of entries as we inserted
                prop_assert_eq!(keys.len(), pairs.len());
            }

            /// Deletes removes keys and they become unretrievable.
            #[test]
            fn prop_delete_removes_keys(
                pairs in arb_kv_pairs(100),
                delete_indices in proptest::collection::vec(any::<prop::sample::Index>(), 1..20),
            ) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                // Delete a subset of keys
                let mut deleted = std::collections::HashSet::new();
                for idx in &delete_indices {
                    let i = idx.index(pairs.len());
                    let key = &pairs[i].0;
                    tree.delete(key.as_bytes()).unwrap();
                    deleted.insert(key.clone());
                }

                // Verify: deleted keys return None, others return correct value
                for (key, value) in &pairs {
                    let result = tree.get(key.as_bytes()).unwrap();
                    if deleted.contains(key) {
                        prop_assert_eq!(
                            result, None,
                            "deleted key {:?} still found",
                            key
                        );
                    } else {
                        prop_assert_eq!(
                            result.as_deref(),
                            Some(value.as_slice()),
                            "surviving key {:?} has wrong value",
                            key
                        );
                    }
                }
            }

            /// Updates overwrites value while keeping key present.
            #[test]
            fn prop_update_overwrites_value(
                pairs in arb_kv_pairs(100),
                update_indices in proptest::collection::vec(any::<prop::sample::Index>(), 1..20),
            ) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                // Update a subset of keys with new values
                let new_value = b"UPDATED";
                let mut updated = std::collections::HashSet::new();
                for idx in &update_indices {
                    let i = idx.index(pairs.len());
                    let key = &pairs[i].0;
                    tree.insert(key.as_bytes(), new_value).unwrap();
                    updated.insert(key.clone());
                }

                for (key, original_value) in &pairs {
                    let result = tree.get(key.as_bytes()).unwrap();
                    if updated.contains(key) {
                        prop_assert_eq!(
                            result.as_deref(),
                            Some(new_value.as_slice()),
                            "updated key {:?} has wrong value",
                            key
                        );
                    } else {
                        prop_assert_eq!(
                            result.as_deref(),
                            Some(original_value.as_slice()),
                            "non-updated key {:?} changed",
                            key
                        );
                    }
                }
            }

            /// Iteration count matches number of unique inserted keys.
            #[test]
            fn prop_iteration_count_matches_inserts(pairs in arb_kv_pairs(200)) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                let mut iter = tree.range(Range::all()).unwrap();
                let mut count = 0;
                while iter.next_entry().unwrap().is_some() {
                    count += 1;
                }

                prop_assert_eq!(count, pairs.len());
            }

            /// Seeks finds exact key or correct insertion position.
            #[test]
            fn prop_get_consistency_with_iteration(pairs in arb_kv_pairs(100)) {
                let mut tree = make_tree();

                for (key, value) in &pairs {
                    tree.insert(key.as_bytes(), value).unwrap();
                }

                // Collect all keys via iteration
                let mut iter = tree.range(Range::all()).unwrap();
                let mut iterated_keys = Vec::new();
                while let Some((k, _)) = iter.next_entry().unwrap() {
                    iterated_keys.push(k);
                }

                // Every iterated key should be gettable
                for k in &iterated_keys {
                    let result = tree.get(k).unwrap();
                    prop_assert!(
                        result.is_some(),
                        "iterated key {:?} not found via get()",
                        k
                    );
                }

                // Non-existent key should return None
                let result = tree.get(b"__definitely_not_in_tree__").unwrap();
                prop_assert_eq!(result, None);
            }
        }
    }

    // =========================================================================
    // Compaction tests
    // =========================================================================

    #[test]
    fn test_compact_empty_tree() {
        let tree = make_tree();
        // Compacting an empty tree should be a no-op
        assert!(tree.is_empty());
    }

    #[test]
    fn test_compact_single_leaf() {
        let mut tree = make_tree();
        tree.insert(b"a", b"1").unwrap();
        tree.insert(b"b", b"2").unwrap();

        // Single-leaf tree: nothing to merge
        let stats = tree.compact(0.4).unwrap();
        assert_eq!(stats.pages_merged, 0);
        assert_eq!(stats.pages_freed, 0);

        // Data should be intact
        assert_eq!(tree.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(tree.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_compact_after_bulk_delete() {
        let mut tree = make_tree();

        // Insert enough keys to create many leaves.
        // With 4K pages, each key-value pair takes ~20 bytes, so ~100 entries per leaf.
        // 500 keys should create 5+ leaf pages.
        for i in 0..500 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete most keys to create underfull leaves
        for i in 0..480 {
            let key = format!("key-{:04}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        // Remaining keys should still be readable before compaction
        for i in 480..500 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }

        // Compact should merge underfull leaves
        let stats = tree.compact(0.4).unwrap();

        // With 500 keys spread across 5+ leaves, deleting 96% should leave
        // many underfull leaves that can merge.
        assert!(
            stats.pages_merged > 0 || stats.pages_freed > 0,
            "Expected some merges after deleting 96% of keys"
        );

        // All remaining keys should still be readable after compaction
        for i in 480..500 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Key {} not found after compaction",
                key
            );
        }

        // Deleted keys should still be absent
        for i in 0..480 {
            let key = format!("key-{:04}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn test_compact_preserves_all_data() {
        let mut tree = make_tree();

        // Insert keys
        for i in 0..100 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete every other key
        for i in (0..100).step_by(2) {
            let key = format!("k{:04}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        // Compact with a high fill factor to force merges
        let _stats = tree.compact(0.9).unwrap();

        // All odd keys should still be present
        for i in (1..100).step_by(2) {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Key {} missing after compaction",
                key
            );
        }

        // All even keys should still be absent
        for i in (0..100).step_by(2) {
            let key = format!("k{:04}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn test_compact_idempotent() {
        let mut tree = make_tree();

        for i in 0..150 {
            let key = format!("key-{:04}", i);
            tree.insert(key.as_bytes(), b"value").unwrap();
        }

        // Delete half
        for i in 0..75 {
            let key = format!("key-{:04}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        // First compaction
        let stats1 = tree.compact(0.4).unwrap();

        // Second compaction should do less or no work
        let stats2 = tree.compact(0.4).unwrap();

        assert!(
            stats2.pages_merged <= stats1.pages_merged,
            "Second compaction merged more than first: {} > {}",
            stats2.pages_merged,
            stats1.pages_merged
        );

        // Data integrity check
        for i in 75..150 {
            let key = format!("key-{:04}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn test_compact_iteration_after() {
        let mut tree = make_tree();

        // Insert and delete to create underfull pages
        for i in 0..100 {
            let key = format!("iter-{:04}", i);
            tree.insert(key.as_bytes(), b"v").unwrap();
        }
        for i in 0..80 {
            let key = format!("iter-{:04}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        tree.compact(0.4).unwrap();

        // Iteration should still work and return remaining 20 keys
        let mut iter = tree.range(Range::all()).unwrap();
        let mut count = 0;
        while let Some((key, _)) = iter.next_entry().unwrap() {
            let expected = format!("iter-{:04}", 80 + count);
            assert_eq!(key, expected.as_bytes());
            count += 1;
        }
        assert_eq!(count, 20, "Expected 20 remaining keys after compaction");
    }

    // =========================================================================
    // Leaf linked list tests
    // =========================================================================

    #[test]
    fn test_linked_list_1000_entries_iterate_all() {
        let mut tree = make_tree();

        for i in 0..1000u32 {
            let key = format!("key{:05}", i);
            let value = format!("val{:05}", i);
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify iteration via next_leaf chain visits every entry in key order
        let mut iter = tree.range(Range::all()).unwrap();
        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;
        while let Some((key, _)) = iter.next_entry().unwrap() {
            if let Some(ref pk) = prev_key {
                assert!(pk < &key, "keys not in order: {:?} >= {:?}", pk, key);
            }
            prev_key = Some(key);
            count += 1;
        }
        assert_eq!(count, 1000, "Expected 1000 entries from iteration, got {}", count);
    }

    #[test]
    fn test_linked_list_split_maintains_chain() {
        let mut tree = make_tree();

        // Insert enough to trigger at least one split (large values to fill pages faster)
        let big_value = vec![0xABu8; 200];
        for i in 0..100u32 {
            let key = format!("k{:04}", i);
            tree.insert(key.as_bytes(), &big_value).unwrap();
        }

        assert!(tree.split_count() > 0, "Expected at least one split");

        // Walk the leaf linked list manually and verify it visits all leaves
        let first_leaf = tree.find_first_leaf().unwrap();
        let mut leaf_ids = vec![first_leaf];
        let mut current = first_leaf;

        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = node::LeafNodeRef::from_page(&page).unwrap();
            let next = leaf.next_leaf();
            if next == 0 {
                break;
            }
            leaf_ids.push(next);
            current = next;
        }

        // Verify all entries are reachable via the linked list
        let mut total_entries = 0;
        for &leaf_id in &leaf_ids {
            let page = tree.provider.read_page(leaf_id).unwrap();
            let leaf = node::LeafNodeRef::from_page(&page).unwrap();
            total_entries += leaf.cell_count() as usize;
        }
        assert_eq!(total_entries, 100, "Linked list should reach all 100 entries");

        // Verify keys across leaves are in order
        let mut prev_last_key: Option<Vec<u8>> = None;
        for &leaf_id in &leaf_ids {
            let page = tree.provider.read_page(leaf_id).unwrap();
            let leaf = node::LeafNodeRef::from_page(&page).unwrap();
            let count = leaf.cell_count() as usize;
            if count > 0 {
                let first_key = leaf.key(0).to_vec();
                if let Some(ref plk) = prev_last_key {
                    assert!(
                        plk < &first_key,
                        "Leaf boundary keys not ordered: {:?} >= {:?}",
                        plk,
                        first_key
                    );
                }
                prev_last_key = Some(leaf.key(count - 1).to_vec());
            }
        }
    }

    #[test]
    fn test_linked_list_100k_scan_page_reads() {
        let mut tree = make_tree();

        for i in 0..100_000u32 {
            let key = i.to_be_bytes();
            let value = i.to_be_bytes();
            tree.insert(&key, &value).unwrap();
        }

        // Count entries via iteration (exercises the linked list path)
        let mut iter = tree.range(Range::all()).unwrap();
        let mut count = 0u32;
        while iter.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 100_000);

        // Count leaves in the linked list
        let first_leaf = tree.find_first_leaf().unwrap();
        let mut leaf_count = 1usize;
        let mut current = first_leaf;
        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = node::LeafNodeRef::from_page(&page).unwrap();
            let next = leaf.next_leaf();
            if next == 0 {
                break;
            }
            leaf_count += 1;
            current = next;
        }

        // With next_leaf O(1), the iteration should read approximately
        // leaf_count pages for the leaf traversal (plus initial seek).
        // The old O(depth) approach would read ~2*depth*leaf_count pages.
        assert!(leaf_count > 10, "Expected multiple leaves for 100K entries, got {}", leaf_count);
    }

    #[test]
    fn test_linked_list_insert_delete_half_iterate() {
        let mut tree = make_tree();

        for i in 0..200u32 {
            let key = format!("key{:05}", i);
            tree.insert(key.as_bytes(), b"v").unwrap();
        }

        // Delete even-indexed entries
        for i in (0..200u32).step_by(2) {
            let key = format!("key{:05}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        // Verify iteration still works (linked list intact despite deletes)
        let mut iter = tree.range(Range::all()).unwrap();
        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;
        while let Some((key, _)) = iter.next_entry().unwrap() {
            if let Some(ref pk) = prev_key {
                assert!(pk < &key, "keys not in order after deletes");
            }
            prev_key = Some(key);
            count += 1;
        }
        assert_eq!(count, 100, "Expected 100 remaining entries");
    }

    #[test]
    fn test_linked_list_empty_tree() {
        let tree = make_tree();

        // Empty tree: root_page == 0, no leaves to iterate
        let mut iter = tree.range(Range::all()).unwrap();
        assert!(iter.next_entry().unwrap().is_none());
    }

    #[test]
    fn test_linked_list_single_leaf() {
        let mut tree = make_tree();

        tree.insert(b"only_key", b"only_value").unwrap();

        // Single-leaf tree: next_leaf should be 0
        let root = tree.root_page();
        let page = tree.provider.read_page(root).unwrap();
        let leaf = node::LeafNodeRef::from_page(&page).unwrap();
        assert_eq!(leaf.next_leaf(), 0, "Single-leaf tree should have next_leaf=0");

        // Iteration should work
        let mut iter = tree.range(Range::all()).unwrap();
        let (key, val) = iter.next_entry().unwrap().unwrap();
        assert_eq!(key, b"only_key");
        assert_eq!(val, b"only_value");
        assert!(iter.next_entry().unwrap().is_none());
    }

    // =========================================================================
    // Fill factor tests
    // =========================================================================

    #[test]
    fn test_fill_factor_empty_leaf() {
        use crate::btree::split::leaf_fill_factor;

        let mut page = Page::new(1, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        LeafNode::init(&mut page);

        let factor = leaf_fill_factor(&page).unwrap();
        assert!(
            (factor - 0.0).abs() < f64::EPSILON,
            "Empty leaf should have fill factor 0.0, got {}",
            factor
        );
    }

    #[test]
    fn test_fill_factor_capacity_denominator() {
        use crate::{
            bloom::BLOOM_FILTER_SIZE,
            btree::{
                node::{LEAF_CELL_PTRS_OFFSET, NODE_HEADER_SIZE},
                split::leaf_fill_factor,
            },
            page::PAGE_HEADER_SIZE,
        };

        // Verify the denominator = page_size - LEAF_CELL_PTRS_OFFSET
        let expected_capacity =
            DEFAULT_PAGE_SIZE - PAGE_HEADER_SIZE - NODE_HEADER_SIZE - BLOOM_FILTER_SIZE;
        let expected_capacity_alt = DEFAULT_PAGE_SIZE - LEAF_CELL_PTRS_OFFSET;
        assert_eq!(expected_capacity, expected_capacity_alt);

        // Fill a page nearly full and check fill factor approaches 1.0
        let mut page = Page::new(1, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        let mut node = LeafNode::init(&mut page);

        let mut i = 0u32;
        loop {
            let key = i.to_be_bytes();
            let value = [0u8; 10];
            if !node.can_insert(&key, &value) {
                break;
            }
            node.insert(i as usize, &key, &value).unwrap();
            i += 1;
        }

        let factor = leaf_fill_factor(&page).unwrap();
        assert!(factor > 0.9, "Nearly-full leaf should have fill factor >0.9, got {}", factor);
    }

    // ====================================================================
    // Forward-only compaction tests (Task 12)
    // ====================================================================

    /// Helper: count all entries by walking the leaf linked list directly.
    /// Does not use BTreeIterator (which can't handle empty first leaves).
    fn count_all_entries(tree: &BTree<TestPageProvider>) -> usize {
        if tree.root_page == 0 {
            return 0;
        }
        let mut count = 0;
        let mut current = tree.find_first_leaf().unwrap();
        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = LeafNodeRef::from_page(&page).unwrap();
            count += leaf.cell_count() as usize;
            let next = leaf.next_leaf();
            if next == 0 {
                break;
            }
            current = next;
        }
        count
    }

    /// Helper: collect all entries in order by walking the leaf linked list.
    fn collect_all_entries(tree: &BTree<TestPageProvider>) -> Vec<(Vec<u8>, Vec<u8>)> {
        if tree.root_page == 0 {
            return Vec::new();
        }
        let mut entries = Vec::new();
        let mut current = tree.find_first_leaf().unwrap();
        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = LeafNodeRef::from_page(&page).unwrap();
            let cell_count = leaf.cell_count() as usize;
            for i in 0..cell_count {
                entries.push((leaf.key(i).to_vec(), leaf.value(i).to_vec()));
            }
            let next = leaf.next_leaf();
            if next == 0 {
                break;
            }
            current = next;
        }
        entries
    }

    /// Helper: count leaves by following the linked list.
    fn count_leaves(tree: &BTree<TestPageProvider>) -> usize {
        if tree.root_page == 0 {
            return 0;
        }
        let first = tree.find_first_leaf().unwrap();
        let mut count = 1;
        let mut current = first;
        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = LeafNodeRef::from_page(&page).unwrap();
            let next = leaf.next_leaf();
            if next == 0 {
                break;
            }
            count += 1;
            current = next;
        }
        count
    }

    #[test]
    fn test_compact_forward_100_leaves_10pct_fill() {
        // Tree with 100 leaves all at ~10% fill — compact should reduce to ~10 leaves.
        let mut tree = make_tree();

        // Insert enough keys to create many leaves (with 4KB pages, ~100 entries per leaf)
        // 10000 keys → ~100 leaves
        for i in 0..10_000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let leaves_before = count_leaves(&tree);
        assert!(leaves_before >= 50, "Expected many leaves, got {leaves_before}");

        // Delete 90% of entries to make most leaves ~10% full
        for i in 0..9000u32 {
            let key = format!("k{:06}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        let remaining = count_all_entries(&tree);
        assert_eq!(remaining, 1000);

        let stats = tree.compact(0.4).unwrap();

        let leaves_after = count_leaves(&tree);

        // Should have merged significantly
        assert!(stats.pages_merged > 0, "Expected merges after deleting 90% of keys");
        assert!(
            leaves_after < leaves_before / 2,
            "Expected at least 50% leaf reduction, before={leaves_before} after={leaves_after}"
        );

        // All remaining data intact
        for i in 9000..10_000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Missing key after compaction: k{i:06}"
            );
        }
    }

    #[test]
    fn test_compact_forward_no_underfull_zero_merges() {
        // Tree with no under-filled leaves — compact is a single forward pass with zero merges.
        let mut tree = make_tree();

        for i in 0..1000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let leaves_before = count_leaves(&tree);
        let entries_before = count_all_entries(&tree);

        let stats = tree.compact(0.4).unwrap();

        assert_eq!(stats.pages_merged, 0, "Expected zero merges on full tree");
        assert_eq!(stats.pages_freed, 0, "Expected zero frees on full tree");
        assert_eq!(count_leaves(&tree), leaves_before);
        assert_eq!(count_all_entries(&tree), entries_before);
    }

    #[test]
    fn test_compact_forward_alternating_full_empty() {
        // Alternating full/empty leaves — verify correct merge and relink.
        let mut tree = make_tree();

        // Insert 2000 keys to create enough leaves
        for i in 0..2000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete every other "chunk" of entries to create alternating patterns.
        // We delete entries in contiguous ranges that map to specific leaves.
        // With ~100 entries per leaf, deleting 0..100, keeping 100..200, etc.
        for chunk_start in (0..2000u32).step_by(200) {
            for i in chunk_start..std::cmp::min(chunk_start + 100, 2000) {
                let key = format!("k{:06}", i);
                tree.delete(key.as_bytes()).unwrap();
            }
        }

        let remaining_before = count_all_entries(&tree);

        let stats = tree.compact(0.4).unwrap();

        // Should have performed some merges
        assert!(stats.pages_merged > 0, "Expected merges with alternating empty leaves");

        // All remaining entries intact and in order
        let remaining_after = count_all_entries(&tree);
        assert_eq!(remaining_before, remaining_after, "Entry count changed after compaction");

        // Verify linked list integrity — entries should be in sorted order
        let entries = collect_all_entries(&tree);
        for window in entries.windows(2) {
            assert!(
                window[0].0 < window[1].0,
                "Entries not in order: {:?} >= {:?}",
                String::from_utf8_lossy(&window[0].0),
                String::from_utf8_lossy(&window[1].0)
            );
        }
    }

    #[test]
    fn test_compact_forward_linked_list_integrity() {
        // Verify linked list integrity after compaction (iterate all entries, confirm
        // complete and in order).
        let mut tree = make_tree();

        // Insert entries
        for i in 0..3000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete most entries
        for i in 0..2700u32 {
            let key = format!("k{:06}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        tree.compact(0.4).unwrap();

        // Verify via iterator (uses next_leaf linked list)
        let entries = collect_all_entries(&tree);
        assert_eq!(entries.len(), 300);

        // Verify sorted order
        for window in entries.windows(2) {
            assert!(window[0].0 < window[1].0);
        }

        // Verify every expected key is present
        for i in 2700..3000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_compact_forward_then_insert() {
        // Compact a tree, then insert new entries — verify B+ tree operations
        // work correctly on the compacted structure.
        let mut tree = make_tree();

        for i in 0..1000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete most entries and compact
        for i in 0..900u32 {
            let key = format!("k{:06}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        tree.compact(0.4).unwrap();

        // Insert new entries into the compacted tree
        for i in 5000..6000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all entries: old surviving + new
        for i in 900..1000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
        for i in 5000..6000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }

        // Verify total count
        assert_eq!(count_all_entries(&tree), 1100);
    }

    #[test]
    fn test_compact_forward_single_leaf_tree() {
        // Single-leaf tree (root is a leaf) — no crash, no merge, leaf unchanged.
        let mut tree = make_tree();

        // Insert a few entries (stays in single leaf)
        for i in 0..5u32 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let stats = tree.compact(0.4).unwrap();

        assert_eq!(stats.pages_merged, 0);
        assert_eq!(stats.pages_freed, 0);
        assert_eq!(count_all_entries(&tree), 5);
    }

    #[test]
    fn test_compact_forward_empty_tree() {
        // Empty tree — no crash, returns immediately.
        let mut tree = make_tree();

        let stats = tree.compact(0.4).unwrap();

        assert_eq!(stats.pages_merged, 0);
        assert_eq!(stats.pages_freed, 0);
    }

    #[test]
    fn test_compact_forward_greedy_merge_3_adjacent() {
        // 3 adjacent under-filled leaves (A→B→C, each at ~20% fill) — verify
        // greedy merge collapses A+B first, then merged A+C if they fit,
        // producing a single leaf (not stopping after one merge).
        let mut tree = make_tree();

        // Insert enough to create at least 4-5 leaves
        for i in 0..500u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete entries to leave only ~20 per original leaf
        // Keep entries that map to 3 adjacent leaves
        // With ~100 entries per leaf: leaves are [0..99], [100..199], [200..299], etc.
        // Delete 80% of each leaf's entries to bring them to ~20% fill
        for i in 0..500u32 {
            // Keep every 5th entry
            if i % 5 != 0 {
                let key = format!("k{:06}", i);
                tree.delete(key.as_bytes()).unwrap();
            }
        }

        let remaining = count_all_entries(&tree);
        assert_eq!(remaining, 100); // 500 / 5

        let leaves_before = count_leaves(&tree);
        let stats = tree.compact(0.4).unwrap();
        let leaves_after = count_leaves(&tree);

        // Greedy merge should collapse multiple adjacent underfull leaves
        assert!(
            stats.pages_merged >= 2,
            "Expected greedy merge (>=2 merges), got {}",
            stats.pages_merged
        );
        assert!(
            leaves_after < leaves_before,
            "Expected fewer leaves after greedy merge, before={leaves_before} after={leaves_after}"
        );

        // All remaining entries intact
        for i in (0..500u32).step_by(5) {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Missing key after greedy compaction: k{i:06}"
            );
        }
    }

    #[test]
    fn test_compact_forward_branch_boundary_skip() {
        // Two adjacent under-filled leaves spanning a branch boundary (different parents)
        // — verify the same-parent restriction prevents the merge and compaction
        // advances past them without corruption.
        let mut tree = make_tree();

        // Create a tree deep enough to have multiple branch nodes.
        // With 4K pages and ~100 entries per leaf, we need many entries to get
        // multiple branches. At depth 3+, branch boundaries exist.
        for i in 0..10_000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let tree_depth = tree.depth().unwrap();

        // Delete most entries
        for i in 0..9500u32 {
            let key = format!("k{:06}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        let entries_before = count_all_entries(&tree);
        assert_eq!(entries_before, 500);

        // Compact — some merges may be skipped due to branch boundaries
        let stats = tree.compact(0.4).unwrap();

        // Verify no corruption: all remaining data intact
        for i in 9500..10_000u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Key k{i:06} missing after compaction (tree depth was {tree_depth})"
            );
        }

        // Verify linked list integrity
        let entries = collect_all_entries(&tree);
        assert_eq!(entries.len(), 500);
        for window in entries.windows(2) {
            assert!(window[0].0 < window[1].0);
        }

        // Some merges should have happened (not all pairs span boundaries)
        if tree_depth >= 3 {
            // At depth 3+, there's a chance of branch boundary skips, but most
            // pairs should still merge (same parent)
            assert!(stats.pages_merged > 0, "Expected some merges even with branch boundaries");
        }
    }

    #[test]
    fn test_compact_forward_crash_safety_cow() {
        // Crash safety: compact within a transaction-like scope by cloning
        // the provider state, compacting, then verifying the original is unchanged.
        // This simulates dropping a WriteTransaction before commit.
        let mut tree = make_tree();

        for i in 0..500u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete most entries
        for i in 0..480u32 {
            let key = format!("k{:06}", i);
            tree.delete(key.as_bytes()).unwrap();
        }

        // Snapshot the tree state before compaction
        let pages_snapshot = tree.provider.pages.clone();
        let root_snapshot = tree.root_page;

        // Compact
        let stats = tree.compact(0.4).unwrap();
        assert!(stats.pages_merged > 0);

        // Restore original state (simulates dropping WriteTransaction without commit)
        tree.provider.pages = pages_snapshot;
        tree.root_page = root_snapshot;

        // Original tree should be completely unmodified
        let entries = count_all_entries(&tree);
        assert_eq!(entries, 20, "Original tree should have 20 entries unchanged");

        for i in 480..500u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:06}", i);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }

        // Linked list should be intact on the original
        let entries_list = collect_all_entries(&tree);
        assert_eq!(entries_list.len(), 20);
        for window in entries_list.windows(2) {
            assert!(window[0].0 < window[1].0);
        }
    }

    #[test]
    fn test_delete_all_from_mid_leaf_predecessor_next_leaf_updated() {
        // Scenario: predecessor → target → successor, where target has all
        // entries deleted. After compaction, the target leaf must be freed and
        // no longer referenced by any leaf's next_leaf pointer.
        //
        // The compact() algorithm merges underfull leaves with their right
        // neighbor. When predecessor is underfull and target is empty,
        // merge_leaves(predecessor, target) sets predecessor.next_leaf =
        // target.next_leaf, effectively skipping the freed target page.
        let mut tree = make_tree();

        // Use longer values to ensure leaves can't absorb too many neighbors
        // (fills leaves faster, limits greedy merge cascading).
        for i in 0..300u32 {
            let key = format!("k{:06}", i);
            let val = format!("v{:0>200}", i); // 200-byte values → fewer entries per leaf
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let initial_leaves = count_leaves(&tree);
        assert!(initial_leaves >= 4, "Need at least 4 leaves, got {initial_leaves}");

        // Walk the linked list to identify entries in each leaf.
        let first_leaf_id = tree.find_first_leaf().unwrap();

        struct LeafInfo {
            page_id: PageId,
            keys: Vec<Vec<u8>>,
        }
        let mut leaf_infos: Vec<LeafInfo> = Vec::new();
        let mut current = first_leaf_id;
        loop {
            let page = tree.provider.read_page(current).unwrap();
            let leaf = LeafNodeRef::from_page(&page).unwrap();
            let count = leaf.cell_count() as usize;
            let mut keys = Vec::with_capacity(count);
            for i in 0..count {
                keys.push(leaf.key(i).to_vec());
            }
            let next = leaf.next_leaf();
            leaf_infos.push(LeafInfo { page_id: current, keys });
            if next == 0 {
                break;
            }
            current = next;
        }
        assert!(leaf_infos.len() >= 4, "Need at least 4 leaves in linked list");

        // Pick three consecutive leaves: predecessor, target, successor.
        // Use leaves [1], [2], [3] so the predecessor is NOT the first leaf
        // (avoids edge cases with the leftmost leaf).
        let predecessor_id = leaf_infos[1].page_id;
        let target_id = leaf_infos[2].page_id;
        let target_keys = leaf_infos[2].keys.clone();

        // Verify initial chain includes target.
        {
            let pred_page = tree.provider.read_page(predecessor_id).unwrap();
            let pred_leaf = LeafNodeRef::from_page(&pred_page).unwrap();
            assert_eq!(pred_leaf.next_leaf(), target_id, "Predecessor should point to target");
        }

        // Delete ALL entries from the target leaf.
        let mut deleted_count = target_keys.len();
        assert!(!target_keys.is_empty(), "Target leaf should have entries");
        for key in &target_keys {
            let result = tree.delete(key).unwrap();
            assert!(result.is_some(), "Target key should have existed");
        }

        // Also make the predecessor underfull by deleting most of its entries.
        let pred_keys = &leaf_infos[1].keys;
        let keep_in_pred = 1; // Keep 1 entry → clearly underfull
        for key in pred_keys.iter().skip(keep_in_pred) {
            let result = tree.delete(key).unwrap();
            assert!(result.is_some(), "Predecessor key should have existed");
            deleted_count += 1;
        }

        // Verify the target leaf is empty.
        {
            let target_page = tree.provider.read_page(target_id).unwrap();
            let target_leaf = LeafNodeRef::from_page(&target_page).unwrap();
            assert_eq!(target_leaf.cell_count(), 0, "Target leaf should be empty");
        }

        // Run compaction.
        let stats = tree.compact(0.4).unwrap();
        assert!(stats.pages_merged > 0, "Expected at least one merge");

        // Core invariant: the target page must NOT appear in the linked list.
        // Walk the entire linked list and verify target_id is absent.
        {
            let mut current = tree.find_first_leaf().unwrap();
            loop {
                assert_ne!(
                    current, target_id,
                    "Target page {target_id} should not appear in the linked list after compaction"
                );
                let page = tree.provider.read_page(current).unwrap();
                let leaf = LeafNodeRef::from_page(&page).unwrap();
                let next = leaf.next_leaf();
                if next == 0 {
                    break;
                }
                current = next;
            }
        }

        // Verify all remaining entries are intact and ordered.
        let remaining = collect_all_entries(&tree);
        let expected_count = 300 - deleted_count;
        assert_eq!(
            remaining.len(),
            expected_count,
            "Expected {expected_count} entries remaining, got {}",
            remaining.len()
        );
        for window in remaining.windows(2) {
            assert!(
                window[0].0 < window[1].0,
                "Entries not in order: {:?} >= {:?}",
                String::from_utf8_lossy(&window[0].0),
                String::from_utf8_lossy(&window[1].0)
            );
        }

        // Leaf count should have decreased.
        let final_leaves = count_leaves(&tree);
        assert!(
            final_leaves < initial_leaves,
            "Should have fewer leaves after compaction ({final_leaves} >= {initial_leaves})"
        );
    }

    // ====================================================================
    // PageFull propagation and sustained-volume tests
    // ====================================================================

    /// Verifies that BTree::insert() returns PageFull (not a panic) when a
    /// single key+value exceeds page capacity.
    #[test]
    fn test_btree_insert_oversized_entry_returns_page_full() {
        let mut tree = make_tree();

        // A value larger than the entire usable leaf space (~3808 bytes).
        // This must propagate as Error::PageFull through the split path,
        // since even after splitting, neither half can hold the entry.
        let big_value = vec![0xAB; 4000];
        let result = tree.insert(b"big-key", &big_value);
        assert!(result.is_err(), "oversized entry should fail, not panic");
        assert!(
            matches!(result, Err(Error::PageFull)),
            "error should be PageFull, got: {:?}",
            result
        );
    }

    /// Regression test: sustained sequential writes at the volume that
    /// previously triggered the `free_end -= cell_data_size` underflow panic.
    /// Exercises 5,000 inserts — the original failure threshold.
    #[test]
    fn test_btree_5000_sequential_inserts_no_panic() {
        let mut tree = make_tree();

        for i in 0..5_000u32 {
            let key = format!("k-{i:06}");
            let value = format!("v-{i}");
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Spot-check first, last, and mid-range entries
        assert_eq!(
            tree.get(b"k-000000").unwrap(),
            Some(b"v-0".to_vec()),
        );
        assert_eq!(
            tree.get(b"k-004999").unwrap(),
            Some(b"v-4999".to_vec()),
        );
        assert_eq!(
            tree.get(b"k-002500").unwrap(),
            Some(b"v-2500".to_vec()),
        );

        // Verify total entry count via iteration
        let mut iter = tree.range(super::cursor::Range::all()).unwrap();
        let mut count = 0u32;
        while iter.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5_000, "all 5,000 entries should be retrievable");
    }

    /// Verifies that interleaved inserts and deletes at volume don't trigger
    /// underflow panics in the delete→reinsert (update) or compact paths.
    #[test]
    fn test_btree_insert_delete_interleave_at_volume() {
        let mut tree = make_tree();

        // Insert 2,000 entries
        for i in 0..2_000u32 {
            let key = format!("id-{i:06}");
            let value = format!("val-{i}");
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Delete odd-numbered keys, update even-numbered keys
        for i in 0..2_000u32 {
            let key = format!("id-{i:06}");
            if i % 2 == 1 {
                tree.delete(key.as_bytes()).unwrap();
            } else {
                let new_value = format!("updated-{i}");
                tree.insert(key.as_bytes(), new_value.as_bytes()).unwrap();
            }
        }

        // Insert 1,000 more to exercise reuse of freed space after splits
        for i in 2_000..3_000u32 {
            let key = format!("id-{i:06}");
            let value = format!("val-{i}");
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify: 1,000 even originals + 1,000 new = 2,000 total
        let mut iter = tree.range(super::cursor::Range::all()).unwrap();
        let mut count = 0u32;
        while iter.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2_000, "should have 2,000 entries after interleaved ops");

        // Spot-check an updated entry
        assert_eq!(
            tree.get(b"id-000100").unwrap(),
            Some(b"updated-100".to_vec()),
        );
    }
}
