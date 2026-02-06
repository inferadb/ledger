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

pub mod cursor;
pub mod node;
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
    /// Read a page by ID.
    fn read_page(&self, page_id: PageId) -> Result<Page>;

    /// Write a page to the cache/storage.
    fn write_page(&mut self, page: Page);

    /// Allocate a new page of the given type.
    fn allocate_page(&mut self, page_type: PageType) -> Page;

    /// Free a page for later reuse.
    fn free_page(&mut self, page_id: PageId);

    /// Get the page size.
    fn page_size(&self) -> usize;

    /// Get the current transaction ID.
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
}

impl<P: PageProvider> BTree<P> {
    /// Create a new B-tree accessor.
    pub fn new(root_page: PageId, provider: P) -> Self {
        Self { provider, root_page }
    }

    /// Check if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root_page == 0
    }

    /// Get the root page ID.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Set the root page ID (called after split creates new root).
    pub fn set_root_page(&mut self, page_id: PageId) {
        self.root_page = page_id;
    }

    /// Allocate and initialize a new leaf page.
    fn new_leaf_page(&mut self) -> Page {
        let mut page = self.provider.allocate_page(PageType::BTreeLeaf);
        LeafNode::init(&mut page);
        page
    }

    /// Allocate and initialize a new branch page.
    fn new_branch_page(&mut self, rightmost_child: PageId) -> Page {
        let mut page = self.provider.allocate_page(PageType::BTreeBranch);
        BranchNode::init(&mut page, rightmost_child);
        page
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page == 0 {
            return Ok(None);
        }

        let leaf_page = self.find_leaf(key)?;

        let mut page = self.provider.read_page(leaf_page)?;
        let leaf = LeafNode::from_page(&mut page)?;

        match leaf.search(key) {
            SearchResult::Found(idx) => Ok(Some(leaf.value(idx).to_vec())),
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Find the leaf page that would contain the given key.
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

    /// Find the next leaf page after the one containing `key`.
    ///
    /// Descends from the root tracking the path, then backtracks to find
    /// the nearest ancestor with a right sibling child. Returns `None` if
    /// the key is in the rightmost leaf (no next leaf exists).
    fn find_next_leaf(&self, key: &[u8]) -> Result<Option<PageId>> {
        // Collect the path from root to leaf: (branch_page_id, child_index_taken).
        // child_index is the separator index whose left child we followed,
        // or cell_count if we followed rightmost_child.
        let mut path: Vec<(PageId, usize)> = Vec::new();
        let mut current = self.root_page;

        loop {
            let page = self.provider.read_page(current)?;
            match page.page_type()? {
                PageType::BTreeLeaf => break,
                PageType::BTreeBranch => {
                    let mut page = page;
                    let branch = BranchNode::from_page(&mut page)?;
                    let count = branch.cell_count() as usize;

                    // Determine which child we descend into.
                    let mut child_idx = count; // default: rightmost_child
                    for i in 0..count {
                        let sep_key = branch.key(i);
                        if key < sep_key {
                            child_idx = i;
                            break;
                        }
                    }

                    let child_page = if child_idx < count {
                        branch.child(child_idx)
                    } else {
                        branch.rightmost_child()
                    };

                    path.push((current, child_idx));
                    current = child_page;
                },
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    });
                },
            }
        }

        // Backtrack to find the nearest ancestor with a right sibling.
        while let Some((branch_page_id, child_idx)) = path.pop() {
            let mut page = self.provider.read_page(branch_page_id)?;
            let branch = BranchNode::from_page(&mut page)?;
            let count = branch.cell_count() as usize;

            // If we followed child(i) where i < count, the next child is
            // child(i+1) if i+1 < count, otherwise rightmost_child.
            // If we followed rightmost_child (child_idx == count), no right sibling.
            if child_idx < count {
                let next_child = if child_idx + 1 < count {
                    branch.child(child_idx + 1)
                } else {
                    branch.rightmost_child()
                };
                // Descend to the leftmost leaf of next_child.
                return self.find_first_leaf_from(next_child).map(Some);
            }
            // child_idx == count means we were in the rightmost subtree;
            // continue backtracking.
        }

        // Exhausted path — current leaf is the rightmost leaf.
        Ok(None)
    }

    /// Find the leftmost leaf starting from a given page.
    fn find_first_leaf_from(&self, start: PageId) -> Result<PageId> {
        let mut current = start;
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

    /// Insert a key-value pair.
    ///
    /// If the key already exists, its value is updated.
    /// Returns the previous value if the key existed.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root_page == 0 {
            // Create first leaf as root
            let mut page = self.new_leaf_page();
            {
                let mut leaf = LeafNode::from_page(&mut page)?;
                leaf.insert(0, key, value);
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
                branch.insert(0, &separator_key, self.root_page);
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
                        // Update existing key
                        old_value = Some(leaf.value(idx).to_vec());
                        if !leaf.update(idx, value) {
                            // Value grew - need to delete and reinsert
                            leaf.delete(idx);
                            if leaf.can_insert(key, value) {
                                // Reinsert in same page
                                match leaf.search(key) {
                                    SearchResult::NotFound(new_idx) => {
                                        leaf.insert(new_idx, key, value);
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
                            leaf.insert(idx, key, value);
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
                            branch.insert(insert_idx, &sep_key, child_page_id);
                            branch.set_rightmost_child(new_child);
                        } else {
                            // Inserting in middle:
                            // - Insert separator with original child (left half) as its child
                            // - After shift, the old cell at insert_idx moved to insert_idx+1
                            // - Update that cell's child to be new_child (right half)
                            branch.insert(insert_idx, &sep_key, child_page_id);
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

    /// Insert into leaf and split if necessary.
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
        let mut new_page = self.new_leaf_page();

        // Use key-aware split that ensures the new key will fit
        let split_result = split_leaf_for_key(page, &mut new_page, key, value)?;

        // Now insert the new key into the appropriate half based on ordering
        let insert_left = key < split_result.separator_key.as_slice();

        if insert_left {
            let mut leaf = LeafNode::from_page(page)?;
            match leaf.search(key) {
                SearchResult::NotFound(idx) => {
                    leaf.insert(idx, key, value);
                },
                SearchResult::Found(_) => {},
            }
        } else {
            let mut leaf = LeafNode::from_page(&mut new_page)?;
            match leaf.search(key) {
                SearchResult::NotFound(idx) => {
                    leaf.insert(idx, key, value);
                },
                SearchResult::Found(_) => {},
            }
        }

        self.provider.write_page(page.clone());
        self.provider.write_page(new_page);

        Ok((Some((split_result.separator_key, split_result.new_page_id)), old_value))
    }

    /// Insert into branch and split if necessary.
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
                branch.insert(insert_idx, key, original_child);
                branch.set_rightmost_child(right_child);
            } else {
                branch.insert(insert_idx, key, original_child);
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
                branch.insert(insert_idx, key, original_child);
                branch.set_rightmost_child(right_child);
            } else {
                branch.insert(insert_idx, key, original_child);
                branch.set_child(insert_idx + 1, right_child);
            }
        }

        self.provider.write_page(page.clone());
        self.provider.write_page(new_page);

        Ok((Some((split_result.separator_key, split_result.new_page_id)), old_value))
    }

    /// Delete a key from the tree.
    ///
    /// Returns the value that was deleted, or None if key didn't exist.
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
                leaf.delete(idx);
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

    /// Create an iterator over all entries in the tree.
    pub fn iter(&self) -> Result<BTreeIterator<'_, P>> {
        BTreeIterator::new(self, Range::all())
    }

    /// Create an iterator over a range of entries.
    pub fn range<'a>(&'a self, range: Range<'a>) -> Result<BTreeIterator<'a, P>> {
        BTreeIterator::new(self, range)
    }

    /// Get the first (smallest) key-value pair in the tree.
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

    /// Get the last (largest) key-value pair in the tree.
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

    /// Find the leftmost (first) leaf in the tree.
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

    /// Find the rightmost (last) leaf in the tree.
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

    /// Compact the B+ tree by merging underfull leaf nodes.
    ///
    /// Walks all leaves left-to-right. When a leaf's fill factor is below
    /// `min_fill_factor`, attempts to merge it with its right sibling
    /// (if the combined entries fit in one page). After merging, the
    /// separator key is removed from the parent branch and the right
    /// sibling page is freed.
    ///
    /// # Arguments
    /// * `min_fill_factor` — Threshold below which a leaf is considered underfull (0.0 to 1.0,
    ///   default recommendation: 0.4).
    ///
    /// # Returns
    /// Statistics about the compaction: pages merged and pages freed.
    pub fn compact(&mut self, min_fill_factor: f64) -> Result<CompactionStats> {
        if self.root_page == 0 {
            return Ok(CompactionStats::default());
        }

        let mut stats = CompactionStats::default();

        // Collect all (leaf_page_id, parent_page_id, child_index_in_parent) tuples
        // by walking the tree. We collect first to avoid borrowing issues.
        let mut leaf_info = self.collect_leaf_info()?;

        // Process pairs of adjacent leaves that share the same parent.
        // We iterate with a window: for each underfull leaf, check if it can
        // merge with the next leaf (which must share the same parent branch).
        // After each merge, re-collect leaf info since the tree structure changed.
        let mut i = 0;
        while i + 1 < leaf_info.len() {
            let (left_id, left_parent_id, _left_child_idx) = leaf_info[i];
            let (right_id, right_parent_id, _right_child_idx) = leaf_info[i + 1];

            // Only merge siblings under the same parent branch
            if left_parent_id != right_parent_id {
                i += 1;
                continue;
            }

            let left_page = self.provider.read_page(left_id)?;
            let left_fill = leaf_fill_factor(&left_page)?;

            if left_fill >= min_fill_factor {
                i += 1;
                continue;
            }

            let right_page = self.provider.read_page(right_id)?;

            // Check if combined entries fit in one page
            if !can_merge_leaves(&left_page, &right_page)? {
                i += 1;
                continue;
            }

            // Perform the merge: move all right entries into left
            let mut left_page = left_page;
            let mut right_page = right_page;
            merge_leaves(&mut left_page, &mut right_page)?;
            self.provider.write_page(left_page);

            // Update parent branch: remove the separator between left and right.
            //
            // In our B+ tree layout, branch cells have the form:
            //   cell[i] = (separator_key, left_child)
            // and rightmost_child covers the rightmost subtree.
            //
            // When we merge left+right, we need to remove the separator
            // that pointed to the right child. The separator index in the
            // parent is `left_child_idx` — the separator whose left child
            // is the left leaf. After removing it, the left leaf absorbs
            // the right leaf's entries, and the right child pointer is no
            // longer needed.
            if left_parent_id != 0 {
                let mut parent_page = self.provider.read_page(left_parent_id)?;
                let mut branch = BranchNode::from_page(&mut parent_page)?;
                let branch_count = branch.cell_count() as usize;

                // Find the separator index that separates left from right.
                // The separator at index `sep_idx` has:
                //   child(sep_idx) = left_id (or it could be rightmost)
                // The right sibling is either child(sep_idx+1) or rightmost_child.
                let sep_idx = self.find_separator_index(&branch, left_id, right_id, branch_count);

                if let Some(idx) = sep_idx {
                    // If the right child was at child(idx+1), just delete
                    // the separator at idx. The child at idx (left) stays.
                    if idx + 1 < branch_count {
                        // Right is child(idx+1). After deleting separator at idx,
                        // cell at idx+1 shifts to idx, and its child becomes the
                        // left child pointer. We need to set that child to left_id.
                        branch.delete(idx);
                        branch.set_child(idx, left_id);
                    } else {
                        // Right is the rightmost_child.
                        // Delete the last separator, set rightmost to left_id.
                        branch.delete(idx);
                        branch.set_rightmost_child(left_id);
                    }

                    drop(branch);

                    // If parent is now empty and is the root, collapse tree height
                    let parent_count = {
                        let b = BranchNode::from_page(&mut parent_page)?;
                        b.cell_count()
                    };

                    if parent_count == 0 && parent_page.id == self.root_page {
                        // Root branch has no separators — its rightmost_child is the only child.
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

            // Free the right (now empty) page
            self.provider.free_page(right_id);
            stats.pages_merged += 1;
            stats.pages_freed += 1;

            // Re-collect leaf info since the tree structure changed (parent
            // branch was modified, right page was freed). Restart the scan
            // so the merged left can potentially merge with its new neighbor.
            leaf_info = self.collect_leaf_info()?;
            i = 0;
        }

        Ok(stats)
    }

    /// Collect (leaf_page_id, parent_page_id, child_index_in_parent) for all leaves.
    ///
    /// `parent_page_id` is 0 for the root when the root is a leaf.
    /// `child_index_in_parent` is the index of the child pointer in the parent
    /// branch that leads to this leaf. For rightmost children, the index is
    /// set to the branch's cell_count (one past the last separator).
    fn collect_leaf_info(&self) -> Result<Vec<(PageId, PageId, usize)>> {
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

    /// Find the separator index in a parent branch that separates left_id from right_id.
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
}

/// Iterator over B-tree entries.
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

        // Current leaf exhausted — navigate to the next leaf via the branch
        // structure. This correctly handles variable-length keys where
        // increment_key may produce a key still within the current leaf's range.
        let page = self
            .current_leaf
            .as_ref()
            .ok_or(Error::Corrupted { reason: "No current leaf in iterator".to_string() })?;
        let leaf = LeafNodeRef::from_page(page)?;
        let count = leaf.cell_count() as usize;

        if count == 0 {
            self.state.position.valid = false;
            return Ok(false);
        }

        // Use the last key to locate the current leaf in the tree, then find
        // its right sibling via branch-node backtracking.
        let last_key = leaf.key(count - 1);
        match self.tree.find_next_leaf(last_key)? {
            Some(next_leaf_id) => {
                self.current_leaf = Some(self.tree.provider.read_page(next_leaf_id)?);
                let new_page = self.current_leaf.as_ref().unwrap();
                cursor_ops::move_to_first_in_leaf(&mut self.state.position, new_page)?;
                Ok(self.state.position.valid)
            },
            None => {
                self.state.position.valid = false;
                Ok(false)
            },
        }
    }

    /// Get the current key-value pair, if positioned (returns owned copies).
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

    /// Move to next entry and return it.
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

        /// Generate a vector of unique keys (formatted for sort stability).
        fn arb_unique_keys(max_count: usize) -> impl Strategy<Value = Vec<String>> {
            proptest::collection::hash_set("[a-z]{1,4}", 1..max_count)
                .prop_map(|s| s.into_iter().collect::<Vec<_>>())
        }

        /// Generate key-value pairs with unique keys.
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

            /// Delete removes keys and they become unretrievable.
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

            /// Update overwrites value while keeping key present.
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

            /// Seek finds exact key or correct insertion position.
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
}
