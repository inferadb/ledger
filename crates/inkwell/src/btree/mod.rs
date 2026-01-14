//! B+ tree implementation for Inkwell.
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

use crate::error::{Error, PageId, PageType, Result};
use crate::page::Page;
use cursor::{Range, RangeIterState, SeekResult, cursor_ops};
use node::{BranchNode, LeafNode, SearchResult};
use split::{split_leaf, split_branch};

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
        Self {
            provider,
            root_page,
        }
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

        // Navigate to the leaf that would contain this key
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
                }
                _ => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: page_type,
                    })
                }
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

        // Find path to leaf
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
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                // Need to split
                                drop(leaf);
                                return self.insert_and_split_leaf(&mut page, key, value, old_value);
                            }
                        }
                        drop(leaf);
                        self.provider.write_page(page);
                        Ok((None, old_value))
                    }
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
                    }
                }
            }
            PageType::BTreeBranch => {
                let child_page_id = {
                    let branch = BranchNode::from_page(&mut page)?;
                    branch.child_for_key(key)
                };

                // Recurse into child
                let (promoted, old_value) = self.insert_recursive(child_page_id, key, value)?;

                if let Some((sep_key, new_child)) = promoted {
                    // Child was split, need to insert separator into this branch
                    // child_page_id is the original child (now left half after split)
                    // new_child is the new page (right half after split)
                    let mut branch = BranchNode::from_page(&mut page)?;

                    if branch.can_insert(&sep_key) {
                        // Find position and insert
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
                            // Inserting at end: original child (left half) becomes left child of sep,
                            // new_child (right half) becomes new rightmost
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
                        self.insert_and_split_branch(&mut page, &sep_key, child_page_id, new_child, old_value)
                    }
                } else {
                    // No split needed
                    Ok((None, old_value))
                }
            }
            _ => Err(Error::PageTypeMismatch {
                expected: PageType::BTreeLeaf,
                found: page_type,
            }),
        }
    }

    /// Insert into leaf and split if necessary.
    fn insert_and_split_leaf(
        &mut self,
        page: &mut Page,
        key: &[u8],
        value: &[u8],
        old_value: Option<Vec<u8>>,
    ) -> Result<(Option<(Vec<u8>, PageId)>, Option<Vec<u8>>)> {
        // Allocate new page for right half
        let mut new_page = self.new_leaf_page();

        // Do the split
        let split_result = split_leaf(page, &mut new_page)?;

        // Now insert the new key into the appropriate half
        // Prefer the side based on key ordering, but fall back to the other side if needed
        let prefer_left = key < split_result.separator_key.as_slice();

        // Try preferred side first, fall back to other side if no space
        let (target_page, fallback_page) = if prefer_left {
            (page as &mut Page, &mut new_page as &mut Page)
        } else {
            (&mut new_page as &mut Page, page as &mut Page)
        };

        // Check if preferred side has space
        {
            let leaf = LeafNode::from_page(target_page)?;
            if leaf.can_insert(key, value) {
                drop(leaf);
                let mut leaf = LeafNode::from_page(target_page)?;
                match leaf.search(key) {
                    SearchResult::NotFound(idx) => {
                        leaf.insert(idx, key, value);
                    }
                    SearchResult::Found(_) => {}
                }
            } else {
                // Try fallback side
                drop(leaf);
                let mut leaf = LeafNode::from_page(fallback_page)?;
                if leaf.can_insert(key, value) {
                    match leaf.search(key) {
                        SearchResult::NotFound(idx) => {
                            leaf.insert(idx, key, value);
                        }
                        SearchResult::Found(_) => {}
                    }
                } else {
                    // Neither side has space - this shouldn't happen with proper splitting
                    // but handle gracefully by returning an error
                    return Err(Error::PageFull);
                }
            }
        }

        // Write both pages
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
        // Allocate new page and split first
        let mut new_page = self.new_branch_page(0);

        // Do the split
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
                    // Tree is now empty
                    self.provider.free_page(page.id);
                    self.root_page = 0;
                } else {
                    self.provider.write_page(page);
                }

                Ok(Some(old_value))
            }
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

        // Navigate to leftmost leaf
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

        // Navigate to rightmost leaf
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
                }
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    })
                }
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
                }
                pt => {
                    return Err(Error::PageTypeMismatch {
                        expected: PageType::BTreeLeaf,
                        found: pt,
                    })
                }
            }
        }
    }
}

/// Increment a key lexicographically to get its successor.
///
/// For fixed-width big-endian encoded keys, this properly increments
/// the key as a large integer. Returns None if the key is all 0xFF
/// (maximum value, no successor exists).
fn increment_key(key: &[u8]) -> Option<Vec<u8>> {
    let mut result = key.to_vec();

    // Increment from the least significant byte (rightmost)
    for i in (0..result.len()).rev() {
        if result[i] == 0xFF {
            result[i] = 0x00; // Carry
        } else {
            result[i] += 1;
            return Some(result);
        }
    }

    // All bytes were 0xFF - no successor
    None
}

/// Iterator over B-tree entries.
pub struct BTreeIterator<'a, P: PageProvider> {
    tree: &'a BTree<P>,
    state: RangeIterState<'a>,
    current_leaf: Option<Page>,
}

impl<'a, P: PageProvider> BTreeIterator<'a, P> {
    fn new(tree: &'a BTree<P>, range: Range<'a>) -> Result<Self> {
        let mut iter = Self {
            tree,
            state: RangeIterState::new(range),
            current_leaf: None,
        };

        // Position at start of range
        iter.seek_to_start()?;

        Ok(iter)
    }

    fn seek_to_start(&mut self) -> Result<()> {
        if self.tree.root_page == 0 {
            self.state.position.valid = false;
            return Ok(());
        }

        // Find starting position based on range
        let start_key: Option<&[u8]> = match &self.state.range.start {
            cursor::Bound::Unbounded => None,
            cursor::Bound::Included(k) => Some(k),
            cursor::Bound::Excluded(k) => Some(k),
        };

        // Navigate to leaf
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
            if matches!(&self.state.range.start, cursor::Bound::Excluded(_)) && result == SeekResult::Found {
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

        let page = self.current_leaf.as_ref().ok_or(Error::Corrupted {
            reason: "No current leaf in iterator".to_string(),
        })?;

        // Try to advance within current leaf
        if cursor_ops::next_in_leaf(&mut self.state.position, page)? {
            return Ok(true);
        }

        // Current leaf exhausted - try to move to next leaf
        // Get the last key in current leaf to find the next leaf
        let page = self.current_leaf.as_mut().ok_or(Error::Corrupted {
            reason: "No current leaf in iterator".to_string(),
        })?;
        let current_page_id = page.id;
        let leaf = LeafNode::from_page(page)?;
        let count = leaf.cell_count() as usize;

        if count == 0 {
            self.state.position.valid = false;
            return Ok(false);
        }

        // Get the last key from this leaf
        let (last_key, _) = leaf.get(count - 1);
        let last_key = last_key.to_vec();

        // Create a successor key by incrementing the last key
        // This handles fixed-width big-endian keys correctly
        let next_key = match increment_key(&last_key) {
            Some(k) => k,
            None => {
                // Key is all 0xFF - no successor exists
                self.state.position.valid = false;
                return Ok(false);
            }
        };

        // Find the leaf that would contain the successor key
        let next_leaf_id = self.tree.find_leaf(&next_key)?;

        // If we end up at the same leaf, we're done
        if next_leaf_id == current_page_id {
            self.state.position.valid = false;
            return Ok(false);
        }

        // Load the new leaf
        self.current_leaf = Some(self.tree.provider.read_page(next_leaf_id)?);
        let new_page = self.current_leaf.as_ref().unwrap();

        // Position at the first entry in the new leaf
        cursor_ops::move_to_first_in_leaf(&mut self.state.position, new_page)?;

        Ok(self.state.position.valid)
    }

    /// Get the current key-value pair, if positioned (returns owned copies).
    pub fn current(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.state.position.valid {
            return Ok(None);
        }

        let page = self.current_leaf.as_ref().ok_or(Error::Corrupted {
            reason: "No current leaf".to_string(),
        })?;

        let key = cursor_ops::current_key(&self.state.position, page)?;
        let value = cursor_ops::current_value(&self.state.position, page)?;

        match (key, value) {
            (Some(k), Some(v)) => {
                // Check if still within range
                if self.state.range.should_continue(&k) {
                    Ok(Some((k, v)))
                } else {
                    Ok(None)
                }
            }
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
            }
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
}
