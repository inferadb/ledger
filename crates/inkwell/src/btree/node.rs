//! B-tree node structure and operations.
//!
//! Node layout for Inkwell's B+ tree:
//!
//! ## Leaf Node Layout
//! ```text
//! [Page Header: 16 bytes]
//! [Cell Count: 2 bytes]
//! [Free Space Start: 2 bytes]
//! [Free Space End: 2 bytes]
//! [Reserved: 2 bytes]
//! [Cell Pointers: 2 bytes each, pointing to cells from end of page]
//! ... free space ...
//! [Cells: (key_len:2, val_len:2, key_bytes, val_bytes)]
//! ```
//!
//! ## Branch Node Layout
//! ```text
//! [Page Header: 16 bytes]
//! [Cell Count: 2 bytes]
//! [Free Space Start: 2 bytes]
//! [Free Space End: 2 bytes]
//! [Rightmost Child: 8 bytes]
//! [Cell Pointers: 2 bytes each]
//! ... free space ...
//! [Cells: (key_len:2, child_page:8, key_bytes)]
//! ```

use crate::error::{PageId, PageType, Result, Error};
use crate::page::{Page, PAGE_HEADER_SIZE};
use std::cmp::Ordering;

/// Node header size (after page header).
pub const NODE_HEADER_SIZE: usize = 8;

/// Offset of cell count in node (after page header).
const CELL_COUNT_OFFSET: usize = PAGE_HEADER_SIZE;

/// Offset of free space start pointer.
const FREE_START_OFFSET: usize = PAGE_HEADER_SIZE + 2;

/// Offset of free space end pointer.
const FREE_END_OFFSET: usize = PAGE_HEADER_SIZE + 4;

/// Offset of rightmost child (branch nodes only).
const RIGHTMOST_CHILD_OFFSET: usize = PAGE_HEADER_SIZE + 6;

/// Offset where cell pointers begin (leaf nodes).
const LEAF_CELL_PTRS_OFFSET: usize = PAGE_HEADER_SIZE + NODE_HEADER_SIZE;

/// Offset where cell pointers begin (branch nodes, after rightmost child).
const BRANCH_CELL_PTRS_OFFSET: usize = PAGE_HEADER_SIZE + 6 + 8; // +8 for rightmost child

/// Size of a cell pointer.
const CELL_PTR_SIZE: usize = 2;

/// Minimum cell size for leaf: key_len(2) + val_len(2) + min_key(1) = 5
#[allow(dead_code)]
const MIN_LEAF_CELL_SIZE: usize = 5;

/// Minimum cell size for branch: key_len(2) + child_page(8) + min_key(1) = 11
#[allow(dead_code)]
const MIN_BRANCH_CELL_SIZE: usize = 11;

/// A search result for finding a key in a node.
#[derive(Debug, Clone, Copy)]
pub enum SearchResult {
    /// Key was found at this index.
    Found(usize),
    /// Key was not found; this is where it would be inserted.
    NotFound(usize),
}

impl SearchResult {
    /// Get the index, whether found or not.
    pub fn index(&self) -> usize {
        match self {
            SearchResult::Found(i) | SearchResult::NotFound(i) => *i,
        }
    }

    /// Returns true if the key was found.
    pub fn is_found(&self) -> bool {
        matches!(self, SearchResult::Found(_))
    }
}

/// Read/write operations on a B-tree leaf node.
pub struct LeafNode<'a> {
    page: &'a mut Page,
}

impl<'a> LeafNode<'a> {
    /// Wrap a page as a leaf node.
    pub fn from_page(page: &'a mut Page) -> Result<Self> {
        let page_type = page.page_type()?;
        if page_type != PageType::BTreeLeaf {
            return Err(Error::PageTypeMismatch {
                expected: PageType::BTreeLeaf,
                found: page_type,
            });
        }
        Ok(Self { page })
    }

    /// Initialize an empty leaf node.
    pub fn init(page: &'a mut Page) -> Self {
        let page_size = page.size();

        // Set cell count to 0
        page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].copy_from_slice(&0u16.to_le_bytes());

        // Free space starts right after node header
        let free_start = LEAF_CELL_PTRS_OFFSET as u16;
        page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].copy_from_slice(&free_start.to_le_bytes());

        // Free space ends at page end
        let free_end = page_size as u16;
        page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].copy_from_slice(&free_end.to_le_bytes());

        page.dirty = true;
        Self { page }
    }

    /// Get the number of cells in this node.
    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].try_into().unwrap())
    }

    /// Get the free space available for new cells.
    pub fn free_space(&self) -> usize {
        let free_start = u16::from_le_bytes(
            self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let free_end = u16::from_le_bytes(
            self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        if free_end > free_start {
            free_end - free_start
        } else {
            0
        }
    }

    /// Get a cell pointer (offset into page where cell data is stored).
    fn cell_ptr(&self, index: usize) -> usize {
        let ptr_offset = LEAF_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        u16::from_le_bytes(self.page.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize
    }

    /// Set a cell pointer.
    fn set_cell_ptr(&mut self, index: usize, offset: usize) {
        let ptr_offset = LEAF_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        self.page.data[ptr_offset..ptr_offset + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        self.page.dirty = true;
    }

    /// Get the key at a given index.
    pub fn key(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.page.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        &self.page.data[cell_offset + 4..cell_offset + 4 + key_len]
    }

    /// Get the value at a given index.
    pub fn value(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.page.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        let val_len = u16::from_le_bytes(
            self.page.data[cell_offset + 2..cell_offset + 4].try_into().unwrap()
        ) as usize;
        &self.page.data[cell_offset + 4 + key_len..cell_offset + 4 + key_len + val_len]
    }

    /// Get key and value at a given index.
    pub fn get(&self, index: usize) -> (&[u8], &[u8]) {
        (self.key(index), self.value(index))
    }

    /// Binary search for a key. Returns Found(index) or NotFound(insert_position).
    pub fn search(&self, key: &[u8]) -> SearchResult {
        let count = self.cell_count() as usize;
        if count == 0 {
            return SearchResult::NotFound(0);
        }

        let mut lo = 0;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key(mid);

            match key.cmp(mid_key) {
                Ordering::Equal => return SearchResult::Found(mid),
                Ordering::Less => hi = mid,
                Ordering::Greater => lo = mid + 1,
            }
        }

        SearchResult::NotFound(lo)
    }

    /// Calculate the space needed for a key-value pair.
    pub fn cell_size(key: &[u8], value: &[u8]) -> usize {
        CELL_PTR_SIZE + 2 + 2 + key.len() + value.len() // ptr + key_len + val_len + key + value
    }

    /// Check if there's room for a new cell.
    pub fn can_insert(&self, key: &[u8], value: &[u8]) -> bool {
        self.free_space() >= Self::cell_size(key, value)
    }

    /// Insert a key-value pair at the given index.
    ///
    /// Caller must ensure there's enough space (use `can_insert` first).
    pub fn insert(&mut self, index: usize, key: &[u8], value: &[u8]) {
        let count = self.cell_count() as usize;
        assert!(index <= count);

        // Calculate cell size (excluding pointer, which is separate)
        let cell_data_size = 2 + 2 + key.len() + value.len(); // key_len + val_len + key + value

        // Get current free space markers
        let mut free_start = u16::from_le_bytes(
            self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let mut free_end = u16::from_le_bytes(
            self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        // Allocate cell from end of free space
        free_end -= cell_data_size;
        let cell_offset = free_end;

        // Write cell data
        self.page.data[cell_offset..cell_offset + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        self.page.data[cell_offset + 2..cell_offset + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
        self.page.data[cell_offset + 4..cell_offset + 4 + key.len()].copy_from_slice(key);
        self.page.data[cell_offset + 4 + key.len()..cell_offset + 4 + key.len() + value.len()].copy_from_slice(value);

        // Shift cell pointers to make room
        if index < count {
            let src_start = LEAF_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
            let src_end = LEAF_CELL_PTRS_OFFSET + count * CELL_PTR_SIZE;
            let dst_start = src_start + CELL_PTR_SIZE;
            self.page.data.copy_within(src_start..src_end, dst_start);
        }

        // Write new cell pointer
        self.set_cell_ptr(index, cell_offset);

        // Update free space markers
        free_start += CELL_PTR_SIZE;
        self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].copy_from_slice(&(free_start as u16).to_le_bytes());
        self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].copy_from_slice(&(free_end as u16).to_le_bytes());

        // Update cell count
        let new_count = (count + 1) as u16;
        self.page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].copy_from_slice(&new_count.to_le_bytes());

        self.page.dirty = true;
    }

    /// Update the value at a given index (in place if same size, or rewrite).
    pub fn update(&mut self, index: usize, value: &[u8]) -> bool {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.page.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        let old_val_len = u16::from_le_bytes(
            self.page.data[cell_offset + 2..cell_offset + 4].try_into().unwrap()
        ) as usize;

        if value.len() == old_val_len {
            // In-place update
            self.page.data[cell_offset + 4 + key_len..cell_offset + 4 + key_len + value.len()]
                .copy_from_slice(value);
            self.page.dirty = true;
            true
        } else if value.len() < old_val_len {
            // Value shrunk - update in place, leave dead space
            self.page.data[cell_offset + 2..cell_offset + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
            self.page.data[cell_offset + 4 + key_len..cell_offset + 4 + key_len + value.len()]
                .copy_from_slice(value);
            self.page.dirty = true;
            true
        } else {
            // Value grew - need to check if we have space
            let extra_needed = value.len() - old_val_len;
            if self.free_space() >= extra_needed {
                // Allocate new cell at end, mark old one as dead
                // For simplicity, we reinsert (could optimize later with compaction)
                false
            } else {
                false
            }
        }
    }

    /// Delete the cell at a given index.
    pub fn delete(&mut self, index: usize) {
        let count = self.cell_count() as usize;
        assert!(index < count);

        // Shift cell pointers down
        if index < count - 1 {
            let src_start = LEAF_CELL_PTRS_OFFSET + (index + 1) * CELL_PTR_SIZE;
            let src_end = LEAF_CELL_PTRS_OFFSET + count * CELL_PTR_SIZE;
            let dst_start = LEAF_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
            self.page.data.copy_within(src_start..src_end, dst_start);
        }

        // Update free space start (one less pointer)
        let mut free_start = u16::from_le_bytes(
            self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        free_start -= CELL_PTR_SIZE;
        self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].copy_from_slice(&(free_start as u16).to_le_bytes());

        // Update cell count
        let new_count = (count - 1) as u16;
        self.page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].copy_from_slice(&new_count.to_le_bytes());

        // Note: Cell data becomes dead space (would need compaction to reclaim)
        self.page.dirty = true;
    }

    /// Get the underlying page.
    pub fn page(&self) -> &Page {
        self.page
    }

    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        self.page.id
    }
}

/// Read/write operations on a B-tree branch (internal) node.
pub struct BranchNode<'a> {
    page: &'a mut Page,
}

impl<'a> BranchNode<'a> {
    /// Wrap a page as a branch node.
    pub fn from_page(page: &'a mut Page) -> Result<Self> {
        let page_type = page.page_type()?;
        if page_type != PageType::BTreeBranch {
            return Err(Error::PageTypeMismatch {
                expected: PageType::BTreeBranch,
                found: page_type,
            });
        }
        Ok(Self { page })
    }

    /// Initialize an empty branch node.
    pub fn init(page: &'a mut Page, rightmost_child: PageId) -> Self {
        let page_size = page.size();

        // Set cell count to 0
        page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].copy_from_slice(&0u16.to_le_bytes());

        // Free space starts right after node header (including rightmost child)
        let free_start = BRANCH_CELL_PTRS_OFFSET as u16;
        page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].copy_from_slice(&free_start.to_le_bytes());

        // Free space ends at page end
        let free_end = page_size as u16;
        page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].copy_from_slice(&free_end.to_le_bytes());

        // Set rightmost child
        page.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 8]
            .copy_from_slice(&rightmost_child.to_le_bytes());

        page.dirty = true;
        Self { page }
    }

    /// Get the number of cells (separator keys) in this node.
    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].try_into().unwrap())
    }

    /// Get the rightmost child page ID.
    pub fn rightmost_child(&self) -> PageId {
        u64::from_le_bytes(
            self.page.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 8].try_into().unwrap()
        )
    }

    /// Set the rightmost child page ID.
    pub fn set_rightmost_child(&mut self, page_id: PageId) {
        self.page.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 8]
            .copy_from_slice(&page_id.to_le_bytes());
        self.page.dirty = true;
    }

    /// Get the free space available.
    pub fn free_space(&self) -> usize {
        let free_start = u16::from_le_bytes(
            self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let free_end = u16::from_le_bytes(
            self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        if free_end > free_start {
            free_end - free_start
        } else {
            0
        }
    }

    /// Get a cell pointer.
    fn cell_ptr(&self, index: usize) -> usize {
        let ptr_offset = BRANCH_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        u16::from_le_bytes(self.page.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize
    }

    /// Set a cell pointer.
    fn set_cell_ptr(&mut self, index: usize, offset: usize) {
        let ptr_offset = BRANCH_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        self.page.data[ptr_offset..ptr_offset + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        self.page.dirty = true;
    }

    /// Get the separator key at a given index.
    pub fn key(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.page.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        &self.page.data[cell_offset + 2 + 8..cell_offset + 2 + 8 + key_len]
    }

    /// Get the child page ID at a given index (left child of separator key).
    pub fn child(&self, index: usize) -> PageId {
        let cell_offset = self.cell_ptr(index);
        u64::from_le_bytes(
            self.page.data[cell_offset + 2..cell_offset + 2 + 8].try_into().unwrap()
        )
    }

    /// Set the child page ID at a given index.
    pub fn set_child(&mut self, index: usize, child: PageId) {
        let cell_offset = self.cell_ptr(index);
        self.page.data[cell_offset + 2..cell_offset + 2 + 8].copy_from_slice(&child.to_le_bytes());
        self.page.dirty = true;
    }

    /// Get the child page for a given key (navigating to the appropriate subtree).
    pub fn child_for_key(&self, key: &[u8]) -> PageId {
        let count = self.cell_count() as usize;

        // Binary search for the right child
        for i in 0..count {
            let sep_key = self.key(i);
            if key < sep_key {
                return self.child(i);
            }
        }

        // Key is >= all separators, go to rightmost child
        self.rightmost_child()
    }

    /// Calculate the space needed for a separator entry.
    pub fn cell_size(key: &[u8]) -> usize {
        CELL_PTR_SIZE + 2 + 8 + key.len() // ptr + key_len + child_page + key
    }

    /// Check if there's room for a new cell.
    pub fn can_insert(&self, key: &[u8]) -> bool {
        self.free_space() >= Self::cell_size(key)
    }

    /// Insert a separator key with its left child at the given index.
    pub fn insert(&mut self, index: usize, key: &[u8], left_child: PageId) {
        let count = self.cell_count() as usize;
        assert!(index <= count);

        // Calculate cell size (excluding pointer)
        let cell_data_size = 2 + 8 + key.len(); // key_len + child_page + key

        // Get current free space markers
        let mut free_start = u16::from_le_bytes(
            self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let mut free_end = u16::from_le_bytes(
            self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        // Allocate cell from end of free space
        free_end -= cell_data_size;
        let cell_offset = free_end;

        // Write cell data: key_len, child_page, key
        self.page.data[cell_offset..cell_offset + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        self.page.data[cell_offset + 2..cell_offset + 2 + 8].copy_from_slice(&left_child.to_le_bytes());
        self.page.data[cell_offset + 2 + 8..cell_offset + 2 + 8 + key.len()].copy_from_slice(key);

        // Shift cell pointers to make room
        if index < count {
            let src_start = BRANCH_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
            let src_end = BRANCH_CELL_PTRS_OFFSET + count * CELL_PTR_SIZE;
            let dst_start = src_start + CELL_PTR_SIZE;
            self.page.data.copy_within(src_start..src_end, dst_start);
        }

        // Write new cell pointer
        self.set_cell_ptr(index, cell_offset);

        // Update free space markers
        free_start += CELL_PTR_SIZE;
        self.page.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].copy_from_slice(&(free_start as u16).to_le_bytes());
        self.page.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].copy_from_slice(&(free_end as u16).to_le_bytes());

        // Update cell count
        let new_count = (count + 1) as u16;
        self.page.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].copy_from_slice(&new_count.to_le_bytes());

        self.page.dirty = true;
    }

    /// Get the underlying page.
    pub fn page(&self) -> &Page {
        self.page
    }

    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        self.page.id
    }
}

// ============================================================================
// Read-only node accessors (for cursor operations without mutable access)
// ============================================================================

/// Read-only accessor for leaf node data.
///
/// This provides read access to leaf node contents without requiring
/// mutable access to the page, suitable for cursor iteration.
pub struct LeafNodeRef<'a> {
    data: &'a [u8],
}

impl<'a> LeafNodeRef<'a> {
    /// Create a read-only view of a leaf node.
    pub fn from_page(page: &'a Page) -> Result<Self> {
        let page_type = page.page_type()?;
        if page_type != PageType::BTreeLeaf {
            return Err(Error::PageTypeMismatch {
                expected: PageType::BTreeLeaf,
                found: page_type,
            });
        }
        Ok(Self { data: &page.data })
    }

    /// Get the number of cells.
    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].try_into().unwrap())
    }

    /// Get a cell pointer.
    fn cell_ptr(&self, index: usize) -> usize {
        let ptr_offset = LEAF_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        u16::from_le_bytes(self.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize
    }

    /// Get the key at a given index.
    pub fn key(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        &self.data[cell_offset + 4..cell_offset + 4 + key_len]
    }

    /// Get the value at a given index.
    pub fn value(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        let val_len = u16::from_le_bytes(
            self.data[cell_offset + 2..cell_offset + 4].try_into().unwrap()
        ) as usize;
        &self.data[cell_offset + 4 + key_len..cell_offset + 4 + key_len + val_len]
    }

    /// Binary search for a key.
    pub fn search(&self, key: &[u8]) -> SearchResult {
        let count = self.cell_count() as usize;
        if count == 0 {
            return SearchResult::NotFound(0);
        }

        let mut lo = 0;
        let mut hi = count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key(mid);

            match key.cmp(mid_key) {
                std::cmp::Ordering::Equal => return SearchResult::Found(mid),
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }

        SearchResult::NotFound(lo)
    }

    /// Get the free space available for new cells.
    pub fn free_space(&self) -> usize {
        let free_start = u16::from_le_bytes(
            self.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let free_end = u16::from_le_bytes(
            self.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        if free_end > free_start {
            free_end - free_start
        } else {
            0
        }
    }

    /// Check if there's room for a new cell.
    pub fn can_insert(&self, key: &[u8], value: &[u8]) -> bool {
        // Cell size: ptr(2) + key_len(2) + val_len(2) + key + value
        let cell_size = CELL_PTR_SIZE + 2 + 2 + key.len() + value.len();
        self.free_space() >= cell_size
    }
}

/// Read-only accessor for branch node data.
pub struct BranchNodeRef<'a> {
    data: &'a [u8],
}

impl<'a> BranchNodeRef<'a> {
    /// Create a read-only view of a branch node.
    pub fn from_page(page: &'a Page) -> Result<Self> {
        let page_type = page.page_type()?;
        if page_type != PageType::BTreeBranch {
            return Err(Error::PageTypeMismatch {
                expected: PageType::BTreeBranch,
                found: page_type,
            });
        }
        Ok(Self { data: &page.data })
    }

    /// Get the number of cells.
    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.data[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2].try_into().unwrap())
    }

    /// Get the rightmost child.
    pub fn rightmost_child(&self) -> PageId {
        u64::from_le_bytes(
            self.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 8].try_into().unwrap()
        )
    }

    /// Get a cell pointer.
    fn cell_ptr(&self, index: usize) -> usize {
        let ptr_offset = BRANCH_CELL_PTRS_OFFSET + index * CELL_PTR_SIZE;
        u16::from_le_bytes(self.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize
    }

    /// Get the key at a given index.
    pub fn key(&self, index: usize) -> &[u8] {
        let cell_offset = self.cell_ptr(index);
        let key_len = u16::from_le_bytes(
            self.data[cell_offset..cell_offset + 2].try_into().unwrap()
        ) as usize;
        &self.data[cell_offset + 2 + 8..cell_offset + 2 + 8 + key_len]
    }

    /// Get the child page ID at a given index.
    pub fn child(&self, index: usize) -> PageId {
        let cell_offset = self.cell_ptr(index);
        u64::from_le_bytes(
            self.data[cell_offset + 2..cell_offset + 2 + 8].try_into().unwrap()
        )
    }

    /// Get the child page for a given key.
    pub fn child_for_key(&self, key: &[u8]) -> PageId {
        let count = self.cell_count() as usize;

        for i in 0..count {
            let sep_key = self.key(i);
            if key < sep_key {
                return self.child(i);
            }
        }

        self.rightmost_child()
    }

    /// Get the free space available.
    pub fn free_space(&self) -> usize {
        let free_start = u16::from_le_bytes(
            self.data[FREE_START_OFFSET..FREE_START_OFFSET + 2].try_into().unwrap()
        ) as usize;
        let free_end = u16::from_le_bytes(
            self.data[FREE_END_OFFSET..FREE_END_OFFSET + 2].try_into().unwrap()
        ) as usize;

        if free_end > free_start {
            free_end - free_start
        } else {
            0
        }
    }

    /// Check if there's room for a new cell.
    pub fn can_insert(&self, key: &[u8]) -> bool {
        // Cell size: ptr(2) + key_len(2) + child_page(8) + key
        let cell_size = CELL_PTR_SIZE + 2 + 8 + key.len();
        self.free_space() >= cell_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;

    fn make_leaf_page(id: PageId) -> Page {
        Page::new(id, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1)
    }

    fn make_branch_page(id: PageId) -> Page {
        Page::new(id, DEFAULT_PAGE_SIZE, PageType::BTreeBranch, 1)
    }

    #[test]
    fn test_leaf_node_basic() {
        let mut page = make_leaf_page(0);
        let mut node = LeafNode::init(&mut page);

        assert_eq!(node.cell_count(), 0);
        assert!(node.free_space() > 4000); // Most of 4KB should be free

        // Insert some data
        node.insert(0, b"key1", b"value1");
        assert_eq!(node.cell_count(), 1);
        assert_eq!(node.key(0), b"key1");
        assert_eq!(node.value(0), b"value1");

        // Insert more
        node.insert(1, b"key2", b"value2");
        node.insert(0, b"key0", b"value0"); // Insert at beginning

        assert_eq!(node.cell_count(), 3);
        assert_eq!(node.key(0), b"key0");
        assert_eq!(node.key(1), b"key1");
        assert_eq!(node.key(2), b"key2");
    }

    #[test]
    fn test_leaf_node_search() {
        let mut page = make_leaf_page(0);
        let mut node = LeafNode::init(&mut page);

        node.insert(0, b"apple", b"1");
        node.insert(1, b"banana", b"2");
        node.insert(2, b"cherry", b"3");

        assert!(matches!(node.search(b"apple"), SearchResult::Found(0)));
        assert!(matches!(node.search(b"banana"), SearchResult::Found(1)));
        assert!(matches!(node.search(b"cherry"), SearchResult::Found(2)));
        assert!(matches!(node.search(b"aardvark"), SearchResult::NotFound(0)));
        assert!(matches!(node.search(b"blueberry"), SearchResult::NotFound(2)));
        assert!(matches!(node.search(b"zebra"), SearchResult::NotFound(3)));
    }

    #[test]
    fn test_leaf_node_delete() {
        let mut page = make_leaf_page(0);
        let mut node = LeafNode::init(&mut page);

        node.insert(0, b"a", b"1");
        node.insert(1, b"b", b"2");
        node.insert(2, b"c", b"3");

        node.delete(1); // Delete middle

        assert_eq!(node.cell_count(), 2);
        assert_eq!(node.key(0), b"a");
        assert_eq!(node.key(1), b"c");
    }

    #[test]
    fn test_branch_node_basic() {
        let mut page = make_branch_page(0);
        let mut node = BranchNode::init(&mut page, 100);

        assert_eq!(node.cell_count(), 0);
        assert_eq!(node.rightmost_child(), 100);

        // Insert separator: keys < "m" go to child 10
        node.insert(0, b"m", 10);
        assert_eq!(node.cell_count(), 1);
        assert_eq!(node.key(0), b"m");
        assert_eq!(node.child(0), 10);

        // Navigate
        assert_eq!(node.child_for_key(b"apple"), 10); // < "m"
        assert_eq!(node.child_for_key(b"zebra"), 100); // >= "m"
    }
}
