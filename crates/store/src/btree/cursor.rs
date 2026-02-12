//! B-tree cursor for iteration and range scans.
//!
//! The cursor provides efficient iteration over key-value pairs in sorted order.
//! It maintains a position within the B-tree and supports:
//! - Forward iteration (next)
//! - Seeking to a specific key
//! - Range queries

use super::node::SearchResult;
use crate::{
    error::{PageId, Result},
    page::Page,
};

/// Stack entry for tracking position during tree traversal.
#[derive(Debug, Clone)]
pub struct StackEntry {
    /// Page ID of the node.
    pub page_id: PageId,
    /// Current cell index within the node.
    pub cell_index: usize,
}

/// Cursor position within a B-tree.
///
/// The cursor maintains a stack of (page_id, cell_index) pairs representing
/// the path from root to the current leaf position.
#[derive(Debug, Clone)]
pub struct CursorPosition {
    /// Stack of branch node positions (root at index 0).
    pub stack: Vec<StackEntry>,
    /// Current leaf page ID (if positioned on a leaf).
    pub leaf_page_id: Option<PageId>,
    /// Current index within the leaf.
    pub leaf_index: usize,
    /// Whether cursor is positioned at a valid entry.
    pub valid: bool,
}

impl CursorPosition {
    /// Creates a new empty cursor position.
    pub fn new() -> Self {
        Self { stack: Vec::new(), leaf_page_id: None, leaf_index: 0, valid: false }
    }

    /// Creates a cursor positioned at the start of the tree.
    pub fn at_start() -> Self {
        Self { stack: Vec::new(), leaf_page_id: None, leaf_index: 0, valid: true }
    }

    /// Checks if cursor is valid (positioned on an entry).
    pub fn is_valid(&self) -> bool {
        self.valid && self.leaf_page_id.is_some()
    }

    /// Invalidates the cursor.
    pub fn invalidate(&mut self) {
        self.valid = false;
        self.leaf_page_id = None;
    }
}

impl Default for CursorPosition {
    fn default() -> Self {
        Self::new()
    }
}

/// Range bound specification.
#[derive(Debug, Clone)]
pub enum Bound<'a> {
    /// No bound (unbounded).
    Unbounded,
    /// Inclusive bound.
    Included(&'a [u8]),
    /// Exclusive bound.
    Excluded(&'a [u8]),
}

/// A range query specification.
#[derive(Debug, Clone)]
pub struct Range<'a> {
    /// Lower bound.
    pub start: Bound<'a>,
    /// Upper bound.
    pub end: Bound<'a>,
}

impl<'a> Range<'a> {
    /// Creates a range covering all keys.
    pub fn all() -> Self {
        Self { start: Bound::Unbounded, end: Bound::Unbounded }
    }

    /// Creates a range from start (inclusive) to end (exclusive).
    pub fn new(start: &'a [u8], end: &'a [u8]) -> Self {
        Self { start: Bound::Included(start), end: Bound::Excluded(end) }
    }

    /// Creates a range starting from a key (inclusive).
    pub fn from(start: &'a [u8]) -> Self {
        Self { start: Bound::Included(start), end: Bound::Unbounded }
    }

    /// Creates a range starting from `prefix` (inclusive) with unbounded end.
    /// Note: includes all keys >= prefix, not just those sharing the prefix.
    pub fn prefix(prefix: &'a [u8]) -> Self {
        Self { start: Bound::Included(prefix), end: Bound::Unbounded }
    }

    /// Checks if a key is within this range.
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(start) => key >= *start,
            Bound::Excluded(start) => key > *start,
        };

        let before_end = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= *end,
            Bound::Excluded(end) => key < *end,
        };

        after_start && before_end
    }

    /// Checks if iteration should continue based on current key.
    pub fn should_continue(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= *end,
            Bound::Excluded(end) => key < *end,
        }
    }
}

/// Iterator state for a B-tree range scan.
pub struct RangeIterState<'a> {
    /// The range to iterate over.
    pub range: Range<'a>,
    /// Current position.
    pub position: CursorPosition,
    /// Whether we've started iteration.
    pub started: bool,
}

impl<'a> RangeIterState<'a> {
    /// Creates a new range iterator state.
    pub fn new(range: Range<'a>) -> Self {
        Self { range, position: CursorPosition::new(), started: false }
    }

    /// Creates an iterator over all entries.
    pub fn all() -> Self {
        Self::new(Range::all())
    }

    /// Creates an iterator starting from a specific key.
    pub fn from_key(key: &'a [u8]) -> Self {
        Self::new(Range::from(key))
    }
}

/// Result of a cursor seek operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekResult {
    /// Found the exact key.
    Found,
    /// Key not found, positioned at next greater key.
    NotFound,
    /// No more entries (past end of tree).
    EndOfTree,
}

/// Functions for manipulating cursor positions.
///
/// These are stateless helper functions that operate on cursor positions
/// and page data provided externally. The actual B-tree struct will use
/// these to implement cursor operations.
pub mod cursor_ops {
    use super::{super::node::LeafNodeRef, *};

    /// Moves to the first entry in a leaf node.
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn move_to_first_in_leaf(position: &mut CursorPosition, page: &Page) -> Result<bool> {
        let leaf = LeafNodeRef::from_page(page)?;

        if leaf.cell_count() == 0 {
            position.valid = false;
            return Ok(false);
        }

        position.leaf_page_id = Some(page.id);
        position.leaf_index = 0;
        position.valid = true;
        Ok(true)
    }

    /// Moves to the last entry in a leaf node.
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn move_to_last_in_leaf(position: &mut CursorPosition, page: &Page) -> Result<bool> {
        let leaf = LeafNodeRef::from_page(page)?;

        let count = leaf.cell_count();
        if count == 0 {
            position.valid = false;
            return Ok(false);
        }

        position.leaf_page_id = Some(page.id);
        position.leaf_index = count as usize - 1;
        position.valid = true;
        Ok(true)
    }

    /// Seeks to a key within a leaf node.
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn seek_in_leaf(
        position: &mut CursorPosition,
        page: &Page,
        key: &[u8],
    ) -> Result<SeekResult> {
        let leaf = LeafNodeRef::from_page(page)?;

        let count = leaf.cell_count();
        if count == 0 {
            position.valid = false;
            return Ok(SeekResult::EndOfTree);
        }

        position.leaf_page_id = Some(page.id);

        match leaf.search(key) {
            SearchResult::Found(idx) => {
                position.leaf_index = idx;
                position.valid = true;
                Ok(SeekResult::Found)
            },
            SearchResult::NotFound(idx) => {
                if idx >= count as usize {
                    position.valid = false;
                    Ok(SeekResult::EndOfTree)
                } else {
                    position.leaf_index = idx;
                    position.valid = true;
                    Ok(SeekResult::NotFound)
                }
            },
        }
    }

    /// Advances to next entry within current leaf.
    ///
    /// Returns true if there's a next entry in this leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn next_in_leaf(position: &mut CursorPosition, page: &Page) -> Result<bool> {
        if !position.valid || position.leaf_page_id != Some(page.id) {
            return Ok(false);
        }

        let leaf = LeafNodeRef::from_page(page)?;

        let count = leaf.cell_count() as usize;
        if position.leaf_index + 1 < count {
            position.leaf_index += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns key at current cursor position (returns owned copy).
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn current_key(position: &CursorPosition, page: &Page) -> Result<Option<Vec<u8>>> {
        if !position.valid || position.leaf_page_id != Some(page.id) {
            return Ok(None);
        }

        let leaf = LeafNodeRef::from_page(page)?;

        if position.leaf_index < leaf.cell_count() as usize {
            Ok(Some(leaf.key(position.leaf_index).to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Returns value at current cursor position (returns owned copy).
    ///
    /// # Errors
    ///
    /// Returns an error if the page type is not a B-tree leaf.
    pub fn current_value(position: &CursorPosition, page: &Page) -> Result<Option<Vec<u8>>> {
        if !position.valid || position.leaf_page_id != Some(page.id) {
            return Ok(None);
        }

        let leaf = LeafNodeRef::from_page(page)?;

        if position.leaf_index < leaf.cell_count() as usize {
            Ok(Some(leaf.value(position.leaf_index).to_vec()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_contains() {
        let range = Range::new(b"apple", b"cherry");

        assert!(!range.contains(b"aardvark"));
        assert!(range.contains(b"apple"));
        assert!(range.contains(b"banana"));
        assert!(!range.contains(b"cherry")); // Exclusive end
        assert!(!range.contains(b"zebra"));
    }

    #[test]
    fn test_range_all() {
        let range = Range::all();

        assert!(range.contains(b""));
        assert!(range.contains(b"anything"));
        assert!(range.contains(b"\xff\xff\xff"));
    }

    #[test]
    fn test_cursor_position() {
        let mut pos = CursorPosition::new();
        assert!(!pos.is_valid());

        pos.leaf_page_id = Some(42);
        pos.valid = true;
        assert!(pos.is_valid());

        pos.invalidate();
        assert!(!pos.is_valid());
    }
}
