//! Node splitting, merging, and fill factor analysis for the B+ tree.
//!
//! # Algorithm Overview
//!
//! ## Splitting (insertion path)
//!
//! When a leaf or branch node cannot accommodate a new entry, it is split:
//!
//! 1. **Leaf split** (`split_leaf_for_key`): A new page is allocated. Entries are distributed so
//!    the new key lands in the correct half. The first key of the right (new) leaf becomes the
//!    separator promoted to the parent branch.
//!
//! 2. **Branch split** (`split_branch`): Similar to leaf split, but the median separator key is
//!    promoted to the parent rather than copied — branches store separators, not data, so the
//!    promoted key is removed from the split node.
//!
//! Splits propagate upward: if the parent branch is also full, it splits too,
//! potentially growing the tree height by one level (when the root splits).
//!
//! ## Merging (compaction path)
//!
//! After deletions, leaf nodes may become underfull. `merge_leaves` combines two
//! adjacent siblings:
//!
//! 1. All live entries from both leaves are collected.
//! 2. The left page is rebuilt from scratch via `LeafNode::init()`, reclaiming dead space left by
//!    prior deletes.
//! 3. All entries are inserted into the rebuilt left page.
//! 4. The right page is freed.
//!
//! The rebuild-from-scratch approach is deliberate: after deletes, leaf pages
//! contain dead cell data that inflates their physical size. Rebuilding ensures
//! the merged page has zero fragmentation.
//!
//! ## Fill Factor Analysis
//!
//! `leaf_fill_factor` computes the ratio of live data to page capacity by
//! iterating live cells — not by inferring from free space. This distinction
//! matters because `free_space()` (the gap between `free_start` and `free_end`)
//! does not account for dead cell data left by deletions.

use super::node::{BranchNode, BranchNodeRef, LeafNode, LeafNodeRef};
use crate::{
    bloom::BLOOM_FILTER_SIZE,
    error::{Error, PageId, Result},
    page::Page,
};

/// Result of splitting a leaf node.
#[derive(Debug)]
pub struct LeafSplitResult {
    /// The page ID of the new (right) leaf.
    pub new_page_id: PageId,
    /// The separator key to promote to the parent.
    /// This is the first key of the new (right) leaf.
    pub separator_key: Vec<u8>,
}

/// Result of splitting a branch node.
#[derive(Debug)]
pub struct BranchSplitResult {
    /// The page ID of the new (right) branch.
    pub new_page_id: PageId,
    /// The separator key to promote to the parent.
    pub separator_key: Vec<u8>,
}

/// Splits a leaf node, moving roughly half the entries to the new node.
///
/// # Arguments
/// * `original` - The original leaf page (will be modified to contain left half)
/// * `new_page` - A new empty page to use for the right half
///
/// # Returns
/// The separator key (first key of new node) and page ID of new node.
///
/// # Note
/// If the original node is empty, both pages are reinitialized and an
/// empty separator key (`Vec::new()`) is returned. The caller places the
/// new key into either page based on comparison. This handles edge cases
/// where large values exhaust page space.
///
/// # Errors
///
/// Returns an error if the page data is corrupted or the page type is invalid.
pub fn split_leaf(original: &mut Page, new_page: &mut Page) -> Result<LeafSplitResult> {
    // Collect all entries from original
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let node = LeafNode::from_page(original)?;
        let count = node.cell_count() as usize;
        for i in 0..count {
            let (k, v) = node.get(i);
            entries.push((k.to_vec(), v.to_vec()));
        }
    }

    // Handle edge case: empty or single-entry leaf
    // This can happen when updating a large value that doesn't fit even after deletion
    if entries.is_empty() {
        // Re-initialize both pages as empty leaves
        LeafNode::init(original);
        LeafNode::init(new_page);
        // Return an empty separator key - the caller will insert the new key
        // into either the left or right page based on comparison
        return Ok(LeafSplitResult { new_page_id: new_page.id, separator_key: Vec::new() });
    }

    // Calculate split point (middle)
    let split_at = entries.len() / 2;
    // When there's only 1 entry, split_at=0, so we need at least index 0 to exist
    let separator_key = entries[split_at].0.clone();

    // Re-initialize original as empty leaf
    LeafNode::init(original);

    // Re-initialize new page as empty leaf
    LeafNode::init(new_page);

    // Insert left half into original
    {
        let mut left = LeafNode::from_page(original)?;
        for (i, (key, value)) in entries[..split_at].iter().enumerate() {
            left.insert(i, key, value);
        }
    }

    // Insert right half into new page
    {
        let mut right = LeafNode::from_page(new_page)?;
        for (i, (key, value)) in entries[split_at..].iter().enumerate() {
            right.insert(i, key, value);
        }
    }

    Ok(LeafSplitResult { new_page_id: new_page.id, separator_key })
}

/// Splits a leaf node with awareness of the key being inserted.
///
/// This finds a split point that ensures the new key can fit in the correct
/// side based on B-tree ordering (key < separator goes left, key >= separator goes right).
///
/// # Arguments
/// * `original` - The original leaf page (will be modified to contain left half)
/// * `new_page` - A new empty page to use for the right half
/// * `new_key` - The key that will be inserted after the split
/// * `new_value` - The value that will be inserted after the split
///
/// # Returns
/// The separator key (first key of new node) and page ID of new node.
///
/// # Errors
///
/// Returns [`Error::PageFull`] if no valid split point can be found.
/// Returns an error if the page data is corrupted.
pub fn split_leaf_for_key(
    original: &mut Page,
    new_page: &mut Page,
    new_key: &[u8],
    new_value: &[u8],
) -> Result<LeafSplitResult> {
    // Collect all entries from original
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let node = LeafNode::from_page(original)?;
        let count = node.cell_count() as usize;
        for i in 0..count {
            let (k, v) = node.get(i);
            entries.push((k.to_vec(), v.to_vec()));
        }
    }

    // Handle edge case: empty leaf
    if entries.is_empty() {
        LeafNode::init(original);
        LeafNode::init(new_page);
        return Ok(LeafSplitResult { new_page_id: new_page.id, separator_key: Vec::new() });
    }

    // Helper function to test if a split point works
    let try_split = |split_at: usize,
                     separator_key: &[u8],
                     entries: &[(Vec<u8>, Vec<u8>)],
                     original: &mut Page,
                     new_page: &mut Page,
                     new_key: &[u8],
                     new_value: &[u8]|
     -> Result<bool> {
        let new_key_goes_left = new_key < separator_key;

        // Re-initialize pages
        LeafNode::init(original);
        LeafNode::init(new_page);

        // Try to insert left half (entries[..split_at])
        {
            let mut left = LeafNode::from_page(original)?;
            for (i, (key, value)) in entries[..split_at].iter().enumerate() {
                if !left.can_insert(key, value) {
                    return Ok(false);
                }
                left.insert(i, key, value);
            }
            // Check if new key would fit in left (if it goes there)
            if new_key_goes_left && !left.can_insert(new_key, new_value) {
                return Ok(false);
            }
        }

        // Try to insert right half (entries[split_at..])
        {
            let mut right = LeafNode::from_page(new_page)?;
            for (i, (key, value)) in entries[split_at..].iter().enumerate() {
                if !right.can_insert(key, value) {
                    return Ok(false);
                }
                right.insert(i, key, value);
            }
            // Check if new key would fit in right (if it goes there)
            if !new_key_goes_left && !right.can_insert(new_key, new_value) {
                return Ok(false);
            }
        }

        Ok(true)
    };

    // Try different split points to find one where the new key fits on the correct side.
    // Start from middle and work outward to prefer balanced splits.
    let mid = entries.len() / 2;
    let max_offset = entries.len();

    for offset in 0..=max_offset {
        // Try split points at mid-offset and mid+offset
        let candidates = [mid.saturating_sub(offset), (mid + offset).min(entries.len())];

        for &split_at in &candidates {
            // Determine separator key based on split point
            let separator_key: Vec<u8> = if split_at == 0 {
                // All existing entries go right; separator is first existing key
                // new_key must be < separator to go left (into empty left page)
                if new_key >= entries[0].0.as_slice() {
                    // new_key would go right, not useful for this case
                    continue;
                }
                entries[0].0.clone()
            } else if split_at >= entries.len() {
                // All existing entries go left; new_key goes right
                // separator is new_key itself (keys >= new_key go right)
                if new_key <= entries[entries.len() - 1].0.as_slice() {
                    // new_key would go left, not useful for this case
                    continue;
                }
                new_key.to_vec()
            } else {
                // Normal case: separator is first key of right half
                entries[split_at].0.clone()
            };

            if try_split(
                split_at,
                &separator_key,
                &entries,
                original,
                new_page,
                new_key,
                new_value,
            )? {
                return Ok(LeafSplitResult { new_page_id: new_page.id, separator_key });
            }
        }
    }

    // If we couldn't find a valid split point, return an error
    // This can happen with very large values that don't fit even in an empty page
    Err(Error::PageFull)
}

/// Splits a branch node, moving roughly half the entries to the new node.
///
/// Branch splits are different from leaf splits:
/// - The middle key is promoted (not copied) to the parent
/// - Children pointers must be properly distributed
///
/// # Arguments
/// * `original` - The original branch page (will be modified)
/// * `new_page` - A new empty page to use for the right half
///
/// # Returns
/// The separator key (middle key, removed from both children) and new page ID.
///
/// # Errors
///
/// Returns an error if the page data is corrupted or the page type is invalid.
pub fn split_branch(original: &mut Page, new_page: &mut Page) -> Result<BranchSplitResult> {
    // Collect all entries from original: (key, left_child)
    let mut entries: Vec<(Vec<u8>, PageId)> = Vec::new();
    let rightmost_child: PageId;

    {
        let node = BranchNode::from_page(original)?;
        let count = node.cell_count() as usize;
        for i in 0..count {
            entries.push((node.key(i).to_vec(), node.child(i)));
        }
        rightmost_child = node.rightmost_child();
    }

    // Calculate split point
    // For branch nodes, the middle key is promoted (not kept in either child)
    let split_at = entries.len() / 2;
    let separator_key = entries[split_at].0.clone();

    // Left half: entries[0..split_at], rightmost = left child of promoted key
    // Right half: entries[split_at+1..], rightmost = original rightmost

    // Re-initialize original with left half
    let left_rightmost = entries[split_at].1; // The left child of promoted key becomes rightmost of left node
    {
        let mut left = BranchNode::init(original, left_rightmost);
        for (i, (key, child)) in entries[..split_at].iter().enumerate() {
            left.insert(i, key, *child);
        }
    }

    // Initialize new page with right half
    {
        let mut right = BranchNode::init(new_page, rightmost_child);
        for (i, (key, child)) in entries[split_at + 1..].iter().enumerate() {
            right.insert(i, key, *child);
        }
    }

    Ok(BranchSplitResult { new_page_id: new_page.id, separator_key })
}

/// Checks if a leaf node needs splitting based on remaining free space.
///
/// # Errors
///
/// Returns an error if the page type is not a B-tree leaf.
pub fn leaf_needs_split(page: &Page, key: &[u8], value: &[u8]) -> Result<bool> {
    let node = LeafNodeRef::from_page(page)?;
    Ok(!node.can_insert(key, value))
}

/// Checks if a branch node needs splitting.
///
/// # Errors
///
/// Returns an error if the page type is not a B-tree branch.
pub fn branch_needs_split(page: &Page, key: &[u8]) -> Result<bool> {
    let node = BranchNodeRef::from_page(page)?;
    Ok(!node.can_insert(key))
}

/// Merges two leaf nodes by moving all entries from `right` into `left`.
///
/// Rebuilds `left` from scratch to reclaim dead space left by prior deletes.
/// After this call, `right` is empty and should be freed by the caller.
///
/// # Errors
///
/// Returns an error if the page type is not a B-tree leaf or the page data is corrupted.
pub fn merge_leaves(left: &mut Page, right: &mut Page) -> Result<()> {
    // Collect all live entries from both pages.
    // By rebuilding from scratch, we reclaim dead space left by prior deletes.
    let mut all_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    {
        let node = LeafNode::from_page(left)?;
        let count = node.cell_count() as usize;
        for i in 0..count {
            let (k, v) = node.get(i);
            all_entries.push((k.to_vec(), v.to_vec()));
        }
    }

    {
        let node = LeafNode::from_page(right)?;
        let count = node.cell_count() as usize;
        for i in 0..count {
            let (k, v) = node.get(i);
            all_entries.push((k.to_vec(), v.to_vec()));
        }
    }

    // Re-initialize left page fresh (reclaims all dead space)
    {
        let mut left_node = LeafNode::init(left);
        for (i, (key, value)) in all_entries.iter().enumerate() {
            left_node.insert(i, key, value);
        }
    }

    // Clear right (will be freed by caller)
    LeafNode::init(right);

    Ok(())
}

/// Calculates the fill factor of a leaf node (0.0 to 1.0).
///
/// # Errors
///
/// Returns an error if the page type is not a B-tree leaf.
pub fn leaf_fill_factor(page: &Page) -> Result<f64> {
    let node = LeafNodeRef::from_page(page)?;
    let count = node.cell_count() as usize;

    if count == 0 {
        return Ok(0.0);
    }

    // Calculate actual live data: cell pointers + cell data for each entry.
    // This correctly ignores dead space left by deleted entries.
    let mut live_bytes = 0usize;
    for i in 0..count {
        let key = node.key(i);
        let value = node.value(i);
        // cell_ptr(2) + key_len(2) + val_len(2) + key + value
        live_bytes += 2 + 2 + 2 + key.len() + value.len();
    }

    // Total usable space = page size - page header - node header (cell_count + free_start +
    // free_end = 6 bytes) - bloom filter region
    let total_content = page.size() - crate::page::PAGE_HEADER_SIZE - 6 - BLOOM_FILTER_SIZE;

    Ok(live_bytes as f64 / total_content as f64)
}

/// Checks whether two leaf nodes can be merged into a single page.
///
/// Returns `true` if the total live data from both leaves fits within one page's
/// usable space, ignoring dead cells (since merging rebuilds the page from scratch).
///
/// # Errors
///
/// Returns an error if either page type is not a B-tree leaf.
pub fn can_merge_leaves(left: &Page, right: &Page) -> Result<bool> {
    let left_node = LeafNodeRef::from_page(left)?;
    let right_node = LeafNodeRef::from_page(right)?;

    let right_count = right_node.cell_count() as usize;
    if right_count == 0 {
        return Ok(true);
    }

    let left_count = left_node.cell_count() as usize;

    // Calculate total live data size for both leaves (ignoring dead space).
    // After merge, all entries will be compacted into a fresh page layout.
    let mut total_live = 0usize;

    for i in 0..left_count {
        let key = left_node.key(i);
        let value = left_node.value(i);
        // cell_ptr(2) + key_len(2) + val_len(2) + key + value
        total_live += 2 + 2 + 2 + key.len() + value.len();
    }

    for i in 0..right_count {
        let key = right_node.key(i);
        let value = right_node.value(i);
        total_live += 2 + 2 + 2 + key.len() + value.len();
    }

    // Total usable space in one page (page_header + node_header + bloom_filter overhead)
    let total_content = left.size() - crate::page::PAGE_HEADER_SIZE - 6 - BLOOM_FILTER_SIZE;

    Ok(total_live <= total_content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::DEFAULT_PAGE_SIZE, error::PageType};

    fn make_leaf_page(id: PageId) -> Page {
        Page::new(id, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1)
    }

    fn make_branch_page(id: PageId) -> Page {
        Page::new(id, DEFAULT_PAGE_SIZE, PageType::BTreeBranch, 1)
    }

    #[test]
    fn test_leaf_split() {
        let mut original = make_leaf_page(0);
        let mut new_page = make_leaf_page(1);

        // Initialize and fill original
        {
            let mut node = LeafNode::init(&mut original);
            for i in 0..10u8 {
                let key = vec![i];
                let value = vec![i; 100];
                node.insert(i as usize, &key, &value);
            }
            assert_eq!(node.cell_count(), 10);
        }

        // Split
        let result = split_leaf(&mut original, &mut new_page).unwrap();

        // Check results
        {
            let left = LeafNode::from_page(&mut original).unwrap();
            let right = LeafNode::from_page(&mut new_page).unwrap();

            assert_eq!(left.cell_count(), 5);
            assert_eq!(right.cell_count(), 5);

            // Verify separator key is first key of right node
            assert_eq!(result.separator_key, right.key(0));

            // Verify ordering is maintained
            assert!(left.key(4) < right.key(0));
        }
    }

    #[test]
    fn test_branch_split() {
        let mut original = make_branch_page(0);
        let mut new_page = make_branch_page(1);

        // Initialize and fill original
        {
            let mut node = BranchNode::init(&mut original, 100);
            for i in 0..9u8 {
                let key = vec![i * 10]; // Keys: 0, 10, 20, ..., 80
                node.insert(i as usize, &key, i as PageId);
            }
            assert_eq!(node.cell_count(), 9);
        }

        // Split
        let result = split_branch(&mut original, &mut new_page).unwrap();

        // Check results
        {
            let left = BranchNode::from_page(&mut original).unwrap();
            let right = BranchNode::from_page(&mut new_page).unwrap();

            // 9 keys split: 4 left, 1 promoted, 4 right
            assert_eq!(left.cell_count(), 4);
            assert_eq!(right.cell_count(), 4);

            // Middle key should be promoted
            assert_eq!(result.separator_key, vec![40]);
        }
    }

    #[test]
    fn test_merge_leaves() {
        let mut left = make_leaf_page(0);
        let mut right = make_leaf_page(1);

        // Add entries to both
        {
            let mut l = LeafNode::init(&mut left);
            l.insert(0, b"a", b"1");
            l.insert(1, b"b", b"2");
        }
        {
            let mut r = LeafNode::init(&mut right);
            r.insert(0, b"c", b"3");
            r.insert(1, b"d", b"4");
        }

        // Merge right into left
        merge_leaves(&mut left, &mut right).unwrap();

        // Check merged result
        {
            let merged = LeafNode::from_page(&mut left).unwrap();
            assert_eq!(merged.cell_count(), 4);
            assert_eq!(merged.key(0), b"a");
            assert_eq!(merged.key(3), b"d");
        }

        // Right should be empty
        {
            let empty = LeafNode::from_page(&mut right).unwrap();
            assert_eq!(empty.cell_count(), 0);
        }
    }

    #[test]
    fn test_can_merge_leaves_both_empty() {
        let mut left = Page::new(1, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        let mut right = Page::new(2, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        // init() writes leaf header fields (cell_count, free_start, free_end)
        LeafNode::init(&mut left);
        LeafNode::init(&mut right);

        assert!(can_merge_leaves(&left, &right).unwrap());
    }

    #[test]
    fn test_can_merge_leaves_right_empty() {
        let mut left = Page::new(1, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        let mut right = Page::new(2, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        {
            let mut l = LeafNode::init(&mut left);
            l.insert(0, b"a", b"1");
        }
        // init() writes the leaf header so free_end is valid
        LeafNode::init(&mut right);

        assert!(can_merge_leaves(&left, &right).unwrap());
    }

    #[test]
    fn test_can_merge_leaves_small_entries() {
        let mut left = Page::new(1, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);
        let mut right = Page::new(2, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);

        {
            let mut l = LeafNode::init(&mut left);
            l.insert(0, b"a", b"1");
            l.insert(1, b"b", b"2");
        }
        {
            let mut r = LeafNode::init(&mut right);
            r.insert(0, b"c", b"3");
        }

        // Small entries should easily fit
        assert!(can_merge_leaves(&left, &right).unwrap());
    }
}
