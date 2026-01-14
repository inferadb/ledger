//! Node splitting logic for B-tree insertions.
//!
//! When a node becomes full, it must be split into two nodes.
//! The middle key is promoted to the parent, and entries are
//! distributed between the original and new node.

use crate::error::{PageId, Result};
use crate::page::Page;
use super::node::{LeafNode, BranchNode, LeafNodeRef, BranchNodeRef};

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

/// Split a leaf node, moving roughly half the entries to the new node.
///
/// # Arguments
/// * `original` - The original leaf page (will be modified to contain left half)
/// * `new_page` - A new empty page to use for the right half
///
/// # Returns
/// The separator key (first key of new node) and page ID of new node.
///
/// # Note
/// If the original node is empty or has only 1 entry, this creates an
/// empty left node and returns a synthetic separator key. This handles
/// edge cases where large values don't fit even in an empty page.
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
        return Ok(LeafSplitResult {
            new_page_id: new_page.id,
            separator_key: Vec::new(),
        });
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

    Ok(LeafSplitResult {
        new_page_id: new_page.id,
        separator_key,
    })
}

/// Split a branch node, moving roughly half the entries to the new node.
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

    Ok(BranchSplitResult {
        new_page_id: new_page.id,
        separator_key,
    })
}

/// Check if a leaf node needs splitting based on remaining free space.
pub fn leaf_needs_split(page: &Page, key: &[u8], value: &[u8]) -> Result<bool> {
    let node = LeafNodeRef::from_page(page)?;
    Ok(!node.can_insert(key, value))
}

/// Check if a branch node needs splitting.
pub fn branch_needs_split(page: &Page, key: &[u8]) -> Result<bool> {
    let node = BranchNodeRef::from_page(page)?;
    Ok(!node.can_insert(key))
}

/// Merge two leaf nodes (for deletion, when underfull).
///
/// All entries from `right` are moved into `left`.
/// Returns the key that was separating them in the parent.
pub fn merge_leaves(left: &mut Page, right: &mut Page) -> Result<()> {
    // Collect entries from right
    let right_entries: Vec<(Vec<u8>, Vec<u8>)> = {
        let node = LeafNode::from_page(right)?;
        let count = node.cell_count() as usize;
        (0..count)
            .map(|i| {
                let (k, v) = node.get(i);
                (k.to_vec(), v.to_vec())
            })
            .collect()
    };

    // Append to left
    {
        let mut left_node = LeafNode::from_page(left)?;
        let start_index = left_node.cell_count() as usize;
        for (i, (key, value)) in right_entries.iter().enumerate() {
            left_node.insert(start_index + i, key, value);
        }
    }

    // Clear right (will be freed by caller)
    LeafNode::init(right);

    Ok(())
}

/// Calculate the fill factor of a leaf node (0.0 to 1.0).
pub fn leaf_fill_factor(page: &Page) -> Result<f64> {
    let node = LeafNodeRef::from_page(page)?;

    let total_content = page.size() - crate::page::PAGE_HEADER_SIZE - 8; // Rough usable space
    let free = node.free_space();
    let used = total_content.saturating_sub(free);

    Ok(used as f64 / total_content as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;
    use crate::error::PageType;

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
}
