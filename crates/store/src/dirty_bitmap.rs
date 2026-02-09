//! Dirty page bitmap for incremental backup tracking.
//!
//! Tracks which pages have been modified since the last backup checkpoint.
//! Uses a `Vec<u64>` as a compact bitset where each bit corresponds to a page ID.
//! The bitmap grows dynamically as higher page IDs are marked dirty.

use crate::error::PageId;

/// Number of bits per word in the backing store.
const BITS_PER_WORD: usize = 64;

/// Bitmap tracking pages modified since the last backup.
///
/// Each bit at position `page_id` indicates whether that page has been
/// modified. The bitmap auto-extends when marking pages beyond its current
/// capacity.
#[derive(Debug, Clone, Default)]
pub struct DirtyBitmap {
    /// Backing store: each u64 tracks 64 page IDs.
    words: Vec<u64>,
}

impl DirtyBitmap {
    /// Create an empty bitmap.
    pub fn new() -> Self {
        Self { words: Vec::new() }
    }

    /// Mark a page as dirty.
    pub fn mark(&mut self, page_id: PageId) {
        let word_idx = page_id as usize / BITS_PER_WORD;
        let bit_idx = page_id as usize % BITS_PER_WORD;

        if word_idx >= self.words.len() {
            self.words.resize(word_idx + 1, 0);
        }

        self.words[word_idx] |= 1u64 << bit_idx;
    }

    /// Check if a page is marked dirty.
    pub fn is_dirty(&self, page_id: PageId) -> bool {
        let word_idx = page_id as usize / BITS_PER_WORD;
        let bit_idx = page_id as usize % BITS_PER_WORD;

        if word_idx >= self.words.len() {
            return false;
        }

        (self.words[word_idx] & (1u64 << bit_idx)) != 0
    }

    /// Return all dirty page IDs in ascending order.
    pub fn dirty_ids(&self) -> Vec<PageId> {
        let mut ids = Vec::new();
        for (word_idx, &word) in self.words.iter().enumerate() {
            if word == 0 {
                continue;
            }
            for bit in 0..BITS_PER_WORD {
                if (word & (1u64 << bit)) != 0 {
                    ids.push((word_idx * BITS_PER_WORD + bit) as PageId);
                }
            }
        }
        ids
    }

    /// Number of dirty pages.
    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Clear all dirty flags.
    pub fn clear(&mut self) {
        self.words.clear();
    }

    /// Serialize the bitmap to bytes for persistence.
    ///
    /// Format: `[word_count: u64][word_0: u64][word_1: u64]...`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.words.len() * 8);
        buf.extend_from_slice(&(self.words.len() as u64).to_le_bytes());
        for &word in &self.words {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf
    }

    /// Deserialize a bitmap from bytes.
    ///
    /// Returns `None` if the data is malformed (truncated or invalid length).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let word_count = u64::from_le_bytes(data[..8].try_into().ok()?) as usize;
        let expected_len = 8 + word_count * 8;

        if data.len() < expected_len {
            return None;
        }

        let mut words = Vec::with_capacity(word_count);
        for i in 0..word_count {
            let offset = 8 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            words.push(word);
        }

        Some(Self { words })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_bitmap() {
        let bm = DirtyBitmap::new();
        assert_eq!(bm.count(), 0);
        assert!(bm.dirty_ids().is_empty());
        assert!(!bm.is_dirty(0));
        assert!(!bm.is_dirty(999));
    }

    #[test]
    fn test_mark_and_check() {
        let mut bm = DirtyBitmap::new();
        bm.mark(0);
        bm.mark(5);
        bm.mark(63);
        bm.mark(64);
        bm.mark(200);

        assert!(bm.is_dirty(0));
        assert!(bm.is_dirty(5));
        assert!(bm.is_dirty(63));
        assert!(bm.is_dirty(64));
        assert!(bm.is_dirty(200));

        assert!(!bm.is_dirty(1));
        assert!(!bm.is_dirty(62));
        assert!(!bm.is_dirty(65));
        assert!(!bm.is_dirty(199));

        assert_eq!(bm.count(), 5);
    }

    #[test]
    fn test_dirty_ids_sorted() {
        let mut bm = DirtyBitmap::new();
        bm.mark(100);
        bm.mark(5);
        bm.mark(200);
        bm.mark(0);

        let ids = bm.dirty_ids();
        assert_eq!(ids, vec![0, 5, 100, 200]);
    }

    #[test]
    fn test_clear() {
        let mut bm = DirtyBitmap::new();
        bm.mark(10);
        bm.mark(20);
        assert_eq!(bm.count(), 2);

        bm.clear();
        assert_eq!(bm.count(), 0);
        assert!(!bm.is_dirty(10));
        assert!(!bm.is_dirty(20));
    }

    #[test]
    fn test_serialization_round_trip() {
        let mut bm = DirtyBitmap::new();
        bm.mark(0);
        bm.mark(63);
        bm.mark(64);
        bm.mark(1000);

        let bytes = bm.to_bytes();
        let restored = DirtyBitmap::from_bytes(&bytes).unwrap();

        assert_eq!(bm.dirty_ids(), restored.dirty_ids());
        assert_eq!(bm.count(), restored.count());
    }

    #[test]
    fn test_serialization_empty() {
        let bm = DirtyBitmap::new();
        let bytes = bm.to_bytes();
        let restored = DirtyBitmap::from_bytes(&bytes).unwrap();
        assert_eq!(restored.count(), 0);
    }

    #[test]
    fn test_from_bytes_invalid() {
        // Too short
        assert!(DirtyBitmap::from_bytes(&[]).is_none());
        assert!(DirtyBitmap::from_bytes(&[1, 2, 3]).is_none());

        // Claims 1 word but data truncated
        let mut data = vec![0u8; 8];
        data[0] = 1; // word_count = 1
        assert!(DirtyBitmap::from_bytes(&data).is_none());
    }

    #[test]
    fn test_idempotent_mark() {
        let mut bm = DirtyBitmap::new();
        bm.mark(42);
        bm.mark(42);
        bm.mark(42);
        assert_eq!(bm.count(), 1);
        assert_eq!(bm.dirty_ids(), vec![42]);
    }

    #[test]
    fn test_large_page_id() {
        let mut bm = DirtyBitmap::new();
        bm.mark(10_000);
        assert!(bm.is_dirty(10_000));
        assert!(!bm.is_dirty(9_999));
        assert_eq!(bm.count(), 1);

        // Verify memory usage is reasonable (10000/64 = ~157 words = ~1.2KB)
        let bytes = bm.to_bytes();
        assert!(bytes.len() < 2048);
    }
}
