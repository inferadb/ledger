//! Page management for the store engine.
//!
//! This module handles:
//! - Page layout and structure
//! - Page allocation (free list)
//! - Page caching (LRU)
//! - Checksum computation and verification

mod allocator;
mod cache;

pub use allocator::PageAllocator;
pub use cache::PageCache;

use crate::error::{Error, PageId, PageType, Result};

/// Page header size in bytes.
pub const PAGE_HEADER_SIZE: usize = 16;

/// Page header structure (16 bytes).
///
/// ```text
/// Offset  Size   Field
/// ------  ----   -----
/// 0       1      Page type (PageType enum)
/// 1       1      Flags (reserved)
/// 2       2      Item count (for B-tree pages)
/// 4       4      Checksum (XXH32 of page content after header)
/// 8       8      Transaction ID that last modified this page
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Page type.
    pub page_type: PageType,
    /// Flags (reserved for future use).
    pub flags: u8,
    /// Number of items in this page (for B-tree pages).
    pub item_count: u16,
    /// XXH3-64 checksum (truncated to 32 bits) of page content (bytes after header).
    pub checksum: u32,
    /// Transaction ID that last modified this page.
    pub txn_id: u64,
}

impl PageHeader {
    /// Creates a new page header.
    pub fn new(page_type: PageType, txn_id: u64) -> Self {
        Self { page_type, flags: 0, item_count: 0, checksum: 0, txn_id }
    }

    /// Serializes header to bytes.
    pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        buf[0] = self.page_type as u8;
        buf[1] = self.flags;
        buf[2..4].copy_from_slice(&self.item_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.checksum.to_le_bytes());
        buf[8..16].copy_from_slice(&self.txn_id.to_le_bytes());
        buf
    }

    /// Deserializes header from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupted`] if the buffer is shorter than [`PAGE_HEADER_SIZE`].
    /// Returns an error if the page type byte is invalid.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < PAGE_HEADER_SIZE {
            return Err(Error::Corrupted { reason: "Page header too short".to_string() });
        }

        Ok(Self {
            page_type: PageType::try_from(buf[0])?,
            flags: buf[1],
            item_count: u16::from_le_bytes(buf[2..4].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            txn_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        })
    }
}

/// A page of data with header and content.
#[derive(Clone)]
pub struct Page {
    /// Page ID (position in file).
    pub id: PageId,
    /// Raw page data including header.
    pub data: Vec<u8>,
    /// Whether this page has been modified.
    pub dirty: bool,
}

impl Page {
    /// Creates a new empty page.
    pub fn new(id: PageId, page_size: usize, page_type: PageType, txn_id: u64) -> Self {
        let mut data = vec![0u8; page_size];
        let header = PageHeader::new(page_type, txn_id);
        data[..PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());

        Self { id, data, dirty: true }
    }

    /// Creates a page from raw bytes read from storage.
    pub fn from_bytes(id: PageId, data: Vec<u8>) -> Self {
        Self { id, data, dirty: false }
    }

    /// Returns the page header.
    ///
    /// # Errors
    ///
    /// Returns an error if the header bytes are corrupted or the page type is invalid.
    pub fn header(&self) -> Result<PageHeader> {
        PageHeader::from_bytes(&self.data)
    }

    /// Returns the page type.
    ///
    /// # Errors
    ///
    /// Returns an error if the header bytes are corrupted or the page type is invalid.
    pub fn page_type(&self) -> Result<PageType> {
        Ok(self.header()?.page_type)
    }

    /// Returns the item count.
    ///
    /// # Errors
    ///
    /// Returns an error if the header bytes are corrupted or the page type is invalid.
    pub fn item_count(&self) -> Result<u16> {
        Ok(self.header()?.item_count)
    }

    /// Sets the item count.
    pub fn set_item_count(&mut self, count: u16) {
        self.data[2..4].copy_from_slice(&count.to_le_bytes());
        self.dirty = true;
    }

    /// Returns the content portion of the page (after header).
    pub fn content(&self) -> &[u8] {
        &self.data[PAGE_HEADER_SIZE..]
    }

    /// Returns mutable content portion.
    pub fn content_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.data[PAGE_HEADER_SIZE..]
    }

    /// Computes and update the checksum using XXH3-64 (truncated to 32 bits).
    ///
    /// Uses XXH3-64 for better hash quality and performance on modern CPUs,
    /// truncated to 32 bits to maintain format compatibility with existing pages.
    pub fn update_checksum(&mut self) {
        // XXH3-64 is faster and higher quality than XXH32 on modern CPUs
        let hash64 = xxhash_rust::xxh3::xxh3_64(&self.data[PAGE_HEADER_SIZE..]);
        let checksum = hash64 as u32; // Truncate to 32 bits for format compatibility
        self.data[4..8].copy_from_slice(&checksum.to_le_bytes());
    }

    /// Verifies the page checksum using XXH3-64 (truncated to 32 bits).
    pub fn verify_checksum(&self) -> bool {
        let stored_checksum = u32::from_le_bytes(self.data[4..8].try_into().unwrap());
        let hash64 = xxhash_rust::xxh3::xxh3_64(&self.data[PAGE_HEADER_SIZE..]);
        let computed_checksum = hash64 as u32; // Truncate to 32 bits
        stored_checksum == computed_checksum
    }

    /// Returns the page size.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Returns the usable content size (total size minus header).
    pub fn content_size(&self) -> usize {
        self.data.len() - PAGE_HEADER_SIZE
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("size", &self.data.len())
            .field("dirty", &self.dirty)
            .field("header", &self.header())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;

    #[test]
    fn test_page_header_round_trip() {
        let header = PageHeader {
            page_type: PageType::BTreeLeaf,
            flags: 0,
            item_count: 42,
            checksum: 0xDEADBEEF,
            txn_id: 12345,
        };

        let bytes = header.to_bytes();
        let recovered = PageHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.page_type, recovered.page_type);
        assert_eq!(header.item_count, recovered.item_count);
        assert_eq!(header.checksum, recovered.checksum);
        assert_eq!(header.txn_id, recovered.txn_id);
    }

    #[test]
    fn test_page_checksum() {
        let mut page = Page::new(0, DEFAULT_PAGE_SIZE, PageType::BTreeLeaf, 1);

        // Write some content
        page.content_mut()[0] = 0x42;
        page.content_mut()[100] = 0xFF;

        // Update checksum
        page.update_checksum();

        // Verify
        assert!(page.verify_checksum());

        // Corrupt content
        page.data[PAGE_HEADER_SIZE + 50] ^= 0xFF;
        assert!(!page.verify_checksum());
    }
}
