//! Storage backend abstraction for Inkwell.
//!
//! The backend trait abstracts the underlying storage mechanism,
//! allowing both file-based (production) and in-memory (testing) implementations.

mod file;
mod memory;

pub use file::FileBackend;
pub use memory::InMemoryBackend;

use crate::error::{PageId, Result};

/// Default page size power: 12 (meaning 2^12 = 4KB).
pub const DEFAULT_PAGE_SIZE_POWER: u8 = 12;
/// Default page size: 4KB (4096 bytes).
pub const DEFAULT_PAGE_SIZE: usize = 1 << DEFAULT_PAGE_SIZE_POWER;

/// Database header size (fixed at 512 bytes).
pub const HEADER_SIZE: usize = 512;

/// Magic number for Inkwell database files.
pub const MAGIC: &[u8; 8] = b"INKWELL\0";

/// Current format version.
pub const FORMAT_VERSION: u16 = 1;

/// Storage backend trait for abstracting file I/O.
pub trait StorageBackend: Send + Sync {
    /// Read the database header (512 bytes).
    fn read_header(&self) -> Result<Vec<u8>>;

    /// Write the database header.
    fn write_header(&self, header: &[u8]) -> Result<()>;

    /// Read a page by its ID.
    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>>;

    /// Write a page at the given ID.
    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()>;

    /// Flush all writes to durable storage.
    fn sync(&self) -> Result<()>;

    /// Get the current file size in bytes.
    fn file_size(&self) -> Result<u64>;

    /// Extend the file to accommodate more pages.
    fn extend(&self, new_size: u64) -> Result<()>;

    /// Get the page size for this backend.
    fn page_size(&self) -> usize;

    /// Calculate the byte offset for a page ID.
    fn page_offset(&self, page_id: PageId) -> u64 {
        HEADER_SIZE as u64 + (page_id * self.page_size() as u64)
    }
}

/// Database header structure.
///
/// The header is always 512 bytes (padded with zeros).
#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    /// Magic number: "INKWELL\0"
    pub magic: [u8; 8],
    /// Format version (currently 1).
    pub version: u16,
    /// Page size as power of 2 (default: 12 = 4KB).
    pub page_size_power: u8,
    /// Reserved byte.
    pub reserved: u8,
    /// Reserved for future use.
    pub reserved2: u32,
    /// Total pages allocated.
    pub total_pages: u64,
    /// Head of the free page list (0 = none).
    pub free_list_head: u64,
    /// Page containing the table directory.
    pub table_directory_page: u64,
    /// Last committed transaction ID.
    pub last_txn_id: u64,
    /// Timestamp of last write (Unix epoch seconds).
    pub last_write_timestamp: u64,
    /// Checksum of header fields (bytes 0-55).
    pub checksum: u64,
}

impl DatabaseHeader {
    /// Total header size on disk.
    pub const SIZE: usize = HEADER_SIZE;

    /// Size of checksum-protected region.
    const CHECKSUMMED_SIZE: usize = 56;

    /// Create a new empty header.
    pub fn new(page_size_power: u8) -> Self {
        Self {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            page_size_power,
            reserved: 0,
            reserved2: 0,
            total_pages: 0,
            free_list_head: 0,
            table_directory_page: 0,
            last_txn_id: 0,
            last_write_timestamp: 0,
            checksum: 0,
        }
    }

    /// Serialize the header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];

        buf[0..8].copy_from_slice(&self.magic);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10] = self.page_size_power;
        buf[11] = self.reserved;
        buf[12..16].copy_from_slice(&self.reserved2.to_le_bytes());
        buf[16..24].copy_from_slice(&self.total_pages.to_le_bytes());
        buf[24..32].copy_from_slice(&self.free_list_head.to_le_bytes());
        buf[32..40].copy_from_slice(&self.table_directory_page.to_le_bytes());
        buf[40..48].copy_from_slice(&self.last_txn_id.to_le_bytes());
        buf[48..56].copy_from_slice(&self.last_write_timestamp.to_le_bytes());

        // Compute checksum over bytes 0-55
        let checksum = xxhash_rust::xxh3::xxh3_64(&buf[0..Self::CHECKSUMMED_SIZE]);
        buf[56..64].copy_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        use crate::error::Error;

        if buf.len() < Self::SIZE {
            return Err(Error::Corrupted {
                reason: "Header too short".to_string(),
            });
        }

        let header = Self {
            magic: buf[0..8].try_into().unwrap(),
            version: u16::from_le_bytes(buf[8..10].try_into().unwrap()),
            page_size_power: buf[10],
            reserved: buf[11],
            reserved2: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            total_pages: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            free_list_head: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            table_directory_page: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            last_txn_id: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            last_write_timestamp: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
            checksum: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
        };

        // Verify magic
        if header.magic != *MAGIC {
            return Err(Error::InvalidMagic);
        }

        // Verify version
        if header.version > FORMAT_VERSION {
            return Err(Error::UnsupportedVersion {
                version: header.version,
            });
        }

        // Verify checksum
        let expected_checksum = xxhash_rust::xxh3::xxh3_64(&buf[0..Self::CHECKSUMMED_SIZE]);
        if header.checksum != expected_checksum {
            return Err(Error::HeaderChecksumMismatch);
        }

        Ok(header)
    }

    /// Update the checksum field.
    pub fn update_checksum(&mut self) {
        let buf = self.to_bytes();
        self.checksum = xxhash_rust::xxh3::xxh3_64(&buf[0..Self::CHECKSUMMED_SIZE]);
    }

    /// Get the page size in bytes.
    pub fn page_size(&self) -> usize {
        1 << self.page_size_power
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);
        let bytes = header.to_bytes();
        let recovered = DatabaseHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.magic, recovered.magic);
        assert_eq!(header.version, recovered.version);
        assert_eq!(header.page_size_power, recovered.page_size_power);
    }

    #[test]
    fn test_header_checksum_verification() {
        let header = DatabaseHeader::new(DEFAULT_PAGE_SIZE_POWER);
        let mut bytes = header.to_bytes();

        // Corrupt one byte
        bytes[20] ^= 0xFF;

        let result = DatabaseHeader::from_bytes(&bytes);
        assert!(result.is_err());
    }
}
