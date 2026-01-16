//! In-memory storage backend for testing.

use super::{DEFAULT_PAGE_SIZE, DatabaseHeader, HEADER_SIZE, StorageBackend};
use crate::error::{Error, PageId, Result};
use parking_lot::RwLock;
use std::collections::HashMap;

/// In-memory storage backend for testing.
///
/// All data is stored in memory and lost when the backend is dropped.
/// This is useful for unit tests and benchmarking.
pub struct InMemoryBackend {
    /// The database header.
    header: RwLock<Vec<u8>>,
    /// Pages indexed by page ID.
    pages: RwLock<HashMap<PageId, Vec<u8>>>,
    /// Page size in bytes.
    page_size: usize,
    /// Simulated file size.
    file_size: RwLock<u64>,
}

impl InMemoryBackend {
    /// Create a new in-memory backend with default page size.
    pub fn new() -> Self {
        Self::with_page_size(DEFAULT_PAGE_SIZE)
    }

    /// Create a new in-memory backend with specified page size.
    pub fn with_page_size(page_size: usize) -> Self {
        // Ensure page size is power of 2
        assert!(
            page_size.is_power_of_two() && page_size >= 512 && page_size <= 65536,
            "Invalid page size: {}",
            page_size
        );

        let page_size_power = page_size.trailing_zeros() as u8;
        let header = DatabaseHeader::new(page_size_power);
        let header_bytes = header.to_bytes().to_vec();

        Self {
            header: RwLock::new(header_bytes),
            pages: RwLock::new(HashMap::new()),
            page_size,
            file_size: RwLock::new(HEADER_SIZE as u64),
        }
    }

    /// Get the number of pages currently stored.
    pub fn page_count(&self) -> usize {
        self.pages.read().len()
    }

    /// Clear all data (for testing).
    pub fn clear(&self) {
        self.pages.write().clear();
        *self.file_size.write() = HEADER_SIZE as u64;
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for InMemoryBackend {
    fn read_header(&self) -> Result<Vec<u8>> {
        Ok(self.header.read().clone())
    }

    fn write_header(&self, header: &[u8]) -> Result<()> {
        if header.len() != HEADER_SIZE {
            return Err(Error::Corrupted {
                reason: format!(
                    "Invalid header size: {} (expected {})",
                    header.len(),
                    HEADER_SIZE
                ),
            });
        }

        *self.header.write() = header.to_vec();
        Ok(())
    }

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let pages = self.pages.read();
        match pages.get(&page_id) {
            Some(data) => Ok(data.clone()),
            None => Ok(vec![0u8; self.page_size]),
        }
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != self.page_size {
            return Err(Error::Corrupted {
                reason: format!(
                    "Invalid page data size: {} (expected {})",
                    data.len(),
                    self.page_size
                ),
            });
        }

        self.pages.write().insert(page_id, data.to_vec());

        // Update simulated file size
        let offset = self.page_offset(page_id);
        let required_size = offset + self.page_size as u64;
        let mut file_size = self.file_size.write();
        if *file_size < required_size {
            *file_size = required_size;
        }

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        // No-op for in-memory backend
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        Ok(*self.file_size.read())
    }

    fn extend(&self, new_size: u64) -> Result<()> {
        let mut file_size = self.file_size.write();
        if new_size > *file_size {
            *file_size = new_size;
        }
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_basic() {
        let backend = InMemoryBackend::new();

        // Write a page
        let mut data = vec![0u8; DEFAULT_PAGE_SIZE];
        data[0] = 0x42;
        backend.write_page(0, &data).unwrap();

        // Read it back
        let read_data = backend.read_page(0).unwrap();
        assert_eq!(read_data[0], 0x42);

        // Read non-existent page (should return zeros)
        let empty = backend.read_page(999).unwrap();
        assert!(empty.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_in_memory_header() {
        let backend = InMemoryBackend::new();

        let header = backend.read_header().unwrap();
        let parsed = DatabaseHeader::from_bytes(&header).unwrap();
        assert_eq!(parsed.page_size(), DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_in_memory_file_size() {
        let backend = InMemoryBackend::new();

        assert_eq!(backend.file_size().unwrap(), HEADER_SIZE as u64);

        // Write page 0
        backend
            .write_page(0, &vec![0u8; DEFAULT_PAGE_SIZE])
            .unwrap();
        assert_eq!(
            backend.file_size().unwrap(),
            HEADER_SIZE as u64 + DEFAULT_PAGE_SIZE as u64
        );

        // Write page 10 (should extend)
        backend
            .write_page(10, &vec![0u8; DEFAULT_PAGE_SIZE])
            .unwrap();
        assert_eq!(
            backend.file_size().unwrap(),
            HEADER_SIZE as u64 + (11 * DEFAULT_PAGE_SIZE) as u64
        );
    }
}
