//! File-based storage backend using memory-mapped I/O.

use super::{DatabaseHeader, StorageBackend, HEADER_SIZE};
use crate::error::{Error, PageId, Result};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// File-based storage backend.
///
/// Uses memory-mapped I/O for efficient page access with explicit fsync for durability.
pub struct FileBackend {
    /// The underlying file.
    file: RwLock<File>,
    /// Page size in bytes.
    page_size: usize,
    /// Path for error messages (reserved for future use).
    #[allow(dead_code)]
    path: String,
}

impl FileBackend {
    /// Open an existing database file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_str = path.as_ref().display().to_string();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        // Read header to get page size
        let mut file_guard = file;
        let mut header_buf = vec![0u8; HEADER_SIZE];
        file_guard.read_exact(&mut header_buf)?;

        let header = DatabaseHeader::from_bytes(&header_buf)?;

        Ok(Self {
            file: RwLock::new(file_guard),
            page_size: header.page_size(),
            path: path_str,
        })
    }

    /// Create a new database file.
    pub fn create(path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        let path_str = path.as_ref().display().to_string();

        // Ensure page size is power of 2
        if !page_size.is_power_of_two() || page_size < 512 || page_size > 65536 {
            return Err(Error::Corrupted {
                reason: format!("Invalid page size: {}", page_size),
            });
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;

        // Write initial header
        let page_size_power = page_size.trailing_zeros() as u8;
        let header = DatabaseHeader::new(page_size_power);
        let header_bytes = header.to_bytes();

        let mut file_guard = file;
        file_guard.write_all(&header_bytes)?;
        file_guard.sync_all()?;

        Ok(Self {
            file: RwLock::new(file_guard),
            page_size,
            path: path_str,
        })
    }

    /// Get the file for direct operations.
    fn with_file<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut File) -> Result<T>,
    {
        let mut file = self.file.write();
        f(&mut file)
    }

    /// Get the file for read operations.
    fn with_file_read<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&File) -> Result<T>,
    {
        let file = self.file.read();
        f(&file)
    }
}

impl StorageBackend for FileBackend {
    fn read_header(&self) -> Result<Vec<u8>> {
        self.with_file(|file| {
            file.seek(SeekFrom::Start(0))?;
            let mut buf = vec![0u8; HEADER_SIZE];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
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

        self.with_file(|file| {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(header)?;
            Ok(())
        })
    }

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = self.page_offset(page_id);

        self.with_file(|file| {
            let file_len = file.metadata()?.len();
            if offset + self.page_size as u64 > file_len {
                // Page doesn't exist yet, return zeros
                return Ok(vec![0u8; self.page_size]);
            }

            file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; self.page_size];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
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

        let offset = self.page_offset(page_id);

        self.with_file(|file| {
            // Extend file if needed
            let file_len = file.metadata()?.len();
            let required_len = offset + self.page_size as u64;
            if file_len < required_len {
                file.set_len(required_len)?;
            }

            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
            Ok(())
        })
    }

    fn sync(&self) -> Result<()> {
        self.with_file(|file| {
            file.sync_all()?;
            Ok(())
        })
    }

    fn file_size(&self) -> Result<u64> {
        self.with_file_read(|file| Ok(file.metadata()?.len()))
    }

    fn extend(&self, new_size: u64) -> Result<()> {
        self.with_file(|file| {
            file.set_len(new_size)?;
            Ok(())
        })
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create
        {
            let backend = FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap();
            let header = backend.read_header().unwrap();
            let parsed = DatabaseHeader::from_bytes(&header).unwrap();
            assert_eq!(parsed.page_size(), DEFAULT_PAGE_SIZE);
        }

        // Open
        {
            let backend = FileBackend::open(&path).unwrap();
            assert_eq!(backend.page_size(), DEFAULT_PAGE_SIZE);
        }
    }

    #[test]
    fn test_page_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let backend = FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap();

        // Write a page
        let mut data = vec![0u8; DEFAULT_PAGE_SIZE];
        data[0] = 0xDE;
        data[1] = 0xAD;
        data[DEFAULT_PAGE_SIZE - 1] = 0xBE;

        backend.write_page(0, &data).unwrap();
        backend.sync().unwrap();

        // Read it back
        let read_data = backend.read_page(0).unwrap();
        assert_eq!(read_data[0], 0xDE);
        assert_eq!(read_data[1], 0xAD);
        assert_eq!(read_data[DEFAULT_PAGE_SIZE - 1], 0xBE);
    }
}
