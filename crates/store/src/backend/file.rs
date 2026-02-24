//! File-based storage backend using position-based I/O (`pread`/`pwrite`).
//!
//! On Unix, reads are completely lock-free via [`std::os::unix::fs::FileExt`]:
//! `read_exact_at()` takes `&self` and never touches the file cursor, allowing
//! concurrent reads from multiple threads with zero synchronization.
//!
//! Writes are serialized via a lightweight [`parking_lot::Mutex`] guard (not
//! wrapping the `File` — just a unit `()` sentinel) to prevent concurrent
//! file extensions and torn writes. The COW model guarantees at most one
//! `WriteTransaction` commits at a time, so write serialization is not a
//! bottleneck.

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use parking_lot::Mutex;

use super::{DatabaseHeader, HEADER_SIZE, StorageBackend};
use crate::error::{Error, PageId, Result};

/// File-based storage backend.
///
/// Uses position-based I/O (`pread`/`pwrite`) for page access. On Unix,
/// reads are completely lock-free — `read_exact_at()` takes `&self` and
/// does not touch the file cursor. Writes are serialized via `write_lock`
/// to prevent concurrent file extensions and torn writes.
///
/// Durability requires the caller to invoke [`sync`](StorageBackend::sync)
/// after writes; page data is not guaranteed on disk until `fsync` completes.
/// Crash safety is provided by the dual-slot commit protocol in
/// [`DatabaseHeader`], not by this backend alone.
pub struct FileBackend {
    /// The underlying file handle.
    ///
    /// On Unix, `read_exact_at()` takes `&self` — no lock needed for reads.
    file: File,
    /// Serializes writes and file extension operations.
    ///
    /// This is a `Mutex<()>` (not `Mutex<File>`) because the file handle
    /// itself does not need to be wrapped — `write_all_at()` takes `&self`
    /// on Unix. The mutex prevents concurrent `set_len()` / `write_all_at()`
    /// calls from interleaving.
    write_lock: Mutex<()>,
    /// Page size in bytes.
    page_size: usize,
    /// Path for error messages (reserved for future use).
    #[allow(dead_code)] // retained for debug/display and error reporting
    path: String,
}

impl FileBackend {
    /// Opens an existing database file.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be opened or the header cannot be read.
    /// Returns [`Error::InvalidMagic`] if the file is not an InferaDB database.
    /// Returns [`Error::Corrupted`] if the header is malformed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_str = path.as_ref().display().to_string();
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;

        let mut header_buf = vec![0u8; HEADER_SIZE];
        // Use pread at offset 0 to read the header without mutating cursor.
        read_exact_at_offset(&file, &mut header_buf, 0)?;

        let header = DatabaseHeader::from_bytes(&header_buf)?;

        Ok(Self { file, write_lock: Mutex::new(()), page_size: header.page_size(), path: path_str })
    }

    /// Creates a new database file.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be created or written.
    /// Returns [`Error::Corrupted`] if `page_size` is not a power of two or
    /// falls outside the 512..=65536 range.
    pub fn create(path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        let path_str = path.as_ref().display().to_string();

        // Ensure page size is power of 2
        if !page_size.is_power_of_two() || page_size < 512 || page_size > 65536 {
            return Err(Error::Corrupted { reason: format!("Invalid page size: {page_size}") });
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;

        let page_size_power = page_size.trailing_zeros() as u8;
        let header = DatabaseHeader::new(page_size_power);
        let header_bytes = header.to_bytes();

        // Initial file creation uses standard Write — file is exclusively
        // owned at this point (no concurrent access possible).
        let mut file_mut = file;
        file_mut.write_all(&header_bytes)?;
        file_mut.sync_all()?;

        Ok(Self { file: file_mut, write_lock: Mutex::new(()), page_size, path: path_str })
    }
}

impl StorageBackend for FileBackend {
    fn read_header(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; HEADER_SIZE];
        read_exact_at_offset(&self.file, &mut buf, 0)?;
        Ok(buf)
    }

    fn write_header(&self, header: &[u8]) -> Result<()> {
        if header.len() != HEADER_SIZE {
            return Err(Error::Corrupted {
                reason: format!("Invalid header size: {} (expected {})", header.len(), HEADER_SIZE),
            });
        }

        let _guard = self.write_lock.lock();
        write_all_at_offset(&self.file, header, 0)?;
        Ok(())
    }

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = self.page_offset(page_id);

        // Check file size without a lock — metadata() takes &self.
        let file_len = self.file.metadata()?.len();
        if offset + self.page_size as u64 > file_len {
            // Page is beyond current file size; return zeros.
            return Ok(vec![0u8; self.page_size]);
        }

        let mut buf = vec![0u8; self.page_size];
        read_exact_at_offset(&self.file, &mut buf, offset)?;
        Ok(buf)
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

        let _guard = self.write_lock.lock();

        // Extend file if needed.
        let file_len = self.file.metadata()?.len();
        let required_len = offset + self.page_size as u64;
        if file_len < required_len {
            self.file.set_len(required_len)?;
        }

        write_all_at_offset(&self.file, data, offset)?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        // sync_data() takes &self — no lock needed.
        self.file.sync_data()?;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        // metadata() takes &self — no lock needed.
        Ok(self.file.metadata()?.len())
    }

    fn extend(&self, new_size: u64) -> Result<()> {
        let _guard = self.write_lock.lock();
        self.file.set_len(new_size)?;
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
}

// ---------------------------------------------------------------------------
// Platform-specific position-based I/O helpers
// ---------------------------------------------------------------------------

/// Reads exactly `buf.len()` bytes from `file` at the given byte `offset`.
///
/// On Unix, this uses `pread(2)` via [`FileExt::read_exact_at`] — no lock,
/// no cursor mutation. On Windows, this falls back to `seek_read()` which
/// does update the cursor (callers must ensure external synchronization).
#[cfg(unix)]
fn read_exact_at_offset(file: &File, buf: &mut [u8], offset: u64) -> Result<()> {
    file.read_exact_at(buf, offset)?;
    Ok(())
}

/// Windows fallback: `seek_read` updates the file cursor, so this is NOT
/// lock-free. Callers that need concurrent reads on Windows must provide
/// external synchronization.
#[cfg(windows)]
fn read_exact_at_offset(file: &File, buf: &mut [u8], offset: u64) -> Result<()> {
    let mut pos = 0;
    while pos < buf.len() {
        let n = file.seek_read(&mut buf[pos..], offset + pos as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of file during seek_read",
            )
            .into());
        }
        pos += n;
    }
    Ok(())
}

/// Writes all of `data` to `file` at the given byte `offset`.
///
/// On Unix, this uses `pwrite(2)` via [`FileExt::write_all_at`]. On Windows,
/// this uses `seek_write()` in a loop.
#[cfg(unix)]
fn write_all_at_offset(file: &File, data: &[u8], offset: u64) -> Result<()> {
    file.write_all_at(data, offset)?;
    Ok(())
}

/// Windows fallback for positional writes.
#[cfg(windows)]
fn write_all_at_offset(file: &File, data: &[u8], offset: u64) -> Result<()> {
    let mut pos = 0;
    while pos < data.len() {
        let n = file.seek_write(&data[pos..], offset + pos as u64)?;
        pos += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::backend::DEFAULT_PAGE_SIZE;

    #[allow(clippy::unwrap_used)]
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

    #[allow(clippy::unwrap_used)]
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

    /// Concurrent read + write: one thread writes page X while another reads
    /// page Y — verify no deadlock, no corruption, both complete.
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_concurrent_read_write_different_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backend = Arc::new(FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap());

        // Pre-write page 1 with known data.
        let mut page1_data = vec![0xAAu8; DEFAULT_PAGE_SIZE];
        page1_data[0] = 0x11;
        backend.write_page(1, &page1_data).unwrap();
        backend.sync().unwrap();

        let b_write = Arc::clone(&backend);
        let b_read = Arc::clone(&backend);

        // Writer: repeatedly writes page 5.
        let writer = std::thread::spawn(move || {
            for i in 0u8..50 {
                let mut data = vec![i; DEFAULT_PAGE_SIZE];
                data[0] = 0xFF;
                b_write.write_page(5, &data).unwrap();
            }
        });

        // Reader: repeatedly reads page 1 (should always see original data).
        let reader = std::thread::spawn(move || {
            for _ in 0..50 {
                let data = b_read.read_page(1).unwrap();
                assert_eq!(data[0], 0x11, "page 1 corrupted during concurrent write to page 5");
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        // Verify page 5 has last written value.
        let final_page5 = backend.read_page(5).unwrap();
        assert_eq!(final_page5[0], 0xFF);
        assert_eq!(final_page5[1], 49);
    }

    /// Concurrent file extension + read: extend file while reading existing
    /// pages — verify reads succeed with correct data.
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_concurrent_extend_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backend = Arc::new(FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap());

        // Pre-write page 0 with known data.
        let mut page0_data = vec![0u8; DEFAULT_PAGE_SIZE];
        page0_data[0] = 0x42;
        page0_data[DEFAULT_PAGE_SIZE - 1] = 0x99;
        backend.write_page(0, &page0_data).unwrap();
        backend.sync().unwrap();

        let b_extend = Arc::clone(&backend);
        let b_read = Arc::clone(&backend);

        // Extender: progressively extends the file.
        let extender = std::thread::spawn(move || {
            for i in 1u64..20 {
                let new_size = HEADER_SIZE as u64 + (i + 10) * DEFAULT_PAGE_SIZE as u64;
                b_extend.extend(new_size).unwrap();
            }
        });

        // Reader: repeatedly reads page 0 — should always see the original data.
        let reader = std::thread::spawn(move || {
            for _ in 0..100 {
                let data = b_read.read_page(0).unwrap();
                assert_eq!(data[0], 0x42, "page 0 corrupted during concurrent extend");
                assert_eq!(
                    data[DEFAULT_PAGE_SIZE - 1],
                    0x99,
                    "page 0 tail corrupted during concurrent extend"
                );
            }
        });

        extender.join().unwrap();
        reader.join().unwrap();
    }

    /// Verify that reading a page beyond the file returns zeros (not an error).
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_read_beyond_file_returns_zeros() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backend = FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap();

        let data = backend.read_page(999).unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    /// Verify sync, file_size, and extend work correctly.
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_file_size_and_extend() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backend = FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap();

        let initial_size = backend.file_size().unwrap();
        assert_eq!(initial_size, HEADER_SIZE as u64);

        let target = HEADER_SIZE as u64 + 10 * DEFAULT_PAGE_SIZE as u64;
        backend.extend(target).unwrap();
        assert_eq!(backend.file_size().unwrap(), target);
    }

    /// Multiple concurrent readers on different pages — no serialization.
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_multiple_concurrent_readers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backend = Arc::new(FileBackend::create(&path, DEFAULT_PAGE_SIZE).unwrap());

        // Pre-write 8 pages with distinct patterns.
        for page_id in 0u64..8 {
            let mut data = vec![page_id as u8; DEFAULT_PAGE_SIZE];
            data[0] = (page_id + 0x10) as u8;
            backend.write_page(page_id, &data).unwrap();
        }
        backend.sync().unwrap();

        // Spawn 8 readers, each reading its own page 100 times.
        let mut handles = Vec::new();
        for page_id in 0u64..8 {
            let b = Arc::clone(&backend);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let data = b.read_page(page_id).unwrap();
                    assert_eq!(data[0], (page_id + 0x10) as u8, "page {page_id} header byte wrong");
                    assert_eq!(data[1], page_id as u8, "page {page_id} body byte wrong");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
