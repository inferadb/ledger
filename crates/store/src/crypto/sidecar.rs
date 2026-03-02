//! Crypto metadata sidecar storage.
//!
//! Each encrypted page has 72 bytes of crypto metadata (RMK version,
//! wrapped DEK, nonce, auth tag). This metadata is stored in a
//! separate sidecar indexed by page ID, keeping the page format
//! and page_size unchanged.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use parking_lot::Mutex;

use super::types::{CRYPTO_METADATA_SIZE, CryptoMetadata};
use crate::error::Result;

/// Sidecar storage for per-page crypto metadata.
///
/// Two implementations:
/// - **File-based**: For `FileBackend`, stores metadata in a `.crypto` file alongside the database
///   file. Uses `pread`/`pwrite` for lock-free concurrent access (each page ID maps to a fixed
///   offset).
/// - **In-memory**: For `InMemoryBackend`, uses a `HashMap`.
pub enum CryptoSidecar {
    /// File-backed sidecar for production use.
    File {
        /// The sidecar file handle.
        file: File,
        /// Path for debug/error messages.
        path: PathBuf,
    },
    /// In-memory sidecar for testing.
    Memory {
        /// Page ID → crypto metadata bytes.
        entries: Mutex<HashMap<u64, [u8; CRYPTO_METADATA_SIZE]>>,
    },
}

impl CryptoSidecar {
    /// Opens or creates a file-backed sidecar alongside the given database path.
    ///
    /// The sidecar file is named `<db_path>.crypto`.
    pub fn open_file(db_path: &Path) -> Result<Self> {
        let path = db_path.with_extension("crypto");
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        Ok(Self::File { file, path })
    }

    /// Creates a new in-memory sidecar.
    pub fn new_memory() -> Self {
        Self::Memory { entries: Mutex::new(HashMap::new()) }
    }

    /// Reads crypto metadata for a page.
    ///
    /// Returns `None` if the page has no metadata (unencrypted or
    /// never written).
    pub fn read(&self, page_id: u64) -> Result<Option<CryptoMetadata>> {
        match self {
            Self::File { file, .. } => {
                let offset = page_id * CRYPTO_METADATA_SIZE as u64;
                let mut buf = [0u8; CRYPTO_METADATA_SIZE];

                // Check file size — page may not exist yet
                let file_len = file.metadata()?.len();
                if offset + CRYPTO_METADATA_SIZE as u64 > file_len {
                    return Ok(None);
                }

                let n = file.read_at(&mut buf, offset)?;
                if n < CRYPTO_METADATA_SIZE {
                    return Ok(None);
                }

                Ok(CryptoMetadata::from_bytes(&buf))
            },
            Self::Memory { entries } => {
                let entries = entries.lock();
                match entries.get(&page_id) {
                    Some(buf) => Ok(CryptoMetadata::from_bytes(buf)),
                    None => Ok(None),
                }
            },
        }
    }

    /// Writes crypto metadata for a page.
    pub fn write(&self, page_id: u64, metadata: &CryptoMetadata) -> Result<()> {
        let buf = metadata.to_bytes();

        match self {
            Self::File { file, .. } => {
                let offset = page_id * CRYPTO_METADATA_SIZE as u64;

                // Extend file if needed
                let required_len = offset + CRYPTO_METADATA_SIZE as u64;
                let file_len = file.metadata()?.len();
                if required_len > file_len {
                    file.set_len(required_len)?;
                }

                file.write_at(&buf, offset)?;
                Ok(())
            },
            Self::Memory { entries } => {
                entries.lock().insert(page_id, buf);
                Ok(())
            },
        }
    }

    /// Syncs sidecar data to disk (no-op for in-memory).
    pub fn sync(&self) -> Result<()> {
        match self {
            Self::File { file, .. } => {
                file.sync_all()?;
                Ok(())
            },
            Self::Memory { .. } => Ok(()),
        }
    }

    /// Returns the total number of page slots in the sidecar.
    ///
    /// For the file backend, this is `file_len / CRYPTO_METADATA_SIZE`.
    /// For in-memory, this is `max(page_id) + 1` or 0 if empty.
    /// Not all slots contain valid metadata — use [`Self::read`] to check.
    pub fn page_count(&self) -> Result<u64> {
        match self {
            Self::File { file, .. } => {
                let file_len = file.metadata()?.len();
                Ok(file_len / CRYPTO_METADATA_SIZE as u64)
            },
            Self::Memory { entries } => {
                let entries = entries.lock();
                Ok(entries.keys().max().map_or(0, |&max| max + 1))
            },
        }
    }
}

impl std::fmt::Debug for CryptoSidecar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File { path, .. } => {
                f.debug_struct("CryptoSidecar::File").field("path", path).finish()
            },
            Self::Memory { entries } => f
                .debug_struct("CryptoSidecar::Memory")
                .field("entries", &entries.lock().len())
                .finish(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{super::types::WrappedDek, *};

    fn test_metadata() -> CryptoMetadata {
        CryptoMetadata {
            rmk_version: 1,
            wrapped_dek: WrappedDek::from_bytes([0xAA; 40]),
            nonce: [0xBB; 12],
            auth_tag: [0xCC; 16],
        }
    }

    #[test]
    fn test_memory_sidecar_write_read() {
        let sidecar = CryptoSidecar::new_memory();
        let meta = test_metadata();

        sidecar.write(42, &meta).unwrap();
        let read = sidecar.read(42).unwrap().unwrap();

        assert_eq!(read.rmk_version, 1);
        assert_eq!(read.nonce, [0xBB; 12]);
    }

    #[test]
    fn test_memory_sidecar_read_missing() {
        let sidecar = CryptoSidecar::new_memory();
        let result = sidecar.read(99).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_memory_sidecar_overwrite() {
        let sidecar = CryptoSidecar::new_memory();
        let meta1 = test_metadata();
        let meta2 = CryptoMetadata { rmk_version: 2, ..test_metadata() };

        sidecar.write(1, &meta1).unwrap();
        sidecar.write(1, &meta2).unwrap();

        let read = sidecar.read(1).unwrap().unwrap();
        assert_eq!(read.rmk_version, 2);
    }

    #[test]
    fn test_file_sidecar_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        std::fs::File::create(&db_path).unwrap();

        let sidecar = CryptoSidecar::open_file(&db_path).unwrap();
        let meta = test_metadata();

        sidecar.write(0, &meta).unwrap();
        sidecar.write(5, &meta).unwrap();

        let read0 = sidecar.read(0).unwrap().unwrap();
        assert_eq!(read0.rmk_version, 1);

        let read5 = sidecar.read(5).unwrap().unwrap();
        assert_eq!(read5.rmk_version, 1);

        // Gap pages should return None
        let read3 = sidecar.read(3).unwrap();
        assert!(read3.is_none());
    }

    #[test]
    fn test_file_sidecar_read_beyond_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test2.db");
        std::fs::File::create(&db_path).unwrap();

        let sidecar = CryptoSidecar::open_file(&db_path).unwrap();
        let result = sidecar.read(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_memory_sidecar_sync_is_noop() {
        let sidecar = CryptoSidecar::new_memory();
        sidecar.sync().unwrap();
    }
}
