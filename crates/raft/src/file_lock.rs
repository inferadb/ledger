//! Data directory file locking.
//!
//! Prevents concurrent access to the same data directory by multiple processes.
//! Each node exclusively locks its data directory using an OS-level file lock.
//!
//! ## Usage
//!
//! ```no_run
//! # use inferadb_ledger_raft::file_lock::DataDirLock;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let lock = DataDirLock::acquire("/path/to/data")?;
//! // Lock is held until `lock` is dropped
//! # Ok(())
//! # }
//! ```

use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use tracing::{debug, error, info};

/// Error type for data directory locking operations.
#[derive(Debug)]
pub enum LockError {
    /// The data directory is already locked by another process.
    AlreadyLocked(PathBuf),
    /// Failed to create the lock file.
    CreateFailed(PathBuf, io::Error),
    /// Failed to create the data directory.
    DirectoryCreateFailed(PathBuf, io::Error),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyLocked(path) => {
                write!(f, "Data directory is already locked by another process: {}", path.display())
            },
            LockError::CreateFailed(path, err) => {
                write!(f, "Failed to create lock file {}: {}", path.display(), err)
            },
            LockError::DirectoryCreateFailed(path, err) => {
                write!(f, "Failed to create data directory {}: {}", path.display(), err)
            },
        }
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::CreateFailed(_, err) | LockError::DirectoryCreateFailed(_, err) => Some(err),
            LockError::AlreadyLocked(_) => None,
        }
    }
}

/// An exclusive lock on a data directory.
///
/// The lock is automatically released when this struct is dropped.
/// Uses OS-level file locking (`flock` on Unix, `LockFileEx` on Windows)
/// which is automatically released even if the process crashes.
pub struct DataDirLock {
    /// The lock file handle (kept open to maintain the lock).
    #[allow(dead_code)] // retained to keep file handle open for lock
    file: File,
    /// Path to the lock file.
    path: PathBuf,
}

impl DataDirLock {
    /// Acquires an exclusive lock on the data directory.
    ///
    /// Creates a `.lock` file in the data directory and acquires an exclusive
    /// lock on it. If another process already holds the lock, this returns
    /// an error immediately (non-blocking).
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the data directory to lock
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data directory cannot be created
    /// - The lock file cannot be created
    /// - Another process already holds the lock
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_raft::file_lock::DataDirLock;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let lock = DataDirLock::acquire("/var/lib/ledger/data")?;
    /// // Data directory is now exclusively locked
    /// // Lock is released when `lock` goes out of scope
    /// # Ok(())
    /// # }
    /// ```
    pub fn acquire<P: AsRef<Path>>(data_dir: P) -> Result<Self, LockError> {
        let data_dir = data_dir.as_ref();
        let lock_path = data_dir.join(".lock");

        // Ensure data directory exists
        if !data_dir.exists() {
            fs::create_dir_all(data_dir)
                .map_err(|e| LockError::DirectoryCreateFailed(data_dir.to_path_buf(), e))?;
            debug!(path = %data_dir.display(), "Created data directory");
        }

        // Create/open the lock file
        let file =
            File::create(&lock_path).map_err(|e| LockError::CreateFailed(lock_path.clone(), e))?;

        // Try to acquire an exclusive lock (non-blocking)
        match file.try_lock_exclusive() {
            Ok(()) => {
                info!(
                    path = %lock_path.display(),
                    "Acquired exclusive lock on data directory"
                );
                Ok(Self { file, path: lock_path })
            },
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                error!(
                    path = %data_dir.display(),
                    "Data directory is already locked by another process"
                );
                Err(LockError::AlreadyLocked(data_dir.to_path_buf()))
            },
            Err(e) => {
                // On some systems, try_lock returns other errors for "already locked"
                // Check raw_os_error for EWOULDBLOCK (11) or EAGAIN (11 on Linux)
                if e.raw_os_error() == Some(11) || e.raw_os_error() == Some(35) {
                    // 35 is EAGAIN on macOS
                    error!(
                        path = %data_dir.display(),
                        "Data directory is already locked by another process"
                    );
                    Err(LockError::AlreadyLocked(data_dir.to_path_buf()))
                } else {
                    error!(
                        path = %lock_path.display(),
                        error = %e,
                        "Failed to acquire lock on data directory"
                    );
                    Err(LockError::CreateFailed(lock_path, e))
                }
            },
        }
    }

    /// Returns the path to the lock file.
    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.path
    }

    /// Returns the data directory path (parent of lock file).
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        self.path.parent().unwrap_or(Path::new("."))
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        // The lock is automatically released when the file is closed.
        // We explicitly unlock here to log it.
        if let Err(e) = FileExt::unlock(&self.file) {
            error!(
                path = %self.path.display(),
                error = %e,
                "Failed to release data directory lock"
            );
        } else {
            info!(
                path = %self.path.display(),
                "Released data directory lock"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    #[test]
    fn test_acquire_lock_creates_directory() {
        let temp = TestDir::new();
        let data_dir = temp.join("new_data_dir");

        assert!(!data_dir.exists());

        let lock = DataDirLock::acquire(&data_dir).unwrap();

        assert!(data_dir.exists());
        assert!(lock.lock_path().exists());
        assert_eq!(lock.data_dir(), data_dir);
    }

    #[test]
    fn test_acquire_lock_existing_directory() {
        let temp = TestDir::new();
        let data_dir = temp.path();

        let lock = DataDirLock::acquire(data_dir).unwrap();

        assert!(lock.lock_path().exists());
        assert_eq!(lock.lock_path(), data_dir.join(".lock"));
    }

    #[test]
    fn test_double_lock_fails() {
        let temp = TestDir::new();
        let data_dir = temp.path();

        let lock1 = DataDirLock::acquire(data_dir).unwrap();

        // Second lock attempt should fail
        let result = DataDirLock::acquire(data_dir);
        assert!(matches!(result, Err(LockError::AlreadyLocked(_))));

        // First lock is still valid
        assert!(lock1.lock_path().exists());
    }

    #[test]
    fn test_lock_released_on_drop() {
        let temp = TestDir::new();
        let data_dir = temp.path();

        {
            let _lock = DataDirLock::acquire(data_dir).unwrap();
            // Lock is held here
        }
        // Lock is released after drop

        // Should be able to acquire again
        let lock2 = DataDirLock::acquire(data_dir).unwrap();
        assert!(lock2.lock_path().exists());
    }

    #[test]
    fn test_lock_error_display() {
        let path = PathBuf::from("/test/path");

        let err = LockError::AlreadyLocked(path.clone());
        assert!(err.to_string().contains("already locked"));

        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let err = LockError::CreateFailed(path.clone(), io_err);
        assert!(err.to_string().contains("Failed to create lock file"));

        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let err = LockError::DirectoryCreateFailed(path, io_err);
        assert!(err.to_string().contains("Failed to create data directory"));
    }
}
