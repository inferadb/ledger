//! Temporary directory management for tests.
//!
//! [`TestDir`] wraps [`tempfile::TempDir`] with a cleaner API for common test patterns.

// Test utilities are expected to panic on failure - that's their purpose
#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// A managed temporary directory for tests.
///
/// The directory is automatically cleaned up when this struct is dropped.
///
/// # Example
///
/// ```
/// use inferadb_ledger_test_utils::TestDir;
///
/// let dir = TestDir::new();
/// let db_path = dir.join("test.db");
/// // Use db_path for test database...
/// // Directory cleaned up when `dir` goes out of scope
/// ```
pub struct TestDir {
    inner: TempDir,
}

impl TestDir {
    /// Create a new temporary directory.
    ///
    /// # Panics
    ///
    /// Panics if the temporary directory cannot be created.
    #[must_use]
    pub fn new() -> Self {
        let inner = TempDir::new().expect("failed to create temp directory");
        Self { inner }
    }

    /// Returns the path to the temporary directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Join a relative path to the temporary directory.
    ///
    /// This is a convenience method equivalent to `dir.path().join(path)`.
    #[must_use]
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.inner.path().join(path)
    }
}

impl Default for TestDir {
    fn default() -> Self {
        Self::new()
    }
}
