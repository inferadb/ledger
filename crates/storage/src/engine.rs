//! inkwell storage engine wrapper.
//!
//! Provides a thin wrapper around inkwell with:
//! - Database lifecycle management
//! - Convenient constructors

use std::path::Path;
use std::sync::Arc;

use inkwell::{Database, FileBackend, InMemoryBackend};
use snafu::Snafu;

/// Error context for storage operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum EngineError {
    #[snafu(display("Failed to open database at {path}: {source}"))]
    Open {
        path: String,
        source: inkwell::Error,
    },

    #[snafu(display("Storage operation failed: {source}"))]
    Storage { source: inkwell::Error },
}

/// Storage engine backed by inkwell (file-based).
///
/// Wraps an inkwell Database with a FileBackend for persistent storage.
pub struct StorageEngine {
    db: Arc<Database<FileBackend>>,
}

#[allow(clippy::result_large_err)]
impl StorageEngine {
    /// Open or create a database at the given path.
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, EngineError> {
        let path = path.as_ref();
        let db = if path.exists() {
            Database::open(path)
        } else {
            Database::create(path)
        }
        .map_err(|e| EngineError::Open {
            path: path.display().to_string(),
            source: e,
        })?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get a clone of the database handle.
    pub fn db(&self) -> Arc<Database<FileBackend>> {
        Arc::clone(&self.db)
    }
}

impl Clone for StorageEngine {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

/// In-memory storage engine for testing.
///
/// Wraps an inkwell Database with an InMemoryBackend.
pub struct InMemoryStorageEngine {
    db: Arc<Database<InMemoryBackend>>,
}

#[allow(clippy::result_large_err)]
impl InMemoryStorageEngine {
    /// Create a new in-memory database.
    pub fn open() -> std::result::Result<Self, EngineError> {
        let db = Database::open_in_memory().map_err(|e| EngineError::Open {
            path: ":memory:".to_string(),
            source: e,
        })?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get a clone of the database handle.
    pub fn db(&self) -> Arc<Database<InMemoryBackend>> {
        Arc::clone(&self.db)
    }
}

impl Clone for InMemoryStorageEngine {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    unused_mut
)]
mod tests {
    use super::*;
    use inkwell::tables;

    #[test]
    fn test_open_in_memory() {
        let engine = InMemoryStorageEngine::open().expect("should open");
        let db = engine.db();
        let _read = db.read().expect("should begin read");
        let _write = db.write().expect("should begin write");
    }

    #[test]
    fn test_write_and_read() {
        let engine = InMemoryStorageEngine::open().expect("should open");
        let db = engine.db();

        // Write some data
        {
            let mut txn = db.write().expect("should begin write");
            txn.insert::<tables::Entities>(&b"test_key".to_vec(), &b"test_value".to_vec())
                .expect("insert");
            txn.commit().expect("commit");
        }

        // Read it back
        {
            let txn = db.read().expect("should begin read");
            let value = txn
                .get::<tables::Entities>(&b"test_key".to_vec())
                .expect("get");
            assert!(value.is_some());
            assert_eq!(value.unwrap(), b"test_value");
        }
    }
}
