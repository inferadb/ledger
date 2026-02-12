//! Storage engine wrapper around [`inferadb-ledger-store`].
//!
//! Provides lifecycle management for the underlying B+ tree database:
//! - File-based ([`StorageEngine`]) and in-memory ([`InMemoryStorageEngine`]) backends
//! - Shared ownership via [`Arc`] for concurrent access across state layer components

use std::{path::Path, sync::Arc};

use inferadb_ledger_store::{Database, FileBackend, InMemoryBackend};
use snafu::Snafu;

/// Errors returned when opening or operating on a [`StorageEngine`].
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum EngineError {
    /// Engine failed to open the storage backend.
    #[snafu(display("Failed to open database at {path}: {source}"))]
    Open { path: String, source: inferadb_ledger_store::Error },

    /// Underlying storage operation failed.
    #[snafu(display("Storage operation failed: {source}"))]
    Storage { source: inferadb_ledger_store::Error },
}

/// File-backed storage engine wrapping [`Database<FileBackend>`].
///
/// Use [`open`](Self::open) to create or open a database at a filesystem path.
/// The database handle is reference-counted; cloning this struct shares the
/// same underlying database.
pub struct StorageEngine {
    db: Arc<Database<FileBackend>>,
}

#[allow(clippy::result_large_err)]
impl StorageEngine {
    /// Opens or creates a database at the given path.
    ///
    /// # Errors
    ///
    /// Returns `EngineError::Open` if the database cannot be opened or created at the given path.
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, EngineError> {
        let path = path.as_ref();
        let db = if path.exists() { Database::open(path) } else { Database::create(path) }
            .map_err(|e| EngineError::Open { path: path.display().to_string(), source: e })?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Returns a shared reference to the underlying database (via [`Arc::clone`]).
    pub fn db(&self) -> Arc<Database<FileBackend>> {
        Arc::clone(&self.db)
    }
}

impl Clone for StorageEngine {
    fn clone(&self) -> Self {
        Self { db: Arc::clone(&self.db) }
    }
}

/// In-memory storage engine wrapping [`Database<InMemoryBackend>`].
///
/// Intended for testing. Data is not persisted across process restarts.
/// The database handle is reference-counted; cloning this struct shares
/// the same underlying database.
pub struct InMemoryStorageEngine {
    db: Arc<Database<InMemoryBackend>>,
}

#[allow(clippy::result_large_err)]
impl InMemoryStorageEngine {
    /// Creates a new in-memory database.
    ///
    /// # Errors
    ///
    /// Returns `EngineError::Open` if the in-memory database cannot be created.
    pub fn open() -> std::result::Result<Self, EngineError> {
        let db = Database::open_in_memory()
            .map_err(|e| EngineError::Open { path: ":memory:".to_string(), source: e })?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Returns a shared reference to the underlying database (via [`Arc::clone`]).
    pub fn db(&self) -> Arc<Database<InMemoryBackend>> {
        Arc::clone(&self.db)
    }
}

impl Clone for InMemoryStorageEngine {
    fn clone(&self) -> Self {
        Self { db: Arc::clone(&self.db) }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use inferadb_ledger_store::tables;

    use super::*;

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
            let value = txn.get::<tables::Entities>(&b"test_key".to_vec()).expect("get");
            assert!(value.is_some());
            assert_eq!(value.unwrap(), b"test_value");
        }
    }
}
