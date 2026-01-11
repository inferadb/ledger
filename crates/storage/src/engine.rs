//! redb storage engine wrapper.
//!
//! Provides a thin wrapper around redb with:
//! - Database lifecycle management
//! - Transaction helpers
//! - Table creation on first open

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadTransaction, WriteTransaction};
use snafu::ResultExt;

use crate::tables::Tables;

/// Error context for storage operations.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum EngineError {
    #[snafu(display("Failed to open database at {path}: {source}"))]
    Open {
        path: String,
        source: redb::DatabaseError,
    },

    #[snafu(display("Failed to begin transaction: {source}"))]
    BeginTransaction { source: redb::TransactionError },

    #[snafu(display("Failed to commit transaction: {source}"))]
    Commit { source: redb::CommitError },

    #[snafu(display("Table operation failed: {source}"))]
    Table { source: redb::TableError },

    #[snafu(display("Storage operation failed: {source}"))]
    Storage { source: redb::StorageError },
}

/// Storage engine backed by redb.
pub struct StorageEngine {
    db: Arc<Database>,
}

#[allow(clippy::result_large_err)]
impl StorageEngine {
    /// Open or create a database at the given path.
    ///
    /// Creates all required tables on first open.
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, EngineError> {
        let path = path.as_ref();
        let db = Database::create(path).context(OpenSnafu {
            path: path.display().to_string(),
        })?;

        let engine = Self { db: Arc::new(db) };

        // Initialize tables on first open
        engine.init_tables()?;

        Ok(engine)
    }

    /// Open an in-memory database (for testing).
    pub fn open_in_memory() -> std::result::Result<Self, EngineError> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| EngineError::Open {
                path: ":memory:".to_string(),
                source: e,
            })?;

        let engine = Self { db: Arc::new(db) };
        engine.init_tables()?;

        Ok(engine)
    }

    /// Initialize all tables.
    fn init_tables(&self) -> std::result::Result<(), EngineError> {
        let txn = self.db.begin_write().context(BeginTransactionSnafu)?;

        // Create all tables (no-op if they already exist)
        txn.open_table(Tables::ENTITIES).context(TableSnafu)?;
        txn.open_table(Tables::RELATIONSHIPS).context(TableSnafu)?;
        txn.open_table(Tables::OBJ_INDEX).context(TableSnafu)?;
        txn.open_table(Tables::SUBJ_INDEX).context(TableSnafu)?;
        txn.open_table(Tables::BLOCKS).context(TableSnafu)?;
        txn.open_table(Tables::VAULT_BLOCK_INDEX)
            .context(TableSnafu)?;
        txn.open_table(Tables::RAFT_LOG).context(TableSnafu)?;
        txn.open_table(Tables::RAFT_STATE).context(TableSnafu)?;
        txn.open_table(Tables::VAULT_META).context(TableSnafu)?;
        txn.open_table(Tables::NAMESPACE_META).context(TableSnafu)?;
        txn.open_table(Tables::SEQUENCES).context(TableSnafu)?;
        txn.open_table(Tables::CLIENT_SEQUENCES)
            .context(TableSnafu)?;

        txn.commit().context(CommitSnafu)?;
        Ok(())
    }

    /// Begin a read transaction.
    pub fn begin_read(&self) -> std::result::Result<ReadTxn, EngineError> {
        let txn = self.db.begin_read().context(BeginTransactionSnafu)?;
        Ok(ReadTxn { txn })
    }

    /// Begin a write transaction.
    pub fn begin_write(&self) -> std::result::Result<WriteTxn, EngineError> {
        let txn = self.db.begin_write().context(BeginTransactionSnafu)?;
        Ok(WriteTxn { txn })
    }

    /// Get a clone of the database handle.
    pub fn db(&self) -> Arc<Database> {
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

/// Read-only transaction wrapper.
pub struct ReadTxn {
    txn: ReadTransaction,
}

#[allow(clippy::result_large_err)]
impl ReadTxn {
    /// Access the underlying redb transaction.
    pub fn inner(&self) -> &ReadTransaction {
        &self.txn
    }

    /// Open a table for reading.
    pub fn open_table<K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        definition: redb::TableDefinition<'static, K, V>,
    ) -> std::result::Result<redb::ReadOnlyTable<K, V>, EngineError> {
        self.txn.open_table(definition).context(TableSnafu)
    }
}

/// Write transaction wrapper.
pub struct WriteTxn {
    txn: WriteTransaction,
}

#[allow(clippy::result_large_err)]
impl WriteTxn {
    /// Access the underlying redb transaction.
    pub fn inner(&self) -> &WriteTransaction {
        &self.txn
    }

    /// Access the underlying redb transaction mutably.
    pub fn inner_mut(&mut self) -> &mut WriteTransaction {
        &mut self.txn
    }

    /// Open a table for writing.
    pub fn open_table<K: redb::Key + 'static, V: redb::Value + 'static>(
        &self,
        definition: redb::TableDefinition<'static, K, V>,
    ) -> std::result::Result<redb::Table<'_, K, V>, EngineError> {
        self.txn.open_table(definition).context(TableSnafu)
    }

    /// Commit the transaction.
    pub fn commit(self) -> std::result::Result<(), EngineError> {
        self.txn.commit().context(CommitSnafu)
    }

    /// Abort the transaction (explicit drop).
    pub fn abort(self) {
        // WriteTransaction is aborted on drop
        drop(self.txn);
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

    #[test]
    fn test_open_in_memory() {
        let engine = StorageEngine::open_in_memory().expect("should open");
        let _read = engine.begin_read().expect("should begin read");
        let _write = engine.begin_write().expect("should begin write");
    }

    #[test]
    fn test_tables_created() {
        let engine = StorageEngine::open_in_memory().expect("should open");
        let read = engine.begin_read().expect("should begin read");

        // All tables should exist
        read.open_table(Tables::ENTITIES).expect("entities table");
        read.open_table(Tables::RELATIONSHIPS)
            .expect("relationships table");
        read.open_table(Tables::BLOCKS).expect("blocks table");
        read.open_table(Tables::RAFT_LOG).expect("raft_log table");
    }

    #[test]
    fn test_write_and_read() {
        let engine = StorageEngine::open_in_memory().expect("should open");

        // Write some data
        {
            let mut txn = engine.begin_write().expect("should begin write");
            {
                let mut table = txn.open_table(Tables::ENTITIES).expect("open table");
                table
                    .insert(&b"test_key"[..], &b"test_value"[..])
                    .expect("insert");
            }
            txn.commit().expect("commit");
        }

        // Read it back
        {
            let txn = engine.begin_read().expect("should begin read");
            let table = txn.open_table(Tables::ENTITIES).expect("open table");
            let value = table.get(&b"test_key"[..]).expect("get");
            assert!(value.is_some());
            assert_eq!(value.unwrap().value(), b"test_value");
        }
    }
}
