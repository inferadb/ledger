//! Inkwell: A purpose-built embedded storage engine for InferaDB.
//!
//! Inkwell is a simplified B+ tree storage engine designed for InferaDB's
//! specific requirements:
//!
//! - **Fixed schema**: 13 tables known at compile time
//! - **Single writer**: Leverages Raft's serialization (no MVCC needed)
//! - **Append-optimized**: Raft log access patterns
//! - **Checksummed pages**: Crash safety with XXHash verification
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │                Database API                  │
//! │  (open, read_txn, write_txn, commit, etc.)  │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │             Transaction Layer                │
//! │   (ReadTxn: snapshot, WriteTxn: COW+WAL)    │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │              B+ Tree Layer                   │
//! │  (get, insert, delete, range, cursor)       │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │              Page Layer                      │
//! │  (allocator, cache, checksum, COW)          │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │            Storage Backend                   │
//! │      (FileBackend / InMemoryBackend)        │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```ignore
//! use inkwell::{Database, DatabaseConfig};
//!
//! // Open or create a database
//! let db = Database::open("my.db", DatabaseConfig::default())?;
//!
//! // Write transaction
//! let mut txn = db.write_txn()?;
//! txn.insert::<RaftLog>(42, &log_entry)?;
//! txn.commit()?;
//!
//! // Read transaction
//! let txn = db.read_txn()?;
//! let entry = txn.get::<RaftLog>(42)?;
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod backend;
pub mod btree;
pub mod db;
pub mod error;
pub mod page;
pub mod tables;
pub mod types;

// Re-export commonly used types
pub use backend::{DatabaseHeader, FileBackend, InMemoryBackend, StorageBackend};
pub use backend::{DEFAULT_PAGE_SIZE, HEADER_SIZE, MAGIC};
pub use btree::{BTree, PageProvider};
pub use db::{Database, DatabaseConfig, DatabaseStats, ReadTransaction, WriteTransaction, TableIterator};
pub use error::{Error, PageId, PageType, Result};
pub use page::{Page, PageAllocator, PageCache, PAGE_HEADER_SIZE};
pub use tables::{Table, TableEntry, TableId};
pub use types::{Key, KeyType, Value};

/// Inkwell format version.
pub const VERSION: u16 = 1;
