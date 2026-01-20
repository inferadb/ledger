//! inferadb-ledger-store: A purpose-built embedded storage engine for InferaDB.
//!
//! inferadb-ledger-store is a simplified B+ tree storage engine designed for InferaDB's
//! specific requirements:
//!
//! - **Fixed schema**: 13 tables known at compile time
//! - **Single writer**: Leverages Raft's serialization (no MVCC needed)
//! - **Append-optimized**: Raft log access patterns
//! - **Checksummed pages**: Crash safety with XXHash verification

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
//! use inferadb_ledger_store::{Database, DatabaseConfig};
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

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]
// All unwraps in this crate are infallible:
// - try_into().unwrap() on slices with pre-validated sizes (e.g., u16::from_le_bytes)
// - write_all().unwrap() on growable Vec<u8> buffers
#![allow(clippy::disallowed_methods)]
// Explicit drops used for clarity in COW page management (releasing borrows)
#![allow(clippy::drop_non_drop)]
// Test code style - allow field reassignment after default
#![cfg_attr(test, allow(clippy::field_reassign_with_default))]
// B+ tree operations use complex return types for split propagation
#![allow(clippy::type_complexity)]
// Low-level page arithmetic uses explicit bounds checking for clarity
#![allow(clippy::manual_range_contains)]
#![allow(clippy::implicit_saturating_sub)]

pub mod backend;
pub mod btree;
pub mod db;
pub mod error;
pub mod page;
pub mod tables;
pub mod transaction;
pub mod types;

// Re-export commonly used types
pub use backend::{
    DEFAULT_PAGE_SIZE, DatabaseHeader, FileBackend, HEADER_SIZE, InMemoryBackend, MAGIC,
    StorageBackend,
};
pub use btree::{BTree, PageProvider};
pub use db::{
    Database, DatabaseConfig, DatabaseStats, ReadTransaction, TableIterator, WriteTransaction,
};
pub use error::{Error, PageId, PageType, Result};
pub use page::{PAGE_HEADER_SIZE, Page, PageAllocator, PageCache};
pub use tables::{Table, TableEntry, TableId};
pub use types::{Key, KeyType, Value};

/// Inkwell format version.
pub const VERSION: u16 = 1;
