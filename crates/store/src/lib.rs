//! inferadb-ledger-store: A purpose-built embedded storage engine for InferaDB.
//!
//! inferadb-ledger-store is a simplified B+ tree storage engine designed for InferaDB's
//! specific requirements:
//!
//! - **Fixed schema**: 15 tables known at compile time
//! - **Single writer**: Leverages Raft's serialization (no write-write MVCC needed)
//! - **Append-optimized**: Raft log access patterns
//! - **Checksummed pages**: Crash safety with XXH3-64 verification
//! - **Dual-slot commit**: Atomic commits via shadow paging (no WAL)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │                Database API                  │
//! │     (open, read, write, commit, etc.)       │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │             Transaction Layer                │
//! │  (ReadTxn: snapshot, WriteTxn: COW+commit)  │
//! └────────────────┬────────────────────────────┘
//!                  │
//! ┌────────────────▼────────────────────────────┐
//! │              B+ Tree Layer                   │
//! │  (get, insert, delete, range, compact)      │
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
//! ```no_run
//! use inferadb_ledger_store::{Database, DatabaseConfig};
//! use inferadb_ledger_store::tables::Entities;
//!
//! // Create an in-memory database
//! let db = Database::open_in_memory()?;
//!
//! // Write transaction
//! let mut txn = db.write()?;
//! txn.insert::<Entities>(&b"key".to_vec(), &b"value".to_vec())?;
//! txn.commit()?;
//!
//! // Read transaction
//! let txn = db.read()?;
//! let value = txn.get::<Entities>(&b"key".to_vec())?;
//! # Ok::<(), inferadb_ledger_store::Error>(())
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
pub mod integrity;
pub mod page;
pub mod tables;
pub mod transaction;
pub mod types;

// Re-export commonly used types
pub use backend::{
    DEFAULT_PAGE_SIZE, DatabaseHeader, FileBackend, HEADER_SIZE, InMemoryBackend, MAGIC,
    StorageBackend,
};
pub use btree::{BTree, CompactionStats, PageProvider};
pub use db::{
    Database, DatabaseConfig, DatabaseStats, ReadTransaction, TableIterator, WriteTransaction,
};
pub use error::{Error, PageId, PageType, Result};
pub use integrity::{IntegrityScrubber, ScrubError, ScrubResult};
pub use page::{PAGE_HEADER_SIZE, Page, PageAllocator, PageCache};
pub use tables::{Table, TableEntry, TableId};
pub use types::{Key, KeyType, Value};

/// Store format version.
pub const VERSION: u16 = 1;
