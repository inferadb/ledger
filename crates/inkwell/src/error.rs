//! Error types for Inkwell storage engine.

use std::io;
use thiserror::Error;

/// Page identifier type.
pub type PageId = u64;

/// Result type alias for Inkwell operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during Inkwell operations.
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error from the underlying storage backend.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Database file is corrupted or has invalid format.
    #[error("Corrupted database: {reason}")]
    Corrupted {
        /// Description of what was corrupted.
        reason: String,
    },

    /// Database header checksum verification failed.
    #[error("Header checksum mismatch")]
    HeaderChecksumMismatch,

    /// Page checksum verification failed.
    #[error("Page {page_id} checksum mismatch")]
    PageChecksumMismatch {
        /// The page whose checksum failed.
        page_id: PageId,
    },

    /// Invalid magic number in database header.
    #[error("Invalid database magic number")]
    InvalidMagic,

    /// Unsupported database format version.
    #[error("Unsupported format version: {version}")]
    UnsupportedVersion {
        /// The unsupported version number.
        version: u16,
    },

    /// Page type mismatch (expected different type).
    #[error("Page type mismatch: expected {expected:?}, found {found:?}")]
    PageTypeMismatch {
        /// The expected page type.
        expected: PageType,
        /// The actual page type found.
        found: PageType,
    },

    /// Key not found in table.
    #[error("Key not found")]
    KeyNotFound,

    /// Page not found.
    #[error("Page {page_id} not found")]
    PageNotFound {
        /// The missing page ID.
        page_id: PageId,
    },

    /// Table is full (cannot allocate more pages).
    #[error("Out of space: cannot allocate more pages")]
    OutOfSpace,

    /// Write transaction already in progress.
    #[error("Write transaction already in progress")]
    WriteTransactionInProgress,

    /// Transaction was aborted.
    #[error("Transaction aborted")]
    TransactionAborted,

    /// Invalid table ID.
    #[error("Invalid table ID: {0}")]
    InvalidTableId(u8),

    /// Key too large for inline storage.
    #[error("Key too large: {size} bytes (max {max})")]
    KeyTooLarge {
        /// Actual size of the key in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Value too large for inline storage (would need overflow).
    #[error("Value too large: {size} bytes (max {max})")]
    ValueTooLarge {
        /// Actual size of the value in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Database is read-only.
    #[error("Database is read-only")]
    ReadOnly,

    /// Recovery required after unclean shutdown.
    #[error("Recovery required")]
    RecoveryRequired,

    /// Internal lock was poisoned (another thread panicked while holding it).
    #[error("Internal lock poisoned")]
    Poisoned,

    /// Page is full and cannot accept more data even after splitting.
    #[error("Page is full")]
    PageFull,
}

/// Page types in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Free/unused page.
    Free = 0,
    /// B-tree branch (internal) node.
    BTreeBranch = 1,
    /// B-tree leaf node.
    BTreeLeaf = 2,
    /// Overflow page for large values.
    Overflow = 3,
    /// Free list continuation page.
    FreeList = 4,
    /// Table directory page.
    TableDirectory = 5,
}

impl TryFrom<u8> for PageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Free),
            1 => Ok(Self::BTreeBranch),
            2 => Ok(Self::BTreeLeaf),
            3 => Ok(Self::Overflow),
            4 => Ok(Self::FreeList),
            5 => Ok(Self::TableDirectory),
            _ => Err(Error::Corrupted {
                reason: format!("Invalid page type: {}", value),
            }),
        }
    }
}
