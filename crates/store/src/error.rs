//! Error types for Inkwell storage engine.

use std::io;

use snafu::Snafu;

/// Page identifier type.
pub type PageId = u64;

/// Result type alias for Inkwell operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during Inkwell operations.
#[derive(Debug, Snafu)]
pub enum Error {
    /// I/O error from the underlying storage backend.
    #[snafu(display("I/O error: {source}"))]
    Io {
        /// The underlying I/O error.
        source: io::Error,
    },

    /// Database file is corrupted or has invalid format.
    #[snafu(display("Corrupted database: {reason}"))]
    Corrupted {
        /// Description of what was corrupted.
        reason: String,
    },

    /// Database header checksum verification failed.
    #[snafu(display("Header checksum mismatch"))]
    HeaderChecksumMismatch,

    /// Page checksum verification failed.
    #[snafu(display("Page {page_id} checksum mismatch"))]
    PageChecksumMismatch {
        /// The page whose checksum failed.
        page_id: PageId,
    },

    /// Invalid magic number in database header.
    #[snafu(display("Invalid database magic number"))]
    InvalidMagic,

    /// Unsupported database format version.
    #[snafu(display("Unsupported format version: {version}"))]
    UnsupportedVersion {
        /// The unsupported version number.
        version: u16,
    },

    /// Page type mismatch (expected different type).
    #[snafu(display("Page type mismatch: expected {expected:?}, found {found:?}"))]
    PageTypeMismatch {
        /// The expected page type.
        expected: PageType,
        /// The actual page type found.
        found: PageType,
    },

    /// Key not found in table.
    #[snafu(display("Key not found"))]
    KeyNotFound,

    /// Page not found.
    #[snafu(display("Page {page_id} not found"))]
    PageNotFound {
        /// The missing page ID.
        page_id: PageId,
    },

    /// Table is full (cannot allocate more pages).
    #[snafu(display("Out of space: cannot allocate more pages"))]
    OutOfSpace,

    /// Write transaction already in progress.
    #[snafu(display("Write transaction already in progress"))]
    WriteTransactionInProgress,

    /// Transaction was aborted.
    #[snafu(display("Transaction aborted"))]
    TransactionAborted,

    /// Invalid table ID.
    #[snafu(display("Invalid table ID: {id}"))]
    InvalidTableId {
        /// The invalid table ID.
        id: u8,
    },

    /// Key too large for inline storage.
    #[snafu(display("Key too large: {size} bytes (max {max})"))]
    KeyTooLarge {
        /// Actual size of the key in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Value too large for inline storage (would need overflow).
    #[snafu(display("Value too large: {size} bytes (max {max})"))]
    ValueTooLarge {
        /// Actual size of the value in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Database is read-only.
    #[snafu(display("Database is read-only"))]
    ReadOnly,

    /// Recovery required after unclean shutdown.
    #[snafu(display("Recovery required"))]
    RecoveryRequired,

    /// Internal lock was poisoned (another thread panicked while holding it).
    #[snafu(display("Internal lock poisoned"))]
    Poisoned,

    /// Page is full and cannot accept more data even after splitting.
    #[snafu(display("Page is full"))]
    PageFull,
}

// Provide automatic conversion from io::Error to Error::Io for ergonomic ? usage
impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::Io { source }
    }
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
            _ => Err(Error::Corrupted { reason: format!("Invalid page type: {}", value) }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_io() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::from(io_err);
        let display = format!("{err}");
        assert!(display.starts_with("I/O error:"), "got: {display}");
    }

    #[test]
    fn test_error_display_corrupted() {
        let err = Error::Corrupted { reason: "bad header".to_string() };
        assert_eq!(format!("{err}"), "Corrupted database: bad header");
    }

    #[test]
    fn test_error_display_header_checksum_mismatch() {
        let err = Error::HeaderChecksumMismatch;
        assert_eq!(format!("{err}"), "Header checksum mismatch");
    }

    #[test]
    fn test_error_display_page_checksum_mismatch() {
        let err = Error::PageChecksumMismatch { page_id: 42 };
        assert_eq!(format!("{err}"), "Page 42 checksum mismatch");
    }

    #[test]
    fn test_error_display_invalid_magic() {
        let err = Error::InvalidMagic;
        assert_eq!(format!("{err}"), "Invalid database magic number");
    }

    #[test]
    fn test_error_display_unsupported_version() {
        let err = Error::UnsupportedVersion { version: 99 };
        assert_eq!(format!("{err}"), "Unsupported format version: 99");
    }

    #[test]
    fn test_error_display_page_type_mismatch() {
        let err =
            Error::PageTypeMismatch { expected: PageType::BTreeLeaf, found: PageType::BTreeBranch };
        let display = format!("{err}");
        assert!(display.contains("BTreeLeaf"), "got: {display}");
        assert!(display.contains("BTreeBranch"), "got: {display}");
    }

    #[test]
    fn test_error_display_key_not_found() {
        let err = Error::KeyNotFound;
        assert_eq!(format!("{err}"), "Key not found");
    }

    #[test]
    fn test_error_display_page_not_found() {
        let err = Error::PageNotFound { page_id: 123 };
        assert_eq!(format!("{err}"), "Page 123 not found");
    }

    #[test]
    fn test_error_display_out_of_space() {
        let err = Error::OutOfSpace;
        assert_eq!(format!("{err}"), "Out of space: cannot allocate more pages");
    }

    #[test]
    fn test_error_display_write_transaction_in_progress() {
        let err = Error::WriteTransactionInProgress;
        assert_eq!(format!("{err}"), "Write transaction already in progress");
    }

    #[test]
    fn test_error_display_transaction_aborted() {
        let err = Error::TransactionAborted;
        assert_eq!(format!("{err}"), "Transaction aborted");
    }

    #[test]
    fn test_error_display_invalid_table_id() {
        let err = Error::InvalidTableId { id: 255 };
        assert_eq!(format!("{err}"), "Invalid table ID: 255");
    }

    #[test]
    fn test_error_display_key_too_large() {
        let err = Error::KeyTooLarge { size: 1000, max: 500 };
        assert_eq!(format!("{err}"), "Key too large: 1000 bytes (max 500)");
    }

    #[test]
    fn test_error_display_value_too_large() {
        let err = Error::ValueTooLarge { size: 2000, max: 1000 };
        assert_eq!(format!("{err}"), "Value too large: 2000 bytes (max 1000)");
    }

    #[test]
    fn test_error_display_read_only() {
        let err = Error::ReadOnly;
        assert_eq!(format!("{err}"), "Database is read-only");
    }

    #[test]
    fn test_error_display_recovery_required() {
        let err = Error::RecoveryRequired;
        assert_eq!(format!("{err}"), "Recovery required");
    }

    #[test]
    fn test_error_display_poisoned() {
        let err = Error::Poisoned;
        assert_eq!(format!("{err}"), "Internal lock poisoned");
    }

    #[test]
    fn test_error_display_page_full() {
        let err = Error::PageFull;
        assert_eq!(format!("{err}"), "Page is full");
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let err: Error = io_err.into();
        // Verify we can use ? operator result
        match err {
            Error::Io { source } => assert_eq!(source.kind(), io::ErrorKind::PermissionDenied),
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_error_source_chain() {
        use std::error::Error as StdError;

        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::from(io_err);

        // snafu should preserve the source chain
        let source = err.source();
        assert!(source.is_some(), "Error::Io should have a source");
    }
}
