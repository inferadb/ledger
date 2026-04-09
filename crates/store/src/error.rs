//! Error types for the store engine.

use std::io;

use snafu::Snafu;

/// Page identifier type.
pub type PageId = u64;

/// Result type alias for store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during store operations.
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
        reason: std::borrow::Cow<'static, str>,
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

    /// Encryption/decryption operation failed (wrong key, tampered data, etc).
    #[snafu(display("Encryption error: {reason}"))]
    Encryption {
        /// Description of the encryption failure.
        reason: std::borrow::Cow<'static, str>,
    },

    /// Authentication tag verification failed (data tampered or wrong key).
    #[snafu(display(
        "Authentication failed for page {page_id}: data may be tampered or wrong key"
    ))]
    AuthenticationFailed {
        /// The page whose authentication check failed.
        page_id: PageId,
    },
}

/// Automatic conversion from [`io::Error`] for ergonomic `?` usage.
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
            _ => Err(Error::Corrupted { reason: format!("Invalid page type: {}", value).into() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every Error variant must produce the expected Display string.
    #[test]
    fn test_error_display_all_variants() {
        // (error, expected display substring or exact match)
        let cases: Vec<(Error, &str)> = vec![
            (Error::Corrupted { reason: "bad header".into() }, "Corrupted database: bad header"),
            (Error::HeaderChecksumMismatch, "Header checksum mismatch"),
            (Error::PageChecksumMismatch { page_id: 42 }, "Page 42 checksum mismatch"),
            (Error::InvalidMagic, "Invalid database magic number"),
            (Error::UnsupportedVersion { version: 99 }, "Unsupported format version: 99"),
            (Error::KeyNotFound, "Key not found"),
            (Error::PageNotFound { page_id: 123 }, "Page 123 not found"),
            (Error::OutOfSpace, "Out of space: cannot allocate more pages"),
            (Error::WriteTransactionInProgress, "Write transaction already in progress"),
            (Error::TransactionAborted, "Transaction aborted"),
            (Error::InvalidTableId { id: 255 }, "Invalid table ID: 255"),
            (Error::KeyTooLarge { size: 1000, max: 500 }, "Key too large: 1000 bytes (max 500)"),
            (
                Error::ValueTooLarge { size: 2000, max: 1000 },
                "Value too large: 2000 bytes (max 1000)",
            ),
            (Error::ReadOnly, "Database is read-only"),
            (Error::RecoveryRequired, "Recovery required"),
            (Error::Poisoned, "Internal lock poisoned"),
            (Error::PageFull, "Page is full"),
            (Error::Encryption { reason: "bad key".into() }, "Encryption error: bad key"),
            (
                Error::AuthenticationFailed { page_id: 7 },
                "Authentication failed for page 7: data may be tampered or wrong key",
            ),
        ];

        for (err, expected) in &cases {
            let display = format!("{err}");
            assert!(
                display.contains(expected),
                "Error variant {err:?} displayed as {display:?}, expected to contain {expected:?}"
            );
        }
    }

    /// I/O Error variant Display starts with the expected prefix.
    #[test]
    fn test_error_display_io_prefix() {
        let err = Error::from(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        assert!(format!("{err}").starts_with("I/O error:"));
    }

    /// PageTypeMismatch Display includes both expected and found type names.
    #[test]
    fn test_error_display_page_type_mismatch_includes_both_types() {
        let err =
            Error::PageTypeMismatch { expected: PageType::BTreeLeaf, found: PageType::BTreeBranch };
        let display = format!("{err}");
        assert!(display.contains("BTreeLeaf"), "missing expected type in: {display}");
        assert!(display.contains("BTreeBranch"), "missing found type in: {display}");
    }

    /// From<io::Error> preserves the original error kind.
    #[test]
    fn test_from_io_error_preserves_kind() {
        let err: Error = io::Error::new(io::ErrorKind::PermissionDenied, "access denied").into();
        match err {
            Error::Io { source } => assert_eq!(source.kind(), io::ErrorKind::PermissionDenied),
            other => panic!("Expected Io variant, got {other:?}"),
        }
    }

    /// Error::Io preserves the source chain for error reporting.
    #[test]
    fn test_io_error_has_source() {
        use std::error::Error as StdError;
        let err = Error::from(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        assert!(err.source().is_some(), "Error::Io should have a source");
    }
}
