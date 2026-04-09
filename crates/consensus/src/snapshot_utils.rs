//! Snapshot file utilities including COW (copy-on-write) file copies.

use std::path::Path;

/// Copies a file using the most efficient method available on the platform.
///
/// On Linux kernels ≥ 4.5 with btrfs or xfs, `std::fs::copy` uses
/// `copy_file_range(2)` which supports server-side COW. On macOS 10.12+ with
/// APFS, the standard library does not yet use `clonefile(2)`, so this falls
/// back to a full copy. Explicit reflink support via the `reflink` crate can
/// be added in the future when the dependency is appropriate.
///
/// # Errors
///
/// Returns an error if the source file does not exist or the copy fails.
///
/// # Example
///
/// ```no_run
/// use std::path::Path;
/// use inferadb_ledger_consensus::snapshot_utils::reflink_or_copy;
///
/// reflink_or_copy(Path::new("/tmp/snapshot.db"), Path::new("/tmp/snapshot.db.bak"))
///     .expect("copy failed");
/// ```
pub fn reflink_or_copy(src: &Path, dst: &Path) -> Result<(), SnapshotUtilError> {
    std::fs::copy(src, dst).map_err(|source| SnapshotUtilError::Copy {
        src: src.display().to_string(),
        dst: dst.display().to_string(),
        message: source.to_string(),
    })?;
    Ok(())
}

/// Errors from snapshot file operations.
#[derive(Debug, snafu::Snafu)]
pub enum SnapshotUtilError {
    /// The file copy failed.
    #[snafu(display("Copy {src} → {dst} failed: {message}"))]
    Copy {
        /// Source path.
        src: String,
        /// Destination path.
        dst: String,
        /// Underlying OS error.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn copies_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.db");
        let dst = dir.path().join("dst.db");

        std::fs::write(&src, b"snapshot data").unwrap();

        reflink_or_copy(&src, &dst).unwrap();

        let contents = std::fs::read(&dst).unwrap();
        assert_eq!(contents, b"snapshot data");
    }

    #[test]
    fn nonexistent_source_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("does_not_exist.db");
        let dst = dir.path().join("dst.db");

        let result = reflink_or_copy(&src, &dst);
        assert!(result.is_err(), "expected error for nonexistent source");
    }
}
