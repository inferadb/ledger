//! Cluster ID persistence.
//!
//! Handles loading and persisting the cluster identifier to disk. The cluster
//! ID is generated once during `init` and persisted to `{data_dir}/cluster_id`.
//! Its presence on disk indicates that this node belongs to an initialized
//! cluster and should resume on restart rather than waiting for `init`.

use std::path::Path;

use snafu::{ResultExt, Snafu};

/// Errors from cluster ID persistence.
#[derive(Debug, Snafu)]
pub enum ClusterIdError {
    /// Failed to read cluster ID from file.
    #[snafu(display("failed to read cluster ID from {path}: {source}"))]
    Read {
        /// The path that could not be read.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// Failed to write cluster ID to file.
    #[snafu(display("failed to write cluster ID to {path}: {source}"))]
    Write {
        /// The path that could not be written.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// Failed to parse cluster ID from file content.
    #[snafu(display("failed to parse cluster ID from {path}: {source}"))]
    Parse {
        /// The path containing invalid content.
        path: String,
        /// The parse error.
        source: std::num::ParseIntError,
    },

    /// Failed to create parent directory.
    #[snafu(display("failed to create directory {path}: {source}"))]
    CreateDir {
        /// The directory path that could not be created.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
}

/// Loads the cluster ID from `{data_dir}/cluster_id`.
///
/// Returns `None` if the file does not exist (node is uninitialized).
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_cluster_id(data_dir: &Path) -> Result<Option<u64>, ClusterIdError> {
    let path = data_dir.join("cluster_id");
    if !path.exists() {
        return Ok(None);
    }
    let path_str = path.display().to_string();
    let content = std::fs::read_to_string(&path).context(ReadSnafu { path: &path_str })?;
    let id: u64 = content.trim().parse().context(ParseSnafu { path: &path_str })?;
    tracing::info!(cluster_id = id, path = %path.display(), "loaded cluster ID from disk");
    Ok(Some(id))
}

/// Persists the cluster ID to `{data_dir}/cluster_id`.
///
/// Creates parent directories if they don't exist.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn write_cluster_id(data_dir: &Path, cluster_id: u64) -> Result<(), ClusterIdError> {
    let path = data_dir.join("cluster_id");
    let path_str = path.display().to_string();

    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)
            .context(CreateDirSnafu { path: parent.display().to_string() })?;
    }

    std::fs::write(&path, cluster_id.to_string()).context(WriteSnafu { path: &path_str })?;
    tracing::info!(cluster_id, path = %path.display(), "persisted cluster ID to disk");
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_load_returns_none_when_file_absent() {
        let temp_dir = TempDir::new().expect("create temp dir");
        assert_eq!(load_cluster_id(temp_dir.path()).expect("load"), None);
    }

    #[test]
    fn test_write_then_load_roundtrips() {
        let temp_dir = TempDir::new().expect("create temp dir");

        write_cluster_id(temp_dir.path(), 42).expect("write");
        let loaded = load_cluster_id(temp_dir.path()).expect("load");

        assert_eq!(loaded, Some(42));
    }

    #[test]
    fn test_write_creates_parent_directories() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let nested = temp_dir.path().join("a").join("b");

        write_cluster_id(&nested, 99).expect("write");
        assert_eq!(load_cluster_id(&nested).expect("load"), Some(99));
    }

    #[test]
    fn test_load_rejects_invalid_content() {
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::write(temp_dir.path().join("cluster_id"), "not-a-number").expect("write");

        let result = load_cluster_id(temp_dir.path());
        assert!(matches!(result, Err(ClusterIdError::Parse { .. })));
    }
}
