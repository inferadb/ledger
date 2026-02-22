//! Node ID persistence.
//!
//! Handles loading and persisting Snowflake-based node IDs to disk. The core
//! ID generation logic lives in [`inferadb_ledger_types::snowflake`]; this
//! module adds filesystem persistence so nodes maintain identity across
//! restarts.
//!
//! # Persistence
//!
//! IDs are persisted to `{data_dir}/node_id` on first generation and loaded
//! on subsequent startups to maintain cluster identity across restarts.

use std::path::Path;

use snafu::{ResultExt, Snafu};

/// Errors from node ID persistence.
#[derive(Debug, Snafu)]
pub enum NodeIdError {
    /// Failed to read node ID from file.
    #[snafu(display("failed to read node ID from {path}: {source}"))]
    Read {
        /// The path that could not be read.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// Failed to write node ID to file.
    #[snafu(display("failed to write node ID to {path}: {source}"))]
    Write {
        /// The path that could not be written.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// Failed to parse node ID from file content.
    #[snafu(display("failed to parse node ID from {path}: {source}"))]
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

    /// Snowflake ID generation failed.
    #[snafu(display("failed to generate Snowflake ID: {source}"))]
    Generate {
        /// The underlying Snowflake error.
        source: inferadb_ledger_types::snowflake::SnowflakeError,
    },
}

/// Generates a new Snowflake ID.
///
/// Delegates to [`inferadb_ledger_types::snowflake::generate()`].
///
/// # Errors
///
/// Returns an error if the system clock is before the Unix epoch.
pub fn generate_snowflake_id() -> Result<u64, NodeIdError> {
    inferadb_ledger_types::snowflake::generate().context(GenerateSnafu)
}

/// Loads an existing node ID or generates and persists a new one.
///
/// On first startup, generates a new Snowflake ID and persists it to
/// `{data_dir}/node_id`. On subsequent startups, loads the existing ID
/// from the file to maintain cluster identity.
///
/// # Arguments
///
/// * `data_dir` - Path to the node's data directory
///
/// # Errors
///
/// Returns an error if:
/// - The node ID file exists but cannot be read or parsed
/// - A new node ID cannot be written (e.g., permission denied)
/// - The parent directory cannot be created
pub fn load_or_generate_node_id(data_dir: &Path) -> Result<u64, NodeIdError> {
    let path = data_dir.join("node_id");
    let path_str = path.display().to_string();

    if path.exists() {
        let content = std::fs::read_to_string(&path).context(ReadSnafu { path: &path_str })?;
        let id: u64 = content.trim().parse().context(ParseSnafu { path: &path_str })?;
        tracing::info!(node_id = id, path = %path.display(), "loaded existing node ID");
        Ok(id)
    } else {
        ensure_parent_dir(&path)?;

        let id = generate_snowflake_id()?;
        std::fs::write(&path, id.to_string()).context(WriteSnafu { path: &path_str })?;
        tracing::info!(node_id = id, path = %path.display(), "generated new node ID");
        Ok(id)
    }
}

/// Writes a specific node ID to the data directory.
///
/// Primarily useful for tests that need deterministic node IDs.
/// Creates parent directories if they don't exist.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn write_node_id(data_dir: &Path, node_id: u64) -> Result<(), NodeIdError> {
    let path = data_dir.join("node_id");
    let path_str = path.display().to_string();

    ensure_parent_dir(&path)?;

    std::fs::write(&path, node_id.to_string()).context(WriteSnafu { path: &path_str })?;
    Ok(())
}

/// Creates parent directories for the given path if they don't exist.
fn ensure_parent_dir(path: &Path) -> Result<(), NodeIdError> {
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)
            .context(CreateDirSnafu { path: parent.display().to_string() })?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_load_or_generate_creates_new_id() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        let id = load_or_generate_node_id(data_dir).expect("generate node ID");
        assert!(id > 0, "generated ID should be positive");

        // Verify file was created
        let path = data_dir.join("node_id");
        assert!(path.exists(), "node_id file should exist");

        // Verify file content
        let content = std::fs::read_to_string(&path).expect("read file");
        assert_eq!(content.trim().parse::<u64>().expect("parse"), id, "file should contain the ID");
    }

    #[test]
    fn test_load_or_generate_loads_existing_id() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        let id1 = load_or_generate_node_id(data_dir).expect("generate node ID");
        let id2 = load_or_generate_node_id(data_dir).expect("load node ID");

        assert_eq!(id1, id2, "loaded ID should match original");
    }

    #[test]
    fn test_load_or_generate_handles_preexisting_id() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        let expected_id: u64 = 123_456_789_012_345;
        let path = data_dir.join("node_id");
        std::fs::write(&path, expected_id.to_string()).expect("write file");

        let loaded_id = load_or_generate_node_id(data_dir).expect("load node ID");
        assert_eq!(loaded_id, expected_id, "should load existing ID from file");
    }

    #[test]
    fn test_load_or_generate_creates_parent_directories() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path().join("nested").join("path");

        assert!(!data_dir.exists());

        let id = load_or_generate_node_id(&data_dir).expect("generate node ID");
        assert!(id > 0);
        assert!(data_dir.join("node_id").exists());
    }

    #[test]
    fn test_load_or_generate_handles_invalid_content() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        let path = data_dir.join("node_id");
        std::fs::write(&path, "not-a-number").expect("write file");

        let result = load_or_generate_node_id(data_dir);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, NodeIdError::Parse { .. }), "should be parse error, got: {err:?}");
    }
}
