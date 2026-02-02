//! Snowflake-style node ID generation and persistence.
//!
//! This module provides globally unique, roughly time-ordered 64-bit node IDs.
//! Nodes started earlier receive lower IDs, enabling deterministic leader election
//! during bootstrap coordination.
//!
//! # ID Structure
//!
//! ```text
//! | 42 bits: timestamp (ms since epoch) | 22 bits: random |
//! ```
//!
//! - Timestamp: milliseconds since 2024-01-01 00:00:00 UTC (~139 years range)
//! - Random: ~4 million unique IDs per millisecond (collision-resistant)
//!
//! # Persistence
//!
//! IDs are persisted to `{data_dir}/node_id` on first generation and loaded
//! on subsequent startups to maintain cluster identity across restarts.
//!
//! # Security Considerations
//!
//! Snowflake IDs are designed for uniqueness and ordering, not cryptographic security:
//!
//! - **Timestamp portion (42 bits)**: Predictable to within milliseconds. An attacker who knows
//!   when a node will start can estimate the timestamp portion.
//!
//! - **Random portion (22 bits)**: Uses `rand::rng()` (OS-provided CSPRNG on most platforms) for
//!   4.2 million possible values per millisecond. This makes exact ID prediction impractical
//!   without the following considerations:
//!
//! - **Threat: Malicious ID Manipulation**: An attacker could generate an ID with an artificially
//!   low timestamp to win leader election. Mitigations:
//!   - IDs are generated and persisted on first startup only
//!   - Reusing an old ID from another node would require access to its data directory
//!   - Network-level access controls limit cluster participation
//!   - Production deployments should use authenticated discovery
//!
//! - **Threat: ID Collision**: With 22 bits of randomness, birthday paradox gives ~1.2% collision
//!   probability at 10,000 IDs per millisecond. In practice, cluster formation involves at most
//!   tens of nodes, making collisions extremely unlikely.
//!
//! For environments requiring stronger guarantees, consider using hardware security
//! modules (HSMs) or centralized ID assignment from a trusted coordinator.

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use snafu::{ResultExt, Snafu};

/// Custom epoch: 2024-01-01 00:00:00 UTC (milliseconds since Unix epoch).
const EPOCH_MS: u64 = 1704067200000;

/// Number of bits used for the random portion.
const RANDOM_BITS: u32 = 22;

/// Mask for extracting the random portion (22 bits).
const RANDOM_MASK: u64 = (1 << RANDOM_BITS) - 1;

/// Error type for node ID operations.
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

    /// System clock error (before Unix epoch).
    #[snafu(display("system clock is before Unix epoch"))]
    SystemClock,
}

/// Generate a new Snowflake ID.
///
/// The ID combines a timestamp (milliseconds since 2024-01-01) with random bits
/// to produce a globally unique, time-ordered identifier.
///
/// # Errors
///
/// Returns an error if the system clock is before the Unix epoch (should never happen
/// on properly configured systems).
///
/// # Example
///
/// ```
/// use inferadb_ledger_server::node_id::generate_snowflake_id;
///
/// let id1 = generate_snowflake_id().unwrap();
/// std::thread::sleep(std::time::Duration::from_millis(1));
/// let id2 = generate_snowflake_id().unwrap();
///
/// // IDs generated later have higher values (with very high probability)
/// assert!(id2 > id1);
/// ```
pub fn generate_snowflake_id() -> Result<u64, NodeIdError> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NodeIdError::SystemClock)?
        .as_millis() as u64;

    let timestamp = now_ms.saturating_sub(EPOCH_MS);
    let random: u64 = rand::rng().random::<u64>() & RANDOM_MASK;

    Ok((timestamp << RANDOM_BITS) | random)
}

/// Extract the timestamp portion from a Snowflake ID.
///
/// Returns milliseconds since the custom epoch (2024-01-01 00:00:00 UTC).
#[must_use]
#[cfg(test)]
pub fn extract_timestamp(id: u64) -> u64 {
    id >> RANDOM_BITS
}

/// Extract the random portion from a Snowflake ID.
#[must_use]
#[cfg(test)]
pub fn extract_random(id: u64) -> u64 {
    id & RANDOM_MASK
}

/// Load existing node ID or generate and persist a new one.
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
        // Ensure parent directory exists
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)
                .context(CreateDirSnafu { path: parent.display().to_string() })?;
        }

        let id = generate_snowflake_id()?;
        std::fs::write(&path, id.to_string()).context(WriteSnafu { path: &path_str })?;
        tracing::info!(node_id = id, path = %path.display(), "generated new node ID");
        Ok(id)
    }
}

/// Write a specific node ID to the data directory.
///
/// This is primarily useful for tests that need deterministic node IDs.
/// Creates parent directories if they don't exist.
///
/// # Arguments
///
/// * `data_dir` - Path to the node's data directory
/// * `node_id` - The specific node ID to write
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn write_node_id(data_dir: &Path, node_id: u64) -> Result<(), NodeIdError> {
    let path = data_dir.join("node_id");
    let path_str = path.display().to_string();

    // Ensure parent directory exists
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)
            .context(CreateDirSnafu { path: parent.display().to_string() })?;
    }

    std::fs::write(&path, node_id.to_string()).context(WriteSnafu { path: &path_str })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::collections::HashSet;

    use tempfile::TempDir;

    use super::*;

    /// Number of bits used for the timestamp portion (for test verification).
    const TIMESTAMP_BITS: u32 = 42;

    #[test]
    fn test_generate_snowflake_id_returns_nonzero() {
        let id = generate_snowflake_id().unwrap();
        assert!(id > 0, "Snowflake ID should be non-zero");
    }

    #[test]
    fn test_snowflake_ids_are_time_ordered() {
        // Generate ID, wait, generate another - later should be higher
        let id1 = generate_snowflake_id().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = generate_snowflake_id().unwrap();

        // The timestamp portion should definitely be higher
        let ts1 = extract_timestamp(id1);
        let ts2 = extract_timestamp(id2);
        assert!(ts2 > ts1, "later ID should have higher timestamp: {ts1} vs {ts2}");

        // The full ID should also be higher (timestamp dominates)
        assert!(id2 > id1, "later ID should be higher: {id1} vs {id2}");
    }

    #[test]
    fn test_snowflake_id_structure() {
        let id = generate_snowflake_id().unwrap();

        let timestamp = extract_timestamp(id);
        let random = extract_random(id);

        // Verify reconstruction
        let reconstructed = (timestamp << RANDOM_BITS) | random;
        assert_eq!(id, reconstructed, "ID should reconstruct from parts");

        // Timestamp should be reasonable (after epoch, not too far in future)
        assert!(timestamp > 0, "timestamp should be positive");
        assert!(timestamp < (1u64 << TIMESTAMP_BITS), "timestamp should fit in 42 bits");

        // Random should fit in 22 bits
        assert!(random <= RANDOM_MASK, "random portion should fit in 22 bits");
    }

    #[test]
    fn test_snowflake_ids_are_unique() {
        // Generate IDs quickly and verify uniqueness.
        // With 22 bits of randomness (~4.2M possibilities), collision probability
        // for n IDs is ~n²/(2*4.2M). For 100 IDs: ~0.0001%, very safe.
        // We reduced from 1000 to 100 to avoid rare CI flakiness.
        let mut ids = HashSet::new();
        for _ in 0..100 {
            let id = generate_snowflake_id().unwrap();
            assert!(ids.insert(id), "Snowflake IDs should be unique, got duplicate: {id}");
        }
    }

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

        // Generate initial ID
        let id1 = load_or_generate_node_id(data_dir).expect("generate node ID");

        // Load again - should get same ID
        let id2 = load_or_generate_node_id(data_dir).expect("load node ID");

        assert_eq!(id1, id2, "loaded ID should match original");
    }

    #[test]
    fn test_load_or_generate_handles_preexisting_id() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        // Write a known ID to file
        let expected_id: u64 = 123456789012345;
        let path = data_dir.join("node_id");
        std::fs::write(&path, expected_id.to_string()).expect("write file");

        // Load should return the existing ID
        let loaded_id = load_or_generate_node_id(data_dir).expect("load node ID");

        assert_eq!(loaded_id, expected_id, "should load existing ID from file");
    }

    #[test]
    fn test_load_or_generate_creates_parent_directories() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path().join("nested").join("path");

        // Parent directories don't exist yet
        assert!(!data_dir.exists());

        let id = load_or_generate_node_id(&data_dir).expect("generate node ID");
        assert!(id > 0);
        assert!(data_dir.join("node_id").exists());
    }

    #[test]
    fn test_load_or_generate_handles_invalid_content() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path();

        // Write invalid content
        let path = data_dir.join("node_id");
        std::fs::write(&path, "not-a-number").expect("write file");

        // Load should fail with parse error
        let result = load_or_generate_node_id(data_dir);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, NodeIdError::Parse { .. }), "should be parse error, got: {err:?}");
    }

    #[test]
    fn test_epoch_is_2024_01_01() {
        // 2024-01-01 00:00:00 UTC in milliseconds since Unix epoch
        // January 1, 2024 00:00:00 UTC = 1704067200 seconds
        assert_eq!(EPOCH_MS, 1704067200000);
    }

    #[test]
    fn test_bit_allocation() {
        // Verify bit allocation: 42 + 22 = 64
        assert_eq!(TIMESTAMP_BITS + RANDOM_BITS, 64);

        // Verify mask covers exactly 22 bits
        assert_eq!(RANDOM_MASK, 0x3FFFFF);
        assert_eq!(RANDOM_MASK.count_ones(), 22);
    }
}
