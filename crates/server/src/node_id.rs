//! Snowflake-style node ID generation and persistence.
//!
//! Generates globally unique, roughly time-ordered 64-bit node IDs. Nodes
//! started earlier receive lower IDs, enabling deterministic leader election
//! during bootstrap coordination.
//!
//! # ID Structure
//!
//! ```text
//! | 42 bits: timestamp (ms since epoch) | 22 bits: sequence |
//! ```
//!
//! - Timestamp: milliseconds since 2024-01-01 00:00:00 UTC (~139 years range)
//! - Sequence: counter that increments within each millisecond (4.2M IDs/ms guaranteed unique)
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
//! - **Sequence portion (22 bits)**: Deterministic counter within each millisecond. IDs are
//!   predictable if the generation time and sequence position are known.
//!
//! - **Threat: Malicious ID Manipulation**: An attacker could generate an ID with an artificially
//!   low timestamp to win leader election. Mitigations:
//!   - IDs are generated and persisted on first startup only
//!   - Reusing an old ID from another node would require access to its data directory
//!   - Network-level access controls limit cluster participation
//!   - Production deployments should use authenticated discovery
//!
//! - **Uniqueness Guarantee**: Unlike random-based approaches, the sequence counter guarantees no
//!   collisions within the same process. Cross-process collisions are still possible if multiple
//!   processes generate IDs at the exact same millisecond, but this is extremely rare in practice
//!   since node ID generation happens once at startup.
//!
//! For environments requiring stronger guarantees, consider using hardware security
//! modules (HSMs) or centralized ID assignment from a trusted coordinator.

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

/// Custom epoch: 2024-01-01 00:00:00 UTC (milliseconds since Unix epoch).
const EPOCH_MS: u64 = 1704067200000;

/// Number of bits used for the sequence portion.
const SEQUENCE_BITS: u32 = 22;

/// Mask for extracting the sequence portion (22 bits).
const SEQUENCE_MASK: u64 = (1 << SEQUENCE_BITS) - 1;

/// State for sequence-based ID generation.
struct SnowflakeState {
    /// Last timestamp used for ID generation.
    last_timestamp: u64,
    /// Sequence counter within the current millisecond.
    sequence: u64,
}

/// Global state for thread-safe ID generation.
static SNOWFLAKE_STATE: Mutex<SnowflakeState> =
    Mutex::new(SnowflakeState { last_timestamp: 0, sequence: 0 });

/// Errors from Snowflake ID generation and persistence.
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

/// Generates a new Snowflake ID.
///
/// The ID combines a timestamp (milliseconds since 2024-01-01) with a sequence counter
/// to produce a globally unique, time-ordered identifier. The sequence counter guarantees
/// uniqueness for up to 4.2 million IDs per millisecond.
///
/// # Errors
///
/// Returns an error if the system clock is before the Unix epoch (should never happen
/// on properly configured systems).
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_server::node_id::generate_snowflake_id;
///
/// let id1 = generate_snowflake_id().unwrap();
/// std::thread::sleep(std::time::Duration::from_millis(1));
/// let id2 = generate_snowflake_id().unwrap();
///
/// // IDs generated later have higher values (guaranteed)
/// assert!(id2 > id1);
/// ```
pub fn generate_snowflake_id() -> Result<u64, NodeIdError> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NodeIdError::SystemClock)?
        .as_millis() as u64;

    let timestamp = now_ms.saturating_sub(EPOCH_MS);

    let mut state = SNOWFLAKE_STATE.lock();

    let sequence = if timestamp > state.last_timestamp {
        // New millisecond - reset sequence
        state.last_timestamp = timestamp;
        state.sequence = 0;
        0
    } else if timestamp == state.last_timestamp {
        // Same millisecond - increment sequence
        state.sequence += 1;
        if state.sequence > SEQUENCE_MASK {
            // Sequence overflow - wait for next millisecond
            // This is extremely rare (>4.2M IDs in 1ms) but we handle it safely
            drop(state); // Release lock while waiting
            std::thread::sleep(std::time::Duration::from_millis(1));
            return generate_snowflake_id(); // Recurse with new timestamp
        }
        state.sequence
    } else {
        // Clock went backwards - use last timestamp to maintain monotonicity
        state.sequence += 1;
        if state.sequence > SEQUENCE_MASK {
            drop(state);
            std::thread::sleep(std::time::Duration::from_millis(1));
            return generate_snowflake_id();
        }
        state.sequence
    };

    Ok((state.last_timestamp << SEQUENCE_BITS) | sequence)
}

/// Extracts the timestamp portion from a Snowflake ID.
///
/// Returns milliseconds since the custom epoch (2024-01-01 00:00:00 UTC).
#[must_use]
#[cfg(test)]
pub fn extract_timestamp(id: u64) -> u64 {
    id >> SEQUENCE_BITS
}

/// Extracts the sequence portion from a Snowflake ID.
#[must_use]
#[cfg(test)]
pub fn extract_sequence(id: u64) -> u64 {
    id & SEQUENCE_MASK
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

/// Writes a specific node ID to the data directory.
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
        let sequence = extract_sequence(id);

        // Verify reconstruction
        let reconstructed = (timestamp << SEQUENCE_BITS) | sequence;
        assert_eq!(id, reconstructed, "ID should reconstruct from parts");

        // Timestamp should be reasonable (after epoch, not too far in future)
        assert!(timestamp > 0, "timestamp should be positive");
        assert!(timestamp < (1u64 << TIMESTAMP_BITS), "timestamp should fit in 42 bits");

        // Sequence should fit in 22 bits
        assert!(sequence <= SEQUENCE_MASK, "sequence portion should fit in 22 bits");
    }

    #[test]
    fn test_snowflake_ids_are_unique() {
        // Generate IDs quickly and verify uniqueness.
        // With a sequence counter, uniqueness is guaranteed within the same process.
        // Test with 1000 IDs to verify the sequence counter works correctly.
        let mut ids = HashSet::new();
        for _ in 0..1000 {
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
        assert_eq!(TIMESTAMP_BITS + SEQUENCE_BITS, 64);

        // Verify mask covers exactly 22 bits
        assert_eq!(SEQUENCE_MASK, 0x3FFFFF);
        assert_eq!(SEQUENCE_MASK.count_ones(), 22);
    }

    #[test]
    fn test_sequence_increments_within_same_millisecond() {
        // Generate multiple IDs rapidly - sequence should increment
        let id1 = generate_snowflake_id().unwrap();
        let id2 = generate_snowflake_id().unwrap();
        let id3 = generate_snowflake_id().unwrap();

        // All IDs should be unique and increasing
        assert!(id2 > id1, "IDs should be monotonically increasing");
        assert!(id3 > id2, "IDs should be monotonically increasing");

        // If they're in the same millisecond, sequence should increment
        let ts1 = extract_timestamp(id1);
        let ts2 = extract_timestamp(id2);

        if ts1 == ts2 {
            let seq1 = extract_sequence(id1);
            let seq2 = extract_sequence(id2);
            assert_eq!(seq2, seq1 + 1, "sequence should increment within same millisecond");
        }
    }
}
