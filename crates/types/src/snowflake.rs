//! Snowflake-style globally unique ID generation.
//!
//! Generates 64-bit IDs that are globally unique, roughly time-ordered, and
//! monotonically increasing within a single process. Used for both node IDs
//! and organization slugs.
//!
//! # ID Structure
//!
//! ```text
//! | 42 bits: timestamp (ms since epoch) | 22 bits: sequence |
//! ```
//!
//! - **Timestamp**: milliseconds since 2024-01-01 00:00:00 UTC (~139 years range)
//! - **Sequence**: counter within each millisecond (4.2M IDs/ms guaranteed unique)
//!
//! # Thread Safety
//!
//! Uses a global `parking_lot::Mutex` to ensure uniqueness across threads.
//! The lock is held only for the duration of the increment operation.
//!
//! # Security Considerations
//!
//! Snowflake IDs are designed for uniqueness and ordering, not cryptographic
//! security. The timestamp is predictable, and the sequence is deterministic.
//! For environments requiring stronger guarantees, use hardware security
//! modules or centralized ID assignment.

use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use snafu::Snafu;

use crate::types::OrganizationSlug;

/// Custom epoch: 2024-01-01 00:00:00 UTC (milliseconds since Unix epoch).
const EPOCH_MS: u64 = 1_704_067_200_000;

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

/// Errors from Snowflake ID generation.
#[derive(Debug, Snafu)]
pub enum SnowflakeError {
    /// System clock is before the Unix epoch.
    #[snafu(display("system clock is before Unix epoch"))]
    SystemClock,
}

/// Generates a new Snowflake ID.
///
/// Combines a timestamp (milliseconds since 2024-01-01) with a sequence counter
/// to produce a globally unique, time-ordered identifier. The sequence counter
/// guarantees uniqueness for up to 4.2 million IDs per millisecond.
///
/// # Errors
///
/// Returns [`SnowflakeError::SystemClock`] if the system clock is before the
/// Unix epoch.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::snowflake;
/// let id1 = snowflake::generate().unwrap();
/// std::thread::sleep(std::time::Duration::from_millis(1));
/// let id2 = snowflake::generate().unwrap();
/// assert!(id2 > id1);
/// ```
pub fn generate() -> Result<u64, SnowflakeError> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SnowflakeError::SystemClock)?
        .as_millis() as u64;

    let timestamp = now_ms.saturating_sub(EPOCH_MS);

    let mut state = SNOWFLAKE_STATE.lock();

    let sequence = if timestamp > state.last_timestamp {
        // New millisecond — reset sequence
        state.last_timestamp = timestamp;
        state.sequence = 0;
        0
    } else if timestamp == state.last_timestamp {
        // Same millisecond — increment sequence
        state.sequence += 1;
        if state.sequence > SEQUENCE_MASK {
            // Sequence overflow — wait for next millisecond
            // Extremely rare (>4.2M IDs in 1ms) but handled safely
            drop(state);
            std::thread::sleep(std::time::Duration::from_millis(1));
            return generate();
        }
        state.sequence
    } else {
        // Clock went backwards — use last timestamp to maintain monotonicity
        state.sequence += 1;
        if state.sequence > SEQUENCE_MASK {
            drop(state);
            std::thread::sleep(std::time::Duration::from_millis(1));
            return generate();
        }
        state.sequence
    };

    Ok((state.last_timestamp << SEQUENCE_BITS) | sequence)
}

/// Generates a new [`OrganizationSlug`] from a Snowflake ID.
///
/// Convenience wrapper around [`generate()`] that returns the ID wrapped in
/// the `OrganizationSlug` newtype.
///
/// # Errors
///
/// Returns [`SnowflakeError::SystemClock`] if the system clock is before the
/// Unix epoch.
pub fn generate_organization_slug() -> Result<OrganizationSlug, SnowflakeError> {
    generate().map(OrganizationSlug::new)
}

/// Extracts the timestamp portion from a Snowflake ID.
///
/// Returns milliseconds since the custom epoch (2024-01-01 00:00:00 UTC).
#[must_use]
pub fn extract_timestamp(id: u64) -> u64 {
    id >> SEQUENCE_BITS
}

/// Extracts the sequence portion from a Snowflake ID.
///
/// Returns the sequence counter value (0 to 4,194,303).
#[must_use]
pub fn extract_sequence(id: u64) -> u64 {
    id & SEQUENCE_MASK
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Number of bits used for the timestamp portion (for test verification).
    const TIMESTAMP_BITS: u32 = 42;

    #[test]
    fn test_generate_returns_nonzero() {
        let id = generate().unwrap();
        assert!(id > 0, "Snowflake ID should be non-zero");
    }

    #[test]
    fn test_ids_are_time_ordered() {
        let id1 = generate().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = generate().unwrap();

        let ts1 = extract_timestamp(id1);
        let ts2 = extract_timestamp(id2);
        assert!(ts2 > ts1, "later ID should have higher timestamp: {ts1} vs {ts2}");
        assert!(id2 > id1, "later ID should be higher: {id1} vs {id2}");
    }

    #[test]
    fn test_id_structure() {
        let id = generate().unwrap();

        let timestamp = extract_timestamp(id);
        let sequence = extract_sequence(id);

        // Verify reconstruction
        let reconstructed = (timestamp << SEQUENCE_BITS) | sequence;
        assert_eq!(id, reconstructed, "ID should reconstruct from parts");

        // Timestamp should be reasonable
        assert!(timestamp > 0, "timestamp should be positive");
        assert!(timestamp < (1u64 << TIMESTAMP_BITS), "timestamp should fit in 42 bits");

        // Sequence should fit in 22 bits
        assert!(sequence <= SEQUENCE_MASK, "sequence portion should fit in 22 bits");
    }

    #[test]
    fn test_ids_are_unique() {
        let mut ids = HashSet::new();
        for _ in 0..1000 {
            let id = generate().unwrap();
            assert!(ids.insert(id), "Snowflake IDs should be unique, got duplicate: {id}");
        }
    }

    #[test]
    fn test_epoch_is_2024_01_01() {
        // 2024-01-01 00:00:00 UTC = 1704067200 seconds since Unix epoch
        assert_eq!(EPOCH_MS, 1_704_067_200_000);
    }

    #[test]
    fn test_bit_allocation() {
        // 42 + 22 = 64
        assert_eq!(TIMESTAMP_BITS + SEQUENCE_BITS, 64);

        // Mask covers exactly 22 bits
        assert_eq!(SEQUENCE_MASK, 0x3FFFFF);
        assert_eq!(SEQUENCE_MASK.count_ones(), 22);
    }

    #[test]
    fn test_sequence_increments_within_same_millisecond() {
        let id1 = generate().unwrap();
        let id2 = generate().unwrap();
        let id3 = generate().unwrap();

        assert!(id2 > id1, "IDs should be monotonically increasing");
        assert!(id3 > id2, "IDs should be monotonically increasing");

        let ts1 = extract_timestamp(id1);
        let ts2 = extract_timestamp(id2);

        if ts1 == ts2 {
            let seq1 = extract_sequence(id1);
            let seq2 = extract_sequence(id2);
            assert!(
                seq2 > seq1,
                "sequence should increment within same millisecond: {seq1} vs {seq2}"
            );
        }
    }

    #[test]
    fn test_generate_organization_slug() {
        let slug = generate_organization_slug().unwrap();
        assert!(slug.value() > 0, "organization slug should be non-zero");

        // Verify the slug can be decomposed as a valid Snowflake ID
        let timestamp = extract_timestamp(slug.value());
        assert!(timestamp > 0, "slug timestamp should be positive");
    }

    #[test]
    fn test_organization_slugs_are_unique() {
        let mut slugs = HashSet::new();
        for _ in 0..100 {
            let slug = generate_organization_slug().unwrap();
            assert!(slugs.insert(slug.value()), "organization slugs should be unique");
        }
    }

    #[test]
    fn test_extract_timestamp_and_sequence_roundtrip() {
        let id = generate().unwrap();
        let ts = extract_timestamp(id);
        let seq = extract_sequence(id);
        assert_eq!((ts << SEQUENCE_BITS) | seq, id);
    }
}
