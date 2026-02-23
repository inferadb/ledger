//! Key encoding for the Events B+ tree table.
//!
//! Events are stored with composite 24-byte keys that support efficient
//! per-organization time-range scans:
//!
//! ```text
//! ┌──────────────┬──────────────────┬──────────────┐
//! │ org_id (8BE) │ timestamp_ns (8BE) │ event_hash (8BE) │
//! └──────────────┴──────────────────┴──────────────┘
//! ```
//!
//! Big-endian encoding ensures lexicographic ordering matches numeric ordering,
//! so B+ tree range scans naturally produce chronologically ordered results
//! within each organization.

use inferadb_ledger_types::OrganizationId;

/// Total size of an encoded event key in bytes.
pub const EVENT_KEY_LEN: usize = 24;

/// Encodes an event key from its components.
///
/// Uses raw `i64::to_be_bytes()` for `org_id` (matching `encode_storage_key`
/// in `keys.rs`), `u64::to_be_bytes()` for timestamp, and
/// `seahash::hash(event_id).to_be_bytes()` for the uniqueness suffix.
pub fn encode_event_key(org_id: OrganizationId, timestamp_ns: u64, event_id: &[u8; 16]) -> Vec<u8> {
    let event_hash = seahash::hash(event_id);
    let mut key = Vec::with_capacity(EVENT_KEY_LEN);
    key.extend_from_slice(&org_id.value().to_be_bytes());
    key.extend_from_slice(&timestamp_ns.to_be_bytes());
    key.extend_from_slice(&event_hash.to_be_bytes());
    key
}

/// Decoded components of an event key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEventKey {
    /// Organization ID (raw i64, matching storage key encoding).
    pub org_id: OrganizationId,
    /// Event timestamp in nanoseconds since Unix epoch.
    pub timestamp_ns: u64,
    /// Seahash of the event ID (uniqueness suffix).
    pub event_hash: u64,
}

/// Decodes a 24-byte event key into its components.
///
/// Returns `None` if the key is not exactly 24 bytes.
pub fn decode_event_key(key: &[u8]) -> Option<DecodedEventKey> {
    if key.len() != EVENT_KEY_LEN {
        return None;
    }

    let org_id = i64::from_be_bytes(key[..8].try_into().ok()?);
    let timestamp_ns = u64::from_be_bytes(key[8..16].try_into().ok()?);
    let event_hash = u64::from_be_bytes(key[16..24].try_into().ok()?);

    Some(DecodedEventKey { org_id: OrganizationId::new(org_id), timestamp_ns, event_hash })
}

/// Returns the 8-byte prefix for scanning all events within an organization.
pub fn org_prefix(org_id: OrganizationId) -> [u8; 8] {
    org_id.value().to_be_bytes()
}

/// Returns the 16-byte prefix for scanning events within an organization
/// at or after a specific timestamp.
pub fn org_time_prefix(org_id: OrganizationId, timestamp_ns: u64) -> [u8; 16] {
    let mut prefix = [0u8; 16];
    prefix[..8].copy_from_slice(&org_id.value().to_be_bytes());
    prefix[8..16].copy_from_slice(&timestamp_ns.to_be_bytes());
    prefix
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let org_id = OrganizationId::new(42);
        let timestamp_ns = 1_700_000_000_000_000_000u64;
        let event_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let key = encode_event_key(org_id, timestamp_ns, &event_id);
        assert_eq!(key.len(), EVENT_KEY_LEN);

        let decoded = decode_event_key(&key).expect("should decode");
        assert_eq!(decoded.org_id, org_id);
        assert_eq!(decoded.timestamp_ns, timestamp_ns);
        assert_eq!(decoded.event_hash, seahash::hash(&event_id));
    }

    #[test]
    fn key_ordering_by_org() {
        let event_id = [0u8; 16];
        let ts = 1_000_000u64;

        let key_a = encode_event_key(OrganizationId::new(1), ts, &event_id);
        let key_b = encode_event_key(OrganizationId::new(2), ts, &event_id);

        assert!(key_a < key_b, "org 1 key should sort before org 2 key");
    }

    #[test]
    fn key_ordering_by_timestamp() {
        let org_id = OrganizationId::new(1);
        let event_id = [0u8; 16];

        let key_early = encode_event_key(org_id, 1_000u64, &event_id);
        let key_late = encode_event_key(org_id, 2_000u64, &event_id);

        assert!(key_early < key_late, "earlier timestamp should sort first");
    }

    #[test]
    fn key_ordering_same_nanosecond() {
        let org_id = OrganizationId::new(1);
        let ts = 1_000_000u64;

        let id_a = [1u8; 16];
        let id_b = [2u8; 16];

        let key_a = encode_event_key(org_id, ts, &id_a);
        let key_b = encode_event_key(org_id, ts, &id_b);

        // Different event IDs produce different keys even at the same nanosecond
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn org_prefix_matches_key_start() {
        let org_id = OrganizationId::new(99);
        let prefix = org_prefix(org_id);
        let key = encode_event_key(org_id, 12345, &[7u8; 16]);

        assert_eq!(&key[..8], &prefix[..]);
    }

    #[test]
    fn org_time_prefix_matches_key_start() {
        let org_id = OrganizationId::new(99);
        let ts = 1_700_000_000_000_000_000u64;
        let prefix = org_time_prefix(org_id, ts);
        let key = encode_event_key(org_id, ts, &[7u8; 16]);

        assert_eq!(&key[..16], &prefix[..]);
    }

    #[test]
    fn decode_wrong_length_returns_none() {
        assert!(decode_event_key(&[]).is_none());
        assert!(decode_event_key(&[0u8; 23]).is_none());
        assert!(decode_event_key(&[0u8; 25]).is_none());
    }

    #[test]
    fn zero_org_key() {
        let org_id = OrganizationId::new(0);
        let key = encode_event_key(org_id, 0, &[0u8; 16]);
        let decoded = decode_event_key(&key).expect("should decode");
        assert_eq!(decoded.org_id, OrganizationId::new(0));
        assert_eq!(decoded.timestamp_ns, 0);
    }

    #[test]
    fn max_timestamp_key() {
        let org_id = OrganizationId::new(1);
        let key = encode_event_key(org_id, u64::MAX, &[0u8; 16]);
        let decoded = decode_event_key(&key).expect("should decode");
        assert_eq!(decoded.timestamp_ns, u64::MAX);
    }

    #[test]
    fn max_event_hash() {
        // An event_id that hashes to a large value still round-trips
        let org_id = OrganizationId::new(1);
        let event_id = [0xFF; 16];
        let key = encode_event_key(org_id, 1000, &event_id);
        let decoded = decode_event_key(&key).expect("should decode");
        assert_eq!(decoded.event_hash, seahash::hash(&event_id));
    }

    #[test]
    fn big_endian_detection() {
        // Verify that big-endian encoding is used (most significant byte first)
        let org_id = OrganizationId::new(256); // 0x0000_0000_0000_0100
        let key = encode_event_key(org_id, 0, &[0u8; 16]);

        // In big-endian, 256i64 = [0, 0, 0, 0, 0, 0, 1, 0]
        assert_eq!(key[6], 1);
        assert_eq!(key[7], 0);
        // First 6 bytes should be zero
        assert_eq!(&key[..6], &[0u8; 6]);
    }
}
