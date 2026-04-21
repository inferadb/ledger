//! Zero-copy access to archived consensus entries via rkyv.
//!
//! When entries are stored as rkyv-archived bytes, field access is a
//! pointer cast — no heap allocation or byte-by-byte reconstruction.
//! This eliminates redundant serde cycles from the propose-to-apply path.
//!
//! # Usage
//!
//! ```no_run
//! use inferadb_ledger_consensus::zero_copy::{to_archived_bytes, access_archived};
//! use inferadb_ledger_consensus::types::{ArchivableEntry, EntryKind};
//!
//! let entry = ArchivableEntry {
//!     term: 1,
//!     index: 42,
//!     data: vec![1, 2, 3],
//!     kind: EntryKind::Normal,
//! };
//!
//! // Serialize to rkyv bytes (once, at propose time).
//! let bytes = to_archived_bytes(&entry).expect("serialize");
//!
//! // Zero-copy access on the apply path — no deserialization.
//! let archived = access_archived::<ArchivableEntry>(&bytes).expect("access");
//! assert_eq!(archived.term, 1);
//! assert_eq!(archived.index, 42);
//! ```

use snafu::Snafu;

/// Serializes a value to rkyv-archived bytes.
///
/// The returned [`rkyv::util::AlignedVec`] holds the archived
/// representation and can be stored directly in the WAL or sent over
/// the wire without further encoding.
pub fn to_archived_bytes<T>(value: &T) -> Result<rkyv::util::AlignedVec, ZeroCopyError>
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map_err(|e| ZeroCopyError::Serialize { message: e.to_string() })
}

/// Validates and accesses an archived value from bytes (zero-copy).
///
/// Uses [`rkyv::access`] which validates the byte layout before
/// returning a reference to the archived type. This is the safe API —
/// no `unsafe` code is involved.
pub fn access_archived<T>(bytes: &[u8]) -> Result<&T::Archived, ZeroCopyError>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Portable
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    rkyv::access::<T::Archived, rkyv::rancor::Error>(bytes)
        .map_err(|e| ZeroCopyError::Validate { message: e.to_string() })
}

/// Fully deserializes a value from rkyv-archived bytes.
///
/// Unlike [`access_archived`], this allocates and returns an owned value.
/// Use when you need to mutate the entry or pass it to code that expects
/// the original type.
pub fn from_archived_bytes<T>(bytes: &[u8]) -> Result<T, ZeroCopyError>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Portable
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes)
        .map_err(|e| ZeroCopyError::Deserialize { message: e.to_string() })
}

/// Zero-copy access errors.
#[derive(Debug, Snafu)]
pub enum ZeroCopyError {
    /// Failed to serialize a value to rkyv bytes.
    #[snafu(display("rkyv serialization failed: {message}"))]
    Serialize {
        /// The underlying rkyv error message.
        message: String,
    },

    /// The archived bytes failed validation (corrupted or truncated).
    #[snafu(display("rkyv validation failed: {message}"))]
    Validate {
        /// The underlying rkyv error message.
        message: String,
    },

    /// Failed to fully deserialize from archived bytes.
    #[snafu(display("rkyv deserialization failed: {message}"))]
    Deserialize {
        /// The underlying rkyv error message.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::types::{
        ArchivableEntry, ArchivedArchivableEntry, ArchivedMembership, EntryKind, Membership,
        NodeId, ConsensusStateId,
    };

    #[test]
    fn roundtrip_node_id() {
        let id = NodeId(42);
        let bytes = to_archived_bytes(&id).expect("serialize NodeId");
        let archived = access_archived::<NodeId>(&bytes).expect("access NodeId");
        assert_eq!(archived.0, 42);

        let deserialized: NodeId = from_archived_bytes(&bytes).expect("deserialize NodeId");
        assert_eq!(deserialized, id);
    }

    #[test]
    fn roundtrip_shard_id() {
        let id = ConsensusStateId(99);
        let bytes = to_archived_bytes(&id).expect("serialize ConsensusStateId");
        let archived = access_archived::<ConsensusStateId>(&bytes).expect("access ConsensusStateId");
        assert_eq!(archived.0, 99);

        let deserialized: ConsensusStateId = from_archived_bytes(&bytes).expect("deserialize ConsensusStateId");
        assert_eq!(deserialized, id);
    }

    #[test]
    fn roundtrip_membership() {
        let membership = Membership {
            voters: BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]),
            learners: BTreeSet::from([NodeId(4)]),
        };
        let bytes = to_archived_bytes(&membership).expect("serialize Membership");
        let deserialized: Membership = from_archived_bytes(&bytes).expect("deserialize Membership");
        assert_eq!(deserialized, membership);
    }

    #[test]
    fn roundtrip_entry_kind_normal() {
        let kind = EntryKind::Normal;
        let bytes = to_archived_bytes(&kind).expect("serialize EntryKind");
        let deserialized: EntryKind = from_archived_bytes(&bytes).expect("deserialize EntryKind");
        assert_eq!(deserialized, kind);
    }

    #[test]
    fn roundtrip_entry_kind_membership() {
        let kind = EntryKind::Membership(Membership {
            voters: BTreeSet::from([NodeId(1), NodeId(2)]),
            learners: BTreeSet::new(),
        });
        let bytes = to_archived_bytes(&kind).expect("serialize EntryKind::Membership");
        let deserialized: EntryKind = from_archived_bytes(&bytes).expect("deserialize EntryKind");
        assert_eq!(deserialized, kind);
    }

    #[test]
    fn roundtrip_archivable_entry() {
        let entry = ArchivableEntry {
            term: 5,
            index: 100,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            kind: EntryKind::Normal,
        };
        let bytes = to_archived_bytes(&entry).expect("serialize ArchivableEntry");

        // Zero-copy field access — no deserialization.
        let archived: &ArchivedArchivableEntry =
            access_archived::<ArchivableEntry>(&bytes).expect("access ArchivableEntry");
        assert_eq!(archived.term, 5);
        assert_eq!(archived.index, 100);
        assert_eq!(archived.data.as_slice(), &[0xDE, 0xAD, 0xBE, 0xEF]);

        // Full deserialization roundtrip.
        let deserialized: ArchivableEntry =
            from_archived_bytes(&bytes).expect("deserialize ArchivableEntry");
        assert_eq!(deserialized.term, entry.term);
        assert_eq!(deserialized.index, entry.index);
        assert_eq!(deserialized.data, entry.data);
    }

    #[test]
    fn entry_to_archivable_roundtrip() {
        use std::sync::Arc;

        use crate::types::Entry;

        let entry = Entry {
            term: 3,
            index: 77,
            data: Arc::from(vec![1, 2, 3]),
            kind: EntryKind::Membership(Membership::new([NodeId(10)])),
        };

        // Entry -> ArchivableEntry -> rkyv bytes -> ArchivableEntry -> Entry
        let archivable = ArchivableEntry::from(&entry);
        let bytes = to_archived_bytes(&archivable).expect("serialize");
        let restored_archivable: ArchivableEntry =
            from_archived_bytes(&bytes).expect("deserialize");
        let restored_entry = Entry::from(restored_archivable);

        assert_eq!(restored_entry.term, entry.term);
        assert_eq!(restored_entry.index, entry.index);
        assert_eq!(&*restored_entry.data, &*entry.data);
        assert_eq!(restored_entry.kind, entry.kind);
    }

    #[test]
    fn zero_copy_membership_field_access() {
        let entry = ArchivableEntry {
            term: 2,
            index: 50,
            data: vec![],
            kind: EntryKind::Membership(Membership {
                voters: BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]),
                learners: BTreeSet::from([NodeId(4)]),
            }),
        };
        let bytes = to_archived_bytes(&entry).expect("serialize");
        let archived = access_archived::<ArchivableEntry>(&bytes).expect("access");

        // Access nested membership fields without deserialization.
        match &archived.kind {
            ArchivedEntryKind::Membership(m) => {
                assert_eq!(m.voters.len(), 3);
                assert_eq!(m.learners.len(), 1);
            },
            ArchivedEntryKind::Normal => panic!("expected Membership variant"),
        }
    }

    #[test]
    fn validate_rejects_garbage_bytes() {
        let garbage = vec![0xFF, 0x00, 0x42, 0x13];
        let result = access_archived::<ArchivableEntry>(&garbage);
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_truncated_bytes() {
        let entry = ArchivableEntry {
            term: 1,
            index: 1,
            data: vec![1, 2, 3, 4, 5],
            kind: EntryKind::Normal,
        };
        let bytes = to_archived_bytes(&entry).expect("serialize");

        // Truncate to half the bytes.
        let truncated = &bytes[..bytes.len() / 2];
        let result = access_archived::<ArchivableEntry>(truncated);
        assert!(result.is_err());
    }

    use crate::types::ArchivedEntryKind;

    #[test]
    fn archived_membership_voter_iteration() {
        let membership = Membership {
            voters: BTreeSet::from([NodeId(10), NodeId(20), NodeId(30)]),
            learners: BTreeSet::new(),
        };
        let bytes = to_archived_bytes(&membership).expect("serialize");
        let archived: &ArchivedMembership = access_archived::<Membership>(&bytes).expect("access");

        let voter_ids: Vec<u64> = archived.voters.iter().map(|n| n.0.to_native()).collect();
        assert_eq!(voter_ids, vec![10, 20, 30]);
    }

    #[test]
    fn roundtrip_entry_with_empty_data() {
        let entry = ArchivableEntry { term: 0, index: 0, data: vec![], kind: EntryKind::Normal };
        let bytes = to_archived_bytes(&entry).expect("serialize");
        let archived = access_archived::<ArchivableEntry>(&bytes).expect("access");
        assert!(archived.data.is_empty());

        let deserialized: ArchivableEntry = from_archived_bytes(&bytes).expect("deserialize");
        assert!(deserialized.data.is_empty());
        assert_eq!(deserialized.term, 0);
    }

    #[test]
    fn from_archived_bytes_rejects_garbage() {
        let garbage = vec![0xFF, 0x00, 0x42, 0x13, 0xAB];
        let result = from_archived_bytes::<ArchivableEntry>(&garbage);
        assert!(result.is_err());
    }

    #[test]
    fn from_archived_bytes_rejects_empty_input() {
        let result = from_archived_bytes::<ArchivableEntry>(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn roundtrip_entry_with_large_data() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let entry =
            ArchivableEntry { term: 7, index: 999, data: data.clone(), kind: EntryKind::Normal };
        let bytes = to_archived_bytes(&entry).expect("serialize large entry");

        let archived = access_archived::<ArchivableEntry>(&bytes).expect("access large entry");
        assert_eq!(archived.term, 7);
        assert_eq!(archived.index, 999);
        assert_eq!(archived.data.len(), 1024);
        assert_eq!(archived.data.as_slice(), data.as_slice());

        let deserialized: ArchivableEntry =
            from_archived_bytes(&bytes).expect("deserialize large entry");
        assert_eq!(deserialized.term, entry.term);
        assert_eq!(deserialized.index, entry.index);
        assert_eq!(deserialized.data, data);
        assert!(matches!(deserialized.kind, EntryKind::Normal));
    }

    #[test]
    fn roundtrip_membership_with_empty_sets() {
        let membership = Membership { voters: BTreeSet::new(), learners: BTreeSet::new() };
        let bytes = to_archived_bytes(&membership).expect("serialize");
        let deserialized: Membership = from_archived_bytes(&bytes).expect("deserialize");
        assert!(deserialized.voters.is_empty());
        assert!(deserialized.learners.is_empty());
    }
}
