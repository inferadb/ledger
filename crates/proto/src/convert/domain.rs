//! Domain model conversions: `Entity`, `Relationship`, `BlockRetentionMode/Policy`,
//! `MerkleProof`, `VaultEntry`, and timestamp helpers.

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{
    BlockRetentionMode, BlockRetentionPolicy, VaultSlug, merkle::MerkleProof as InternalMerkleProof,
};

use crate::proto;

// =============================================================================
// Entity conversions (inferadb_ledger_types::Entity -> proto::Entity)
// =============================================================================

/// Converts a domain [`Entity`](inferadb_ledger_types::Entity) reference to its protobuf
/// representation.
///
/// Entity keys are converted from bytes to UTF-8 via lossy conversion.
/// An `expires_at` of 0 (never expires) maps to `None`.
impl From<&inferadb_ledger_types::Entity> for proto::Entity {
    fn from(e: &inferadb_ledger_types::Entity) -> Self {
        proto::Entity {
            key: String::from_utf8_lossy(&e.key).to_string(),
            value: e.value.clone(),
            version: e.version,
            // Convert 0 (never expires) to None
            expires_at: if e.expires_at == 0 { None } else { Some(e.expires_at) },
        }
    }
}

// =============================================================================
// Relationship conversions (inferadb_ledger_types::Relationship -> proto::Relationship)
// =============================================================================

/// Converts a domain [`Relationship`](inferadb_ledger_types::Relationship) reference to its
/// protobuf representation.
impl From<&inferadb_ledger_types::Relationship> for proto::Relationship {
    fn from(r: &inferadb_ledger_types::Relationship) -> Self {
        proto::Relationship {
            resource: r.resource.clone(),
            relation: r.relation.clone(),
            subject: r.subject.clone(),
        }
    }
}

/// Converts an owned domain [`Relationship`](inferadb_ledger_types::Relationship) to its protobuf
/// representation.
impl From<inferadb_ledger_types::Relationship> for proto::Relationship {
    fn from(r: inferadb_ledger_types::Relationship) -> Self {
        proto::Relationship { resource: r.resource, relation: r.relation, subject: r.subject }
    }
}

// =============================================================================
// BlockRetentionPolicy conversions
// =============================================================================

/// Converts a domain [`BlockRetentionMode`] to its protobuf representation.
impl From<BlockRetentionMode> for proto::BlockRetentionMode {
    fn from(mode: BlockRetentionMode) -> Self {
        match mode {
            BlockRetentionMode::Full => proto::BlockRetentionMode::Full,
            BlockRetentionMode::Compacted => proto::BlockRetentionMode::Compacted,
        }
    }
}

/// Converts a protobuf [`BlockRetentionPolicy`](proto::BlockRetentionPolicy) reference to the
/// domain type.
///
/// Unspecified mode defaults to `Full`. Zero `retention_blocks` defaults to 10,000.
impl From<&proto::BlockRetentionPolicy> for BlockRetentionPolicy {
    fn from(proto_policy: &proto::BlockRetentionPolicy) -> Self {
        let mode = match proto_policy.mode() {
            proto::BlockRetentionMode::Unspecified | proto::BlockRetentionMode::Full => {
                BlockRetentionMode::Full
            },
            proto::BlockRetentionMode::Compacted => BlockRetentionMode::Compacted,
        };

        BlockRetentionPolicy {
            mode,
            retention_blocks: if proto_policy.retention_blocks > 0 {
                proto_policy.retention_blocks
            } else {
                10_000 // Default
            },
        }
    }
}

/// Converts an owned domain [`BlockRetentionPolicy`] to its protobuf representation.
impl From<BlockRetentionPolicy> for proto::BlockRetentionPolicy {
    fn from(policy: BlockRetentionPolicy) -> Self {
        proto::BlockRetentionPolicy {
            mode: proto::BlockRetentionMode::from(policy.mode).into(),
            retention_blocks: policy.retention_blocks,
        }
    }
}

// =============================================================================
// MerkleProof conversions (InternalMerkleProof -> proto::MerkleProof)
// =============================================================================

/// Converts a domain [`MerkleProof`](InternalMerkleProof) reference to its protobuf representation.
///
/// The internal format stores raw sibling hashes with the `leaf_index` to
/// determine direction. The proto format explicitly encodes direction for
/// each sibling.
impl From<&InternalMerkleProof> for proto::MerkleProof {
    fn from(internal: &InternalMerkleProof) -> Self {
        use proto::Direction;

        let mut index = internal.leaf_index;
        let mut siblings = Vec::with_capacity(internal.proof_hashes.len());

        for hash in &internal.proof_hashes {
            // If current index is even (left child), sibling is on the right
            // If current index is odd (right child), sibling is on the left
            let direction = if index.is_multiple_of(2) {
                Direction::Right // sibling is right, so we do hash(current || sibling)
            } else {
                Direction::Left // sibling is left, so we do hash(sibling || current)
            };

            siblings.push(proto::MerkleSibling {
                hash: Some(proto::Hash { value: hash.to_vec() }),
                direction: direction.into(),
            });

            // Move up to parent index
            index /= 2;
        }

        proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: internal.leaf_hash.to_vec() }),
            siblings,
        }
    }
}

/// Converts an owned domain [`MerkleProof`](InternalMerkleProof) to its protobuf representation.
impl From<InternalMerkleProof> for proto::MerkleProof {
    fn from(internal: InternalMerkleProof) -> Self {
        (&internal).into()
    }
}

// =============================================================================
// VaultEntry to Block conversion (special case - requires RegionBlock context)
// =============================================================================

/// Converts a [`VaultEntry`](inferadb_ledger_types::VaultEntry) to a proto [`Block`](proto::Block).
///
/// Free function rather than a `From` impl because it requires additional
/// context (the [`RegionBlock`](inferadb_ledger_types::RegionBlock) and vault slug)
/// beyond the `VaultEntry` itself. The caller provides the resolved vault slug
/// since this layer has no access to the slug index.
pub fn vault_entry_to_proto_block(
    entry: &inferadb_ledger_types::VaultEntry,
    region_block: &inferadb_ledger_types::RegionBlock,
    vault: VaultSlug,
) -> proto::Block {
    use prost_types::Timestamp;

    // Convert VaultEntry transactions to proto transactions
    let transactions = entry
        .transactions
        .iter()
        .map(|tx| proto::Transaction {
            id: Some(proto::TxId { id: tx.id.to_vec() }),
            client_id: Some(proto::ClientId { id: tx.client_id.value().to_owned() }),
            sequence: tx.sequence,
            operations: tx.operations.iter().map(|op| op.into()).collect(),
            timestamp: Some(Timestamp {
                seconds: tx.timestamp.timestamp(),
                nanos: tx.timestamp.timestamp_subsec_nanos() as i32,
            }),
        })
        .collect();

    // Build block header
    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
    let header = proto::BlockHeader {
        height: entry.vault_height,
        organization: Some(proto::OrganizationSlug { slug: entry.organization.value() as u64 }),
        vault: Some(vault.into()),
        previous_hash: Some(proto::Hash { value: entry.previous_vault_hash.to_vec() }),
        tx_merkle_root: Some(proto::Hash { value: entry.tx_merkle_root.to_vec() }),
        state_root: Some(proto::Hash { value: entry.state_root.to_vec() }),
        timestamp: Some(Timestamp {
            seconds: region_block.timestamp.timestamp(),
            nanos: region_block.timestamp.timestamp_subsec_nanos() as i32,
        }),
        leader_id: Some(proto::NodeId { id: region_block.leader_id.value().to_owned() }),
        term: region_block.term,
        committed_index: region_block.committed_index,
        block_hash: Some(proto::Hash { value: block_hash.to_vec() }),
    };

    proto::Block { header: Some(header), transactions }
}

// =============================================================================
// Timestamp conversions (chrono::DateTime<Utc> <-> prost_types::Timestamp)
// =============================================================================

/// Converts a chrono [`DateTime<Utc>`] to a [`prost_types::Timestamp`].
pub fn datetime_to_proto_timestamp(dt: &DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

/// Converts a [`prost_types::Timestamp`] to a chrono [`DateTime<Utc>`].
///
/// Returns [`DateTime::UNIX_EPOCH`] if the timestamp cannot be represented.
pub fn proto_timestamp_to_datetime(ts: &prost_types::Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32).unwrap_or(DateTime::UNIX_EPOCH)
}

/// Converts epoch seconds to a [`prost_types::Timestamp`].
pub(crate) fn datetime_from_epoch_secs(epoch_secs: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: epoch_secs, nanos: 0 }
}
