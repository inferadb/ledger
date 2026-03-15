//! Cryptographic verification types: Merkle proofs, block headers, chain proofs.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::proto_util::proto_timestamp_to_system_time;

/// Direction of a sibling in a Merkle proof.
///
/// Indicates whether the sibling hash should be placed on the left or right
/// when computing the parent hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Sibling is on the left: `hash(sibling || current)`.
    Left,
    /// Sibling is on the right: `hash(current || sibling)`.
    Right,
}

impl Direction {
    /// Creates from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::Direction::try_from(value) {
            Ok(proto::Direction::Left) => Direction::Left,
            _ => Direction::Right, // Default to right for unspecified
        }
    }
}

/// A sibling node in a Merkle proof path.
///
/// Each sibling contains the hash of the neighboring node and which side
/// it appears on for hash computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleSibling {
    /// Hash of the sibling node.
    pub hash: Vec<u8>,
    /// Direction (left or right) relative to the current node.
    pub direction: Direction,
}

impl MerkleSibling {
    /// Creates from protobuf type.
    pub(crate) fn from_proto(proto: proto::MerkleSibling) -> Self {
        Self {
            hash: proto.hash.map(|h| h.value).unwrap_or_default(),
            direction: Direction::from_proto(proto.direction),
        }
    }
}

/// Merkle proof for verifying state inclusion.
///
/// Contains the leaf hash and a path of sibling hashes from leaf to root.
/// Used to verify that a value is included in the state tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    /// Hash of the leaf (the entity key-value).
    pub leaf_hash: Vec<u8>,
    /// Sibling hashes from leaf to root (bottom-up order).
    pub siblings: Vec<MerkleSibling>,
}

impl MerkleProof {
    /// Creates from protobuf type.
    pub(crate) fn from_proto(proto: proto::MerkleProof) -> Self {
        Self {
            leaf_hash: proto.leaf_hash.map(|h| h.value).unwrap_or_default(),
            siblings: proto.siblings.into_iter().map(MerkleSibling::from_proto).collect(),
        }
    }

    /// Verifies this proof against an expected state root.
    ///
    /// Recomputes the root hash from the leaf through the sibling path and
    /// checks if it matches the expected root.
    ///
    /// # Arguments
    ///
    /// * `expected_root` - The expected state root hash to verify against.
    ///
    /// # Returns
    ///
    /// `true` if the proof is valid and matches the expected root.
    pub fn verify(&self, expected_root: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.siblings.is_empty() {
            // Single-element tree: leaf hash equals root
            return self.leaf_hash == expected_root;
        }

        let mut current_hash = self.leaf_hash.clone();

        for sibling in &self.siblings {
            let mut hasher = Sha256::new();
            match sibling.direction {
                Direction::Left => {
                    // Sibling is on left: hash(sibling || current)
                    hasher.update(&sibling.hash);
                    hasher.update(&current_hash);
                },
                Direction::Right => {
                    // Sibling is on right: hash(current || sibling)
                    hasher.update(&current_hash);
                    hasher.update(&sibling.hash);
                },
            }
            current_hash = hasher.finalize().to_vec();
        }

        current_hash == expected_root
    }
}

/// Block header containing cryptographic commitments.
///
/// The block header is the cryptographic anchor for all state at a given height.
/// It contains the state root which can be used to verify Merkle proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// Block height (1-indexed).
    pub height: u64,
    /// Organization slug for the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Hash of the previous block header.
    pub previous_hash: Vec<u8>,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Vec<u8>,
    /// Merkle root of the state tree after this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
    /// Node ID of the leader that committed this block.
    pub leader_id: String,
    /// Raft term number.
    pub term: u64,
    /// Raft committed index.
    pub committed_index: u64,
    /// Server-computed hash of this block header.
    ///
    /// Used by [`ChainProof::verify`] to check chain continuity without
    /// recomputing the hash (which requires internal IDs not exposed via API).
    pub block_hash: Vec<u8>,
}

impl BlockHeader {
    /// Creates from protobuf type.
    pub(crate) fn from_proto(proto: proto::BlockHeader) -> Self {
        let timestamp = proto.timestamp.and_then(|ts| proto_timestamp_to_system_time(&ts));

        Self {
            height: proto.height,
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            previous_hash: proto.previous_hash.map(|h| h.value).unwrap_or_default(),
            tx_merkle_root: proto.tx_merkle_root.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
            leader_id: proto.leader_id.map(|n| n.id).unwrap_or_default(),
            term: proto.term,
            committed_index: proto.committed_index,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
        }
    }
}

/// Chain proof linking a trusted height to a response height.
///
/// Used to verify that a block at response_height descends from trusted_height.
/// Contains block headers in ascending order from trusted_height + 1 to response_height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainProof {
    /// Block headers from trusted_height + 1 to response_height (ascending order).
    pub headers: Vec<BlockHeader>,
}

impl ChainProof {
    /// Creates from protobuf type.
    pub(crate) fn from_proto(proto: proto::ChainProof) -> Self {
        Self { headers: proto.headers.into_iter().map(BlockHeader::from_proto).collect() }
    }

    /// Verifies the chain of blocks links correctly.
    ///
    /// Checks that each block's `previous_hash` matches the server-provided
    /// `block_hash` of the preceding block header.
    ///
    /// # Arguments
    ///
    /// * `trusted_header_hash` - Hash of the block at `trusted_height` that the client already
    ///   trusts.
    ///
    /// # Returns
    ///
    /// `true` if all `previous_hash` links chain correctly.
    pub fn verify(&self, trusted_header_hash: &[u8]) -> bool {
        if self.headers.is_empty() {
            return true;
        }

        // First header should link to trusted header
        if self.headers[0].previous_hash != trusted_header_hash {
            return false;
        }

        // Each subsequent header's previous_hash must match the
        // server-provided block_hash of the preceding header.
        for i in 1..self.headers.len() {
            let prev = &self.headers[i - 1];
            let curr = &self.headers[i];

            if prev.block_hash.is_empty() || curr.previous_hash != prev.block_hash {
                return false;
            }
        }

        true
    }
}

/// Options for verified read operations.
///
/// Controls which proofs to include and at what height to read.
#[derive(Debug, Clone, Default)]
pub struct VerifyOpts {
    /// Reads at a specific block height (None = current height).
    pub at_height: Option<u64>,
    /// Include chain proof linking to a trusted height.
    pub include_chain_proof: bool,
    /// Trusted height for chain proof verification.
    pub trusted_height: Option<u64>,
}

impl VerifyOpts {
    /// Creates options with default values (current height, no chain proof).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes a chain proof from a trusted height.
    pub fn with_chain_proof(mut self, trusted_height: u64) -> Self {
        self.include_chain_proof = true;
        self.trusted_height = Some(trusted_height);
        self
    }
}
