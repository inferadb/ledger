//! Merkle proof utilities for converting between internal and proto formats.
//!
//! Provides helpers for:
//! - Converting `ledger_types::merkle::MerkleProof` to proto `MerkleProof`
//! - Generating transaction proofs from transaction lists
//! - Fetching blocks and generating proofs with proper error handling

use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use ledger_storage::BlockArchive;
use ledger_types::hash::{Hash, tx_hash};
use ledger_types::merkle::{MerkleProof as InternalMerkleProof, MerkleTree};
use ledger_types::{NamespaceId, Transaction, VaultId};

use crate::proto::{self, Direction, MerkleSibling};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during proof generation.
#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)] // Variants contain BlockArchiveError which is 64+ bytes
pub enum ProofError {
    /// Block archive is not available.
    #[snafu(display("block archive not configured"))]
    ArchiveNotAvailable,

    /// Failed to find shard height for the vault block.
    #[snafu(display(
        "block not found: namespace={namespace_id}, vault={vault_id}, height={vault_height}"
    ))]
    BlockNotFound {
        /// The namespace ID that was not found.
        namespace_id: NamespaceId,
        /// The vault ID that was not found.
        vault_id: VaultId,
        /// The vault height that was not found.
        vault_height: u64,
    },

    /// Failed to query the block archive index.
    #[snafu(display("failed to find shard height: {source}"))]
    FindShardHeight {
        /// The underlying block archive error.
        source: ledger_storage::BlockArchiveError,
    },

    /// Failed to read block from archive.
    #[snafu(display("failed to read block at shard height {shard_height}: {source}"))]
    ReadBlock {
        /// The shard height that failed to read.
        shard_height: u64,
        /// The underlying block archive error.
        source: ledger_storage::BlockArchiveError,
    },

    /// Vault entry not found in the block.
    #[snafu(display("vault entry not in block: namespace={namespace_id}, vault={vault_id}"))]
    VaultEntryNotFound {
        /// The namespace ID that was searched for.
        namespace_id: NamespaceId,
        /// The vault ID that was searched for.
        vault_id: VaultId,
    },

    /// Transaction index out of bounds.
    #[snafu(display("transaction index {index} out of bounds (block has {count} transactions)"))]
    TxIndexOutOfBounds {
        /// The requested transaction index.
        index: usize,
        /// The total number of transactions in the block.
        count: usize,
    },

    /// No transactions in the block.
    #[snafu(display("block contains no transactions"))]
    NoTransactions,
}

/// Result type for proof operations.
pub type Result<T> = std::result::Result<T, ProofError>;

// ============================================================================
// Proof Generation with Block Fetching
// ============================================================================

/// Generated proof data including block header and transaction proof.
pub struct WriteProof {
    /// Block header for client verification.
    pub block_header: proto::BlockHeader,
    /// Merkle proof for the transaction.
    pub tx_proof: proto::MerkleProof,
}

/// Generate a write proof by fetching the committed block from the archive.
///
/// This is the main entry point for proof generation after a write commits.
/// It fetches the block, extracts the vault entry, and generates proofs.
#[allow(clippy::result_large_err)] // ProofError contains BlockArchiveError (160+ bytes)
pub fn generate_write_proof(
    archive: &Arc<BlockArchive>,
    namespace_id: NamespaceId,
    vault_id: VaultId,
    vault_height: u64,
    tx_index: usize,
) -> Result<WriteProof> {
    // Find the shard height containing this vault block
    let shard_height = archive
        .find_shard_height(namespace_id, vault_id, vault_height)
        .context(FindShardHeightSnafu)?
        .ok_or(ProofError::BlockNotFound {
            namespace_id,
            vault_id,
            vault_height,
        })?;

    // Read the block
    let block = archive
        .read_block(shard_height)
        .context(ReadBlockSnafu { shard_height })?;

    // Find our vault entry in the block
    let entry = block
        .vault_entries
        .iter()
        .find(|e| e.namespace_id == namespace_id && e.vault_id == vault_id)
        .ok_or(ProofError::VaultEntryNotFound {
            namespace_id,
            vault_id,
        })?;

    // Validate transaction index
    if entry.transactions.is_empty() {
        return Err(ProofError::NoTransactions);
    }
    if tx_index >= entry.transactions.len() {
        return Err(ProofError::TxIndexOutOfBounds {
            index: tx_index,
            count: entry.transactions.len(),
        });
    }

    // Build block header from vault entry
    let block_header = proto::BlockHeader {
        height: entry.vault_height,
        namespace_id: Some(proto::NamespaceId {
            id: entry.namespace_id,
        }),
        vault_id: Some(proto::VaultId { id: entry.vault_id }),
        previous_hash: Some(proto::Hash {
            value: entry.previous_vault_hash.to_vec(),
        }),
        tx_merkle_root: Some(proto::Hash {
            value: entry.tx_merkle_root.to_vec(),
        }),
        state_root: Some(proto::Hash {
            value: entry.state_root.to_vec(),
        }),
        timestamp: Some(prost_types::Timestamp {
            seconds: block.timestamp.timestamp(),
            nanos: block.timestamp.timestamp_subsec_nanos() as i32,
        }),
        leader_id: Some(proto::NodeId {
            id: block.leader_id.clone(),
        }),
        term: block.term,
        committed_index: block.committed_index,
    };

    // Generate transaction proof
    let tx_proof =
        generate_tx_proof(&entry.transactions, tx_index).ok_or(ProofError::TxIndexOutOfBounds {
            index: tx_index,
            count: entry.transactions.len(),
        })?;

    Ok(WriteProof {
        block_header,
        tx_proof,
    })
}

// ============================================================================
// Low-Level Proof Utilities
// ============================================================================

/// Convert an internal MerkleProof to a proto MerkleProof.
///
/// The internal format stores raw sibling hashes with the leaf_index to
/// determine direction. The proto format explicitly encodes direction for
/// each sibling.
pub fn internal_to_proto_proof(internal: &InternalMerkleProof) -> proto::MerkleProof {
    let mut index = internal.leaf_index;
    let mut siblings = Vec::with_capacity(internal.proof_hashes.len());

    for hash in &internal.proof_hashes {
        // If current index is even (left child), sibling is on the right
        // If current index is odd (right child), sibling is on the left
        let direction = if index % 2 == 0 {
            Direction::Right // sibling is right, so we do hash(current || sibling)
        } else {
            Direction::Left // sibling is left, so we do hash(sibling || current)
        };

        siblings.push(MerkleSibling {
            hash: Some(proto::Hash {
                value: hash.to_vec(),
            }),
            direction: direction.into(),
        });

        // Move up to parent index
        index /= 2;
    }

    proto::MerkleProof {
        leaf_hash: Some(proto::Hash {
            value: internal.leaf_hash.to_vec(),
        }),
        siblings,
    }
}

/// Generate a merkle proof for a transaction at the given index.
///
/// Returns `None` if the index is out of bounds or the transaction list is empty.
pub fn generate_tx_proof(
    transactions: &[Transaction],
    tx_index: usize,
) -> Option<proto::MerkleProof> {
    if transactions.is_empty() || tx_index >= transactions.len() {
        return None;
    }

    // Compute leaf hashes
    let leaves: Vec<Hash> = transactions.iter().map(tx_hash).collect();

    // Build merkle tree and generate proof
    let tree = MerkleTree::from_leaves(&leaves);
    let internal_proof = tree.proof(tx_index)?;

    Some(internal_to_proto_proof(&internal_proof))
}

/// Generate a merkle proof for a transaction by its ID.
///
/// Searches the transaction list for a matching ID and generates a proof.
/// Returns `None` if the transaction is not found.
pub fn generate_tx_proof_by_id(
    transactions: &[Transaction],
    tx_id: &[u8; 16],
) -> Option<proto::MerkleProof> {
    let tx_index = transactions.iter().position(|tx| &tx.id == tx_id)?;
    generate_tx_proof(transactions, tx_index)
}

// ============================================================================
// State Proof Generation
// ============================================================================

use ledger_types::{bucket_id, sha256_concat};

/// Generate a state proof for an entity.
///
/// State proofs allow clients to verify entity existence against the state_root
/// without trusting the server. Unlike Merkle proofs, state proofs require
/// providing all 256 bucket roots because the state uses bucket-based hashing.
///
/// # Arguments
///
/// * `entity` - The entity to generate a proof for (key, value, expires_at, version)
/// * `bucket_roots` - All 256 bucket roots for the vault
/// * `block_height` - The block height this proof is valid for
///
/// # Returns
///
/// A StateProof containing the entity data and all bucket roots needed for verification.
pub fn generate_state_proof(
    entity: &ledger_types::Entity,
    bucket_roots: &[Hash; 256],
    block_height: u64,
) -> proto::StateProof {
    let entity_bucket_id = bucket_id(&entity.key) as u32;

    // Collect other bucket roots (excluding the entity's bucket)
    let mut other_bucket_roots: Vec<proto::Hash> = Vec::with_capacity(255);
    for (i, root) in bucket_roots.iter().enumerate() {
        if i != entity_bucket_id as usize {
            other_bucket_roots.push(proto::Hash {
                value: root.to_vec(),
            });
        }
    }

    // Compute state root from all bucket roots
    let state_root = sha256_concat(bucket_roots);

    proto::StateProof {
        key: entity.key.clone(),
        value: entity.value.clone(),
        expires_at: entity.expires_at,
        version: entity.version,
        bucket_id: entity_bucket_id,
        bucket_root: Some(proto::Hash {
            value: bucket_roots[entity_bucket_id as usize].to_vec(),
        }),
        other_bucket_roots,
        block_height,
        state_root: Some(proto::Hash {
            value: state_root.to_vec(),
        }),
    }
}

/// Result of state proof verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateProofVerification {
    /// The proof is valid.
    Valid,
    /// The bucket ID doesn't match seahash(key) % 256.
    InvalidBucketId {
        /// The expected bucket ID from seahash(key) % 256.
        expected: u8,
        /// The actual bucket ID in the proof.
        actual: u32,
    },
    /// The state root doesn't match the computed value.
    InvalidStateRoot,
    /// The proof is missing required fields.
    MissingField(
        /// Description of the missing field.
        &'static str,
    ),
}

/// Verify a state proof against an expected state root.
///
/// This performs client-side verification of a state proof:
/// 1. Verify `bucket_id == seahash(key) % 256`
/// 2. Reconstruct all 256 bucket roots from proof data
/// 3. Compute `state_root = SHA-256(bucket_roots[0..256])`
/// 4. Verify computed state_root matches expected
///
/// # Note
///
/// This does NOT verify that the bucket_root was correctly computed from bucket contents.
/// Full verification would require the server to provide all entities in the bucket,
/// which is expensive. For most use cases, trusting the bucket_root and verifying
/// the state_root is sufficient.
pub fn verify_state_proof(
    proof: &proto::StateProof,
    expected_state_root: &Hash,
) -> StateProofVerification {
    // 1. Verify bucket_id matches key
    let computed_bucket_id = bucket_id(&proof.key);
    if proof.bucket_id != computed_bucket_id as u32 {
        return StateProofVerification::InvalidBucketId {
            expected: computed_bucket_id,
            actual: proof.bucket_id,
        };
    }

    // 2. Check required fields
    let bucket_root = match &proof.bucket_root {
        Some(h) => &h.value,
        None => return StateProofVerification::MissingField("bucket_root"),
    };

    // 3. Reconstruct all 256 bucket roots
    if proof.other_bucket_roots.len() != 255 {
        return StateProofVerification::MissingField("other_bucket_roots (need 255)");
    }

    let mut all_roots: [Hash; 256] = [[0u8; 32]; 256];
    let mut other_idx = 0;
    for i in 0..256 {
        if i == proof.bucket_id as usize {
            // Use the entity's bucket root
            if bucket_root.len() != 32 {
                return StateProofVerification::MissingField("bucket_root (invalid length)");
            }
            all_roots[i].copy_from_slice(bucket_root);
        } else {
            // Use from other_bucket_roots
            let other = &proof.other_bucket_roots[other_idx];
            if other.value.len() != 32 {
                return StateProofVerification::MissingField("other_bucket_roots (invalid length)");
            }
            all_roots[i].copy_from_slice(&other.value);
            other_idx += 1;
        }
    }

    // 4. Compute state root and verify
    let computed_state_root = sha256_concat(&all_roots);
    if &computed_state_root != expected_state_root {
        return StateProofVerification::InvalidStateRoot;
    }

    StateProofVerification::Valid
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_tx(id: u8) -> Transaction {
        Transaction {
            id: [id; 16],
            client_id: format!("client_{}", id),
            sequence: id as u64,
            actor: "test".to_string(),
            operations: vec![],
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn test_single_tx_proof() {
        let txs = vec![make_tx(1)];
        let proof = generate_tx_proof(&txs, 0).unwrap();

        // Single tx: leaf_hash should be the tx hash
        assert!(proof.leaf_hash.is_some());
        assert_eq!(
            proof.leaf_hash.as_ref().unwrap().value,
            tx_hash(&txs[0]).to_vec()
        );

        // Single tx: no siblings needed
        assert!(proof.siblings.is_empty());
    }

    #[test]
    fn test_two_tx_proof() {
        let txs = vec![make_tx(1), make_tx(2)];

        // Proof for first tx
        let proof0 = generate_tx_proof(&txs, 0).unwrap();
        assert_eq!(
            proof0.leaf_hash.as_ref().unwrap().value,
            tx_hash(&txs[0]).to_vec()
        );
        assert_eq!(proof0.siblings.len(), 1);
        // tx[0] is left child, so sibling (tx[1]) is on the right
        assert_eq!(proof0.siblings[0].direction(), Direction::Right);
        assert_eq!(
            proof0.siblings[0].hash.as_ref().unwrap().value,
            tx_hash(&txs[1]).to_vec()
        );

        // Proof for second tx
        let proof1 = generate_tx_proof(&txs, 1).unwrap();
        assert_eq!(
            proof1.leaf_hash.as_ref().unwrap().value,
            tx_hash(&txs[1]).to_vec()
        );
        assert_eq!(proof1.siblings.len(), 1);
        // tx[1] is right child, so sibling (tx[0]) is on the left
        assert_eq!(proof1.siblings[0].direction(), Direction::Left);
    }

    #[test]
    fn test_four_tx_proof() {
        let txs: Vec<Transaction> = (0..4).map(make_tx).collect();

        // All proofs should have 2 siblings (log2(4) = 2)
        for i in 0..4 {
            let proof = generate_tx_proof(&txs, i).unwrap();
            assert_eq!(proof.siblings.len(), 2);
        }
    }

    #[test]
    fn test_proof_by_id() {
        let txs = vec![make_tx(1), make_tx(2), make_tx(3)];
        let target_id = [2u8; 16];

        let proof = generate_tx_proof_by_id(&txs, &target_id).unwrap();
        assert_eq!(
            proof.leaf_hash.as_ref().unwrap().value,
            tx_hash(&txs[1]).to_vec()
        );
    }

    #[test]
    fn test_proof_by_id_not_found() {
        let txs = vec![make_tx(1), make_tx(2)];
        let target_id = [99u8; 16];

        assert!(generate_tx_proof_by_id(&txs, &target_id).is_none());
    }

    #[test]
    fn test_empty_transactions() {
        let txs: Vec<Transaction> = vec![];
        assert!(generate_tx_proof(&txs, 0).is_none());
    }

    #[test]
    fn test_out_of_bounds_index() {
        let txs = vec![make_tx(1)];
        assert!(generate_tx_proof(&txs, 1).is_none());
        assert!(generate_tx_proof(&txs, 100).is_none());
    }

    // ========================================================================
    // State Proof Tests
    // ========================================================================

    use super::{generate_state_proof, verify_state_proof, StateProofVerification};
    use ledger_types::{bucket_id, sha256_concat, Entity, EMPTY_HASH};

    fn make_entity(key: &[u8], value: &[u8]) -> Entity {
        Entity {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at: 0,
            version: 1,
        }
    }

    fn make_bucket_roots() -> [Hash; 256] {
        let mut roots = [EMPTY_HASH; 256];
        // Set some non-empty roots for variety
        for i in 0..10 {
            roots[i] = [i as u8; 32];
        }
        roots
    }

    #[test]
    fn test_generate_state_proof() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();

        let proof = generate_state_proof(&entity, &bucket_roots, 100);

        assert_eq!(proof.key, b"test_key");
        assert_eq!(proof.value, b"test_value");
        assert_eq!(proof.expires_at, 0);
        assert_eq!(proof.version, 1);
        assert_eq!(proof.block_height, 100);
        assert_eq!(proof.bucket_id, bucket_id(b"test_key") as u32);
        assert_eq!(proof.other_bucket_roots.len(), 255);

        // Verify state_root matches computed value
        let expected_state_root = sha256_concat(&bucket_roots);
        assert_eq!(
            proof.state_root.as_ref().unwrap().value,
            expected_state_root.to_vec()
        );
    }

    #[test]
    fn test_verify_state_proof_valid() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();
        let expected_state_root = sha256_concat(&bucket_roots);

        let proof = generate_state_proof(&entity, &bucket_roots, 100);
        let result = verify_state_proof(&proof, &expected_state_root);

        assert_eq!(result, StateProofVerification::Valid);
    }

    #[test]
    fn test_verify_state_proof_invalid_bucket_id() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();
        let expected_state_root = sha256_concat(&bucket_roots);

        let mut proof = generate_state_proof(&entity, &bucket_roots, 100);
        // Tamper with bucket_id
        proof.bucket_id = (proof.bucket_id + 1) % 256;

        let result = verify_state_proof(&proof, &expected_state_root);

        match result {
            StateProofVerification::InvalidBucketId { expected, actual } => {
                assert_eq!(expected, bucket_id(b"test_key"));
                assert_eq!(actual, (bucket_id(b"test_key") as u32 + 1) % 256);
            }
            _ => panic!("Expected InvalidBucketId, got {:?}", result),
        }
    }

    #[test]
    fn test_verify_state_proof_invalid_state_root() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();
        let wrong_state_root = [42u8; 32]; // Wrong state root

        let proof = generate_state_proof(&entity, &bucket_roots, 100);
        let result = verify_state_proof(&proof, &wrong_state_root);

        assert_eq!(result, StateProofVerification::InvalidStateRoot);
    }

    #[test]
    fn test_verify_state_proof_missing_bucket_root() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();
        let expected_state_root = sha256_concat(&bucket_roots);

        let mut proof = generate_state_proof(&entity, &bucket_roots, 100);
        proof.bucket_root = None;

        let result = verify_state_proof(&proof, &expected_state_root);

        assert_eq!(
            result,
            StateProofVerification::MissingField("bucket_root")
        );
    }

    #[test]
    fn test_verify_state_proof_wrong_other_bucket_count() {
        let entity = make_entity(b"test_key", b"test_value");
        let bucket_roots = make_bucket_roots();
        let expected_state_root = sha256_concat(&bucket_roots);

        let mut proof = generate_state_proof(&entity, &bucket_roots, 100);
        proof.other_bucket_roots.pop(); // Remove one, making it 254 instead of 255

        let result = verify_state_proof(&proof, &expected_state_root);

        assert_eq!(
            result,
            StateProofVerification::MissingField("other_bucket_roots (need 255)")
        );
    }

    #[test]
    fn test_state_proof_different_keys_different_buckets() {
        // Keys should hash to different buckets
        let entity1 = make_entity(b"key_a", b"value_a");
        let entity2 = make_entity(b"key_b", b"value_b");

        let bucket1 = bucket_id(b"key_a");
        let bucket2 = bucket_id(b"key_b");

        // They might be the same by chance, but usually different
        // This test just ensures the proof generation works for different keys
        let bucket_roots = make_bucket_roots();
        let state_root = sha256_concat(&bucket_roots);

        let proof1 = generate_state_proof(&entity1, &bucket_roots, 1);
        let proof2 = generate_state_proof(&entity2, &bucket_roots, 2);

        assert_eq!(proof1.bucket_id, bucket1 as u32);
        assert_eq!(proof2.bucket_id, bucket2 as u32);

        // Both should verify against the same state root
        assert_eq!(verify_state_proof(&proof1, &state_root), StateProofVerification::Valid);
        assert_eq!(verify_state_proof(&proof2, &state_root), StateProofVerification::Valid);
    }
}
