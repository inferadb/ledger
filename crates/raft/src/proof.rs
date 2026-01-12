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
}
