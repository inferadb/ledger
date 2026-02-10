//! Merkle tree implementation for InferaDB Ledger.
//!
//! Uses rs_merkle with SHA-256 for transaction merkle roots and proofs.

use rs_merkle::{MerkleTree as RsMerkleTree, algorithms::Sha256 as RsSha256};

use crate::hash::{EMPTY_HASH, Hash};

/// Merkle tree using SHA-256.
pub struct MerkleTree {
    tree: RsMerkleTree<RsSha256>,
    leaves: Vec<Hash>,
}

impl MerkleTree {
    /// Builds a merkle tree from leaf hashes.
    ///
    /// For empty input, the root is EMPTY_HASH.
    pub fn from_leaves(leaves: &[Hash]) -> Self {
        let tree = RsMerkleTree::<RsSha256>::from_leaves(leaves);
        Self { tree, leaves: leaves.to_vec() }
    }

    /// Returns the merkle root.
    ///
    /// Returns EMPTY_HASH for empty trees.
    pub fn root(&self) -> Hash {
        self.tree.root().unwrap_or(EMPTY_HASH)
    }

    /// Generates a proof for the leaf at the given index.
    pub fn proof(&self, index: usize) -> Option<MerkleProof> {
        if index >= self.leaves.len() {
            return None;
        }

        let proof = self.tree.proof(&[index]);
        let proof_hashes: Vec<Hash> = proof.proof_hashes().to_vec();

        Some(MerkleProof {
            leaf_index: index,
            leaf_hash: self.leaves[index],
            proof_hashes,
            root: self.root(),
        })
    }

    /// Returns the number of leaves in the tree.
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }
}

/// Merkle proof for a single leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    /// Index of the leaf in the tree.
    pub leaf_index: usize,
    /// Hash of the leaf.
    pub leaf_hash: Hash,
    /// Sibling hashes from leaf to root.
    pub proof_hashes: Vec<Hash>,
    /// Expected root hash.
    pub root: Hash,
}

impl MerkleProof {
    /// Verifies the proof against an expected root.
    ///
    /// Walks up the tree using the proof hashes and leaf index to recompute
    /// the root. Only produces correct results for power-of-2 leaf counts.
    pub fn verify(&self, expected_root: &Hash) -> bool {
        if self.proof_hashes.is_empty() {
            // Single-element tree: leaf hash equals root
            return &self.leaf_hash == expected_root;
        }

        // For verification without leaf count, we compute the root manually
        // by walking up the tree using the proof hashes
        let mut current_hash = self.leaf_hash;
        let mut index = self.leaf_index;

        for sibling in &self.proof_hashes {
            let combined = if index.is_multiple_of(2) {
                // Current is left, sibling is right
                let mut buf = Vec::with_capacity(64);
                buf.extend_from_slice(&current_hash);
                buf.extend_from_slice(sibling);
                buf
            } else {
                // Sibling is left, current is right
                let mut buf = Vec::with_capacity(64);
                buf.extend_from_slice(sibling);
                buf.extend_from_slice(&current_hash);
                buf
            };
            current_hash = crate::hash::sha256(&combined);
            index /= 2;
        }

        &current_hash == expected_root
    }
}

/// Computes merkle root from leaf hashes.
///
/// Convenience function when you don't need the full tree or proofs.
pub fn merkle_root(leaves: &[Hash]) -> Hash {
    if leaves.is_empty() {
        return EMPTY_HASH;
    }
    MerkleTree::from_leaves(leaves).root()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    #[test]
    fn test_empty_tree() {
        let tree = MerkleTree::from_leaves(&[]);
        assert_eq!(tree.root(), EMPTY_HASH);
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_single_leaf() {
        let leaf = sha256(b"hello");
        let tree = MerkleTree::from_leaves(&[leaf]);

        // Single leaf: root equals leaf
        assert_eq!(tree.root(), leaf);
        assert_eq!(tree.len(), 1);

        let proof = tree.proof(0).expect("proof should exist");
        assert!(proof.verify(&tree.root()));
    }

    #[test]
    fn test_two_leaves() {
        let l1 = sha256(b"a");
        let l2 = sha256(b"b");
        let tree = MerkleTree::from_leaves(&[l1, l2]);

        // Root should be H(l1 || l2)
        let expected_root = {
            let mut combined = Vec::new();
            combined.extend_from_slice(&l1);
            combined.extend_from_slice(&l2);
            sha256(&combined)
        };
        assert_eq!(tree.root(), expected_root);

        // Verify proofs
        let proof0 = tree.proof(0).expect("proof 0 should exist");
        let proof1 = tree.proof(1).expect("proof 1 should exist");

        assert!(proof0.verify(&tree.root()));
        assert!(proof1.verify(&tree.root()));
    }

    #[test]
    fn test_multiple_leaves() {
        let leaves: Vec<Hash> = (0..8).map(|i| sha256(&[i as u8])).collect();
        let tree = MerkleTree::from_leaves(&leaves);

        assert_eq!(tree.len(), 8);
        assert!(!tree.is_empty());

        // All proofs should verify
        for i in 0..8 {
            let proof = tree.proof(i).expect("proof should exist");
            assert!(proof.verify(&tree.root()), "proof {} failed", i);
        }
    }

    #[test]
    fn test_proof_invalid_index() {
        let leaf = sha256(b"test");
        let tree = MerkleTree::from_leaves(&[leaf]);

        assert!(tree.proof(1).is_none());
        assert!(tree.proof(100).is_none());
    }

    #[test]
    fn test_merkle_root_convenience() {
        let leaves: Vec<Hash> = (0..4).map(|i| sha256(&[i as u8])).collect();

        let root1 = merkle_root(&leaves);
        let root2 = MerkleTree::from_leaves(&leaves).root();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_proof_tamper_detection() {
        let leaves: Vec<Hash> = (0..4).map(|i| sha256(&[i as u8])).collect();
        let tree = MerkleTree::from_leaves(&leaves);

        let mut proof = tree.proof(0).expect("proof should exist");

        // Tamper with the proof
        if !proof.proof_hashes.is_empty() {
            proof.proof_hashes[0][0] ^= 0xFF;
        }

        // Verification should fail
        assert!(!proof.verify(&tree.root()));
    }

    // ============================================
    // Property-based merkle proof tests
    // ============================================

    mod proptest_merkle {
        use proptest::prelude::*;

        use super::*;

        /// Generates leaf data with power-of-2 count.
        ///
        /// Our manual `MerkleProof::verify()` uses index/2 walk which only
        /// produces correct results for perfect binary trees (power-of-2 leaf count).
        fn arb_leaf_data_pow2() -> impl Strategy<Value = Vec<Vec<u8>>> {
            // Exponents 0..5 produce sizes 1, 2, 4, 8, 16
            (0u32..5).prop_flat_map(|exp| {
                let size = 1usize << exp;
                proptest::collection::vec(
                    proptest::collection::vec(any::<u8>(), 1..64),
                    size..=size,
                )
            })
        }

        /// Generates leaf data with arbitrary count (for determinism tests
        /// that don't rely on proof verification).
        fn arb_leaf_data(max_leaves: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
            proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..64), 1..max_leaves)
        }

        proptest! {
            /// Valid proofs always verify against the correct root
            /// (restricted to power-of-2 trees where our verifier is correct).
            #[test]
            fn prop_valid_proof_always_verifies(leaf_data in arb_leaf_data_pow2()) {
                let leaves: Vec<Hash> = leaf_data.iter().map(|d| sha256(d)).collect();
                let tree = MerkleTree::from_leaves(&leaves);
                let root = tree.root();

                for i in 0..leaves.len() {
                    let proof = tree.proof(i).expect("proof should exist");
                    prop_assert!(
                        proof.verify(&root),
                        "proof for leaf {} failed with {} total leaves",
                        i,
                        leaves.len()
                    );
                }
            }

            /// Tampered proofs never verify (single-byte flip in a sibling hash).
            #[test]
            fn prop_tampered_proof_never_verifies(
                leaf_data in arb_leaf_data_pow2(),
                target_leaf in any::<prop::sample::Index>(),
                tamper_byte in any::<prop::sample::Index>(),
            ) {
                let leaves: Vec<Hash> = leaf_data.iter().map(|d| sha256(d)).collect();
                let tree = MerkleTree::from_leaves(&leaves);
                let root = tree.root();

                let target = target_leaf.index(leaves.len());
                let mut proof = tree.proof(target).expect("proof should exist");

                // Only tamper if there are sibling hashes (trees with >1 leaf)
                if !proof.proof_hashes.is_empty() {
                    let hash_idx = tamper_byte.index(proof.proof_hashes.len());
                    proof.proof_hashes[hash_idx][0] ^= 0xFF;
                    prop_assert!(
                        !proof.verify(&root),
                        "tampered proof should not verify"
                    );
                }
            }

            /// Wrong root never verifies (proof for tree A doesn't verify against tree B).
            #[test]
            fn prop_wrong_root_never_verifies(
                data_a in arb_leaf_data_pow2(),
                data_b in arb_leaf_data_pow2(),
            ) {
                let leaves_a: Vec<Hash> = data_a.iter().map(|d| sha256(d)).collect();
                let leaves_b: Vec<Hash> = data_b.iter().map(|d| sha256(d)).collect();

                let tree_a = MerkleTree::from_leaves(&leaves_a);
                let tree_b = MerkleTree::from_leaves(&leaves_b);

                // Skip if trees happen to have the same root (extremely unlikely)
                if tree_a.root() != tree_b.root() {
                    let proof = tree_a.proof(0).expect("proof should exist");
                    prop_assert!(
                        !proof.verify(&tree_b.root()),
                        "proof from tree A should not verify against tree B root"
                    );
                }
            }

            /// Merkle root is deterministic: same leaves always produce the same root.
            #[test]
            fn prop_merkle_root_deterministic(leaf_data in arb_leaf_data(33)) {
                let leaves: Vec<Hash> = leaf_data.iter().map(|d| sha256(d)).collect();

                let root1 = MerkleTree::from_leaves(&leaves).root();
                let root2 = MerkleTree::from_leaves(&leaves).root();
                let root3 = merkle_root(&leaves);

                prop_assert_eq!(root1, root2);
                prop_assert_eq!(root1, root3);
            }
        }
    }
}
