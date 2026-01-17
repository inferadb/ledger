//! Bucket-based state commitment.
//!
//! Each vault's state is divided into 256 buckets using seahash % 256.
//! Only dirty buckets are rehashed when computing state_root.
//!
//! This provides O(k) state root updates where k = number of dirty buckets.

use std::collections::BTreeSet;

use inferadb_ledger_types::{BucketHasher, EMPTY_HASH, Entity, Hash, bucket_id, sha256_concat};

/// Number of buckets per vault.
pub const NUM_BUCKETS: usize = 256;

/// Per-vault bucket commitment tracking.
///
/// Tracks which buckets are dirty and computes state roots efficiently
/// by only rehashing modified buckets.
#[derive(Debug, Clone)]
pub struct VaultCommitment {
    /// Root hash for each bucket (0-255).
    bucket_roots: [Hash; NUM_BUCKETS],
    /// Set of buckets that need rehashing.
    dirty_buckets: BTreeSet<u8>,
}

impl VaultCommitment {
    /// Create a new vault commitment with all empty buckets.
    pub fn new() -> Self {
        Self { bucket_roots: [EMPTY_HASH; NUM_BUCKETS], dirty_buckets: BTreeSet::new() }
    }

    /// Create from existing bucket roots (e.g., from snapshot).
    pub fn from_bucket_roots(bucket_roots: [Hash; NUM_BUCKETS]) -> Self {
        Self { bucket_roots, dirty_buckets: BTreeSet::new() }
    }

    /// Mark a bucket as dirty based on a key.
    pub fn mark_dirty_by_key(&mut self, local_key: &[u8]) {
        let bucket = bucket_id(local_key);
        self.dirty_buckets.insert(bucket);
    }

    /// Mark a specific bucket as dirty.
    pub fn mark_dirty(&mut self, bucket: u8) {
        self.dirty_buckets.insert(bucket);
    }

    /// Check if any buckets are dirty.
    pub fn is_dirty(&self) -> bool {
        !self.dirty_buckets.is_empty()
    }

    /// Get the set of dirty bucket IDs.
    pub fn dirty_buckets(&self) -> &BTreeSet<u8> {
        &self.dirty_buckets
    }

    /// Update a bucket's root hash.
    pub fn set_bucket_root(&mut self, bucket: u8, root: Hash) {
        self.bucket_roots[bucket as usize] = root;
    }

    /// Get a bucket's current root hash.
    pub fn bucket_root(&self, bucket: u8) -> Hash {
        self.bucket_roots[bucket as usize]
    }

    /// Get all bucket roots.
    pub fn bucket_roots(&self) -> &[Hash; NUM_BUCKETS] {
        &self.bucket_roots
    }

    /// Clear dirty flags (call after computing state root).
    pub fn clear_dirty(&mut self) {
        self.dirty_buckets.clear();
    }

    /// Compute the vault's state root.
    ///
    /// state_root = SHA-256(bucket_root\[0\] || ... || bucket_root\[255\])
    ///
    /// Note: This does NOT recompute dirty bucket roots. Call `update_dirty_buckets`
    /// first to recompute them, or use `compute_state_root_with_scan` which does both.
    pub fn compute_state_root(&self) -> Hash {
        sha256_concat(&self.bucket_roots)
    }

    /// Compute a bucket's root hash from a list of entities.
    ///
    /// Entities must be sorted by key in lexicographic order.
    /// Uses streaming hash with length-prefixed encoding per DESIGN.md.
    pub fn compute_bucket_root_from_entities(entities: &[Entity]) -> Hash {
        let mut hasher = BucketHasher::new();
        for entity in entities {
            hasher.add_entity(entity);
        }
        hasher.finalize()
    }
}

impl Default for VaultCommitment {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for computing bucket roots incrementally.
///
/// Use this when scanning a bucket to compute its root hash.
pub struct BucketRootBuilder {
    hasher: BucketHasher,
    #[allow(dead_code)]
    bucket_id: u8,
}

impl BucketRootBuilder {
    /// Create a new builder for a specific bucket.
    pub fn new(bucket_id: u8) -> Self {
        Self { hasher: BucketHasher::new(), bucket_id }
    }

    /// Get the bucket ID this builder is for.
    #[allow(dead_code)]
    pub fn bucket_id(&self) -> u8 {
        self.bucket_id
    }

    /// Add an entity to the bucket hash.
    ///
    /// Entities should be added in lexicographic key order.
    pub fn add_entity(&mut self, entity: &Entity) {
        self.hasher.add_entity(entity);
    }

    /// Finalize and return the bucket root hash.
    pub fn finalize(self) -> Hash {
        self.hasher.finalize()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_new_commitment_all_empty() {
        let commitment = VaultCommitment::new();
        for bucket in 0..NUM_BUCKETS {
            assert_eq!(
                commitment.bucket_root(bucket as u8),
                EMPTY_HASH,
                "bucket {} should be empty",
                bucket
            );
        }
    }

    #[test]
    fn test_mark_dirty() {
        let mut commitment = VaultCommitment::new();
        assert!(!commitment.is_dirty());

        commitment.mark_dirty_by_key(b"test_key");
        assert!(commitment.is_dirty());

        let bucket = bucket_id(b"test_key");
        assert!(commitment.dirty_buckets().contains(&bucket));
    }

    #[test]
    fn test_state_root_empty_vault() {
        let commitment = VaultCommitment::new();
        let root = commitment.compute_state_root();

        // Empty vault: SHA-256(EMPTY_HASH * 256)
        let expected = sha256_concat(&[EMPTY_HASH; NUM_BUCKETS]);
        assert_eq!(root, expected);
    }

    #[test]
    fn test_bucket_root_from_entities() {
        let entities = vec![
            Entity { key: b"key1".to_vec(), value: b"value1".to_vec(), expires_at: 0, version: 1 },
            Entity {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                expires_at: 100,
                version: 2,
            },
        ];

        let root = VaultCommitment::compute_bucket_root_from_entities(&entities);
        assert_ne!(root, EMPTY_HASH);

        // Same entities should produce same root
        let root2 = VaultCommitment::compute_bucket_root_from_entities(&entities);
        assert_eq!(root, root2);
    }

    #[test]
    fn test_bucket_root_order_matters() {
        let e1 = Entity { key: b"a".to_vec(), value: b"1".to_vec(), expires_at: 0, version: 1 };
        let e2 = Entity { key: b"b".to_vec(), value: b"2".to_vec(), expires_at: 0, version: 1 };

        let root1 = VaultCommitment::compute_bucket_root_from_entities(&[e1.clone(), e2.clone()]);
        let root2 = VaultCommitment::compute_bucket_root_from_entities(&[e2, e1]);

        // Order matters for hash computation
        assert_ne!(root1, root2);
    }

    #[test]
    fn test_set_and_get_bucket_root() {
        let mut commitment = VaultCommitment::new();
        let new_root = [42u8; 32];

        commitment.set_bucket_root(5, new_root);
        assert_eq!(commitment.bucket_root(5), new_root);
        assert_eq!(commitment.bucket_root(0), EMPTY_HASH); // Other buckets unchanged
    }

    #[test]
    fn test_clear_dirty() {
        let mut commitment = VaultCommitment::new();
        commitment.mark_dirty(0);
        commitment.mark_dirty(100);
        commitment.mark_dirty(255);

        assert!(commitment.is_dirty());
        commitment.clear_dirty();
        assert!(!commitment.is_dirty());
    }

    #[test]
    fn test_bucket_root_builder() {
        let mut builder = BucketRootBuilder::new(42);
        assert_eq!(builder.bucket_id(), 42);

        builder.add_entity(&Entity {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            expires_at: 0,
            version: 1,
        });

        let root = builder.finalize();
        assert_ne!(root, EMPTY_HASH);
    }
}
