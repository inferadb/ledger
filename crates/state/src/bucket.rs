//! Bucket-based state commitment for incremental state root computation.
//!
//! # Design Rationale
//!
//! Each vault's entities and relationships are distributed across 256 buckets
//! using `seahash(key) % 256`. This bucketing scheme serves two purposes:
//!
//! 1. **Incremental state roots:** When a write modifies entities in bucket B, only bucket B's hash
//!    needs recomputation — the other 255 bucket roots are unchanged. The vault's `state_root` is
//!    then `SHA-256(bucket_root[0] || ... || bucket_root[255])`. This reduces state root
//!    computation from O(n) to O(k) where k = number of dirty buckets per commit (typically 1-5 for
//!    a write batch).
//!
//! 2. **Key distribution:** Using seahash (a non-cryptographic hash) for bucket assignment provides
//!    uniform distribution across buckets. Cryptographic strength is not needed here because the
//!    bucket assignment has no security implications — SHA-256 is used for the actual commitment
//!    hashes.
//!
//! # Why 256 Buckets?
//!
//! The value 256 balances three concerns:
//! - **Granularity:** With 1M entities, each bucket averages ~3,900 entries. A single write
//!   rehashes only that bucket, not the full dataset.
//! - **State root overhead:** 256 × 32-byte hashes = 8 KB per vault, stored in `VaultCommitment`.
//!   This fits comfortably in L1 cache.
//! - **Snapshot metadata:** Bucket roots are stored in snapshots for incremental state restoration.
//!   256 is compact enough to include in snapshot headers.
//!
//! # Dirty Tracking
//!
//! `VaultCommitment` maintains a `BTreeSet<u8>` of dirty bucket IDs. On each write,
//! the affected bucket is marked dirty via `mark_dirty_by_key()`. At commit time,
//! only dirty buckets are rehashed (by iterating all entities in that bucket from
//! the storage layer), then `clear_dirty()` resets the set for the next batch.

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
    /// Sets of buckets that need rehashing.
    dirty_buckets: BTreeSet<u8>,
}

impl VaultCommitment {
    /// Creates a new vault commitment with all empty buckets.
    ///
    /// All 256 bucket roots are initialized to `EMPTY_HASH`. Use
    /// [`mark_dirty_by_key`](Self::mark_dirty_by_key) after mutations, then
    /// [`compute_state_root`](Self::compute_state_root) to derive the vault's
    /// state root from the dirty buckets.
    pub fn new() -> Self {
        Self { bucket_roots: [EMPTY_HASH; NUM_BUCKETS], dirty_buckets: BTreeSet::new() }
    }

    /// Creates from existing bucket roots (e.g., from snapshot).
    pub fn from_bucket_roots(bucket_roots: [Hash; NUM_BUCKETS]) -> Self {
        Self { bucket_roots, dirty_buckets: BTreeSet::new() }
    }

    /// Marks a bucket as dirty based on a key.
    pub fn mark_dirty_by_key(&mut self, local_key: &[u8]) {
        let bucket = bucket_id(local_key);
        self.dirty_buckets.insert(bucket);
    }

    /// Marks a specific bucket as dirty.
    pub fn mark_dirty(&mut self, bucket: u8) {
        self.dirty_buckets.insert(bucket);
    }

    /// Checks if any buckets are dirty.
    pub fn is_dirty(&self) -> bool {
        !self.dirty_buckets.is_empty()
    }

    /// Returns the set of dirty bucket IDs.
    pub fn dirty_buckets(&self) -> &BTreeSet<u8> {
        &self.dirty_buckets
    }

    /// Updates a bucket's root hash.
    pub fn set_bucket_root(&mut self, bucket: u8, root: Hash) {
        self.bucket_roots[bucket as usize] = root;
    }

    /// Returns a bucket's current root hash.
    pub fn bucket_root(&self, bucket: u8) -> Hash {
        self.bucket_roots[bucket as usize]
    }

    /// Returns all bucket roots.
    pub fn bucket_roots(&self) -> &[Hash; NUM_BUCKETS] {
        &self.bucket_roots
    }

    /// Clears dirty flags (call after computing state root).
    pub fn clear_dirty(&mut self) {
        self.dirty_buckets.clear();
    }

    /// Computes the vault's state root.
    ///
    /// state_root = SHA-256(bucket_root\[0\] || ... || bucket_root\[255\])
    ///
    /// Note: This does NOT recompute dirty bucket roots. Call `update_dirty_buckets`
    /// first to recompute them, or use `compute_state_root_with_scan` which does both.
    pub fn compute_state_root(&self) -> Hash {
        sha256_concat(&self.bucket_roots)
    }

    /// Computes a bucket's root hash from a list of entities.
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
    #[allow(dead_code)] // public API: stored for bucket identification
    bucket_id: u8,
}

impl BucketRootBuilder {
    /// Creates a new builder for a specific bucket.
    pub fn new(bucket_id: u8) -> Self {
        Self { hasher: BucketHasher::new(), bucket_id }
    }

    /// Returns the bucket ID this builder is for.
    #[allow(dead_code)] // public API: accessor for bucket identification
    pub fn bucket_id(&self) -> u8 {
        self.bucket_id
    }

    /// Adds an entity to the bucket hash.
    ///
    /// Entities should be added in lexicographic key order.
    pub fn add_entity(&mut self, entity: &Entity) {
        self.hasher.add_entity(entity);
    }

    /// Finalizes and return the bucket root hash.
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
