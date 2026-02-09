//! Cryptographic hashing functions for InferaDB Ledger.
//!
//! All hashing uses SHA-256 per DESIGN.md. This module provides:
//! - Basic SHA-256 hashing
//! - Block header hashing (fixed 148-byte encoding)
//! - Transaction hashing (canonical binary encoding)
//! - Bucket/state root hashing (streaming with length-prefixed encoding)

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::types::{BlockHeader, Entity, Transaction};

/// SHA-256 hash output (32 bytes).
pub type Hash = [u8; 32];

/// Hash of empty input: SHA-256("").
/// Used for empty buckets per DESIGN.md line 660.
/// NOT zero bytes - this is critical for cross-node consistency.
pub const EMPTY_HASH: Hash = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
];

/// Zero hash: 32 zero bytes.
/// Used ONLY for genesis block previous_hash per DESIGN.md line 710.
pub const ZERO_HASH: Hash = [0u8; 32];

/// Compute SHA-256 hash of arbitrary data.
#[inline]
pub fn sha256(data: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute SHA-256 hash by concatenating multiple hash inputs.
///
/// Used for state root computation: SHA-256(bucket_root\[0\] || ... || bucket_root\[255\]).
pub fn sha256_concat(hashes: &[Hash]) -> Hash {
    let mut hasher = Sha256::new();
    for h in hashes {
        hasher.update(h);
    }
    hasher.finalize().into()
}

/// Constant-time hash comparison to prevent timing attacks.
///
/// Use this for all security-critical hash comparisons.
#[allow(dead_code)]
#[inline]
pub fn hash_eq(a: &Hash, b: &Hash) -> bool {
    a.ct_eq(b).into()
}

/// Compute block header hash per DESIGN.md fixed 148-byte encoding.
///
/// Encoding layout (DESIGN.md lines 695-708):
/// - height: 8 bytes (u64 BE)
/// - namespace_id: 8 bytes (i64 BE)
/// - vault_id: 8 bytes (i64 BE)
/// - previous_hash: 32 bytes
/// - tx_merkle_root: 32 bytes
/// - state_root: 32 bytes
/// - timestamp_secs: 8 bytes (i64 BE)
/// - timestamp_nanos: 4 bytes (u32 BE)
/// - term: 8 bytes (u64 BE)
/// - committed_index: 8 bytes (u64 BE)
///
/// Total: 148 bytes (fixed)
pub fn block_hash(header: &BlockHeader) -> Hash {
    const BLOCK_ENCODING_SIZE: usize = 148;

    let mut buf = [0u8; BLOCK_ENCODING_SIZE];
    let mut offset = 0;

    // height: u64 BE
    buf[offset..offset + 8].copy_from_slice(&header.height.to_be_bytes());
    offset += 8;

    // namespace_id: i64 BE
    buf[offset..offset + 8].copy_from_slice(&header.namespace_id.value().to_be_bytes());
    offset += 8;

    // vault_id: i64 BE
    buf[offset..offset + 8].copy_from_slice(&header.vault_id.value().to_be_bytes());
    offset += 8;

    // previous_hash: 32 bytes
    buf[offset..offset + 32].copy_from_slice(&header.previous_hash);
    offset += 32;

    // tx_merkle_root: 32 bytes
    buf[offset..offset + 32].copy_from_slice(&header.tx_merkle_root);
    offset += 32;

    // state_root: 32 bytes
    buf[offset..offset + 32].copy_from_slice(&header.state_root);
    offset += 32;

    // timestamp_secs: i64 BE
    buf[offset..offset + 8].copy_from_slice(&header.timestamp.timestamp().to_be_bytes());
    offset += 8;

    // timestamp_nanos: u32 BE
    buf[offset..offset + 4]
        .copy_from_slice(&header.timestamp.timestamp_subsec_nanos().to_be_bytes());
    offset += 4;

    // term: u64 BE
    buf[offset..offset + 8].copy_from_slice(&header.term.to_be_bytes());
    offset += 8;

    // committed_index: u64 BE
    buf[offset..offset + 8].copy_from_slice(&header.committed_index.to_be_bytes());
    // offset += 8; // Final field, no need to increment

    sha256(&buf)
}

/// Compute transaction hash per DESIGN.md canonical binary encoding.
///
/// Encoding includes:
/// - id: 16 bytes
/// - client_id length + bytes
/// - sequence: u64 BE
/// - actor length + bytes
/// - operations (each with type byte + encoded fields)
/// - timestamp
pub fn tx_hash(tx: &Transaction) -> Hash {
    let mut hasher = Sha256::new();

    // id: 16 bytes
    hasher.update(tx.id);

    // client_id: length-prefixed
    hasher.update((tx.client_id.len() as u32).to_le_bytes());
    hasher.update(tx.client_id.as_bytes());

    // sequence: u64 BE
    hasher.update(tx.sequence.to_be_bytes());

    // actor: length-prefixed
    hasher.update((tx.actor.len() as u32).to_le_bytes());
    hasher.update(tx.actor.as_bytes());

    // operations count: u32 LE
    hasher.update((tx.operations.len() as u32).to_le_bytes());

    // Each operation
    for op in &tx.operations {
        hash_operation(&mut hasher, op);
    }

    // timestamp: i64 BE (seconds) + u32 BE (nanos)
    hasher.update(tx.timestamp.timestamp().to_be_bytes());
    hasher.update(tx.timestamp.timestamp_subsec_nanos().to_be_bytes());

    hasher.finalize().into()
}

/// Hash a single operation into the hasher.
fn hash_operation(hasher: &mut Sha256, op: &crate::types::Operation) {
    use crate::types::Operation;

    match op {
        Operation::CreateRelationship { resource, relation, subject } => {
            hasher.update([0x01]); // type byte
            hash_length_prefixed_str(hasher, resource);
            hash_length_prefixed_str(hasher, relation);
            hash_length_prefixed_str(hasher, subject);
        },
        Operation::DeleteRelationship { resource, relation, subject } => {
            hasher.update([0x02]); // type byte
            hash_length_prefixed_str(hasher, resource);
            hash_length_prefixed_str(hasher, relation);
            hash_length_prefixed_str(hasher, subject);
        },
        Operation::SetEntity { key, value, condition, expires_at } => {
            hasher.update([0x03]); // type byte
            hash_length_prefixed_str(hasher, key);
            hash_length_prefixed_bytes(hasher, value);

            // condition: optional
            match condition {
                None => hasher.update([0x00]),
                Some(cond) => {
                    hasher.update([cond.type_byte()]);
                    match cond {
                        crate::types::SetCondition::MustNotExist
                        | crate::types::SetCondition::MustExist => {},
                        crate::types::SetCondition::VersionEquals(v) => {
                            hasher.update(v.to_be_bytes());
                        },
                        crate::types::SetCondition::ValueEquals(v) => {
                            hash_length_prefixed_bytes(hasher, v);
                        },
                    }
                },
            }

            // expires_at: u64 BE (0 if None)
            hasher.update(expires_at.unwrap_or(0).to_be_bytes());
        },
        Operation::DeleteEntity { key } => {
            hasher.update([0x04]); // type byte
            hash_length_prefixed_str(hasher, key);
        },
        Operation::ExpireEntity { key, expired_at } => {
            hasher.update([0x05]); // type byte
            hash_length_prefixed_str(hasher, key);
            hasher.update((*expired_at).to_be_bytes());
        },
    }
}

/// Hash a length-prefixed string.
#[inline]
fn hash_length_prefixed_str(hasher: &mut Sha256, s: &str) {
    hasher.update((s.len() as u32).to_le_bytes());
    hasher.update(s.as_bytes());
}

/// Hash length-prefixed bytes.
#[inline]
fn hash_length_prefixed_bytes(hasher: &mut Sha256, data: &[u8]) {
    hasher.update((data.len() as u32).to_le_bytes());
    hasher.update(data);
}

/// Streaming hasher for bucket root computation.
///
/// Per DESIGN.md lines 821-832: uses streaming hash with length-prefixed
/// key-value encoding including expires_at and version fields.
pub struct BucketHasher {
    hasher: Sha256,
    has_entries: bool,
}

impl BucketHasher {
    /// Create a new bucket hasher.
    pub fn new() -> Self {
        Self { hasher: Sha256::new(), has_entries: false }
    }

    /// Add an entity to the bucket hash.
    ///
    /// Per DESIGN.md lines 803-815:
    /// - key_len: u32 LE
    /// - key: variable
    /// - value_len: u32 LE
    /// - value: variable
    /// - expires_at: u64 BE (0 = never)
    /// - version: u64 BE (block height)
    pub fn add_entity(&mut self, entity: &Entity) {
        self.has_entries = true;

        // key_len: u32 LE
        self.hasher.update((entity.key.len() as u32).to_le_bytes());
        // key: variable
        self.hasher.update(&entity.key);
        // value_len: u32 LE
        self.hasher.update((entity.value.len() as u32).to_le_bytes());
        // value: variable
        self.hasher.update(&entity.value);
        // expires_at: u64 BE (0 = never)
        self.hasher.update(entity.expires_at.to_be_bytes());
        // version: u64 BE (block height)
        self.hasher.update(entity.version.to_be_bytes());
    }

    /// Finalize and return the bucket root hash.
    ///
    /// Returns EMPTY_HASH for empty buckets per DESIGN.md line 660.
    pub fn finalize(self) -> Hash {
        if self.has_entries { self.hasher.finalize().into() } else { EMPTY_HASH }
    }
}

impl Default for BucketHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Assign a key to a bucket using seahash.
///
/// Per DESIGN.md: bucket_id = seahash(key) % 256
#[inline]
pub fn bucket_id(key: &[u8]) -> u8 {
    (seahash::hash(key) % 256) as u8
}

/// Compute the Merkle root of a list of transactions.
///
/// Per DESIGN.md: binary merkle tree where each leaf is SHA-256(tx).
/// Returns EMPTY_HASH for an empty transaction list.
///
/// Uses the same rs_merkle implementation as other merkle operations
/// for consistency across the codebase.
pub fn compute_tx_merkle_root(transactions: &[Transaction]) -> Hash {
    if transactions.is_empty() {
        return EMPTY_HASH;
    }

    let leaves: Vec<Hash> = transactions.iter().map(tx_hash).collect();
    crate::merkle::merkle_root(&leaves)
}

/// Compute a deterministic hash for a vault entry's cryptographic commitments.
///
/// This hash is used to identify a vault entry across all Raft nodes.
/// It uses only deterministic fields that are identical on all nodes:
/// - namespace_id
/// - vault_id
/// - vault_height
/// - previous_vault_hash
/// - tx_merkle_root
/// - state_root
///
/// Notably, it EXCLUDES timestamp and proposer which are non-deterministic.
pub fn vault_entry_hash(entry: &crate::types::VaultEntry) -> Hash {
    let mut hasher = Sha256::new();

    // namespace_id: i32 as LE
    hasher.update(entry.namespace_id.value().to_le_bytes());

    // vault_id: i64 as LE
    hasher.update(entry.vault_id.value().to_le_bytes());

    // vault_height: u64 as BE (matching block_hash style)
    hasher.update(entry.vault_height.to_be_bytes());

    // previous_vault_hash: 32 bytes
    hasher.update(entry.previous_vault_hash);

    // tx_merkle_root: 32 bytes
    hasher.update(entry.tx_merkle_root);

    // state_root: 32 bytes
    hasher.update(entry.state_root);

    hasher.finalize().into()
}

/// Compute a ChainCommitment for a range of blocks.
///
/// Per DESIGN.md §4.4: Proves snapshot's lineage without requiring full block replay.
/// - `accumulated_header_hash`: Sequential hash chain of all block headers
/// - `state_root_accumulator`: Merkle root of all state roots in range
///
/// # Arguments
/// * `headers` - Block headers in order (first header must be at `from_height`)
/// * `from_height` - Start height (inclusive)
/// * `to_height` - End height (inclusive)
///
/// # Returns
/// A ChainCommitment covering the specified range.
pub fn compute_chain_commitment(
    headers: &[BlockHeader],
    from_height: u64,
    to_height: u64,
) -> crate::types::ChainCommitment {
    if headers.is_empty() {
        return crate::types::ChainCommitment {
            accumulated_header_hash: EMPTY_HASH,
            state_root_accumulator: EMPTY_HASH,
            from_height,
            to_height,
        };
    }

    // accumulated_header_hash: Sequential hash chain (not merkle tree)
    // Ensures header ordering is preserved and any tampering invalidates chain
    let mut header_acc = ZERO_HASH;
    for header in headers {
        // hash_chain = SHA-256(previous_acc || block_hash(header))
        let header_hash = block_hash(header);
        header_acc = sha256_concat(&[header_acc, header_hash]);
    }

    // state_root_accumulator: Merkle root of state_roots
    // Enables O(log n) proofs that a specific state_root was in the range
    let state_roots: Vec<Hash> = headers.iter().map(|h| h.state_root).collect();
    let state_acc = crate::merkle::merkle_root(&state_roots);

    crate::types::ChainCommitment {
        accumulated_header_hash: header_acc,
        state_root_accumulator: state_acc,
        from_height,
        to_height,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::types::{NamespaceId, VaultId};

    #[test]
    fn test_empty_hash_is_sha256_of_empty() {
        let computed = sha256(&[]);
        assert_eq!(computed, EMPTY_HASH);
        assert_ne!(EMPTY_HASH, ZERO_HASH);
    }

    #[test]
    fn test_zero_hash_is_all_zeros() {
        assert_eq!(ZERO_HASH, [0u8; 32]);
    }

    #[test]
    fn test_sha256_basic() {
        // SHA-256("hello")
        let hash = sha256(b"hello");
        assert_eq!(
            hex::encode(&hash),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_sha256_concat() {
        let h1 = sha256(b"a");
        let h2 = sha256(b"b");
        let combined = sha256_concat(&[h1, h2]);

        // Should equal SHA-256(h1 || h2)
        let mut expected_input = Vec::new();
        expected_input.extend_from_slice(&h1);
        expected_input.extend_from_slice(&h2);
        let expected = sha256(&expected_input);

        assert_eq!(combined, expected);
    }

    #[test]
    fn test_hash_eq_constant_time() {
        let a = sha256(b"test");
        let b = sha256(b"test");
        let c = sha256(b"other");

        assert!(hash_eq(&a, &b));
        assert!(!hash_eq(&a, &c));
    }

    #[test]
    fn test_bucket_hasher_empty() {
        let hasher = BucketHasher::new();
        assert_eq!(hasher.finalize(), EMPTY_HASH);
    }

    #[test]
    fn test_bucket_hasher_with_entity() {
        let mut hasher = BucketHasher::new();
        hasher.add_entity(&Entity {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            expires_at: 0,
            version: 1,
        });

        let hash = hasher.finalize();
        assert_ne!(hash, EMPTY_HASH);
        assert_ne!(hash, ZERO_HASH);
    }

    #[test]
    fn test_bucket_id_distribution() {
        // Verify keys distribute across buckets
        let mut buckets = [0u32; 256];
        for i in 0..10000 {
            let key = format!("key_{}", i);
            let bucket = bucket_id(key.as_bytes());
            buckets[bucket as usize] += 1;
        }

        // Each bucket should have roughly 39 keys (10000/256)
        // Check that distribution isn't pathologically bad
        let min = *buckets.iter().min().unwrap_or(&0);
        let max = *buckets.iter().max().unwrap_or(&0);
        assert!(min > 10, "Distribution too uneven: min={}", min);
        assert!(max < 100, "Distribution too uneven: max={}", max);
    }

    #[test]
    fn test_block_hash_deterministic() {
        let header = BlockHeader {
            height: 100,
            namespace_id: NamespaceId::new(1),
            vault_id: VaultId::new(2),
            previous_hash: ZERO_HASH,
            tx_merkle_root: sha256(b"tx_root"),
            state_root: sha256(b"state_root"),
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
            term: 5,
            committed_index: 42,
        };

        let hash1 = block_hash(&header);
        let hash2 = block_hash(&header);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_tx_merkle_root_empty() {
        let root = compute_tx_merkle_root(&[]);
        assert_eq!(root, EMPTY_HASH);
    }

    #[test]
    fn test_tx_merkle_root_single() {
        let tx = Transaction {
            id: [0u8; 16],
            client_id: "client1".to_string(),
            sequence: 1,
            actor: "user1".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };

        let root = compute_tx_merkle_root(std::slice::from_ref(&tx));
        // Single tx: root equals the tx hash directly
        assert_eq!(root, tx_hash(&tx));
    }

    #[test]
    fn test_tx_merkle_root_two() {
        let tx1 = Transaction {
            id: [1u8; 16],
            client_id: "client1".to_string(),
            sequence: 1,
            actor: "user1".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };
        let tx2 = Transaction {
            id: [2u8; 16],
            client_id: "client1".to_string(),
            sequence: 2,
            actor: "user1".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };

        let root = compute_tx_merkle_root(&[tx1.clone(), tx2.clone()]);
        // Two txs: root = SHA-256(tx1_hash || tx2_hash)
        let expected = sha256_concat(&[tx_hash(&tx1), tx_hash(&tx2)]);
        assert_eq!(root, expected);
    }

    #[test]
    fn test_tx_merkle_root_three_consistent_with_merkle_module() {
        // Verify tx merkle root uses the same implementation as merkle.rs
        let tx1 = Transaction {
            id: [1u8; 16],
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };
        let tx2 = Transaction {
            id: [2u8; 16],
            client_id: "c".to_string(),
            sequence: 2,
            actor: "a".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };
        let tx3 = Transaction {
            id: [3u8; 16],
            client_id: "c".to_string(),
            sequence: 3,
            actor: "a".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };

        let root = compute_tx_merkle_root(&[tx1.clone(), tx2.clone(), tx3.clone()]);

        // Should be consistent with merkle.rs implementation
        let h1 = tx_hash(&tx1);
        let h2 = tx_hash(&tx2);
        let h3 = tx_hash(&tx3);
        let expected = crate::merkle::merkle_root(&[h1, h2, h3]);

        assert_eq!(root, expected);
    }

    #[test]
    fn test_tx_merkle_root_deterministic() {
        let tx = Transaction {
            id: [42u8; 16],
            client_id: "test".to_string(),
            sequence: 100,
            actor: "actor".to_string(),
            operations: vec![],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
        };

        let root1 = compute_tx_merkle_root(std::slice::from_ref(&tx));
        let root2 = compute_tx_merkle_root(std::slice::from_ref(&tx));
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_chain_commitment_empty() {
        let commitment = compute_chain_commitment(&[], 0, 0);
        assert_eq!(commitment.accumulated_header_hash, EMPTY_HASH);
        assert_eq!(commitment.state_root_accumulator, EMPTY_HASH);
        assert_eq!(commitment.from_height, 0);
        assert_eq!(commitment.to_height, 0);
    }

    #[test]
    fn test_chain_commitment_single_block() {
        let header = BlockHeader {
            height: 1,
            namespace_id: NamespaceId::new(1),
            vault_id: VaultId::new(1),
            previous_hash: ZERO_HASH,
            tx_merkle_root: [1u8; 32],
            state_root: [2u8; 32],
            timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
            term: 1,
            committed_index: 1,
        };

        let commitment = compute_chain_commitment(std::slice::from_ref(&header), 1, 1);

        // accumulated_header_hash = SHA-256(ZERO_HASH || block_hash(header))
        let header_hash = block_hash(&header);
        let expected_acc = sha256_concat(&[ZERO_HASH, header_hash]);
        assert_eq!(commitment.accumulated_header_hash, expected_acc);

        // state_root_accumulator = single state root (merkle of 1 = root itself)
        assert_eq!(commitment.state_root_accumulator, header.state_root);

        assert_eq!(commitment.from_height, 1);
        assert_eq!(commitment.to_height, 1);
    }

    #[test]
    fn test_chain_commitment_multiple_blocks() {
        let headers: Vec<BlockHeader> = (1..=3)
            .map(|i| BlockHeader {
                height: i,
                namespace_id: NamespaceId::new(1),
                vault_id: VaultId::new(1),
                previous_hash: [i as u8; 32],
                tx_merkle_root: [(i + 10) as u8; 32],
                state_root: [(i + 20) as u8; 32],
                timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
                term: i,
                committed_index: i * 10,
            })
            .collect();

        let commitment = compute_chain_commitment(&headers, 1, 3);

        // Verify sequential accumulation
        let mut acc = ZERO_HASH;
        for h in &headers {
            acc = sha256_concat(&[acc, block_hash(h)]);
        }
        assert_eq!(commitment.accumulated_header_hash, acc);

        // Verify merkle root of state roots
        let state_roots: Vec<_> = headers.iter().map(|h| h.state_root).collect();
        let expected_merkle = crate::merkle::merkle_root(&state_roots);
        assert_eq!(commitment.state_root_accumulator, expected_merkle);

        assert_eq!(commitment.from_height, 1);
        assert_eq!(commitment.to_height, 3);
    }

    #[test]
    fn test_chain_commitment_deterministic() {
        let headers: Vec<BlockHeader> = (1..=5)
            .map(|i| BlockHeader {
                height: i,
                namespace_id: NamespaceId::new(1),
                vault_id: VaultId::new(1),
                previous_hash: [i as u8; 32],
                tx_merkle_root: [(i + 10) as u8; 32],
                state_root: [(i + 20) as u8; 32],
                timestamp: Utc.timestamp_opt(1704067200, 0).unwrap(),
                term: 1,
                committed_index: i,
            })
            .collect();

        let c1 = compute_chain_commitment(&headers, 1, 5);
        let c2 = compute_chain_commitment(&headers, 1, 5);

        assert_eq!(c1.accumulated_header_hash, c2.accumulated_header_hash);
        assert_eq!(c1.state_root_accumulator, c2.state_root_accumulator);
    }
}

/// Helper for hex encoding (dev dependency not needed for tests).
#[cfg(test)]
mod hex {
    use std::fmt::Write;

    pub fn encode(data: &[u8]) -> String {
        data.iter().fold(String::with_capacity(data.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{:02x}", b);
            acc
        })
    }
}
