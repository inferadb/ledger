//! Bloom filter for fast negative lookups on B+ tree leaf pages.
//!
//! Each leaf page can embed a 256-byte bloom filter to skip deserialization
//! when a key is definitively absent. The filter uses 5 hash functions
//! (optimal for ~100 keys at <1% false positive rate).
//!
//! ## Theory
//!
//! For a filter with `m` bits, `k` hash functions, and `n` inserted keys:
//! - False positive rate ≈ (1 - e^(-kn/m))^k
//! - For m=2048, k=5, n=100: FPR ≈ 0.7%
//!
//! The filter uses double hashing: h_i(x) = h1(x) + i * h2(x) mod m,
//! where h1 and h2 are derived from a single 128-bit SipHash.

/// Size of the bloom filter in bytes.
pub const BLOOM_FILTER_SIZE: usize = 256;

/// Number of bits in the bloom filter.
const BLOOM_BITS: usize = BLOOM_FILTER_SIZE * 8;

/// Number of hash functions.
const NUM_HASHES: usize = 5;

/// A space-efficient probabilistic data structure for testing set membership.
///
/// Supports `insert` and `may_contain` operations. A negative result is
/// guaranteed correct; a positive result may be a false positive.
#[derive(Clone)]
pub struct BloomFilter {
    bits: [u8; BLOOM_FILTER_SIZE],
}

impl BloomFilter {
    /// Creates an empty bloom filter with all bits clear.
    pub fn new() -> Self {
        Self { bits: [0u8; BLOOM_FILTER_SIZE] }
    }

    /// Creates a bloom filter from a fixed-size byte array.
    ///
    /// Unlike [`from_bytes`](Self::from_bytes), this cannot fail because the
    /// array size is verified at compile time.
    pub fn from_array(data: &[u8; BLOOM_FILTER_SIZE]) -> Self {
        Self { bits: *data }
    }

    /// Creates a bloom filter from raw bytes.
    ///
    /// Returns `None` if the slice length doesn't match `BLOOM_FILTER_SIZE`.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() != BLOOM_FILTER_SIZE {
            return None;
        }
        let mut bits = [0u8; BLOOM_FILTER_SIZE];
        bits.copy_from_slice(data);
        Some(Self { bits })
    }

    /// Returns the raw byte representation.
    pub fn to_bytes(&self) -> &[u8; BLOOM_FILTER_SIZE] {
        &self.bits
    }

    /// Inserts a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hash_pair(key);
        for i in 0..NUM_HASHES {
            let bit_index = Self::bit_index(h1, h2, i);
            self.bits[bit_index / 8] |= 1 << (bit_index % 8);
        }
    }

    /// Tests whether a key might be in the set.
    ///
    /// Returns `false` if the key is definitely absent (true negative).
    /// Returns `true` if the key might be present (could be a false positive).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hash_pair(key);
        for i in 0..NUM_HASHES {
            let bit_index = Self::bit_index(h1, h2, i);
            if self.bits[bit_index / 8] & (1 << (bit_index % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Checks if the filter is empty (no bits set).
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&b| b == 0)
    }

    /// Computes a double-hash pair from a key using FNV-1a-inspired mixing.
    ///
    /// Uses two independent hash values derived from the key bytes.
    /// The approach splits a 128-bit hash into two 64-bit halves.
    fn hash_pair(key: &[u8]) -> (u64, u64) {
        // FNV-1a inspired mixing with two independent seeds.
        // Seed 1 (h1): standard FNV offset basis
        let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in key {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x0100_0000_01b3);
        }

        // Seed 2 (h2): different offset basis for independence
        let mut h2: u64 = 0x6c62_272e_07bb_0142;
        for &b in key {
            h2 ^= b as u64;
            h2 = h2.wrapping_mul(0x0100_0000_01b3);
        }

        (h1, h2)
    }

    /// Computes the bit index for the i-th hash function using double hashing.
    fn bit_index(h1: u64, h2: u64, i: usize) -> usize {
        (h1.wrapping_add((i as u64).wrapping_mul(h2)) % (BLOOM_BITS as u64)) as usize
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set_bits = self.bits.iter().map(|b| b.count_ones()).sum::<u32>();
        f.debug_struct("BloomFilter")
            .field("size_bytes", &BLOOM_FILTER_SIZE)
            .field("set_bits", &set_bits)
            .field("total_bits", &BLOOM_BITS)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_bloom_filter() {
        let bf = BloomFilter::new();
        assert!(bf.is_empty());
        assert!(!bf.may_contain(b"anything"));
    }

    #[test]
    fn test_insert_and_query() {
        let mut bf = BloomFilter::new();
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        assert!(!bf.is_empty());
    }

    #[test]
    fn test_definite_negatives() {
        let mut bf = BloomFilter::new();
        bf.insert(b"alpha");
        bf.insert(b"beta");
        bf.insert(b"gamma");

        // These should be definite negatives (not inserted)
        // With 2048 bits and only 3 keys, false positive rate is negligible
        assert!(!bf.may_contain(b"delta"));
        assert!(!bf.may_contain(b"epsilon"));
        assert!(!bf.may_contain(b"zeta"));
    }

    #[test]
    fn test_serialization_round_trip() {
        let mut bf = BloomFilter::new();
        for i in 0u32..50 {
            bf.insert(&i.to_le_bytes());
        }

        let bytes = bf.to_bytes();
        assert_eq!(bytes.len(), BLOOM_FILTER_SIZE);

        let bf2 = BloomFilter::from_bytes(bytes).expect("valid bytes");

        // All inserted keys should still be found
        for i in 0u32..50 {
            assert!(bf2.may_contain(&i.to_le_bytes()), "key {i} should be found");
        }
    }

    #[test]
    fn test_from_bytes_invalid_length() {
        assert!(BloomFilter::from_bytes(&[0u8; 100]).is_none());
        assert!(BloomFilter::from_bytes(&[0u8; 257]).is_none());
        assert!(BloomFilter::from_bytes(&[]).is_none());
    }

    #[test]
    fn test_from_bytes_valid() {
        assert!(BloomFilter::from_bytes(&[0u8; BLOOM_FILTER_SIZE]).is_some());
    }

    #[test]
    fn test_false_positive_rate_under_one_percent() {
        // Property test: insert 100 keys, check 10000 absent keys.
        // FPR should be under 1% (theoretical: ~0.7% for m=2048, k=5, n=100).
        let mut bf = BloomFilter::new();
        for i in 0u32..100 {
            bf.insert(&i.to_le_bytes());
        }

        let mut false_positives = 0u32;
        let test_count = 10_000u32;
        for i in 1000..1000 + test_count {
            if bf.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }

        let fpr = f64::from(false_positives) / f64::from(test_count);
        assert!(
            fpr < 0.01,
            "False positive rate {fpr:.4} exceeds 1% threshold ({false_positives}/{test_count})"
        );
    }

    #[test]
    fn test_no_false_negatives() {
        // Insert many keys and verify none are missed
        let mut bf = BloomFilter::new();
        let keys: Vec<Vec<u8>> = (0u32..200).map(|i| i.to_le_bytes().to_vec()).collect();

        for key in &keys {
            bf.insert(key);
        }

        for key in &keys {
            assert!(bf.may_contain(key), "Inserted key should always be found");
        }
    }

    #[test]
    fn test_empty_key() {
        let mut bf = BloomFilter::new();
        bf.insert(b"");
        assert!(bf.may_contain(b""));
    }

    #[test]
    fn test_large_key() {
        let mut bf = BloomFilter::new();
        let large_key = vec![0xABu8; 4096];
        bf.insert(&large_key);
        assert!(bf.may_contain(&large_key));
        // Large keys may produce false positives due to hash collisions;
        // we only guarantee no false negatives.
    }

    #[test]
    fn test_clone() {
        let mut bf = BloomFilter::new();
        bf.insert(b"test");

        let bf2 = bf.clone();
        assert!(bf2.may_contain(b"test"));
    }
}
