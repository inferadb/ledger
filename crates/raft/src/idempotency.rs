//! Idempotency cache for preventing duplicate transaction processing.
//!
//! This module implements a bounded, TTL-based cache that stores the results
//! of recently committed transactions. When a client retries a request with
//! the same (client_id, sequence) pair, we return the cached result instead
//! of reprocessing.

use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::proto::{TxId, WriteSuccess};

/// Maximum number of entries in the cache.
const MAX_CACHE_SIZE: usize = 10_000;

/// Time-to-live for cache entries.
const ENTRY_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// Cached write result.
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// The sequence number that was committed.
    pub sequence: u64,
    /// The result of the write operation.
    pub result: WriteSuccess,
    /// When this entry was inserted.
    pub inserted_at: Instant,
}

/// Thread-safe idempotency cache with bounded size and TTL eviction.
///
/// The cache maps `client_id -> (last_sequence, result, timestamp)`.
/// When a duplicate request is detected (same client_id with sequence <= cached),
/// the cached result is returned.
#[derive(Debug)]
pub struct IdempotencyCache {
    /// The underlying cache storage.
    cache: DashMap<String, CachedResult>,
}

impl IdempotencyCache {
    /// Create a new idempotency cache.
    pub fn new() -> Self {
        Self {
            cache: DashMap::with_capacity(MAX_CACHE_SIZE),
        }
    }

    /// Check if a request is a duplicate and return the cached result if so.
    ///
    /// Returns `Some(result)` if this is a duplicate (sequence <= cached sequence
    /// and entry is not expired). Returns `None` if this is a new request.
    pub fn check(&self, client_id: &str, sequence: u64) -> Option<WriteSuccess> {
        if let Some(entry) = self.cache.get(client_id) {
            // Check if entry is still valid and sequence is a duplicate
            if entry.sequence >= sequence && entry.inserted_at.elapsed() < ENTRY_TTL {
                return Some(entry.result.clone());
            }
        }
        None
    }

    /// Insert a new result into the cache.
    ///
    /// If the cache is at capacity, expired entries are evicted first.
    pub fn insert(&self, client_id: String, sequence: u64, result: WriteSuccess) {
        // Evict expired entries if at capacity
        if self.cache.len() >= MAX_CACHE_SIZE {
            self.evict_expired();
        }

        // If still at capacity after eviction, remove oldest entries
        // (simple random eviction via DashMap iteration)
        if self.cache.len() >= MAX_CACHE_SIZE {
            let to_remove: Vec<String> = self
                .cache
                .iter()
                .take(MAX_CACHE_SIZE / 10) // Remove 10% of entries
                .map(|r| r.key().clone())
                .collect();
            for key in to_remove {
                self.cache.remove(&key);
            }
        }

        self.cache.insert(
            client_id,
            CachedResult {
                sequence,
                result,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Check and insert in one operation.
    ///
    /// Returns `Some(result)` if this is a duplicate, `None` otherwise.
    /// If not a duplicate, the new result is inserted.
    pub fn check_and_insert(
        &self,
        client_id: &str,
        sequence: u64,
        result: WriteSuccess,
    ) -> Option<WriteSuccess> {
        // First check for existing entry
        if let Some(cached) = self.check(client_id, sequence) {
            return Some(cached);
        }

        // Not a duplicate, insert the new result
        self.insert(client_id.to_string(), sequence, result);
        None
    }

    /// Remove expired entries from the cache.
    fn evict_expired(&self) {
        self.cache
            .retain(|_, entry| entry.inserted_at.elapsed() < ENTRY_TTL);
    }

    /// Get the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(block_height: u64) -> WriteSuccess {
        WriteSuccess {
            tx_id: Some(TxId {
                id: vec![0u8; 16],
            }),
            block_height,
            block_header: None,
            tx_proof: None,
        }
    }

    #[test]
    fn test_check_and_insert_new_request() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // First request should not be a duplicate
        let cached = cache.check_and_insert("client-1", 1, result.clone());
        assert!(cached.is_none());

        // Should have one entry
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_check_and_insert_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert first request
        cache.insert("client-1".to_string(), 1, result.clone());

        // Retry with same sequence should return cached result
        let cached = cache.check("client-1", 1);
        assert!(cached.is_some());
        assert_eq!(cached.as_ref().map(|r| r.block_height), Some(100));

        // Retry with lower sequence should also return cached
        let cached = cache.check("client-1", 0);
        assert!(cached.is_some());
    }

    #[test]
    fn test_higher_sequence_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert sequence 1
        cache.insert("client-1".to_string(), 1, result);

        // Sequence 2 should not be a duplicate
        let cached = cache.check("client-1", 2);
        assert!(cached.is_none());
    }

    #[test]
    fn test_different_client_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert for client-1
        cache.insert("client-1".to_string(), 1, result);

        // Same sequence for different client should not be a duplicate
        let cached = cache.check("client-2", 1);
        assert!(cached.is_none());
    }

    /// DESIGN.md compliance test: sequence numbers must be monotonically increasing.
    /// Per Invariant 9: "Sequence numbers must be monotonically increasing per client."
    #[test]
    fn test_sequence_monotonicity() {
        let cache = IdempotencyCache::new();

        // Insert sequence 5
        let result5 = make_result(100);
        cache.insert("client-1".to_string(), 5, result5);

        // Sequence 5 (same) should be duplicate
        assert!(cache.check("client-1", 5).is_some());

        // Sequence 4 (lower) should be duplicate (per idempotency semantics)
        assert!(cache.check("client-1", 4).is_some());

        // Sequence 3 (even lower) should still be duplicate
        assert!(cache.check("client-1", 3).is_some());

        // Sequence 6 (higher) should NOT be duplicate - new request
        assert!(cache.check("client-1", 6).is_none());

        // Insert sequence 10
        let result10 = make_result(200);
        cache.insert("client-1".to_string(), 10, result10.clone());

        // Now sequence 5, 6, 7, 8, 9, 10 should all return the result for 10
        for seq in 5..=10 {
            let cached = cache.check("client-1", seq);
            assert!(cached.is_some(), "sequence {} should be cached", seq);
            assert_eq!(cached.unwrap().block_height, 200);
        }

        // Sequence 11 should NOT be cached
        assert!(cache.check("client-1", 11).is_none());
    }
}
