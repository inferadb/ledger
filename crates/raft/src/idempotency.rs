//! Idempotency cache for preventing duplicate transaction processing.
//!
//! This module implements a bounded, TTL-based cache that stores the results
//! of recently committed transactions. When a client retries a request with
//! the same (namespace_id, vault_id, client_id, sequence) tuple, we return the
//! cached result instead of reprocessing.
//!
//! Per DESIGN.md §5.3: Sequence tracking is per (namespace_id, vault_id, client_id).
//!
//! Uses moka's TinyLFU admission policy for superior hit rates (~85% vs ~60% for LRU)
//! and built-in TTL eviction.

use std::time::Duration;

use moka::sync::Cache;

use crate::proto::WriteSuccess;

/// Maximum number of entries in the cache.
const MAX_CACHE_SIZE: u64 = 10_000;

/// Time-to-live for cache entries.
const ENTRY_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// Composite key for idempotency cache.
///
/// Per DESIGN.md: Sequence tracking is per (namespace_id, vault_id, client_id).
/// Using this as the cache key ensures writes to different vaults aren't
/// incorrectly treated as duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    /// Namespace ID.
    pub namespace_id: i64,
    /// Vault ID (0 if not specified).
    pub vault_id: i64,
    /// Client identifier.
    pub client_id: String,
}

impl IdempotencyKey {
    /// Create a new idempotency key.
    pub fn new(namespace_id: i64, vault_id: i64, client_id: String) -> Self {
        Self {
            namespace_id,
            vault_id,
            client_id,
        }
    }
}

/// Cached write result.
///
/// Note: TTL is handled by moka internally, so we don't need an `inserted_at` field.
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// The sequence number that was committed.
    pub sequence: u64,
    /// The result of the write operation.
    pub result: WriteSuccess,
}

/// Thread-safe idempotency cache with bounded size and TTL eviction.
///
/// The cache maps `(namespace_id, vault_id, client_id) -> (last_sequence, result)`.
/// When a duplicate request is detected (same key with sequence <= cached),
/// the cached result is returned.
///
/// Uses moka's TinyLFU admission policy which considers both recency and frequency,
/// achieving significantly better hit rates than traditional LRU.
#[derive(Debug)]
pub struct IdempotencyCache {
    /// The underlying moka cache with built-in TTL and TinyLFU eviction.
    cache: Cache<IdempotencyKey, CachedResult>,
}

impl IdempotencyCache {
    /// Create a new idempotency cache.
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(MAX_CACHE_SIZE)
            .time_to_live(ENTRY_TTL)
            .build();

        Self { cache }
    }

    /// Check if a request is a duplicate and return the cached result if so.
    ///
    /// Returns `Some(result)` if this is a duplicate (sequence <= cached sequence).
    /// Returns `None` if this is a new request. TTL expiration is handled
    /// automatically by moka.
    pub fn check(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        sequence: u64,
    ) -> Option<WriteSuccess> {
        let key = IdempotencyKey::new(namespace_id, vault_id, client_id.to_string());
        self.cache.get(&key).and_then(|entry| {
            // Check if sequence is a duplicate (same or lower than cached)
            if entry.sequence >= sequence {
                Some(entry.result.clone())
            } else {
                None
            }
        })
    }

    /// Insert a new result into the cache.
    ///
    /// Capacity management and TTL eviction are handled automatically by moka.
    pub fn insert(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: String,
        sequence: u64,
        result: WriteSuccess,
    ) {
        let key = IdempotencyKey::new(namespace_id, vault_id, client_id);
        self.cache.insert(key, CachedResult { sequence, result });
    }

    /// Check and insert in one operation.
    ///
    /// Returns `Some(result)` if this is a duplicate, `None` otherwise.
    /// If not a duplicate, the new result is inserted.
    pub fn check_and_insert(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        sequence: u64,
        result: WriteSuccess,
    ) -> Option<WriteSuccess> {
        // First check for existing entry
        if let Some(cached) = self.check(namespace_id, vault_id, client_id, sequence) {
            return Some(cached);
        }

        // Not a duplicate, insert the new result
        self.insert(
            namespace_id,
            vault_id,
            client_id.to_string(),
            sequence,
            result,
        );
        None
    }

    /// Get the current number of entries in the cache.
    ///
    /// Note: This is an approximation as moka performs lazy eviction.
    pub fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.entry_count() == 0
    }

    /// Get the last committed sequence for a client in a specific vault.
    ///
    /// Returns the highest sequence number that has been committed for this
    /// (namespace_id, vault_id, client_id) combination, or 0 if no commits
    /// have been cached (either never written or cache expired).
    pub fn get_last_sequence(&self, namespace_id: i64, vault_id: i64, client_id: &str) -> u64 {
        let key = IdempotencyKey::new(namespace_id, vault_id, client_id.to_string());
        self.cache
            .get(&key)
            .map(|entry| entry.sequence)
            .unwrap_or(0)
    }

    /// Force synchronous eviction of expired entries.
    ///
    /// Normally moka evicts lazily, but this can be called to reclaim memory immediately.
    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::proto::TxId;

    fn make_result(block_height: u64) -> WriteSuccess {
        WriteSuccess {
            tx_id: Some(TxId { id: vec![0u8; 16] }),
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
        let cached = cache.check_and_insert(1, 1, "client-1", 1, result.clone());
        assert!(cached.is_none());

        // Force sync to ensure entry is visible
        cache.run_pending_tasks();

        // Should have one entry
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_check_and_insert_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert first request
        cache.insert(1, 1, "client-1".to_string(), 1, result.clone());

        // Retry with same sequence should return cached result
        let cached = cache.check(1, 1, "client-1", 1);
        assert!(cached.is_some());
        assert_eq!(cached.as_ref().map(|r| r.block_height), Some(100));

        // Retry with lower sequence should also return cached
        let cached = cache.check(1, 1, "client-1", 0);
        assert!(cached.is_some());
    }

    #[test]
    fn test_higher_sequence_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert sequence 1
        cache.insert(1, 1, "client-1".to_string(), 1, result);

        // Sequence 2 should not be a duplicate
        let cached = cache.check(1, 1, "client-1", 2);
        assert!(cached.is_none());
    }

    #[test]
    fn test_different_client_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert for client-1
        cache.insert(1, 1, "client-1".to_string(), 1, result);

        // Same sequence for different client should not be a duplicate
        let cached = cache.check(1, 1, "client-2", 1);
        assert!(cached.is_none());
    }

    /// Test that different vaults are tracked independently.
    /// Per DESIGN.md: Sequence tracking is per (namespace_id, vault_id, client_id).
    #[test]
    fn test_different_vault_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert for vault 1
        cache.insert(1, 1, "client-1".to_string(), 1, result.clone());

        // Same client, same sequence, but different vault should NOT be duplicate
        let cached = cache.check(1, 2, "client-1", 1);
        assert!(
            cached.is_none(),
            "different vault should not be a duplicate"
        );

        // Now insert for vault 2
        cache.insert(1, 2, "client-1".to_string(), 1, result);
        cache.run_pending_tasks();
        assert_eq!(cache.len(), 2, "should have entries for both vaults");

        // Both should now be cached
        assert!(cache.check(1, 1, "client-1", 1).is_some());
        assert!(cache.check(1, 2, "client-1", 1).is_some());
    }

    /// Test that different namespaces are tracked independently.
    #[test]
    fn test_different_namespace_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100);

        // Insert for namespace 1
        cache.insert(1, 1, "client-1".to_string(), 1, result.clone());

        // Same client, same vault, same sequence, but different namespace should NOT be duplicate
        let cached = cache.check(2, 1, "client-1", 1);
        assert!(
            cached.is_none(),
            "different namespace should not be a duplicate"
        );
    }

    /// DESIGN.md compliance test: sequence numbers must be monotonically increasing.
    /// Per Invariant 9: "Sequence numbers must be monotonically increasing per client."
    #[test]
    fn test_sequence_monotonicity() {
        let cache = IdempotencyCache::new();

        // Insert sequence 5
        let result5 = make_result(100);
        cache.insert(1, 1, "client-1".to_string(), 5, result5);

        // Sequence 5 (same) should be duplicate
        assert!(cache.check(1, 1, "client-1", 5).is_some());

        // Sequence 4 (lower) should be duplicate (per idempotency semantics)
        assert!(cache.check(1, 1, "client-1", 4).is_some());

        // Sequence 3 (even lower) should still be duplicate
        assert!(cache.check(1, 1, "client-1", 3).is_some());

        // Sequence 6 (higher) should NOT be duplicate - new request
        assert!(cache.check(1, 1, "client-1", 6).is_none());

        // Insert sequence 10
        let result10 = make_result(200);
        cache.insert(1, 1, "client-1".to_string(), 10, result10.clone());

        // Now sequence 5, 6, 7, 8, 9, 10 should all return the result for 10
        for seq in 5..=10 {
            let cached = cache.check(1, 1, "client-1", seq);
            assert!(cached.is_some(), "sequence {} should be cached", seq);
            assert_eq!(cached.unwrap().block_height, 200);
        }

        // Sequence 11 should NOT be cached
        assert!(cache.check(1, 1, "client-1", 11).is_none());
    }
}
