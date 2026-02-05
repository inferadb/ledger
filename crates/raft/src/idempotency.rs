//! Idempotency cache for preventing duplicate transaction processing.
//!
//! This module implements a bounded, TTL-based cache that stores the results
//! of recently committed transactions. When a client retries a request with
//! the same `(namespace_id, vault_id, client_id, idempotency_key)` tuple, we return the
//! cached result instead of reprocessing.
//!
//! Per ADR server-assigned-sequences:
//! - Clients send a 16-byte UUID idempotency key per write
//! - Server assigns sequences at Raft commit time
//! - Cache hit with same payload returns cached result (idempotent retry)
//! - Cache hit with different payload returns `IDEMPOTENCY_KEY_REUSED` error
//!
//! Uses moka's TinyLFU admission policy for superior hit rates (~85% vs ~60% for LRU)
//! and built-in TTL eviction.

use std::time::Duration;

use moka::sync::Cache;

use crate::proto::WriteSuccess;

/// Maximum number of entries in the cache.
const MAX_CACHE_SIZE: u64 = 100_000;

/// Time-to-live for cache entries (24 hours per ADR).
const ENTRY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Composite key for idempotency cache.
///
/// Per ADR: Cache key is `(namespace_id, vault_id, client_id, idempotency_key)`.
/// The idempotency_key is a 16-byte UUID provided by the client.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    /// Namespace ID.
    pub namespace_id: i64,
    /// Vault ID (0 if not specified).
    pub vault_id: i64,
    /// Client identifier.
    pub client_id: String,
    /// Client-provided idempotency key (16-byte UUID).
    pub idempotency_key: [u8; 16],
}

impl IdempotencyKey {
    /// Create a new idempotency key.
    pub fn new(
        namespace_id: i64,
        vault_id: i64,
        client_id: String,
        idempotency_key: [u8; 16],
    ) -> Self {
        Self { namespace_id, vault_id, client_id, idempotency_key }
    }

    /// Create a new idempotency key from a byte slice.
    ///
    /// Returns `None` if the slice is not exactly 16 bytes.
    #[allow(dead_code)]
    pub fn from_bytes(
        namespace_id: i64,
        vault_id: i64,
        client_id: String,
        idempotency_key_bytes: &[u8],
    ) -> Option<Self> {
        if idempotency_key_bytes.len() != 16 {
            return None;
        }
        let mut key = [0u8; 16];
        key.copy_from_slice(idempotency_key_bytes);
        Some(Self::new(namespace_id, vault_id, client_id, key))
    }
}

/// Result of checking the idempotency cache.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum IdempotencyCheckResult {
    /// Request is new, not in cache.
    NewRequest,
    /// Request is a duplicate with same payload - return cached result.
    Duplicate(WriteSuccess),
    /// Idempotency key was reused with different payload - error.
    KeyReused,
}

/// Cached write result.
///
/// Stores the write result and a hash of the request payload for detecting
/// idempotency key reuse with different payloads.
///
/// Note: TTL is handled by moka internally, so we don't need an `inserted_at` field.
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// Hash of the request payload (for detecting key reuse with different payload).
    pub request_hash: u64,
    /// The result of the write operation (includes assigned_sequence).
    pub result: WriteSuccess,
}

/// Thread-safe idempotency cache with bounded size and TTL eviction.
///
/// The cache maps `(namespace_id, vault_id, client_id, idempotency_key) -> CachedResult`.
///
/// Behavior:
/// - New key: Process write, cache result
/// - Same key + same payload hash: Return cached result (idempotent retry)
/// - Same key + different payload hash: Return `KeyReused` error
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
        let cache = Cache::builder().max_capacity(MAX_CACHE_SIZE).time_to_live(ENTRY_TTL).build();

        Self { cache }
    }

    /// Check if a request is a duplicate.
    ///
    /// Returns:
    /// - `NewRequest` if the idempotency key is not in the cache
    /// - `Duplicate(result)` if the key exists with the same payload hash
    /// - `KeyReused` if the key exists but the payload hash differs
    pub fn check(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
    ) -> IdempotencyCheckResult {
        let key =
            IdempotencyKey::new(namespace_id, vault_id, client_id.to_string(), idempotency_key);
        match self.cache.get(&key) {
            Some(entry) => {
                if entry.request_hash == request_hash {
                    IdempotencyCheckResult::Duplicate(entry.result.clone())
                } else {
                    IdempotencyCheckResult::KeyReused
                }
            },
            None => IdempotencyCheckResult::NewRequest,
        }
    }

    /// Insert a new result into the cache.
    ///
    /// Capacity management and TTL eviction are handled automatically by moka.
    pub fn insert(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: String,
        idempotency_key: [u8; 16],
        request_hash: u64,
        result: WriteSuccess,
    ) {
        let key = IdempotencyKey::new(namespace_id, vault_id, client_id, idempotency_key);
        self.cache.insert(key, CachedResult { request_hash, result });
    }

    /// Check and insert in one operation.
    ///
    /// Returns:
    /// - `NewRequest` if the key was not in cache (and the result has been inserted)
    /// - `Duplicate(result)` if the key exists with the same payload hash
    /// - `KeyReused` if the key exists but the payload hash differs
    pub fn check_and_insert(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
        result: WriteSuccess,
    ) -> IdempotencyCheckResult {
        // First check for existing entry
        let check_result =
            self.check(namespace_id, vault_id, client_id, idempotency_key, request_hash);

        if matches!(check_result, IdempotencyCheckResult::NewRequest) {
            // Not in cache, insert the new result
            self.insert(
                namespace_id,
                vault_id,
                client_id.to_string(),
                idempotency_key,
                request_hash,
                result,
            );
        }

        check_result
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::proto::TxId;

    fn make_result(block_height: u64, assigned_sequence: u64) -> WriteSuccess {
        WriteSuccess {
            tx_id: Some(TxId { id: vec![0u8; 16] }),
            block_height,
            assigned_sequence,
            block_header: None,
            tx_proof: None,
        }
    }

    fn make_key(n: u8) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[0] = n;
        key
    }

    #[test]
    fn test_check_and_insert_new_request() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // First request should not be a duplicate
        let check = cache.check_and_insert(1, 1, "client-1", idem_key, request_hash, result);
        assert!(matches!(check, IdempotencyCheckResult::NewRequest));

        // Force sync to ensure entry is visible
        cache.run_pending_tasks();

        // Should have one entry
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_check_and_insert_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert first request
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash, result);

        // Retry with same key and hash should return cached result
        let check = cache.check(1, 1, "client-1", idem_key, request_hash);
        match check {
            IdempotencyCheckResult::Duplicate(cached) => {
                assert_eq!(cached.block_height, 100);
                assert_eq!(cached.assigned_sequence, 1);
            },
            _ => panic!("expected Duplicate"),
        }
    }

    #[test]
    fn test_key_reused_with_different_payload() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash_1 = 12345u64;
        let request_hash_2 = 99999u64; // Different payload

        // Insert first request
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash_1, result);

        // Retry with same key but different hash should return KeyReused
        let check = cache.check(1, 1, "client-1", idem_key, request_hash_2);
        assert!(matches!(check, IdempotencyCheckResult::KeyReused));
    }

    #[test]
    fn test_different_idempotency_key_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key_1 = make_key(1);
        let idem_key_2 = make_key(2);
        let request_hash = 12345u64;

        // Insert with key 1
        cache.insert(1, 1, "client-1".to_string(), idem_key_1, request_hash, result);

        // Check with key 2 should be new request
        let check = cache.check(1, 1, "client-1", idem_key_2, request_hash);
        assert!(matches!(check, IdempotencyCheckResult::NewRequest));
    }

    #[test]
    fn test_different_client_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert for client-1
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash, result);

        // Same key for different client should be new request
        let check = cache.check(1, 1, "client-2", idem_key, request_hash);
        assert!(matches!(check, IdempotencyCheckResult::NewRequest));
    }

    /// Test that different vaults are tracked independently.
    #[test]
    fn test_different_vault_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert for vault 1
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash, result.clone());

        // Same client, same key, but different vault should be new request
        let check = cache.check(1, 2, "client-1", idem_key, request_hash);
        assert!(
            matches!(check, IdempotencyCheckResult::NewRequest),
            "different vault should be new request"
        );

        // Now insert for vault 2
        cache.insert(1, 2, "client-1".to_string(), idem_key, request_hash, result);
        cache.run_pending_tasks();
        assert_eq!(cache.len(), 2, "should have entries for both vaults");

        // Both should now return duplicate
        assert!(matches!(
            cache.check(1, 1, "client-1", idem_key, request_hash),
            IdempotencyCheckResult::Duplicate(_)
        ));
        assert!(matches!(
            cache.check(1, 2, "client-1", idem_key, request_hash),
            IdempotencyCheckResult::Duplicate(_)
        ));
    }

    /// Test that different namespaces are tracked independently.
    #[test]
    fn test_different_namespace_not_duplicate() {
        let cache = IdempotencyCache::new();
        let result = make_result(100, 1);
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert for namespace 1
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash, result);

        // Same client, same vault, same key, but different namespace should be new request
        let check = cache.check(2, 1, "client-1", idem_key, request_hash);
        assert!(
            matches!(check, IdempotencyCheckResult::NewRequest),
            "different namespace should be new request"
        );
    }

    /// Test IdempotencyKey::from_bytes validation.
    #[test]
    fn test_idempotency_key_from_bytes() {
        // Valid 16-byte key
        let bytes = [1u8; 16];
        let key = IdempotencyKey::from_bytes(1, 1, "client".to_string(), &bytes);
        assert!(key.is_some());
        assert_eq!(key.unwrap().idempotency_key, bytes);

        // Invalid: too short
        let short_bytes = [1u8; 15];
        let key = IdempotencyKey::from_bytes(1, 1, "client".to_string(), &short_bytes);
        assert!(key.is_none());

        // Invalid: too long
        let long_bytes = [1u8; 17];
        let key = IdempotencyKey::from_bytes(1, 1, "client".to_string(), &long_bytes);
        assert!(key.is_none());

        // Invalid: empty
        let empty_bytes: [u8; 0] = [];
        let key = IdempotencyKey::from_bytes(1, 1, "client".to_string(), &empty_bytes);
        assert!(key.is_none());
    }

    /// Test that assigned_sequence is properly stored and retrieved.
    #[test]
    fn test_assigned_sequence_preserved() {
        let cache = IdempotencyCache::new();
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert with assigned_sequence = 42
        let result = make_result(100, 42);
        cache.insert(1, 1, "client-1".to_string(), idem_key, request_hash, result);

        // Retrieve should have same assigned_sequence
        let check = cache.check(1, 1, "client-1", idem_key, request_hash);
        match check {
            IdempotencyCheckResult::Duplicate(cached) => {
                assert_eq!(cached.assigned_sequence, 42);
            },
            _ => panic!("expected Duplicate"),
        }
    }

    /// Test concurrent writes with unique idempotency keys all succeed independently.
    #[test]
    fn test_concurrent_writes_with_unique_keys() {
        let cache = IdempotencyCache::new();
        let request_hash = 12345u64;

        // Simulate 5 concurrent writes with unique keys
        for i in 0..5 {
            let idem_key = make_key(i);
            let result = make_result(100 + i as u64, i as u64 + 1);

            let check = cache.check_and_insert(1, 1, "client-1", idem_key, request_hash, result);
            assert!(matches!(check, IdempotencyCheckResult::NewRequest));
        }

        cache.run_pending_tasks();
        assert_eq!(cache.len(), 5, "all 5 writes should be cached");

        // Each should be retrievable with its assigned sequence
        for i in 0..5 {
            let idem_key = make_key(i);
            let check = cache.check(1, 1, "client-1", idem_key, request_hash);
            match check {
                IdempotencyCheckResult::Duplicate(cached) => {
                    assert_eq!(cached.assigned_sequence, i as u64 + 1);
                },
                _ => panic!("expected Duplicate for key {}", i),
            }
        }
    }
}
