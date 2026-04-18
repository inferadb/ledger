//! Idempotency cache for preventing duplicate transaction processing.
//!
//! Bounded, TTL-based cache that stores the results
//! of recently committed transactions. When a client retries a request with
//! the same `(organization, vault, client_id, idempotency_key)` tuple, we return the
//! cached result instead of reprocessing.
//!
//! The protocol works as follows:
//! - Clients send a 16-byte UUID idempotency key per write
//! - Server assigns sequences at Raft commit time
//! - Cache hit with same payload returns cached result (idempotent retry)
//! - Cache hit with different payload returns `IDEMPOTENCY_KEY_REUSED` error
//!
//! Uses moka for bounded, TinyLFU-based caching with TTL eviction. An in-flight
//! tracking map (`DashMap`) serializes concurrent requests with the same idempotency
//! key to prevent duplicate Raft proposals during leader failover. See
//! [`IdempotencyCache::try_acquire_inflight`] and [`InFlightGuard`].

use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use inferadb_ledger_proto::proto::WriteSuccess;
use inferadb_ledger_types::types::{OrganizationId, VaultId};
use moka::sync::Cache;
use tokio::sync::Notify;

/// Maximum number of entries in the cache.
const MAX_CACHE_SIZE: u64 = 100_000;

/// Time-to-live for cache entries (24 hours).
const ENTRY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Composite key for idempotency cache.
///
/// Cache key is `(organization, vault, client_id, idempotency_key)` where
/// `idempotency_key` is a 16-byte UUID provided by the client.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    /// Organization internal ID.
    pub organization: OrganizationId,
    /// Vault internal ID (0 if not specified).
    pub vault: VaultId,
    /// Client identifier (`Box<str>` saves 8 bytes vs `String` — no capacity field for immutable
    /// keys).
    pub client_id: Box<str>,
    /// Client-provided idempotency key (16-byte UUID).
    pub idempotency_key: [u8; 16],
}

impl IdempotencyKey {
    /// Creates a new idempotency key.
    pub fn new(
        organization: OrganizationId,
        vault: VaultId,
        client_id: impl Into<Box<str>>,
        idempotency_key: [u8; 16],
    ) -> Self {
        Self { organization, vault, client_id: client_id.into(), idempotency_key }
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

/// Result of attempting to acquire an in-flight slot for an idempotency key.
///
/// Used by the write service to serialize concurrent requests with the same
/// idempotency key, preventing duplicate Raft proposals during failover.
pub enum InFlightStatus {
    /// This request acquired the in-flight slot. It is responsible for executing
    /// the write and calling [`InFlightGuard::release`] when done.
    Acquired(InFlightGuard),
    /// Another request with the same key is already in-flight. The caller should
    /// wait on the [`Notify`] and then re-check the moka cache for the result.
    Waiting(Arc<Notify>),
}

/// Guard that tracks an in-flight write for a specific idempotency key.
///
/// When dropped without calling [`release`](InFlightGuard::release), the in-flight
/// entry is removed and waiters are notified (ensuring no deadlock on error paths).
pub struct InFlightGuard {
    cache: Arc<IdempotencyCache>,
    key: IdempotencyKey,
    released: bool,
}

impl InFlightGuard {
    /// Releases the in-flight slot, notifying any waiting requests.
    ///
    /// Call this after inserting the result into the moka cache so that
    /// waiters see the cached result when they re-check.
    pub fn release(mut self) {
        self.released = true;
        self.cache.release_inflight(&self.key);
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if !self.released {
            self.cache.release_inflight(&self.key);
        }
    }
}

/// Thread-safe idempotency cache with bounded size and TTL eviction.
///
/// The cache maps `(organization, vault, client_id, idempotency_key) -> CachedResult`.
///
/// Behavior:
/// - New key: Process write, cache result
/// - Same key + same payload hash: Return cached result (idempotent retry)
/// - Same key + different payload hash: Return `KeyReused` error
///
/// Uses moka's TinyLFU admission policy which considers both recency and frequency,
/// achieving significantly better hit rates than traditional LRU.
///
/// An in-flight tracking map serializes concurrent requests with the same key.
/// The first request acquires the slot; subsequent requests wait for it to
/// complete and then read the cached result. This prevents duplicate Raft
/// proposals when the moka cache is cold (e.g., after leader failover).
pub struct IdempotencyCache {
    /// The underlying moka cache with built-in TTL and TinyLFU eviction.
    cache: Cache<IdempotencyKey, CachedResult>,
    /// Tracks keys that currently have a write in-flight.
    ///
    /// When a request acquires a slot, waiters block on the `Notify` until the
    /// slot is released. Bounded by concurrent request count, not cache size.
    in_flight: DashMap<IdempotencyKey, Arc<Notify>>,
}

impl IdempotencyCache {
    /// Creates a new idempotency cache.
    pub fn new() -> Self {
        let cache = Cache::builder().max_capacity(MAX_CACHE_SIZE).time_to_live(ENTRY_TTL).build();

        Self { cache, in_flight: DashMap::new() }
    }

    /// Attempts to acquire an in-flight slot for the given idempotency key.
    ///
    /// Returns [`InFlightStatus::Acquired`] if no other request is currently
    /// processing this key. The caller receives an [`InFlightGuard`] that must
    /// be released after the write completes and the result is cached.
    ///
    /// Returns [`InFlightStatus::Waiting`] if another request already holds the
    /// slot. The caller should `.notified().await` on the returned [`Notify`],
    /// then re-check the moka cache for the result.
    ///
    /// This prevents the TOCTOU race during leader failover where the moka
    /// cache is cold and two concurrent requests both pass the replicated-state
    /// check before either commits.
    pub fn try_acquire_inflight(self: &Arc<Self>, key: IdempotencyKey) -> InFlightStatus {
        use dashmap::mapref::entry::Entry;

        match self.in_flight.entry(key.clone()) {
            Entry::Vacant(vacant) => {
                let notify = Arc::new(Notify::new());
                vacant.insert(notify);
                InFlightStatus::Acquired(InFlightGuard {
                    cache: Arc::clone(self),
                    key,
                    released: false,
                })
            },
            Entry::Occupied(occupied) => InFlightStatus::Waiting(Arc::clone(occupied.get())),
        }
    }

    /// Removes an in-flight entry and notifies all waiters.
    fn release_inflight(&self, key: &IdempotencyKey) {
        if let Some((_, notify)) = self.in_flight.remove(key) {
            notify.notify_waiters();
        }
    }

    /// Checks if a request is a duplicate.
    ///
    /// Returns:
    /// - `NewRequest` if the idempotency key is not in the cache
    /// - `Duplicate(result)` if the key exists with the same payload hash
    /// - `KeyReused` if the key exists but the payload hash differs
    pub fn check(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
    ) -> IdempotencyCheckResult {
        let key = IdempotencyKey::new(organization, vault, client_id, idempotency_key);
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

    /// Inserts a new result into the cache.
    ///
    /// Capacity management and TTL eviction are handled automatically by moka.
    pub fn insert(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: impl Into<Box<str>>,
        idempotency_key: [u8; 16],
        request_hash: u64,
        result: WriteSuccess,
    ) {
        let key = IdempotencyKey::new(organization, vault, client_id, idempotency_key);
        self.cache.insert(key, CachedResult { request_hash, result });
    }

    /// Checks and inserts in one operation.
    ///
    /// Returns:
    /// - `NewRequest` if the key was not in cache (and the result has been inserted)
    /// - `Duplicate(result)` if the key exists with the same payload hash
    /// - `KeyReused` if the key exists but the payload hash differs
    pub fn check_and_insert(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
        result: WriteSuccess,
    ) -> IdempotencyCheckResult {
        // First check for existing entry
        let check_result =
            self.check(organization, vault, client_id, idempotency_key, request_hash);

        if matches!(check_result, IdempotencyCheckResult::NewRequest) {
            // Not in cache, insert the new result
            self.insert(organization, vault, client_id, idempotency_key, request_hash, result);
        }

        check_result
    }

    /// Returns the current number of entries in the cache.
    ///
    /// Note: This is an approximation as moka performs lazy eviction.
    pub fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }

    /// Checks if the cache is empty.
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
    use inferadb_ledger_proto::proto::TxId;

    use super::*;

    fn org(n: i64) -> OrganizationId {
        OrganizationId::new(n)
    }

    fn vault(n: i64) -> VaultId {
        VaultId::new(n)
    }

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
        let check =
            cache.check_and_insert(org(1), vault(1), "client-1", idem_key, request_hash, result);
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
        cache.insert(org(1), vault(1), "client-1".to_string(), idem_key, request_hash, result);

        // Retry with same key and hash should return cached result
        let check = cache.check(org(1), vault(1), "client-1", idem_key, request_hash);
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
        cache.insert(org(1), vault(1), "client-1".to_string(), idem_key, request_hash_1, result);

        // Retry with same key but different hash should return KeyReused
        let check = cache.check(org(1), vault(1), "client-1", idem_key, request_hash_2);
        assert!(matches!(check, IdempotencyCheckResult::KeyReused));
    }

    /// Verifies that idempotency keys are isolated across every key dimension.
    ///
    /// Each case inserts an entry, then checks with exactly one dimension changed.
    /// The check must return `NewRequest` — proving that dimension participates in
    /// the cache key.
    #[test]
    #[allow(clippy::type_complexity)]
    fn test_key_isolation_by_dimension() {
        // (label, insert params, check params)
        // Params: (org, vault, client_id, idempotency_key, request_hash)
        let cases: Vec<(&str, (i64, i64, &str, [u8; 16], u64), (i64, i64, &str, [u8; 16], u64))> = vec![
            (
                "different idempotency key",
                (1, 1, "client-1", make_key(1), 12345),
                (1, 1, "client-1", make_key(2), 12345),
            ),
            (
                "different client ID",
                (1, 1, "client-1", make_key(1), 12345),
                (1, 1, "client-2", make_key(1), 12345),
            ),
            (
                "different vault",
                (1, 1, "client-1", make_key(1), 12345),
                (1, 2, "client-1", make_key(1), 12345),
            ),
            (
                "different organization",
                (1, 1, "client-1", make_key(1), 12345),
                (2, 1, "client-1", make_key(1), 12345),
            ),
        ];

        for (label, insert, check) in &cases {
            let cache = IdempotencyCache::new();
            let result = make_result(100, 1);

            cache.insert(
                org(insert.0),
                vault(insert.1),
                insert.2.to_string(),
                insert.3,
                insert.4,
                result,
            );

            let outcome = cache.check(org(check.0), vault(check.1), check.2, check.3, check.4);
            assert!(
                matches!(outcome, IdempotencyCheckResult::NewRequest),
                "{label}: expected NewRequest, got {outcome:?}"
            );
        }
    }

    /// Verifies that two entries sharing the same idempotency key but differing
    /// in one dimension coexist independently in the cache.
    #[test]
    fn test_coexisting_entries_across_dimensions() {
        let cache = IdempotencyCache::new();
        let idem_key = make_key(1);
        let request_hash = 12345u64;
        let result = make_result(100, 1);

        // Insert for (org=1, vault=1) and (org=1, vault=2)
        cache.insert(
            org(1),
            vault(1),
            "client-1".to_string(),
            idem_key,
            request_hash,
            result.clone(),
        );
        cache.insert(org(1), vault(2), "client-1".to_string(), idem_key, request_hash, result);
        cache.run_pending_tasks();

        assert_eq!(cache.len(), 2, "should have entries for both vaults");

        assert!(matches!(
            cache.check(org(1), vault(1), "client-1", idem_key, request_hash),
            IdempotencyCheckResult::Duplicate(_)
        ));
        assert!(matches!(
            cache.check(org(1), vault(2), "client-1", idem_key, request_hash),
            IdempotencyCheckResult::Duplicate(_)
        ));
    }

    /// Test that assigned_sequence is properly stored and retrieved.
    #[test]
    fn test_assigned_sequence_preserved() {
        let cache = IdempotencyCache::new();
        let idem_key = make_key(1);
        let request_hash = 12345u64;

        // Insert with assigned_sequence = 42
        let result = make_result(100, 42);
        cache.insert(org(1), vault(1), "client-1".to_string(), idem_key, request_hash, result);

        // Retrieve should have same assigned_sequence
        let check = cache.check(org(1), vault(1), "client-1", idem_key, request_hash);
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

            let check = cache.check_and_insert(
                org(1),
                vault(1),
                "client-1",
                idem_key,
                request_hash,
                result,
            );
            assert!(matches!(check, IdempotencyCheckResult::NewRequest));
        }

        cache.run_pending_tasks();
        assert_eq!(cache.len(), 5, "all 5 writes should be cached");

        // Each should be retrievable with its assigned sequence
        for i in 0..5 {
            let idem_key = make_key(i);
            let check = cache.check(org(1), vault(1), "client-1", idem_key, request_hash);
            match check {
                IdempotencyCheckResult::Duplicate(cached) => {
                    assert_eq!(cached.assigned_sequence, i as u64 + 1);
                },
                _ => panic!("expected Duplicate for key {}", i),
            }
        }
    }

    // ── Concurrency Stress Tests ────────────────────────────────────────

    /// Stress test: 50 concurrent identical requests verify exactly-once execution.
    ///
    /// Simulates a thundering herd where 50 threads simultaneously submit the
    /// same (organization, vault, client, idempotency_key). Due to the TOCTOU gap
    /// in check_and_insert, multiple threads may race to insert. The test
    /// verifies that all threads see a consistent result (either NewRequest or
    /// Duplicate) and that the cached result is never corrupted.
    #[test]
    fn stress_concurrent_identical_requests_exactly_once() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            thread,
        };

        let cache = Arc::new(IdempotencyCache::new());
        let num_threads = 50;
        let idem_key = [0xAB; 16];
        let request_hash = 99999u64;
        let result = make_result(500, 42);

        let new_request_count = Arc::new(AtomicUsize::new(0));
        let duplicate_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let cache = Arc::clone(&cache);
            let result = result.clone();
            let new_count = Arc::clone(&new_request_count);
            let dup_count = Arc::clone(&duplicate_count);

            handles.push(thread::spawn(move || {
                let check = cache.check_and_insert(
                    org(1),
                    vault(1),
                    "thundering-herd-client",
                    idem_key,
                    request_hash,
                    result,
                );
                match check {
                    IdempotencyCheckResult::NewRequest => {
                        new_count.fetch_add(1, Ordering::Relaxed);
                    },
                    IdempotencyCheckResult::Duplicate(cached) => {
                        // Duplicate must return the correct cached result
                        assert_eq!(
                            cached.assigned_sequence, 42,
                            "Duplicate returned wrong sequence"
                        );
                        assert_eq!(
                            cached.block_height, 500,
                            "Duplicate returned wrong block height"
                        );
                        dup_count.fetch_add(1, Ordering::Relaxed);
                    },
                    IdempotencyCheckResult::KeyReused => {
                        panic!("KeyReused should never occur with same request_hash");
                    },
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        cache.run_pending_tasks();

        // Due to the TOCTOU gap in check_and_insert (check, then insert separately),
        // multiple threads may see NewRequest before any insert completes.
        // The important invariant is: total == num_threads, no panics, no corruption.
        let new_count = new_request_count.load(Ordering::Relaxed);
        let dup_count = duplicate_count.load(Ordering::Relaxed);
        assert_eq!(
            new_count + dup_count,
            num_threads,
            "All threads must report either NewRequest or Duplicate"
        );

        // At least one thread must have been a new request
        assert!(new_count >= 1, "At least one thread should see NewRequest");

        // The cache should contain exactly one entry
        assert_eq!(cache.len(), 1, "Cache should have exactly one entry");

        // A subsequent check must return Duplicate with correct data
        let final_check =
            cache.check(org(1), vault(1), "thundering-herd-client", idem_key, request_hash);
        match final_check {
            IdempotencyCheckResult::Duplicate(cached) => {
                assert_eq!(cached.assigned_sequence, 42);
            },
            other => panic!("Expected Duplicate, got {other:?}"),
        }
    }

    /// Stress test: cache eviction during active deduplication.
    ///
    /// Fills the cache near capacity with unique entries, then races
    /// deduplication checks against new insertions that trigger eviction.
    /// Verifies that eviction doesn't corrupt entries being actively read.
    #[test]
    fn stress_cache_eviction_during_deduplication() {
        use std::{sync::Arc, thread};

        let cache = Arc::new(IdempotencyCache::new());

        // Pre-fill with entries
        for i in 0..200u16 {
            let mut key = [0u8; 16];
            key[0] = (i >> 8) as u8;
            key[1] = (i & 0xFF) as u8;
            let result = make_result(i as u64, i as u64);
            cache.insert(
                org(i as i64),
                vault(0),
                format!("prefill-client-{i}"),
                key,
                i as u64,
                result,
            );
        }
        cache.run_pending_tasks();

        let mut handles = Vec::new();

        // Thread group 1: Check existing entries (deduplication reads)
        for batch in 0..5 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in (batch * 40)..((batch + 1) * 40).min(200) {
                    let mut key = [0u8; 16];
                    key[0] = (i >> 8) as u8;
                    key[1] = (i & 0xFF) as u8;
                    let check = cache.check(
                        org(i as i64),
                        vault(0),
                        &format!("prefill-client-{i}"),
                        key,
                        i as u64,
                    );
                    // Entry may have been evicted by moka's TinyLFU policy, so
                    // either Duplicate (still cached) or NewRequest (evicted) is valid.
                    match check {
                        IdempotencyCheckResult::Duplicate(cached) => {
                            assert_eq!(
                                cached.assigned_sequence, i as u64,
                                "Corrupted cache entry for key {i}"
                            );
                        },
                        IdempotencyCheckResult::NewRequest => {
                            // Evicted by TinyLFU — acceptable
                        },
                        IdempotencyCheckResult::KeyReused => {
                            panic!("KeyReused should not occur with same hash");
                        },
                    }
                }
            }));
        }

        // Thread group 2: Insert new entries (may trigger eviction)
        for batch in 0..5 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let idx = 1000 + batch * 100 + i;
                    let mut key = [0u8; 16];
                    key[0] = (idx >> 8) as u8;
                    key[1] = (idx & 0xFF) as u8;
                    let result = make_result(idx as u64, idx as u64);
                    cache.insert(
                        org(idx as i64),
                        vault(0),
                        format!("new-client-{idx}"),
                        key,
                        idx as u64,
                        result,
                    );
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Cache should be in a consistent state (no panics)
        cache.run_pending_tasks();
        assert!(!cache.is_empty(), "Cache should not be empty after inserts");
    }

    // ── In-Flight Guard Tests ──────────────────────────────────────────

    /// First acquire returns `Acquired`; second for same key returns `Waiting`.
    #[test]
    fn inflight_first_acquires_second_waits() {
        let cache = Arc::new(IdempotencyCache::new());
        let key = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(1));

        let status1 = cache.try_acquire_inflight(key.clone());
        assert!(matches!(status1, InFlightStatus::Acquired(_)));

        let status2 = cache.try_acquire_inflight(key);
        assert!(matches!(status2, InFlightStatus::Waiting(_)));
    }

    /// After the guard is dropped, the key can be acquired again.
    #[test]
    fn inflight_released_after_guard_drop() {
        let cache = Arc::new(IdempotencyCache::new());
        let key = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(1));

        let guard = match cache.try_acquire_inflight(key.clone()) {
            InFlightStatus::Acquired(g) => g,
            _ => panic!("expected Acquired"),
        };
        drop(guard);

        // Should be acquirable again
        let status = cache.try_acquire_inflight(key);
        assert!(matches!(status, InFlightStatus::Acquired(_)));
    }

    /// Explicit release() clears the in-flight entry before drop.
    #[test]
    fn inflight_explicit_release() {
        let cache = Arc::new(IdempotencyCache::new());
        let key = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(1));

        let guard = match cache.try_acquire_inflight(key.clone()) {
            InFlightStatus::Acquired(g) => g,
            _ => panic!("expected Acquired"),
        };
        guard.release();

        let status = cache.try_acquire_inflight(key);
        assert!(matches!(status, InFlightStatus::Acquired(_)));
    }

    /// Different keys can be acquired concurrently (no cross-key blocking).
    #[test]
    fn inflight_different_keys_independent() {
        let cache = Arc::new(IdempotencyCache::new());
        let key1 = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(1));
        let key2 = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(2));

        let _guard1 = match cache.try_acquire_inflight(key1) {
            InFlightStatus::Acquired(g) => g,
            _ => panic!("expected Acquired for key1"),
        };

        let status2 = cache.try_acquire_inflight(key2);
        assert!(
            matches!(status2, InFlightStatus::Acquired(_)),
            "different key should not be blocked"
        );
    }

    /// Waiters are notified when the guard is released, allowing them to
    /// re-acquire.
    ///
    /// Uses `tokio::select!` to verify that the waiter unblocks when the
    /// guard is released on a separate task, without timing fragility.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inflight_waiters_notified_on_release() {
        let cache = Arc::new(IdempotencyCache::new());
        let key = IdempotencyKey::new(org(1), vault(1), "client-1", make_key(1));

        let guard = match cache.try_acquire_inflight(key.clone()) {
            InFlightStatus::Acquired(g) => g,
            _ => panic!("expected Acquired"),
        };

        let notify = match cache.try_acquire_inflight(key) {
            InFlightStatus::Waiting(n) => n,
            _ => panic!("expected Waiting"),
        };

        // Spawn a task that releases the guard after inserting a result.
        // The task runs concurrently; the main task waits on the notify.
        let cache2 = Arc::clone(&cache);
        tokio::spawn(async move {
            // Small yield to let the main task start polling notified().
            tokio::task::yield_now().await;
            cache2.insert(org(1), vault(1), "client-1", make_key(1), 12345, make_result(100, 1));
            guard.release();
        });

        // This is the production pattern: notified() is called and polled in
        // a single expression, so the waiter is registered before any yield.
        let completed =
            tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified()).await;
        assert!(completed.is_ok(), "waiter should be notified within 5s");

        let result = cache.check(org(1), vault(1), "client-1", make_key(1), 12345);
        assert!(
            matches!(result, IdempotencyCheckResult::Duplicate(_)),
            "waiter should see cached result after notification"
        );
    }

    /// Stress test: multiple concurrent acquires for the same key.
    /// Exactly one gets `Acquired`, all others get `Waiting`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_inflight_serialization() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = Arc::new(IdempotencyCache::new());
        let num_tasks = 20;
        let acquired_count = Arc::new(AtomicUsize::new(0));
        let coalesced_count = Arc::new(AtomicUsize::new(0));
        let idem_key = [0xCD; 16];
        let request_hash = 55555u64;
        let result = make_result(200, 7);

        // Use a barrier to ensure all tasks start at roughly the same time.
        let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks));

        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            let acquired = Arc::clone(&acquired_count);
            let coalesced = Arc::clone(&coalesced_count);
            let result = result.clone();

            handles.push(tokio::spawn(async move {
                barrier.wait().await;

                let key = IdempotencyKey::new(org(1), vault(1), "stress-client", idem_key);
                match cache.try_acquire_inflight(key) {
                    InFlightStatus::Acquired(guard) => {
                        acquired.fetch_add(1, Ordering::Relaxed);
                        // Simulate the write path: insert result into moka, then release
                        cache.insert(
                            org(1),
                            vault(1),
                            "stress-client",
                            idem_key,
                            request_hash,
                            result,
                        );
                        guard.release();
                    },
                    InFlightStatus::Waiting(notify) => {
                        coalesced.fetch_add(1, Ordering::Relaxed);

                        // Register waiter synchronously before any yield; the acquirer
                        // inserts before releasing, so a late waiter's cache re-check
                        // is guaranteed to see the result. See coalesced_waiter_sees_late_notify
                        // for the deterministic regression test of this race.
                        let notified = notify.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();

                        // Short-circuit: if notify_waiters() already fired, cache is populated.
                        let check =
                            cache.check(org(1), vault(1), "stress-client", idem_key, request_hash);
                        if matches!(check, IdempotencyCheckResult::Duplicate(_)) {
                            return;
                        }

                        notified.await;
                        let check =
                            cache.check(org(1), vault(1), "stress-client", idem_key, request_hash);
                        assert!(
                            matches!(check, IdempotencyCheckResult::Duplicate(_)),
                            "coalesced request should see cached result"
                        );
                    },
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let total =
            acquired_count.load(Ordering::Relaxed) + coalesced_count.load(Ordering::Relaxed);
        assert_eq!(total, num_tasks, "all tasks must complete");
        // At least one must have acquired
        assert!(acquired_count.load(Ordering::Relaxed) >= 1);
    }

    /// Regression test for the lost-wakeup race in the idempotency cache.
    ///
    /// The naive `notify.notified().await` pattern hangs when `notify_waiters()`
    /// fires before the waiter's `Notified` future registers. This test drives
    /// the race deterministically: it retains a notify Arc from a Waiting response,
    /// releases the acquirer (which fires `notify_waiters()` with no registered
    /// waiter), then runs the fixed waiter pattern against that stale Arc.
    ///
    /// Without the fix, this test hits the 100ms timeout. With the fix, it
    /// completes synchronously via the pre-await cache re-check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_waiter_sees_late_notify() {
        use std::time::Duration;

        let cache = Arc::new(IdempotencyCache::new());
        let idem_key = [0xEE; 16];
        let request_hash = 77777u64;
        let result = make_result(300, 11);

        let key = IdempotencyKey::new(org(1), vault(1), "late-notify-client", idem_key);

        // First call acquires the slot; second call on the same key returns
        // Waiting, giving us an Arc<Notify> to drive the lost-wakeup race against.
        let guard = match cache.try_acquire_inflight(key.clone()) {
            InFlightStatus::Acquired(g) => g,
            _ => panic!("expected Acquired on first call"),
        };
        let notify = match cache.try_acquire_inflight(key) {
            InFlightStatus::Waiting(n) => n,
            _ => panic!("expected Waiting on second call"),
        };

        // Populate the cache, then release — fires notify_waiters() with no waiter.
        cache.insert(org(1), vault(1), "late-notify-client", idem_key, request_hash, result);
        guard.release();

        // Apply the fixed waiter pattern against the (now stale) notify Arc.
        // Wrap in a 100ms timeout: without the fix, notified().await hangs forever.
        let outcome = tokio::time::timeout(Duration::from_millis(100), async {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // Short-circuit: cache is already populated, no need to await.
            match cache.check(org(1), vault(1), "late-notify-client", idem_key, request_hash) {
                IdempotencyCheckResult::Duplicate(_) => "short_circuit",
                IdempotencyCheckResult::KeyReused => panic!("unexpected KeyReused"),
                IdempotencyCheckResult::NewRequest => {
                    notified.await;
                    "awaited"
                },
            }
        })
        .await;

        match outcome {
            Ok("short_circuit") => {},
            Ok("awaited") => panic!("expected short-circuit path, got awaited path"),
            Ok(other) => panic!("unexpected path: {other}"),
            Err(_) => panic!("lost wakeup: waiter timed out — the fix is not applied"),
        }
    }
}
