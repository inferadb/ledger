//! Pre-deduplication cache for idempotent proposal handling.
//!
//! Tracks recently committed proposals by their idempotency key so that
//! duplicate submissions can be detected and short-circuited before
//! entering the Raft log.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::clock::{Clock, SystemClock};

/// Result of checking an idempotency key against the cache.
#[derive(Debug, PartialEq, Eq)]
pub enum IdempotencyCheck {
    /// The key has not been seen before (or has expired).
    New,
    /// The key was already committed at the given log index.
    Duplicate {
        /// The Raft commit index where this key was originally applied.
        commit_index: u64,
    },
}

/// A time-bounded cache that maps idempotency keys to their commit index.
///
/// Entries are evicted once they exceed the configured TTL. The cache is
/// designed for single-threaded use inside the Raft state-machine apply
/// loop; external synchronization is the caller's responsibility.
///
/// Time is provided by an injectable [`Clock`], defaulting to
/// [`SystemClock`]. For deterministic testing, pass a
/// [`SimulatedClock`](crate::clock::SimulatedClock) via [`with_clock`](Self::with_clock).
pub struct IdempotencyCache {
    entries: HashMap<[u8; 16], (u64, Instant)>,
    ttl: Duration,
    clock: Box<dyn Clock>,
}

impl IdempotencyCache {
    /// Create a new cache with the given time-to-live for entries.
    ///
    /// Uses the system monotonic clock. For deterministic testing, use
    /// [`with_clock`](Self::with_clock) instead.
    pub fn new(ttl: Duration) -> Self {
        Self { entries: HashMap::new(), ttl, clock: Box::new(SystemClock) }
    }

    /// Create a new cache with the given TTL and an injectable clock.
    pub fn with_clock(ttl: Duration, clock: impl Clock) -> Self {
        Self { entries: HashMap::new(), ttl, clock: Box::new(clock) }
    }

    /// Check whether `key` has already been committed.
    pub fn check(&self, key: &[u8; 16]) -> IdempotencyCheck {
        let now = self.clock.now();
        match self.entries.get(key) {
            Some(&(commit_index, inserted_at)) if now.duration_since(inserted_at) < self.ttl => {
                IdempotencyCheck::Duplicate { commit_index }
            },
            _ => IdempotencyCheck::New,
        }
    }

    /// Record that `key` was committed at `commit_index`.
    pub fn insert(&mut self, key: [u8; 16], commit_index: u64) {
        self.entries.insert(key, (commit_index, self.clock.now()));
    }

    /// Remove all entries whose TTL has elapsed.
    pub fn evict_expired(&mut self) {
        let ttl = self.ttl;
        let now = self.clock.now();
        self.entries.retain(|_, (_, inserted_at)| now.duration_since(*inserted_at) < ttl);
    }

    /// Return the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::clock::SimulatedClock;

    fn key(b: u8) -> [u8; 16] {
        [b; 16]
    }

    fn test_cache(ttl: Duration) -> (IdempotencyCache, Arc<SimulatedClock>) {
        let clock = Arc::new(SimulatedClock::new());
        let cache = IdempotencyCache::with_clock(ttl, Arc::clone(&clock));
        (cache, clock)
    }

    #[test]
    fn unknown_key_returns_new() {
        let (cache, _clock) = test_cache(Duration::from_secs(60));
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::New);
    }

    #[test]
    fn inserted_key_returns_duplicate_with_commit_index() {
        let (mut cache, _clock) = test_cache(Duration::from_secs(60));
        cache.insert(key(1), 42);
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::Duplicate { commit_index: 42 });
    }

    #[test]
    fn different_keys_are_independent() {
        let (mut cache, _clock) = test_cache(Duration::from_secs(60));
        cache.insert(key(1), 10);
        cache.insert(key(2), 20);

        assert_eq!(cache.check(&key(1)), IdempotencyCheck::Duplicate { commit_index: 10 });
        assert_eq!(cache.check(&key(2)), IdempotencyCheck::Duplicate { commit_index: 20 });
        assert_eq!(cache.check(&key(3)), IdempotencyCheck::New);
    }

    #[test]
    fn reinserting_same_key_updates_commit_index() {
        let (mut cache, _clock) = test_cache(Duration::from_secs(60));
        cache.insert(key(1), 10);
        cache.insert(key(1), 50);
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::Duplicate { commit_index: 50 });
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn empty_cache_reports_empty() {
        let (cache, _clock) = test_cache(Duration::from_secs(60));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn expired_key_returns_new() {
        let (mut cache, clock) = test_cache(Duration::from_secs(10));
        cache.insert(key(1), 7);
        clock.advance(Duration::from_secs(11));
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::New);
    }

    #[test]
    fn non_expired_key_returns_duplicate() {
        let (mut cache, clock) = test_cache(Duration::from_secs(10));
        cache.insert(key(1), 7);
        clock.advance(Duration::from_secs(9));
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::Duplicate { commit_index: 7 });
    }

    #[test]
    fn evict_expired_removes_only_stale_entries() {
        let (mut cache, clock) = test_cache(Duration::from_secs(10));
        cache.insert(key(1), 1);
        cache.insert(key(2), 2);
        assert_eq!(cache.len(), 2);

        clock.advance(Duration::from_secs(11));

        // Insert a fresh entry after advancing so it survives eviction.
        cache.insert(key(3), 3);
        cache.evict_expired();

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::New);
        assert_eq!(cache.check(&key(2)), IdempotencyCheck::New);
        assert_eq!(cache.check(&key(3)), IdempotencyCheck::Duplicate { commit_index: 3 });
    }

    #[test]
    fn evict_on_empty_cache_is_noop() {
        let (mut cache, _clock) = test_cache(Duration::from_secs(60));
        cache.evict_expired();
        assert!(cache.is_empty());
    }

    #[test]
    fn exact_ttl_boundary_expires_entry() {
        let (mut cache, clock) = test_cache(Duration::from_secs(10));
        cache.insert(key(1), 1);
        clock.advance(Duration::from_secs(10));
        assert_eq!(cache.check(&key(1)), IdempotencyCheck::New);
    }
}
