//! Per-vault leader cache for fine-grained routing.
//!
//! Phase 6 deliverable: with the three-tier consensus topology (system /
//! region / per-organization Raft groups), individual vaults can have their
//! own delegated leader. Routing every request through the region's gateway
//! defeats the latency benefit of per-org / per-vault leadership, so the SDK
//! caches `(organization_id, vault_id) -> leader endpoint` directly and
//! consults that cache on the hot path.
//!
//! # Relationship to [`crate::region_resolver::RegionLeaderCache`]
//!
//! The vault cache is **additive**, not a replacement. It coexists with the
//! region cache during the rollout:
//!
//! - When a `NotLeader` hint carries `vault_id`, the vault cache is updated.
//! - When a hint omits `vault_id`, the region cache is updated (legacy / region-scoped rejections).
//! - When the connection pool resolves a channel for a vault-scoped request, it consults the vault
//!   cache first; on miss it falls back to the region cache, then to gateway resolution.
//!
//! # Invariants
//!
//! - **Term-gated**: a hint with `term < cached.term` is dropped (root SDK golden rule 6).
//!   Stale-leader hints from older terms must not overwrite newer cache state — letting them
//!   through causes retry storms during leader-flap windows because each side keeps fighting to
//!   install its own cached value.
//! - **Bounded**: capacity is configured via `ClientConfig::vault_cache_capacity` (default 10,000).
//!   On overflow the least-recently-used entry is evicted.
//! - **Lock-free reads on the lookup path** in the steady state: `lookup` takes a read lock and
//!   updates a per-entry atomic access counter, so concurrent readers never serialize on a write
//!   lock.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use parking_lot::RwLock;

use crate::error::LeaderHint;

/// Default capacity for the per-vault leader cache.
pub const DEFAULT_VAULT_CACHE_CAPACITY: usize = 10_000;

/// Cached leader information for a single `(organization, vault)` pair.
#[derive(Debug, Clone)]
pub struct LeaderEntry {
    /// Leader node ID, when known.
    pub leader_node_id: Option<u64>,
    /// Leader endpoint URI (e.g. `"http://10.0.2.5:5000"`), when known.
    pub leader_endpoint: Option<Arc<str>>,
    /// Raft term the leader was last observed in, when known.
    pub term: Option<u64>,
    /// Wall-clock time the entry was last refreshed.
    pub last_updated: Instant,
}

/// Internal cache slot: stores the leader entry plus an access counter for LRU.
#[derive(Debug)]
struct Slot {
    entry: LeaderEntry,
    /// Monotonic access counter. Bumped on every successful `lookup`.
    /// Used as the LRU recency proxy.
    last_accessed: AtomicU64,
}

/// Per-vault leader cache keyed by `(OrganizationSlug, VaultSlug)`.
///
/// The cache is keyed by external **Snowflake slugs**, not internal IDs —
/// the SDK only ever has slugs in hand (the request types take slugs and
/// the gRPC wire carries `{Entity}Slug { slug: u64 }`). Slug→ID translation
/// lives server-side in `crates/services/src/services/slug_resolver.rs`;
/// the SDK never crosses that boundary. The server emits both `leader_*`
/// (internal IDs) and `leader_*_slug` (external slugs) keys in
/// `ErrorDetails` so the cache can populate from a `NotLeader` hint
/// directly.
///
/// Thread-safe: the inner map is guarded by a `parking_lot::RwLock` so
/// concurrent readers do not block each other. Writes (insertions, hint
/// applications, invalidations) take the write lock briefly.
///
/// ```no_run
/// # use inferadb_ledger_sdk::vault_resolver::VaultLeaderCache;
/// # use inferadb_ledger_sdk::LeaderHint;
/// # use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
/// let cache = VaultLeaderCache::with_capacity(1_000);
/// let org = OrganizationSlug::new(42);
/// let vault = VaultSlug::new(7);
/// let hint = LeaderHint {
///     leader_id: Some(3),
///     leader_endpoint: Some("http://10.0.2.5:5000".to_owned()),
///     term: Some(11),
///     organization_id: Some(42),
///     vault_id: Some(7),
///     organization_slug: Some(42),
///     vault_slug: Some(7),
/// };
/// cache.apply_hint(org, vault, &hint);
/// let entry = cache.lookup(org, vault).expect("populated");
/// assert_eq!(entry.term, Some(11));
/// ```
#[derive(Debug)]
pub struct VaultLeaderCache {
    inner: RwLock<std::collections::HashMap<(OrganizationSlug, VaultSlug), Slot>>,
    /// Maximum number of entries before LRU eviction kicks in.
    capacity: usize,
    /// Monotonic counter bumped on every access. Wrapping is benign — the
    /// LRU comparison only cares about relative ordering inside the cache,
    /// and at 1 ns/inc this would take 584 years to wrap.
    access_counter: AtomicU64,
    metrics: RwLock<Arc<dyn crate::metrics::SdkMetrics>>,
}

impl VaultLeaderCache {
    /// Creates a new cache with [`DEFAULT_VAULT_CACHE_CAPACITY`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_VAULT_CACHE_CAPACITY)
    }

    /// Creates a new cache with the given maximum capacity.
    ///
    /// Capacity is interpreted as a soft upper bound: insertions at-or-above
    /// capacity evict the least-recently-used entry before storing the new
    /// one. A capacity of zero produces a cache that drops every insertion.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(std::collections::HashMap::with_capacity(capacity.min(1024))),
            capacity,
            access_counter: AtomicU64::new(0),
            metrics: RwLock::new(Arc::new(crate::metrics::NoopSdkMetrics)),
        }
    }

    /// Installs the metrics sink for this cache.
    pub fn set_metrics(&self, metrics: Arc<dyn crate::metrics::SdkMetrics>) {
        *self.metrics.write() = metrics;
    }

    /// Returns the configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Returns `true` when the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Looks up the leader entry for `(organization, vault)`, if cached.
    ///
    /// Updates the entry's LRU recency on hit. Lock contention is limited to
    /// the read lock plus a single relaxed atomic write — concurrent readers
    /// for different keys never serialize.
    #[must_use]
    pub fn lookup(&self, organization: OrganizationSlug, vault: VaultSlug) -> Option<LeaderEntry> {
        let guard = self.inner.read();
        let slot = guard.get(&(organization, vault))?;
        slot.last_accessed.store(self.next_access(), Ordering::Relaxed);
        Some(slot.entry.clone())
    }

    /// Returns the cached leader endpoint for `(organization, vault)`, if
    /// any. Convenience wrapper around [`Self::lookup`].
    #[must_use]
    pub fn cached_endpoint(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Option<Arc<str>> {
        self.lookup(organization, vault).and_then(|e| e.leader_endpoint)
    }

    /// Applies a leader hint (parsed from a `NotLeader` error) to the cache.
    ///
    /// Term-gated: a hint whose term is older than the currently-cached term
    /// for the same `(organization, vault)` pair is dropped. The metrics
    /// sink is notified so operators can observe how often stale hints are
    /// being rejected.
    pub fn apply_hint(&self, organization: OrganizationSlug, vault: VaultSlug, hint: &LeaderHint) {
        // Hints without an endpoint are useless — they can't be dialed.
        // Match `RegionLeaderCache::apply_hint` semantics here.
        let Some(endpoint) = hint.leader_endpoint.as_deref() else {
            return;
        };
        if endpoint.is_empty() {
            return;
        }

        let mut guard = self.inner.write();
        if let Some(existing) = guard.get(&(organization, vault))
            && !Self::should_accept_write(existing.entry.term, hint.term)
        {
            drop(guard);
            self.metrics().leader_stale_term_rejected(
                &format!("{}:{}", organization.value(), vault.value()),
                "hint",
            );
            return;
        }

        let new_entry = LeaderEntry {
            leader_node_id: hint.leader_id,
            leader_endpoint: Some(Arc::from(endpoint)),
            term: hint.term,
            last_updated: Instant::now(),
        };

        if !guard.contains_key(&(organization, vault)) && guard.len() >= self.capacity {
            // Capacity-zero cache: silently drop.
            if self.capacity == 0 {
                return;
            }
            // Evict the least-recently-used entry.
            let victim = guard
                .iter()
                .min_by_key(|(_, slot)| slot.last_accessed.load(Ordering::Relaxed))
                .map(|(k, _)| *k);
            if let Some(key) = victim {
                guard.remove(&key);
            }
        }

        let slot = Slot { entry: new_entry, last_accessed: AtomicU64::new(self.next_access()) };
        guard.insert((organization, vault), slot);
    }

    /// Removes the cached entry for `(organization, vault)`. No-op when the
    /// entry is absent.
    pub fn invalidate(&self, organization: OrganizationSlug, vault: VaultSlug) {
        self.inner.write().remove(&(organization, vault));
    }

    /// Drops every cached entry.
    pub fn clear(&self) {
        self.inner.write().clear();
    }

    /// Returns `true` when a cache write carrying `incoming_term` should be
    /// accepted given a currently-cached `cached_term`. Mirrors
    /// `RegionLeaderCache::should_accept_write`.
    fn should_accept_write(cached_term: Option<u64>, incoming_term: Option<u64>) -> bool {
        match (cached_term, incoming_term) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(c), Some(i)) => i >= c,
        }
    }

    fn next_access(&self) -> u64 {
        self.access_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn metrics(&self) -> Arc<dyn crate::metrics::SdkMetrics> {
        Arc::clone(&self.metrics.read())
    }
}

impl Default for VaultLeaderCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn org(v: u64) -> OrganizationSlug {
        OrganizationSlug::new(v)
    }
    fn vault(v: u64) -> VaultSlug {
        VaultSlug::new(v)
    }

    fn hint(endpoint: &str, term: Option<u64>) -> LeaderHint {
        LeaderHint {
            leader_id: Some(1),
            leader_endpoint: Some(endpoint.to_owned()),
            term,
            organization_id: None,
            vault_id: None,
            organization_slug: None,
            vault_slug: None,
        }
    }

    #[test]
    fn empty_cache_lookup_returns_none() {
        let cache = VaultLeaderCache::new();
        assert!(cache.lookup(org(1), vault(1)).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn apply_hint_then_lookup_round_trips() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(2), &hint("http://leader:5000", Some(7)));
        let entry = cache.lookup(org(1), vault(2)).expect("populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://leader:5000"));
        assert_eq!(entry.term, Some(7));
        assert_eq!(entry.leader_node_id, Some(1));
    }

    #[test]
    fn apply_hint_without_endpoint_is_noop() {
        let cache = VaultLeaderCache::new();
        let h = LeaderHint {
            leader_id: Some(1),
            leader_endpoint: None,
            term: Some(7),
            organization_id: None,
            vault_id: None,
            organization_slug: None,
            vault_slug: None,
        };
        cache.apply_hint(org(1), vault(2), &h);
        assert!(cache.lookup(org(1), vault(2)).is_none());
    }

    #[test]
    fn apply_hint_with_empty_endpoint_is_noop() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(2), &hint("", Some(7)));
        assert!(cache.lookup(org(1), vault(2)).is_none());
    }

    #[test]
    fn term_gating_rejects_lower_term() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));

        // Older term must NOT overwrite — a stale reply during leader flap
        // cannot replace a newer cache entry, otherwise we'd retry-storm
        // toward the deposed leader.
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", Some(5)));

        let entry = cache.lookup(org(1), vault(1)).expect("populated");
        assert_eq!(
            entry.leader_endpoint.as_deref(),
            Some("http://A:5000"),
            "older-term hint must be dropped",
        );
        assert_eq!(entry.term, Some(7));
    }

    #[test]
    fn term_gating_accepts_equal_term() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", Some(7)));

        // Equal-term re-assert is accepted: it may carry a fresher endpoint
        // (e.g. the same leader's restart).
        let entry = cache.lookup(org(1), vault(1)).expect("populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://B:5000"));
    }

    #[test]
    fn term_gating_accepts_higher_term() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(5)));
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", Some(9)));

        let entry = cache.lookup(org(1), vault(1)).expect("populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://B:5000"));
        assert_eq!(entry.term, Some(9));
    }

    #[test]
    fn term_gating_rejects_none_when_cache_has_term() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", None));

        let entry = cache.lookup(org(1), vault(1)).expect("populated");
        assert_eq!(
            entry.leader_endpoint.as_deref(),
            Some("http://A:5000"),
            "termless hint must not overwrite a termed entry",
        );
    }

    #[test]
    fn term_gating_accepts_any_when_cache_has_no_term() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", None));
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", Some(5)));

        let entry = cache.lookup(org(1), vault(1)).expect("populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://B:5000"));
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        assert!(cache.lookup(org(1), vault(1)).is_some());
        cache.invalidate(org(1), vault(1));
        assert!(cache.lookup(org(1), vault(1)).is_none());
    }

    #[test]
    fn invalidate_only_targets_specified_pair() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        cache.apply_hint(org(1), vault(2), &hint("http://B:5000", Some(7)));
        cache.apply_hint(org(2), vault(1), &hint("http://C:5000", Some(7)));

        cache.invalidate(org(1), vault(1));

        assert!(cache.lookup(org(1), vault(1)).is_none());
        assert!(cache.lookup(org(1), vault(2)).is_some(), "sibling vault must survive");
        assert!(cache.lookup(org(2), vault(1)).is_some(), "sibling org must survive");
    }

    #[test]
    fn distinct_org_vault_pairs_are_independent() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        cache.apply_hint(org(2), vault(1), &hint("http://B:5000", Some(7)));

        // Same vault id but different org id — must not collide.
        assert_eq!(
            cache.lookup(org(1), vault(1)).unwrap().leader_endpoint.as_deref(),
            Some("http://A:5000"),
        );
        assert_eq!(
            cache.lookup(org(2), vault(1)).unwrap().leader_endpoint.as_deref(),
            Some("http://B:5000"),
        );
    }

    #[test]
    fn lru_eviction_at_capacity() {
        // Capacity 3: inserting a 4th entry must evict the LRU.
        let cache = VaultLeaderCache::with_capacity(3);
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(1)));
        cache.apply_hint(org(1), vault(2), &hint("http://B:5000", Some(1)));
        cache.apply_hint(org(1), vault(3), &hint("http://C:5000", Some(1)));
        assert_eq!(cache.len(), 3);

        // Touch (1,1) so it is most-recently-used; (1,2) is now LRU.
        let _ = cache.lookup(org(1), vault(1));
        let _ = cache.lookup(org(1), vault(3));
        // Insert a 4th — (1,2) should evict.
        cache.apply_hint(org(1), vault(4), &hint("http://D:5000", Some(1)));

        assert_eq!(cache.len(), 3);
        assert!(cache.lookup(org(1), vault(1)).is_some(), "MRU survives");
        assert!(cache.lookup(org(1), vault(2)).is_none(), "LRU evicted");
        assert!(cache.lookup(org(1), vault(3)).is_some());
        assert!(cache.lookup(org(1), vault(4)).is_some(), "newly inserted survives");
    }

    #[test]
    fn updating_existing_entry_does_not_evict() {
        let cache = VaultLeaderCache::with_capacity(2);
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(1)));
        cache.apply_hint(org(1), vault(2), &hint("http://B:5000", Some(1)));

        // Re-applying a hint to an existing key must not trigger eviction.
        cache.apply_hint(org(1), vault(1), &hint("http://A2:5000", Some(2)));

        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.lookup(org(1), vault(1)).unwrap().leader_endpoint.as_deref(),
            Some("http://A2:5000"),
        );
        assert!(cache.lookup(org(1), vault(2)).is_some());
    }

    #[test]
    fn capacity_zero_drops_all_inserts() {
        let cache = VaultLeaderCache::with_capacity(0);
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(1)));
        assert!(cache.lookup(org(1), vault(1)).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn clear_drops_all_entries() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(1)));
        cache.apply_hint(org(2), vault(2), &hint("http://B:5000", Some(1)));
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn cached_endpoint_convenience_returns_arc() {
        let cache = VaultLeaderCache::new();
        cache.apply_hint(org(1), vault(1), &hint("http://leader:5000", Some(1)));
        let ep = cache.cached_endpoint(org(1), vault(1)).expect("populated");
        assert_eq!(ep.as_ref(), "http://leader:5000");
    }

    #[test]
    fn slug_keyed_hint_round_trips_through_cache() {
        // The cache is keyed on (OrganizationSlug, VaultSlug). A LeaderHint
        // carrying the new `*_slug` fields populates and looks up
        // successfully — this is the bug fix's happy path.
        let cache = VaultLeaderCache::new();
        let h = LeaderHint {
            leader_id: Some(3),
            leader_endpoint: Some("http://leader:5000".to_owned()),
            term: Some(11),
            organization_id: Some(42),
            vault_id: Some(7),
            organization_slug: Some(1234),
            vault_slug: Some(5678),
        };
        // Note: apply_hint uses the (org_slug, vault_slug) parameters directly
        // — the hint's `organization_slug`/`vault_slug` fields are
        // informational here. The pool's `apply_region_leader_hint_or_invalidate`
        // is what reads those fields off the hint to choose the cache key.
        cache.apply_hint(OrganizationSlug::new(1234), VaultSlug::new(5678), &h);
        let entry =
            cache.lookup(OrganizationSlug::new(1234), VaultSlug::new(5678)).expect("populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://leader:5000"));
    }

    #[test]
    fn stale_term_rejected_metric_fires() {
        use std::sync::atomic::AtomicU64;

        #[derive(Debug, Default)]
        struct Counting {
            stale: AtomicU64,
        }
        impl crate::metrics::SdkMetrics for Counting {
            fn leader_stale_term_rejected(&self, _label: &str, _source: &'static str) {
                self.stale.fetch_add(1, Ordering::Relaxed);
            }
        }

        let metrics = Arc::new(Counting::default());
        let cache = VaultLeaderCache::new();
        cache.set_metrics(Arc::clone(&metrics) as Arc<dyn crate::metrics::SdkMetrics>);

        cache.apply_hint(org(1), vault(1), &hint("http://A:5000", Some(7)));
        cache.apply_hint(org(1), vault(1), &hint("http://B:5000", Some(5)));

        assert_eq!(metrics.stale.load(Ordering::Relaxed), 1);
    }
}
