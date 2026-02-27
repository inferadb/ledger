//! Dynamic VIP organization discovery and caching.
//!
//! VIP organizations receive elevated sampling rates for request logging.
//! VIP status can be configured:
//!
//! 1. **Statically** - via `vip_organizations` config list (always VIP, override)
//! 2. **Dynamically** - via metadata tags in `_system` organization (cached with TTL)
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │                      VipCache                         │
//! │  ┌──────────────────────┐  ┌──────────────────────┐  │
//! │  │     static_vips      │  │    dynamic_cache     │  │
//! │  │ HashSet<Org.Slug>    │  │ RwLock<HashMap>      │  │
//! │  └──────────────────────┘  └──────────────────────┘  │
//! │              │                       │               │
//! │              └───────────┬───────────┘               │
//! │                          ▼                           │
//! │            is_vip(organization: OrganizationSlug)     │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! # VIP Metadata Schema
//!
//! VIP tags are stored in the `_system` organization with:
//! - Key: `{tag_name}:organization:{slug}` (e.g., `vip:organization:42`)
//! - Value: JSON `{"enabled": true, "reason": "production", "updated_at": "RFC3339"}`
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_raft::vip_cache::VipCache;
//! use inferadb_ledger_types::OrganizationSlug;
//!
//! // Create cache with static VIP list
//! let cache = VipCache::new(vec![
//!     OrganizationSlug::new(1),
//!     OrganizationSlug::new(2),
//!     OrganizationSlug::new(3),
//! ]);
//!
//! // Check VIP status (sync, uses cache)
//! assert!(cache.is_vip(OrganizationSlug::new(1)));   // static VIP
//! assert!(!cache.is_vip(OrganizationSlug::new(999))); // not VIP
//! ```

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_types::OrganizationSlug;
use parking_lot::RwLock;

/// Configuration for VIP cache behavior.
#[derive(Debug, Clone)]
pub struct VipCacheConfig {
    /// Whether dynamic discovery from `_system` is enabled.
    pub discovery_enabled: bool,
    /// Cache TTL for dynamic VIP lookups.
    pub cache_ttl: Duration,
    /// Metadata tag name for VIP markers.
    pub tag_name: String,
}

impl Default for VipCacheConfig {
    fn default() -> Self {
        Self {
            discovery_enabled: true,
            cache_ttl: Duration::from_secs(60),
            tag_name: "vip".to_string(),
        }
    }
}

impl VipCacheConfig {
    /// Creates a disabled config for testing.
    pub fn disabled() -> Self {
        Self {
            discovery_enabled: false,
            cache_ttl: Duration::from_secs(60),
            tag_name: "vip".to_string(),
        }
    }
}

/// Cached entry for an organization's VIP status.
#[derive(Debug, Clone)]
struct CacheEntry {
    is_vip: bool,
    cached_at: Instant,
}

impl CacheEntry {
    fn new(is_vip: bool) -> Self {
        Self { is_vip, cached_at: Instant::now() }
    }

    fn is_stale(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Thread-safe cache for VIP organization status.
///
/// Combines static VIP configuration with dynamically discovered VIP tags.
/// Static VIPs always take precedence (override).
#[derive(Debug)]
pub struct VipCache {
    /// Configuration for cache behavior.
    config: VipCacheConfig,
    /// Static VIP organizations (always VIP, config override).
    static_vips: HashSet<OrganizationSlug>,
    /// Dynamic VIP cache from `_system` metadata.
    /// Only populated when discovery is enabled.
    dynamic_cache: RwLock<HashMap<OrganizationSlug, CacheEntry>>,
    /// Last time the cache was refreshed.
    last_refresh: RwLock<Option<Instant>>,
}

impl VipCache {
    /// Creates a new VIP cache with static VIP organization slugs.
    pub fn new(static_vip_organizations: Vec<OrganizationSlug>) -> Self {
        Self::with_config(static_vip_organizations, VipCacheConfig::default())
    }

    /// Creates a VIP cache with custom configuration.
    pub fn with_config(
        static_vip_organizations: Vec<OrganizationSlug>,
        config: VipCacheConfig,
    ) -> Self {
        Self {
            config,
            static_vips: static_vip_organizations.into_iter().collect(),
            dynamic_cache: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(None),
        }
    }

    /// Creates a disabled cache for testing (no VIPs).
    pub fn disabled() -> Self {
        Self::with_config(Vec::new(), VipCacheConfig::disabled())
    }

    /// Checks if an organization is VIP.
    ///
    /// Returns true if:
    /// 1. Organization is in the static VIP list (override), OR
    /// 2. Organization is in the dynamic cache and marked as VIP
    ///
    /// This is a synchronous operation using cached data.
    ///
    /// * `organization` - Organization slug (external identifier).
    pub fn is_vip(&self, organization: OrganizationSlug) -> bool {
        // Static VIPs always take precedence
        if self.static_vips.contains(&organization) {
            return true;
        }

        // Check dynamic cache if discovery is enabled
        if self.config.discovery_enabled {
            let cache = self.dynamic_cache.read();
            if let Some(entry) = cache.get(&organization) {
                // Return cached value even if stale (background refresh)
                return entry.is_vip;
            }
        }

        false
    }

    /// Checks if an organization is VIP and whether the cache entry is stale.
    ///
    /// Returns `(is_vip, is_stale)` tuple.
    ///
    /// * `organization` - Organization slug (external identifier).
    pub fn is_vip_with_staleness(&self, organization: OrganizationSlug) -> (bool, bool) {
        // Static VIPs are never stale
        if self.static_vips.contains(&organization) {
            return (true, false);
        }

        // Check dynamic cache
        if self.config.discovery_enabled {
            let cache = self.dynamic_cache.read();
            if let Some(entry) = cache.get(&organization) {
                let is_stale = entry.is_stale(self.config.cache_ttl);
                return (entry.is_vip, is_stale);
            }
        }

        // Not in cache - consider stale (should trigger refresh)
        (false, true)
    }

    /// Updates the dynamic cache with VIP status for an organization.
    ///
    /// This is called when VIP status is discovered from `_system`.
    ///
    /// * `organization` - Organization slug (external identifier).
    pub fn update(&self, organization: OrganizationSlug, is_vip: bool) {
        if !self.config.discovery_enabled {
            return;
        }

        let mut cache = self.dynamic_cache.write();
        cache.insert(organization, CacheEntry::new(is_vip));
    }

    /// Bulk updates the dynamic cache.
    ///
    /// Replaces the entire dynamic cache with the provided mapping.
    pub fn bulk_update(&self, vip_status: HashMap<OrganizationSlug, bool>) {
        if !self.config.discovery_enabled {
            return;
        }

        let mut cache = self.dynamic_cache.write();
        cache.clear();
        for (organization, is_vip) in vip_status {
            cache.insert(organization, CacheEntry::new(is_vip));
        }

        let mut last_refresh = self.last_refresh.write();
        *last_refresh = Some(Instant::now());
    }

    /// Checks if the cache needs a full refresh.
    ///
    /// Returns true if:
    /// - Cache has never been refreshed, OR
    /// - Last refresh was more than TTL ago
    pub fn needs_refresh(&self) -> bool {
        if !self.config.discovery_enabled {
            return false;
        }

        let last_refresh = self.last_refresh.read();
        match *last_refresh {
            None => true,
            Some(instant) => instant.elapsed() > self.config.cache_ttl,
        }
    }

    /// Clears the dynamic cache.
    pub fn clear(&self) {
        let mut cache = self.dynamic_cache.write();
        cache.clear();

        let mut last_refresh = self.last_refresh.write();
        *last_refresh = None;
    }

    /// Returns statistics about the cache.
    pub fn stats(&self) -> VipCacheStats {
        let cache = self.dynamic_cache.read();
        let stale_count = cache.values().filter(|e| e.is_stale(self.config.cache_ttl)).count();

        VipCacheStats {
            static_vips: self.static_vips.len(),
            dynamic_entries: cache.len(),
            stale_entries: stale_count,
        }
    }

    /// Returns the cache TTL.
    pub fn cache_ttl(&self) -> Duration {
        self.config.cache_ttl
    }

    /// Returns the tag name used for VIP metadata.
    pub fn tag_name(&self) -> &str {
        &self.config.tag_name
    }

    /// Checks if discovery is enabled.
    pub fn discovery_enabled(&self) -> bool {
        self.config.discovery_enabled
    }
}

/// Statistics about the VIP cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VipCacheStats {
    /// Number of static VIP organizations.
    pub static_vips: usize,
    /// Number of entries in the dynamic cache.
    pub dynamic_entries: usize,
    /// Number of stale entries in the dynamic cache.
    pub stale_entries: usize,
}

/// Creates a VipCache from server configuration.
///
/// This is a convenience function to create a cache from the
/// `LoggingConfig` configuration.
///
/// * `vip_organizations` - Organization slugs (external identifiers) for static VIP list.
pub fn create_vip_cache(
    vip_organizations: Vec<OrganizationSlug>,
    discovery_enabled: bool,
    cache_ttl_secs: u64,
    tag_name: String,
) -> Arc<VipCache> {
    let config = VipCacheConfig {
        discovery_enabled,
        cache_ttl: Duration::from_secs(cache_ttl_secs),
        tag_name,
    };
    Arc::new(VipCache::with_config(vip_organizations, config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slug(n: u64) -> OrganizationSlug {
        OrganizationSlug::new(n)
    }

    #[test]
    fn test_vip_cache_static_vips() {
        let cache = VipCache::new(vec![slug(1), slug(2), slug(3)]);

        assert!(cache.is_vip(slug(1)));
        assert!(cache.is_vip(slug(2)));
        assert!(cache.is_vip(slug(3)));
        assert!(!cache.is_vip(slug(4)));
        assert!(!cache.is_vip(slug(0)));
    }

    #[test]
    fn test_vip_cache_empty_static() {
        let cache = VipCache::new(vec![]);

        assert!(!cache.is_vip(slug(1)));
        assert!(!cache.is_vip(slug(0)));
    }

    #[test]
    fn test_vip_cache_disabled() {
        let cache = VipCache::disabled();

        assert!(!cache.is_vip(slug(1)));
        assert!(!cache.discovery_enabled());
    }

    #[test]
    fn test_vip_cache_dynamic_update() {
        let cache = VipCache::new(vec![]);

        // Initially not VIP
        assert!(!cache.is_vip(slug(42)));

        // Update dynamic cache
        cache.update(slug(42), true);
        assert!(cache.is_vip(slug(42)));

        // Update to non-VIP
        cache.update(slug(42), false);
        assert!(!cache.is_vip(slug(42)));
    }

    #[test]
    fn test_vip_cache_static_takes_precedence() {
        let cache = VipCache::new(vec![slug(1), slug(2)]);

        // Dynamic update cannot override static VIP
        cache.update(slug(1), false);
        assert!(cache.is_vip(slug(1))); // Still VIP due to static list
    }

    #[test]
    fn test_vip_cache_bulk_update() {
        let cache = VipCache::new(vec![]);

        let mut vips = HashMap::new();
        vips.insert(slug(10), true);
        vips.insert(slug(20), true);
        vips.insert(slug(30), false);

        cache.bulk_update(vips);

        assert!(cache.is_vip(slug(10)));
        assert!(cache.is_vip(slug(20)));
        assert!(!cache.is_vip(slug(30)));
        assert!(!cache.is_vip(slug(40)));
    }

    #[test]
    fn test_vip_cache_staleness() {
        let config = VipCacheConfig {
            discovery_enabled: true,
            cache_ttl: Duration::from_millis(10), // Very short TTL for testing
            tag_name: "vip".to_string(),
        };
        let cache = VipCache::with_config(vec![slug(1)], config);

        // Static VIP is never stale
        let (is_vip, is_stale) = cache.is_vip_with_staleness(slug(1));
        assert!(is_vip);
        assert!(!is_stale);

        // Dynamic entry starts fresh
        cache.update(slug(42), true);
        let (is_vip, is_stale) = cache.is_vip_with_staleness(slug(42));
        assert!(is_vip);
        assert!(!is_stale);

        // After TTL, entry is stale
        std::thread::sleep(Duration::from_millis(20));
        let (is_vip, is_stale) = cache.is_vip_with_staleness(slug(42));
        assert!(is_vip); // Still returns cached value
        assert!(is_stale); // But marked as stale
    }

    #[test]
    fn test_vip_cache_needs_refresh() {
        let config = VipCacheConfig {
            discovery_enabled: true,
            cache_ttl: Duration::from_millis(10),
            tag_name: "vip".to_string(),
        };
        let cache = VipCache::with_config(vec![], config);

        // Initially needs refresh (never refreshed)
        assert!(cache.needs_refresh());

        // After bulk update, doesn't need refresh
        cache.bulk_update(HashMap::new());
        assert!(!cache.needs_refresh());

        // After TTL, needs refresh again
        std::thread::sleep(Duration::from_millis(20));
        assert!(cache.needs_refresh());
    }

    #[test]
    fn test_vip_cache_disabled_ignores_updates() {
        let cache = VipCache::disabled();

        cache.update(slug(42), true);
        assert!(!cache.is_vip(slug(42))); // Ignored because disabled

        let mut vips = HashMap::new();
        vips.insert(slug(10), true);
        cache.bulk_update(vips);
        assert!(!cache.is_vip(slug(10))); // Still ignored
    }

    #[test]
    fn test_vip_cache_clear() {
        let cache = VipCache::new(vec![slug(1)]);

        cache.update(slug(42), true);
        assert!(cache.is_vip(slug(42)));

        cache.clear();
        assert!(!cache.is_vip(slug(42))); // Cleared
        assert!(cache.is_vip(slug(1))); // Static not affected
        assert!(cache.needs_refresh()); // Needs refresh after clear
    }

    #[test]
    fn test_vip_cache_stats() {
        let config = VipCacheConfig {
            discovery_enabled: true,
            cache_ttl: Duration::from_millis(10),
            tag_name: "vip".to_string(),
        };
        let cache = VipCache::with_config(vec![slug(1), slug(2), slug(3)], config);

        cache.update(slug(10), true);
        cache.update(slug(20), false);

        let stats = cache.stats();
        assert_eq!(stats.static_vips, 3);
        assert_eq!(stats.dynamic_entries, 2);
        assert_eq!(stats.stale_entries, 0);

        // After TTL, entries become stale
        std::thread::sleep(Duration::from_millis(20));
        let stats = cache.stats();
        assert_eq!(stats.stale_entries, 2);
    }

    #[test]
    fn test_vip_cache_config_accessors() {
        let config = VipCacheConfig {
            discovery_enabled: true,
            cache_ttl: Duration::from_secs(120),
            tag_name: "priority".to_string(),
        };
        let cache = VipCache::with_config(vec![], config);

        assert!(cache.discovery_enabled());
        assert_eq!(cache.cache_ttl(), Duration::from_secs(120));
        assert_eq!(cache.tag_name(), "priority");
    }

    #[test]
    fn test_create_vip_cache_helper() {
        let cache = create_vip_cache(vec![slug(1), slug(2)], true, 30, "vip".to_string());

        assert!(cache.is_vip(slug(1)));
        assert!(cache.discovery_enabled());
        assert_eq!(cache.cache_ttl(), Duration::from_secs(30));
    }

    #[test]
    fn test_vip_cache_thread_safety() {
        use std::thread;

        let cache = Arc::new(VipCache::new(vec![slug(1)]));
        let mut handles = vec![];

        // Spawn multiple readers
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = cache_clone.is_vip(slug(i));
                }
            }));
        }

        // Spawn writer
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                cache_clone.update(slug(i), i % 2 == 0);
            }
        }));

        for handle in handles {
            let _ = handle.join();
        }

        // Cache should still be consistent
        assert!(cache.is_vip(slug(1))); // Static VIP preserved
    }
}
