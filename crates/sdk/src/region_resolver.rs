//! Region leader resolution and caching for data residency routing.
//!
//! When the SDK needs to reach the Raft leader for a specific data residency
//! region, it resolves the leader endpoint via the gateway's
//! `ResolveRegionLeader` RPC and caches the result with a TTL. This avoids
//! a discovery round-trip on every request while still reacting to leader
//! changes within seconds.

use std::time::{Duration, Instant};

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::Region;
use tonic::transport::Channel;

use crate::{
    error::{Result, SdkError},
    proto_util::region_to_proto_i32,
};

/// Internal cached leader entry.
#[derive(Debug, Clone)]
struct CachedLeader {
    endpoint: String,
    /// Stored for future term-based cache invalidation (e.g., stale-term eviction).
    #[allow(dead_code)]
    raft_term: u64,
    resolved_at: Instant,
    ttl: Duration,
}

impl CachedLeader {
    /// Returns `true` if the cached entry has not expired.
    fn is_valid(&self) -> bool {
        self.resolved_at.elapsed() < self.ttl
    }
}

/// Default TTL applied when the server returns `ttl_seconds = 0`.
const DEFAULT_TTL_SECS: u64 = 30;

/// Cache for the Raft leader endpoint of a specific data residency region.
///
/// Thread-safe: uses a `parking_lot::RwLock` so concurrent readers do not
/// block each other. Only `resolve` and `invalidate` take a write lock.
#[derive(Debug)]
pub struct RegionLeaderCache {
    region: Region,
    cached: parking_lot::RwLock<Option<CachedLeader>>,
}

impl RegionLeaderCache {
    /// Creates a new empty cache for the given region.
    #[must_use]
    pub fn new(region: Region) -> Self {
        Self { region, cached: parking_lot::RwLock::new(None) }
    }

    /// Returns the cached leader endpoint if the cache entry is still valid.
    ///
    /// Returns `None` when the cache is empty or the TTL has expired.
    #[must_use]
    pub fn cached_endpoint(&self) -> Option<String> {
        let guard = self.cached.read();
        guard.as_ref().filter(|c| c.is_valid()).map(|c| c.endpoint.clone())
    }

    /// Returns the region this cache is associated with.
    #[must_use]
    pub fn region(&self) -> Region {
        self.region
    }

    /// Clears the cached leader entry, forcing the next access to re-resolve.
    pub fn invalidate(&self) {
        *self.cached.write() = None;
    }

    /// Resolves the region leader via the gateway's `ResolveRegionLeader` RPC,
    /// caches the result, and returns the leader endpoint.
    ///
    /// # Errors
    ///
    /// Returns an [`SdkError`] if the gRPC call fails (network error, server
    /// unavailable, unknown region, etc.).
    pub async fn resolve(&self, gateway_channel: &Channel) -> Result<String> {
        let mut client = proto::system_discovery_service_client::SystemDiscoveryServiceClient::new(
            gateway_channel.clone(),
        );

        let response = client
            .resolve_region_leader(proto::ResolveRegionLeaderRequest {
                region: region_to_proto_i32(self.region),
            })
            .await
            .map_err(SdkError::from)?;

        let resp = response.into_inner();
        let ttl = if resp.ttl_seconds > 0 {
            Duration::from_secs(u64::from(resp.ttl_seconds))
        } else {
            Duration::from_secs(DEFAULT_TTL_SECS)
        };

        let endpoint = resp.endpoint.clone();

        *self.cached.write() = Some(CachedLeader {
            endpoint: endpoint.clone(),
            raft_term: resp.raft_term,
            resolved_at: Instant::now(),
            ttl,
        });

        Ok(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_leader_valid_within_ttl() {
        let cached = CachedLeader {
            endpoint: "http://10.0.1.5:5000".to_string(),
            raft_term: 1,
            resolved_at: Instant::now(),
            ttl: Duration::from_secs(30),
        };
        assert!(cached.is_valid());
    }

    #[test]
    fn cached_leader_invalid_after_ttl() {
        let cached = CachedLeader {
            endpoint: "http://10.0.1.5:5000".to_string(),
            raft_term: 1,
            resolved_at: Instant::now() - Duration::from_secs(60),
            ttl: Duration::from_secs(30),
        };
        assert!(!cached.is_valid());
    }

    #[test]
    fn invalidate_clears_cache() {
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        *cache.cached.write() = Some(CachedLeader {
            endpoint: "http://10.0.1.5:5000".to_string(),
            raft_term: 1,
            resolved_at: Instant::now(),
            ttl: Duration::from_secs(30),
        });
        assert!(cache.cached_endpoint().is_some());
        cache.invalidate();
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn new_cache_has_no_endpoint() {
        let cache = RegionLeaderCache::new(Region::US_EAST_VA);
        assert!(cache.cached_endpoint().is_none());
    }

    #[test]
    fn region_accessor() {
        let cache = RegionLeaderCache::new(Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(cache.region(), Region::DE_CENTRAL_FRANKFURT);
    }
}
