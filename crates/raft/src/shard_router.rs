//! Shard router for multi-shard request routing.
//!
//! Per DESIGN.md §4.6: Routes requests to the correct shard based on namespace.
//! Maintains a local cache of namespace → shard mappings with TTL-based invalidation.
//!
//! ## Architecture
//!
//! InferaDB uses a multi-Raft architecture where each shard is an independent
//! Raft group. The `_system` namespace stores the routing table mapping
//! namespaces to shards. This router provides:
//!
//! - Namespace → shard lookups via `_system`
//! - Cached routing with TTL-based expiration
//! - Connection pooling to shard leaders
//! - Leader hint tracking for fast routing
//!
//! ## Routing Flow
//!
//! 1. Check local cache for namespace → shard mapping
//! 2. If miss or stale, query `_system` for current mapping
//! 3. Connect to shard leader (using `leader_hint` or discovery)
//! 4. Cache the connection for subsequent requests
//!
//! ## Cache Invalidation
//!
//! - TTL-based: entries expire after `cache_ttl` (default 60s)
//! - Version-based: `config_version` changes trigger invalidation
//! - Leader redirect: stale leader hints are updated on redirect

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_state::system::{NamespaceStatus, SystemNamespaceService};
use inferadb_ledger_store::{FileBackend, StorageBackend};
use inferadb_ledger_types::{NamespaceId, ShardId};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during shard routing.
// Snafu generates fields for context selectors
#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum RoutingError {
    /// Namespace not found in routing table.
    #[snafu(display("Namespace {namespace_id} not found"))]
    NamespaceNotFound { namespace_id: NamespaceId },

    /// Namespace is not active (migrating, suspended, deleted).
    #[snafu(display("Namespace {namespace_id} is {status:?}"))]
    NamespaceUnavailable { namespace_id: NamespaceId, status: NamespaceStatus },

    /// Shard has no available nodes.
    #[snafu(display("Shard {shard_id} has no available nodes"))]
    NoAvailableNodes { shard_id: ShardId },

    /// Failed to connect to shard leader.
    #[snafu(display("Failed to connect to shard {shard_id}: {message}"))]
    ConnectionFailed { shard_id: ShardId, message: String },

    /// System service error during lookup.
    #[snafu(display("System lookup failed: {source}"))]
    SystemLookup { source: inferadb_ledger_state::system::SystemError },
}

/// Result type for routing operations.
pub type Result<T> = std::result::Result<T, RoutingError>;

// ============================================================================
// Routing Info (Public)
// ============================================================================

/// Public routing information for a namespace.
///
/// This is the public view of cached routing data, suitable for
/// use by other components (like MultiRaftManager).
#[derive(Debug, Clone)]
pub struct RoutingInfo {
    /// Shard hosting this namespace.
    pub shard_id: ShardId,
    /// Member nodes in the shard.
    pub member_nodes: Vec<String>,
    /// Hint for current leader (may be stale).
    pub leader_hint: Option<String>,
}

// ============================================================================
// Cache Entry (Internal)
// ============================================================================

/// Cached routing information for a namespace (internal).
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Shard hosting this namespace.
    shard_id: ShardId,
    /// Member nodes in the shard.
    member_nodes: Vec<String>,
    /// Hint for current leader (may be stale).
    leader_hint: Option<String>,
    /// Configuration version for invalidation (reserved for future use).
    #[allow(dead_code)] // metadata for cache invalidation
    config_version: u64,
    /// When this entry was cached.
    cached_at: Instant,
}

impl CacheEntry {
    /// Check if this cache entry is stale.
    fn is_stale(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }

    /// Convert to public RoutingInfo.
    fn to_routing_info(&self) -> RoutingInfo {
        RoutingInfo {
            shard_id: self.shard_id,
            member_nodes: self.member_nodes.clone(),
            leader_hint: self.leader_hint.clone(),
        }
    }
}

// ============================================================================
// Shard Connection
// ============================================================================

/// Connection to a shard for request forwarding.
#[derive(Debug, Clone)]
pub struct ShardConnection {
    /// Shard identifier.
    pub shard_id: ShardId,
    /// gRPC channel to the shard leader.
    pub channel: Channel,
    /// Address of the connected node.
    pub address: SocketAddr,
    /// Whether this is the confirmed leader.
    pub is_leader: bool,
}

// ============================================================================
// Shard Router
// ============================================================================

/// Configuration for the shard router.
#[derive(Debug, Clone, bon::Builder)]
pub struct RouterConfig {
    /// Time-to-live for cache entries.
    #[builder(default = Duration::from_secs(60))]
    pub cache_ttl: Duration,
    /// Connection timeout for shard connections.
    #[builder(default = Duration::from_secs(5))]
    pub connect_timeout: Duration,
    /// Maximum retry attempts for connection.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Base port for shard gRPC services.
    #[builder(default = 50051)]
    pub grpc_port: u16,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            grpc_port: 50051,
        }
    }
}

/// Shard router for routing requests to the correct shard.
///
/// Maintains a cache of namespace → shard mappings and manages
/// connections to shard leaders.
pub struct ShardRouter<B: StorageBackend + 'static = FileBackend> {
    /// System namespace service for namespace lookups.
    system: Arc<SystemNamespaceService<B>>,
    /// Cached namespace → shard mappings.
    cache: RwLock<HashMap<NamespaceId, CacheEntry>>,
    /// Active shard connections.
    connections: RwLock<HashMap<ShardId, ShardConnection>>,
    /// Router configuration.
    config: RouterConfig,
}

impl<B: StorageBackend + 'static> ShardRouter<B> {
    /// Create a new shard router.
    pub fn new(system: Arc<SystemNamespaceService<B>>) -> Self {
        Self::with_config(system, RouterConfig::default())
    }

    /// Create a new shard router with custom configuration.
    pub fn with_config(system: Arc<SystemNamespaceService<B>>, config: RouterConfig) -> Self {
        Self {
            system,
            cache: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Route a request to the appropriate shard.
    ///
    /// Returns the shard connection for the namespace. The connection
    /// may be cached or freshly established.
    pub async fn route(&self, namespace_id: NamespaceId) -> Result<ShardConnection> {
        // 1. Check cache
        let routing = self.get_routing_internal(namespace_id)?;

        // 2. Get or create connection
        self.get_connection(routing.shard_id, &routing.member_nodes, routing.leader_hint.as_deref())
            .await
    }

    /// Get routing information for a namespace (public API).
    ///
    /// Returns the shard assignment and member nodes for the namespace.
    /// Uses cached data if fresh, otherwise queries `_system`.
    pub fn get_routing(&self, namespace_id: NamespaceId) -> Result<RoutingInfo> {
        self.get_routing_internal(namespace_id).map(|entry| entry.to_routing_info())
    }

    /// Get routing information for a namespace (internal).
    ///
    /// Uses cached data if fresh, otherwise queries `_system`.
    fn get_routing_internal(&self, namespace_id: NamespaceId) -> Result<CacheEntry> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&namespace_id)
                && !entry.is_stale(self.config.cache_ttl)
            {
                debug!(
                    namespace_id = namespace_id.value(),
                    shard_id = entry.shard_id.value(),
                    "Cache hit"
                );
                return Ok(entry.clone());
            }
        }

        // Cache miss or stale - query _system
        self.refresh_routing(namespace_id)
    }

    /// Refresh routing information from `_system`.
    fn refresh_routing(&self, namespace_id: NamespaceId) -> Result<CacheEntry> {
        let registry = self
            .system
            .get_namespace(namespace_id)
            .context(SystemLookupSnafu)?
            .ok_or(RoutingError::NamespaceNotFound { namespace_id })?;

        // Check namespace status
        if registry.status != NamespaceStatus::Active {
            return Err(RoutingError::NamespaceUnavailable {
                namespace_id,
                status: registry.status,
            });
        }

        let entry = CacheEntry {
            shard_id: registry.shard_id,
            member_nodes: registry.member_nodes.clone(),
            leader_hint: registry.member_nodes.first().cloned(), // First node as hint
            config_version: registry.config_version,
            cached_at: Instant::now(),
        };

        // Update cache
        {
            let mut cache = self.cache.write();
            cache.insert(namespace_id, entry.clone());
        }

        info!(
            namespace_id = namespace_id.value(),
            shard_id = registry.shard_id.value(),
            config_version = registry.config_version,
            "Refreshed routing cache"
        );

        Ok(entry)
    }

    /// Get or create a connection to a shard.
    ///
    /// Returns an existing cached connection if available, otherwise creates a new one.
    /// The connection will be cached for future use.
    pub async fn get_connection(
        &self,
        shard_id: ShardId,
        member_nodes: &[String],
        leader_hint: Option<&str>,
    ) -> Result<ShardConnection> {
        // Check for existing connection
        {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(&shard_id) {
                // Verify connection is still valid
                // In a real implementation, we'd ping the connection
                return Ok(conn.clone());
            }
        }

        // No existing connection - create new one
        self.create_connection(shard_id, member_nodes, leader_hint).await
    }

    /// Create a new connection to a shard.
    async fn create_connection(
        &self,
        shard_id: ShardId,
        member_nodes: &[String],
        leader_hint: Option<&str>,
    ) -> Result<ShardConnection> {
        if member_nodes.is_empty() {
            return Err(RoutingError::NoAvailableNodes { shard_id });
        }

        // Try leader hint first, then other nodes
        let mut nodes_to_try: Vec<&str> = Vec::with_capacity(member_nodes.len());
        if let Some(hint) = leader_hint {
            nodes_to_try.push(hint);
        }
        for node in member_nodes {
            if Some(node.as_str()) != leader_hint {
                nodes_to_try.push(node);
            }
        }

        let mut last_error = None;

        for node_id in nodes_to_try {
            match self.connect_to_node(shard_id, node_id).await {
                Ok(conn) => {
                    // Cache the connection
                    {
                        let mut connections = self.connections.write();
                        connections.insert(shard_id, conn.clone());
                    }
                    return Ok(conn);
                },
                Err(e) => {
                    warn!(shard_id = shard_id.value(), node_id, error = %e, "Failed to connect to node");
                    last_error = Some(e);
                },
            }
        }

        Err(last_error.unwrap_or(RoutingError::NoAvailableNodes { shard_id }))
    }

    /// Connect to a specific node.
    async fn connect_to_node(&self, shard_id: ShardId, node_id: &str) -> Result<ShardConnection> {
        // Parse node address (format: "host:port" or just "host")
        let address = self.resolve_node_address(node_id)?;

        let endpoint = format!("http://{}", address);

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| RoutingError::ConnectionFailed {
                shard_id,
                message: format!("Invalid endpoint {}: {}", endpoint, e),
            })?
            .connect_timeout(self.config.connect_timeout)
            .connect()
            .await
            .map_err(|e| RoutingError::ConnectionFailed {
                shard_id,
                message: format!("Connection to {} failed: {}", endpoint, e),
            })?;

        debug!(shard_id = shard_id.value(), node_id, address = %address, "Connected to shard node");

        Ok(ShardConnection {
            shard_id,
            channel,
            address,
            is_leader: false, // Will be confirmed on first request
        })
    }

    /// Resolve a node ID to a socket address.
    fn resolve_node_address(&self, node_id: &str) -> Result<SocketAddr> {
        // Try parsing as socket address first
        if let Ok(addr) = node_id.parse::<SocketAddr>() {
            return Ok(addr);
        }

        // Try parsing as host:port
        if node_id.contains(':') {
            return node_id.parse().map_err(|_| RoutingError::ConnectionFailed {
                shard_id: ShardId::new(0),
                message: format!("Invalid address: {}", node_id),
            });
        }

        // Assume it's just a hostname, append default port
        let addr_str = format!("{}:{}", node_id, self.config.grpc_port);
        addr_str.parse().map_err(|_| RoutingError::ConnectionFailed {
            shard_id: ShardId::new(0),
            message: format!("Invalid hostname: {}", node_id),
        })
    }

    /// Invalidate cached routing for a namespace.
    ///
    /// Call this when a redirect indicates stale routing data.
    pub fn invalidate(&self, namespace_id: NamespaceId) {
        let mut cache = self.cache.write();
        if cache.remove(&namespace_id).is_some() {
            debug!(namespace_id = namespace_id.value(), "Invalidated routing cache");
        }
    }

    /// Invalidate all cached connections to a shard.
    ///
    /// Call this when a shard leader changes or becomes unavailable.
    pub fn invalidate_shard(&self, shard_id: ShardId) {
        let mut connections = self.connections.write();
        if connections.remove(&shard_id).is_some() {
            debug!(shard_id = shard_id.value(), "Invalidated shard connection");
        }
    }

    /// Update leader hint after successful request.
    ///
    /// Call this when a response indicates the actual leader.
    pub fn update_leader_hint(&self, namespace_id: NamespaceId, leader_node: &str) {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&namespace_id)
            && entry.leader_hint.as_deref() != Some(leader_node)
        {
            entry.leader_hint = Some(leader_node.to_string());
            debug!(namespace_id = namespace_id.value(), leader_node, "Updated leader hint");
        }
    }

    /// Get routing statistics.
    pub fn stats(&self) -> RouterStats {
        let cache = self.cache.read();
        let connections = self.connections.read();

        let stale_entries = cache.values().filter(|e| e.is_stale(self.config.cache_ttl)).count();

        RouterStats {
            cached_namespaces: cache.len(),
            stale_entries,
            active_connections: connections.len(),
        }
    }

    /// Clear all caches.
    ///
    /// Useful for testing or after cluster reconfiguration.
    pub fn clear(&self) {
        {
            let mut cache = self.cache.write();
            cache.clear();
        }
        {
            let mut connections = self.connections.write();
            connections.clear();
        }
        info!("Cleared all routing caches");
    }
}

/// Statistics about the router's cache state.
#[derive(Debug, Clone, Default)]
pub struct RouterStats {
    /// Number of cached namespace routings.
    pub cached_namespaces: usize,
    /// Number of stale cache entries.
    pub stale_entries: usize,
    /// Number of active shard connections.
    pub active_connections: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_state::StateLayer;
    use inferadb_ledger_store::{Database, FileBackend};
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    fn create_test_router() -> (ShardRouter<FileBackend>, Arc<StateLayer<FileBackend>>, TestDir) {
        let temp_dir = TestDir::new();
        let db = Arc::new(
            Database::<FileBackend>::create(temp_dir.join("test.db")).expect("create database"),
        );
        let state = Arc::new(StateLayer::new(db));
        let system = Arc::new(SystemNamespaceService::new(Arc::clone(&state)));

        let router = ShardRouter::new(system);
        (router, state, temp_dir)
    }

    #[test]
    fn test_router_config_default() {
        let config = RouterConfig::default();
        assert_eq!(config.cache_ttl, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_router_config_builder_with_defaults() {
        let config = RouterConfig::builder().build();
        assert_eq!(config.cache_ttl, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.grpc_port, 50051);
    }

    #[test]
    fn test_router_config_builder_with_custom_values() {
        let config = RouterConfig::builder()
            .cache_ttl(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10))
            .max_retries(5)
            .grpc_port(9090)
            .build();
        assert_eq!(config.cache_ttl, Duration::from_secs(120));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.grpc_port, 9090);
    }

    #[test]
    fn test_router_config_builder_matches_default() {
        let from_builder = RouterConfig::builder().build();
        let from_default = RouterConfig::default();
        assert_eq!(from_builder.cache_ttl, from_default.cache_ttl);
        assert_eq!(from_builder.connect_timeout, from_default.connect_timeout);
        assert_eq!(from_builder.max_retries, from_default.max_retries);
        assert_eq!(from_builder.grpc_port, from_default.grpc_port);
    }

    #[test]
    fn test_cache_entry_staleness() {
        let entry = CacheEntry {
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1".to_string()],
            leader_hint: Some("node-1".to_string()),
            config_version: 1,
            cached_at: Instant::now() - Duration::from_secs(30),
        };

        // Not stale with 60s TTL
        assert!(!entry.is_stale(Duration::from_secs(60)));

        // Stale with 10s TTL
        assert!(entry.is_stale(Duration::from_secs(10)));
    }

    #[test]
    fn test_resolve_node_address_socket_addr() {
        let (router, _, _temp) = create_test_router();

        let addr = router.resolve_node_address("127.0.0.1:50051").expect("parse address");
        assert_eq!(addr.port(), 50051);
    }

    #[test]
    fn test_resolve_node_address_hostname() {
        let (router, _, _temp) = create_test_router();

        let addr = router.resolve_node_address("127.0.0.1").expect("parse hostname");
        assert_eq!(addr.port(), router.config.grpc_port);
    }

    #[test]
    fn test_namespace_not_found() {
        let (router, _, _temp) = create_test_router();

        let result = router.get_routing(NamespaceId::new(999));
        assert!(
            matches!(result, Err(RoutingError::NamespaceNotFound { namespace_id }) if namespace_id == NamespaceId::new(999))
        );
    }

    #[test]
    fn test_invalidate_cache() {
        let (router, _, _temp) = create_test_router();

        // Manually insert a cache entry
        {
            let mut cache = router.cache.write();
            cache.insert(
                NamespaceId::new(1),
                CacheEntry {
                    shard_id: ShardId::new(1),
                    member_nodes: vec!["node-1".to_string()],
                    leader_hint: None,
                    config_version: 1,
                    cached_at: Instant::now(),
                },
            );
        }

        assert_eq!(router.stats().cached_namespaces, 1);

        router.invalidate(NamespaceId::new(1));

        assert_eq!(router.stats().cached_namespaces, 0);
    }

    #[test]
    fn test_router_stats() {
        let (router, _, _temp) = create_test_router();

        let stats = router.stats();
        assert_eq!(stats.cached_namespaces, 0);
        assert_eq!(stats.stale_entries, 0);
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_clear_caches() {
        let (router, _, _temp) = create_test_router();

        // Add some entries
        {
            let mut cache = router.cache.write();
            cache.insert(
                NamespaceId::new(1),
                CacheEntry {
                    shard_id: ShardId::new(1),
                    member_nodes: vec![],
                    leader_hint: None,
                    config_version: 1,
                    cached_at: Instant::now(),
                },
            );
        }

        router.clear();

        assert_eq!(router.stats().cached_namespaces, 0);
    }

    #[test]
    fn test_routing_after_namespace_registration() {
        let (router, state, _temp) = create_test_router();

        // Register a namespace in _system
        let registry = inferadb_ledger_state::system::NamespaceRegistry {
            namespace_id: NamespaceId::new(42),
            name: "test-ns".to_string(),
            shard_id: ShardId::new(3),
            member_nodes: vec!["node-a:50051".to_string(), "node-b:50051".to_string()],
            status: inferadb_ledger_state::system::NamespaceStatus::Active,
            config_version: 1,
            created_at: chrono::Utc::now(),
        };
        let system = SystemNamespaceService::new(Arc::clone(&state));
        system.register_namespace(&registry).expect("register namespace");

        // Router should resolve the namespace to shard 3
        let info = router.get_routing(NamespaceId::new(42)).expect("get routing");
        assert_eq!(info.shard_id, ShardId::new(3));
        assert_eq!(info.member_nodes.len(), 2);
        assert!(info.leader_hint.is_some(), "should have a leader hint");

        // Should be cached now
        assert_eq!(router.stats().cached_namespaces, 1);
    }

    #[test]
    fn test_routing_cache_populated_on_lookup() {
        let (router, state, _temp) = create_test_router();

        // Register two namespaces on different shards
        let system = SystemNamespaceService::new(Arc::clone(&state));

        for (ns_id, shard_id) in [(10, 1), (20, 2)] {
            let registry = inferadb_ledger_state::system::NamespaceRegistry {
                namespace_id: NamespaceId::new(ns_id),
                name: format!("ns-{}", ns_id),
                shard_id: ShardId::new(shard_id),
                member_nodes: vec![format!("node-{}:50051", shard_id)],
                status: inferadb_ledger_state::system::NamespaceStatus::Active,
                config_version: 1,
                created_at: chrono::Utc::now(),
            };
            system.register_namespace(&registry).expect("register");
        }

        // Look up both — should independently resolve
        let info1 = router.get_routing(NamespaceId::new(10)).expect("ns 10");
        let info2 = router.get_routing(NamespaceId::new(20)).expect("ns 20");

        assert_eq!(info1.shard_id, ShardId::new(1));
        assert_eq!(info2.shard_id, ShardId::new(2));

        // Both should be cached
        assert_eq!(router.stats().cached_namespaces, 2);
    }

    #[test]
    fn test_update_leader_hint_changes_cached_hint() {
        let (router, state, _temp) = create_test_router();

        // Register namespace
        let system = SystemNamespaceService::new(Arc::clone(&state));
        let registry = inferadb_ledger_state::system::NamespaceRegistry {
            namespace_id: NamespaceId::new(5),
            name: "hint-test".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-a:50051".to_string(), "node-b:50051".to_string()],
            status: inferadb_ledger_state::system::NamespaceStatus::Active,
            config_version: 1,
            created_at: chrono::Utc::now(),
        };
        system.register_namespace(&registry).expect("register");

        // Populate cache by doing a lookup
        let info = router.get_routing(NamespaceId::new(5)).expect("initial routing");
        assert_eq!(info.leader_hint.as_deref(), Some("node-a:50051"));

        // Update leader hint
        router.update_leader_hint(NamespaceId::new(5), "node-b:50051");

        // Re-read from cache (should use updated hint)
        let info2 = router.get_routing(NamespaceId::new(5)).expect("updated routing");
        assert_eq!(info2.leader_hint.as_deref(), Some("node-b:50051"));
    }

    #[test]
    fn test_routing_unavailable_namespace_status() {
        let (router, state, _temp) = create_test_router();

        // Register a namespace with Suspended status
        let system = SystemNamespaceService::new(Arc::clone(&state));
        let registry = inferadb_ledger_state::system::NamespaceRegistry {
            namespace_id: NamespaceId::new(77),
            name: "suspended-ns".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1:50051".to_string()],
            status: inferadb_ledger_state::system::NamespaceStatus::Suspended,
            config_version: 1,
            created_at: chrono::Utc::now(),
        };
        system.register_namespace(&registry).expect("register");

        // Routing should fail with NamespaceUnavailable
        let result = router.get_routing(NamespaceId::new(77));
        assert!(
            matches!(
                result,
                Err(RoutingError::NamespaceUnavailable {
                    namespace_id,
                    status: inferadb_ledger_state::system::NamespaceStatus::Suspended
                }) if namespace_id == NamespaceId::new(77)
            ),
            "expected NamespaceUnavailable, got: {:?}",
            result
        );
    }
}
