//! Region router for multi-region request routing.
//!
//! Routes requests to the correct region based on organization. Maintains a local
//! cache of organization to region mappings with TTL-based invalidation.
//!
//! ## Architecture
//!
//! InferaDB uses a multi-Raft architecture where each region is an independent
//! Raft group. The `_system` organization stores the routing table mapping
//! organizations to regions. This router provides:
//!
//! - Organization → region lookups via `_system`
//! - Cached routing with TTL-based expiration
//! - Connection pooling to region leaders
//! - Leader hint tracking for fast routing
//!
//! ## Routing Flow
//!
//! 1. Check local cache for organization → region mapping
//! 2. If miss or stale, query `_system` for current mapping
//! 3. Connect to region leader (using `leader_hint` or discovery)
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

use inferadb_ledger_state::system::{OrganizationStatus, SystemOrganizationService};
use inferadb_ledger_store::{FileBackend, StorageBackend};
use inferadb_ledger_types::{OrganizationId, Region};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during region routing.
#[derive(Debug, Snafu)]
pub enum RoutingError {
    /// Organization not found in routing table.
    #[snafu(display("Organization {organization} not found"))]
    OrganizationNotFound { organization: OrganizationId },

    /// Organization is not active (migrating, suspended, deleted).
    #[snafu(display("Organization {organization} is {status:?}"))]
    OrganizationUnavailable { organization: OrganizationId, status: OrganizationStatus },

    /// Region has no available nodes.
    #[snafu(display("Region {region} has no available nodes"))]
    NoAvailableNodes { region: Region },

    /// Failed to connect to region leader.
    #[snafu(display("Failed to connect to region {region}: {message}"))]
    ConnectionFailed { region: Region, message: String },

    /// System service error during lookup.
    #[snafu(display("System lookup failed: {source}"))]
    SystemLookup { source: inferadb_ledger_state::system::SystemError },
}

/// Result type for routing operations.
pub type Result<T> = std::result::Result<T, RoutingError>;

// ============================================================================
// Routing Info (Public)
// ============================================================================

/// Public routing information for an organization.
///
/// This is the public view of cached routing data, suitable for
/// use by other components (like [`RaftManager`](crate::raft_manager::RaftManager)).
#[derive(Debug, Clone)]
pub struct RoutingInfo {
    /// Region hosting this organization.
    pub region: Region,
    /// Member nodes in the region.
    pub member_nodes: Vec<String>,
    /// Hint for current leader (may be stale).
    pub leader_hint: Option<String>,
}

// ============================================================================
// Cache Entry (Internal)
// ============================================================================

/// Cached routing information for an organization (internal).
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Region hosting this organization.
    region: Region,
    /// Member nodes in the region.
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
    /// Checks if this cache entry is stale.
    fn is_stale(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }

    /// Converts to public RoutingInfo.
    fn to_routing_info(&self) -> RoutingInfo {
        RoutingInfo {
            region: self.region,
            member_nodes: self.member_nodes.clone(),
            leader_hint: self.leader_hint.clone(),
        }
    }
}

// ============================================================================
// Region Connection
// ============================================================================

/// Connection to a region for request forwarding.
#[derive(Debug, Clone)]
pub struct RegionConnection {
    /// Region identifier.
    pub region: Region,
    /// gRPC channel to the region leader.
    pub channel: Channel,
    /// Address of the connected node.
    pub address: SocketAddr,
    /// Whether this is the confirmed leader.
    pub is_leader: bool,
}

// ============================================================================
// Region Router
// ============================================================================

/// Configuration for the region router.
#[derive(Debug, Clone, bon::Builder)]
pub struct RouterConfig {
    /// Time-to-live for cache entries.
    #[builder(default = Duration::from_secs(60))]
    pub cache_ttl: Duration,
    /// Connection timeout for region connections.
    #[builder(default = Duration::from_secs(5))]
    pub connect_timeout: Duration,
    /// Base port for region gRPC services.
    #[builder(default = 50051)]
    pub grpc_port: u16,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Region router for routing requests to the correct region.
///
/// Maintains a cache of organization → region mappings and manages
/// connections to region leaders. Uses `local_region` to determine
/// whether protected-region requests can be served locally or must
/// be forwarded to an in-region node.
pub struct RegionRouter<B: StorageBackend + 'static = FileBackend> {
    /// System organization service for organization lookups.
    system: Arc<SystemOrganizationService<B>>,
    /// Cached organization → region mappings.
    cache: RwLock<HashMap<OrganizationId, CacheEntry>>,
    /// Active region connections.
    connections: RwLock<HashMap<Region, RegionConnection>>,
    /// Router configuration.
    config: RouterConfig,
    /// This node's region tag (set at startup from node config).
    local_region: Region,
}

impl<B: StorageBackend + 'static> RegionRouter<B> {
    /// Creates a new region router.
    pub fn new(system: Arc<SystemOrganizationService<B>>, local_region: Region) -> Self {
        Self::with_config(system, RouterConfig::default(), local_region)
    }

    /// Creates a new region router with custom configuration.
    pub fn with_config(
        system: Arc<SystemOrganizationService<B>>,
        config: RouterConfig,
        local_region: Region,
    ) -> Self {
        Self {
            system,
            cache: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
            config,
            local_region,
        }
    }

    /// Routes a request to the appropriate region.
    ///
    /// Returns the region connection for the organization. The connection
    /// may be cached or freshly established.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns a routing error if the organization is not found in the routing
    /// table or the gRPC connection to the target region cannot be established.
    pub async fn route(&self, organization: OrganizationId) -> Result<RegionConnection> {
        // 1. Check cache
        let routing = self.get_routing_internal(organization)?;

        // 2. Get or create connection
        self.get_connection(routing.region, &routing.member_nodes, routing.leader_hint.as_deref())
            .await
    }

    /// Returns this node's region.
    pub fn local_region(&self) -> Region {
        self.local_region
    }

    /// Whether the given region can be served locally by this node.
    ///
    /// Non-protected regions are replicated to all nodes, so they are
    /// always local. Protected regions are local only when this node's
    /// region matches.
    pub fn is_region_local(&self, region: Region) -> bool {
        if !region.requires_residency() {
            // Non-protected: every node holds a replica
            return true;
        }
        // Protected: only in-region nodes hold replicas
        self.local_region == region
    }

    /// Returns routing information for an organization (public API).
    ///
    /// Returns the region assignment and member nodes for the organization.
    /// Uses cached data if fresh, otherwise queries `_system`.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns a routing error if the organization is not found in the `_system`
    /// organization store or is not assigned to any region.
    pub fn get_routing(&self, organization: OrganizationId) -> Result<RoutingInfo> {
        self.get_routing_internal(organization).map(|entry| entry.to_routing_info())
    }

    /// Returns routing information for an organization (internal).
    ///
    /// Uses cached data if fresh, otherwise queries `_system`.
    fn get_routing_internal(&self, organization: OrganizationId) -> Result<CacheEntry> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&organization)
                && !entry.is_stale(self.config.cache_ttl)
            {
                debug!(
                    organization = organization.value(),
                    region = entry.region.as_str(),
                    "Cache hit"
                );
                return Ok(entry.clone());
            }
        }

        // Cache miss or stale - query _system
        self.refresh_routing(organization)
    }

    /// Refreshes routing information from `_system`.
    fn refresh_routing(&self, organization: OrganizationId) -> Result<CacheEntry> {
        let registry = self
            .system
            .get_organization(organization)
            .context(SystemLookupSnafu)?
            .ok_or(RoutingError::OrganizationNotFound { organization })?;

        // Check organization status
        if registry.status != OrganizationStatus::Active {
            return Err(RoutingError::OrganizationUnavailable {
                organization,
                status: registry.status,
            });
        }

        let entry = CacheEntry {
            region: registry.region,
            member_nodes: registry.member_nodes.clone(),
            leader_hint: registry.member_nodes.first().cloned(), // First node as hint
            config_version: registry.config_version,
            cached_at: Instant::now(),
        };

        // Update cache
        {
            let mut cache = self.cache.write();
            cache.insert(organization, entry.clone());
        }

        info!(
            organization = organization.value(),
            region = %registry.region,
            config_version = registry.config_version,
            "Refreshed routing cache"
        );

        Ok(entry)
    }

    /// Returns or create a connection to a region.
    ///
    /// Returns an existing cached connection if available, otherwise creates a new one.
    /// The connection will be cached for future use.
    pub async fn get_connection(
        &self,
        region: Region,
        member_nodes: &[String],
        leader_hint: Option<&str>,
    ) -> Result<RegionConnection> {
        // Check for existing connection
        {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(&region) {
                // Verify connection is still valid
                // In a real implementation, we'd ping the connection
                return Ok(conn.clone());
            }
        }

        // No existing connection - create new one
        self.create_connection(region, member_nodes, leader_hint).await
    }

    /// Creates a new connection to a region.
    async fn create_connection(
        &self,
        region: Region,
        member_nodes: &[String],
        leader_hint: Option<&str>,
    ) -> Result<RegionConnection> {
        if member_nodes.is_empty() {
            return Err(RoutingError::NoAvailableNodes { region });
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
            match self.connect_to_node(region, node_id).await {
                Ok(conn) => {
                    // Cache the connection
                    {
                        let mut connections = self.connections.write();
                        connections.insert(region, conn.clone());
                    }
                    return Ok(conn);
                },
                Err(e) => {
                    warn!(region = region.as_str(), node_id, error = %e, "Failed to connect to node");
                    last_error = Some(e);
                },
            }
        }

        Err(last_error.unwrap_or(RoutingError::NoAvailableNodes { region }))
    }

    /// Connects to a specific node.
    async fn connect_to_node(&self, region: Region, node_id: &str) -> Result<RegionConnection> {
        // Parse node address (format: "host:port" or just "host")
        let address = self.resolve_node_address(node_id)?;

        let endpoint = format!("http://{}", address);

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| RoutingError::ConnectionFailed {
                region,
                message: format!("Invalid endpoint {}: {}", endpoint, e),
            })?
            .connect_timeout(self.config.connect_timeout)
            .connect()
            .await
            .map_err(|e| RoutingError::ConnectionFailed {
                region,
                message: format!("Connection to {} failed: {}", endpoint, e),
            })?;

        debug!(region = region.as_str(), node_id, address = %address, "Connected to region node");

        Ok(RegionConnection {
            region,
            channel,
            address,
            is_leader: false, // Will be confirmed on first request
        })
    }

    /// Resolves a node ID to a socket address.
    ///
    /// Accepts either `host:port` or just `host` (default gRPC port appended).
    fn resolve_node_address(&self, node_id: &str) -> Result<SocketAddr> {
        // Try parsing as-is first (covers "host:port" and "ip:port")
        if let Ok(addr) = node_id.parse::<SocketAddr>() {
            return Ok(addr);
        }

        // Not a valid socket address — assume bare hostname, append default port
        let addr_str = format!("{}:{}", node_id, self.config.grpc_port);
        addr_str.parse().map_err(|_| RoutingError::ConnectionFailed {
            region: Region::GLOBAL,
            message: format!("Invalid node address: {}", node_id),
        })
    }

    /// Invalidate cached routing for an organization.
    ///
    /// Invalidates cached routing when a redirect indicates stale data.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn invalidate(&self, organization: OrganizationId) {
        let mut cache = self.cache.write();
        if cache.remove(&organization).is_some() {
            debug!(organization = organization.value(), "Invalidated routing cache");
        }
    }

    /// Invalidate all cached connections to a region.
    ///
    /// Invalidates all cached connections when a region leader changes or becomes unavailable.
    pub fn invalidate_region(&self, region: Region) {
        let mut connections = self.connections.write();
        if connections.remove(&region).is_some() {
            debug!(region = region.as_str(), "Invalidated region connection");
        }
    }

    /// Updates leader hint after successful request.
    ///
    /// Updates the leader hint when a response indicates the actual leader.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn update_leader_hint(&self, organization: OrganizationId, leader_node: &str) {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&organization)
            && entry.leader_hint.as_deref() != Some(leader_node)
        {
            entry.leader_hint = Some(leader_node.to_string());
            debug!(organization = organization.value(), leader_node, "Updated leader hint");
        }
    }

    /// Returns routing statistics.
    pub fn stats(&self) -> RouterStats {
        let cache = self.cache.read();
        let connections = self.connections.read();

        let stale_entries = cache.values().filter(|e| e.is_stale(self.config.cache_ttl)).count();

        RouterStats {
            cached_organizations: cache.len(),
            stale_entries,
            active_connections: connections.len(),
        }
    }

    /// Clears all caches.
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
    /// Number of cached organization routings.
    pub cached_organizations: usize,
    /// Number of stale cache entries.
    pub stale_entries: usize,
    /// Number of active region connections.
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

    fn create_test_router_with_region(
        local_region: Region,
    ) -> (RegionRouter<FileBackend>, Arc<StateLayer<FileBackend>>, TestDir) {
        let temp_dir = TestDir::new();
        let db = Arc::new(
            Database::<FileBackend>::create(temp_dir.join("test.db")).expect("create database"),
        );
        let state = Arc::new(StateLayer::new(db));
        let system = Arc::new(SystemOrganizationService::new(Arc::clone(&state)));

        let router = RegionRouter::new(system, local_region);
        (router, state, temp_dir)
    }

    fn create_test_router() -> (RegionRouter<FileBackend>, Arc<StateLayer<FileBackend>>, TestDir) {
        create_test_router_with_region(Region::GLOBAL)
    }

    #[test]
    fn test_router_config_default() {
        let config = RouterConfig::default();
        assert_eq!(config.cache_ttl, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_router_config_builder_with_defaults() {
        let config = RouterConfig::builder().build();
        assert_eq!(config.cache_ttl, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.grpc_port, 50051);
    }

    #[test]
    fn test_router_config_builder_with_custom_values() {
        let config = RouterConfig::builder()
            .cache_ttl(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10))
            .grpc_port(9090)
            .build();
        assert_eq!(config.cache_ttl, Duration::from_secs(120));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.grpc_port, 9090);
    }

    #[test]
    fn test_cache_entry_staleness() {
        let entry = CacheEntry {
            region: Region::GLOBAL,
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
    fn test_organization_not_found() {
        let (router, _, _temp) = create_test_router();

        let result = router.get_routing(OrganizationId::new(999));
        assert!(
            matches!(result, Err(RoutingError::OrganizationNotFound { organization }) if organization == OrganizationId::new(999))
        );
    }

    #[test]
    fn test_invalidate_cache() {
        let (router, _, _temp) = create_test_router();

        // Manually insert a cache entry
        {
            let mut cache = router.cache.write();
            cache.insert(
                OrganizationId::new(1),
                CacheEntry {
                    region: Region::GLOBAL,
                    member_nodes: vec!["node-1".to_string()],
                    leader_hint: None,
                    config_version: 1,
                    cached_at: Instant::now(),
                },
            );
        }

        assert_eq!(router.stats().cached_organizations, 1);

        router.invalidate(OrganizationId::new(1));

        assert_eq!(router.stats().cached_organizations, 0);
    }

    #[test]
    fn test_router_stats() {
        let (router, _, _temp) = create_test_router();

        let stats = router.stats();
        assert_eq!(stats.cached_organizations, 0);
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
                OrganizationId::new(1),
                CacheEntry {
                    region: Region::GLOBAL,
                    member_nodes: vec![],
                    leader_hint: None,
                    config_version: 1,
                    cached_at: Instant::now(),
                },
            );
        }

        router.clear();

        assert_eq!(router.stats().cached_organizations, 0);
    }

    #[test]
    fn test_routing_after_organization_registration() {
        let (router, state, _temp) = create_test_router();

        // Register an organization in _system
        let registry = inferadb_ledger_state::system::OrganizationRegistry {
            organization_id: OrganizationId::new(42),
            region: inferadb_ledger_types::Region::GLOBAL,
            member_nodes: vec!["node-a:50051".to_string(), "node-b:50051".to_string()],
            status: inferadb_ledger_state::system::OrganizationStatus::Active,
            config_version: 1,
            created_at: chrono::Utc::now(),
            deleted_at: None,
        };
        let system = SystemOrganizationService::new(Arc::clone(&state));
        system
            .register_organization(
                &registry,
                inferadb_ledger_types::OrganizationSlug::new(
                    registry.organization_id.value() as u64
                ),
            )
            .expect("register organization");

        // Router should resolve the organization's routing
        let info = router.get_routing(OrganizationId::new(42)).expect("get routing");
        assert_eq!(info.region, Region::GLOBAL);
        assert_eq!(info.member_nodes.len(), 2);
        assert!(info.leader_hint.is_some(), "should have a leader hint");

        // Should be cached now
        assert_eq!(router.stats().cached_organizations, 1);
    }

    #[test]
    fn test_routing_cache_populated_on_lookup() {
        let (router, state, _temp) = create_test_router();

        // Register two organizations on different regions
        let system = SystemOrganizationService::new(Arc::clone(&state));

        for (ns_id, region) in [(10, 1), (20, 2)] {
            let registry = inferadb_ledger_state::system::OrganizationRegistry {
                organization_id: OrganizationId::new(ns_id),
                region: inferadb_ledger_types::Region::GLOBAL,
                member_nodes: vec![format!("node-{}:50051", region)],
                status: inferadb_ledger_state::system::OrganizationStatus::Active,
                config_version: 1,
                created_at: chrono::Utc::now(),
                deleted_at: None,
            };
            system
                .register_organization(
                    &registry,
                    inferadb_ledger_types::OrganizationSlug::new(
                        registry.organization_id.value() as u64
                    ),
                )
                .expect("register");
        }

        // Look up both — should independently resolve
        let info1 = router.get_routing(OrganizationId::new(10)).expect("ns 10");
        let info2 = router.get_routing(OrganizationId::new(20)).expect("ns 20");

        assert_eq!(info1.region, Region::GLOBAL);
        assert_eq!(info2.region, Region::GLOBAL);

        // Both should be cached
        assert_eq!(router.stats().cached_organizations, 2);
    }

    #[test]
    fn test_update_leader_hint_changes_cached_hint() {
        let (router, state, _temp) = create_test_router();

        // Register organization
        let system = SystemOrganizationService::new(Arc::clone(&state));
        let registry = inferadb_ledger_state::system::OrganizationRegistry {
            organization_id: OrganizationId::new(5),
            region: inferadb_ledger_types::Region::GLOBAL,
            member_nodes: vec!["node-a:50051".to_string(), "node-b:50051".to_string()],
            status: inferadb_ledger_state::system::OrganizationStatus::Active,
            config_version: 1,
            created_at: chrono::Utc::now(),
            deleted_at: None,
        };
        system
            .register_organization(
                &registry,
                inferadb_ledger_types::OrganizationSlug::new(
                    registry.organization_id.value() as u64
                ),
            )
            .expect("register");

        // Populate cache by doing a lookup
        let info = router.get_routing(OrganizationId::new(5)).expect("initial routing");
        assert_eq!(info.leader_hint.as_deref(), Some("node-a:50051"));

        // Update leader hint
        router.update_leader_hint(OrganizationId::new(5), "node-b:50051");

        // Re-read from cache (should use updated hint)
        let info2 = router.get_routing(OrganizationId::new(5)).expect("updated routing");
        assert_eq!(info2.leader_hint.as_deref(), Some("node-b:50051"));
    }

    #[test]
    fn test_routing_unavailable_organization_status() {
        let (router, state, _temp) = create_test_router();

        // Register an organization with Suspended status
        let system = SystemOrganizationService::new(Arc::clone(&state));
        let registry = inferadb_ledger_state::system::OrganizationRegistry {
            organization_id: OrganizationId::new(77),
            region: inferadb_ledger_types::Region::GLOBAL,
            member_nodes: vec!["node-1:50051".to_string()],
            status: inferadb_ledger_state::system::OrganizationStatus::Suspended,
            config_version: 1,
            created_at: chrono::Utc::now(),
            deleted_at: None,
        };
        system
            .register_organization(
                &registry,
                inferadb_ledger_types::OrganizationSlug::new(
                    registry.organization_id.value() as u64
                ),
            )
            .expect("register");

        // Routing should fail with OrganizationUnavailable
        let result = router.get_routing(OrganizationId::new(77));
        assert!(
            matches!(
                result,
                Err(RoutingError::OrganizationUnavailable {
                    organization,
                    status: inferadb_ledger_state::system::OrganizationStatus::Suspended
                }) if organization == OrganizationId::new(77)
            ),
            "expected OrganizationUnavailable, got: {:?}",
            result
        );
    }

    #[test]
    fn test_local_region_accessor() {
        let (router, _, _temp) = create_test_router();
        assert_eq!(router.local_region(), Region::GLOBAL);

        let (router, _, _temp) = create_test_router_with_region(Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(router.local_region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn test_is_region_local_non_protected_always_true() {
        // Non-protected regions (GLOBAL, US_EAST_VA, US_WEST_OR) should always be local
        // regardless of node's own region.
        let (router, _, _temp) = create_test_router_with_region(Region::DE_CENTRAL_FRANKFURT);

        assert!(router.is_region_local(Region::GLOBAL));
        assert!(router.is_region_local(Region::US_EAST_VA));
        assert!(router.is_region_local(Region::US_WEST_OR));
    }

    #[test]
    fn test_is_region_local_protected_in_region() {
        // Protected region matches node region → local
        let (router, _, _temp) = create_test_router_with_region(Region::DE_CENTRAL_FRANKFURT);

        assert!(router.is_region_local(Region::DE_CENTRAL_FRANKFURT));
    }

    #[test]
    fn test_is_region_local_protected_out_of_region() {
        // Protected region does not match node region → not local
        let (router, _, _temp) = create_test_router_with_region(Region::DE_CENTRAL_FRANKFURT);

        assert!(!router.is_region_local(Region::FR_NORTH_PARIS));
        assert!(!router.is_region_local(Region::JP_EAST_TOKYO));
        assert!(!router.is_region_local(Region::SA_CENTRAL_RIYADH));
        assert!(!router.is_region_local(Region::CN_NORTH_BEIJING));
    }

    #[test]
    fn test_is_region_local_global_node_sees_non_protected_only() {
        // A GLOBAL-region node can serve non-protected regions locally,
        // but not protected regions (GLOBAL itself is non-protected).
        let (router, _, _temp) = create_test_router_with_region(Region::GLOBAL);

        // Non-protected: local
        assert!(router.is_region_local(Region::GLOBAL));
        assert!(router.is_region_local(Region::US_EAST_VA));
        assert!(router.is_region_local(Region::US_WEST_OR));

        // Protected: not local (GLOBAL != DE_CENTRAL_FRANKFURT)
        assert!(!router.is_region_local(Region::DE_CENTRAL_FRANKFURT));
        assert!(!router.is_region_local(Region::CA_CENTRAL_QC));
    }

    #[test]
    fn test_refresh_routing_uses_actual_region() {
        // Verifies the refresh_routing bug fix: CacheEntry.region should
        // use the organization's assigned region, not hardcoded GLOBAL.
        let (router, state, _temp) = create_test_router_with_region(Region::DE_CENTRAL_FRANKFURT);

        let system = SystemOrganizationService::new(Arc::clone(&state));
        let registry = inferadb_ledger_state::system::OrganizationRegistry {
            organization_id: OrganizationId::new(100),
            region: Region::DE_CENTRAL_FRANKFURT,
            member_nodes: vec!["node-de:50051".to_string()],
            status: inferadb_ledger_state::system::OrganizationStatus::Active,
            config_version: 1,
            created_at: chrono::Utc::now(),
            deleted_at: None,
        };
        system
            .register_organization(&registry, inferadb_ledger_types::OrganizationSlug::new(100))
            .expect("register");

        let info = router.get_routing(OrganizationId::new(100)).expect("get routing");
        assert_eq!(
            info.region,
            Region::DE_CENTRAL_FRANKFURT,
            "routing should reflect org's actual region, not hardcoded GLOBAL"
        );
    }

    #[test]
    fn test_routing_preserves_region_from_registry() {
        // Register orgs on different regions and verify the cache preserves them.
        let (router, state, _temp) = create_test_router_with_region(Region::GLOBAL);
        let system = SystemOrganizationService::new(Arc::clone(&state));

        let regions = [
            (1, Region::US_EAST_VA),
            (2, Region::DE_CENTRAL_FRANKFURT),
            (3, Region::JP_EAST_TOKYO),
        ];

        for (id, region) in &regions {
            let registry = inferadb_ledger_state::system::OrganizationRegistry {
                organization_id: OrganizationId::new(*id),
                region: *region,
                member_nodes: vec![format!("node-{id}:50051")],
                status: inferadb_ledger_state::system::OrganizationStatus::Active,
                config_version: 1,
                created_at: chrono::Utc::now(),
                deleted_at: None,
            };
            system
                .register_organization(
                    &registry,
                    inferadb_ledger_types::OrganizationSlug::new(*id as u64),
                )
                .expect("register");
        }

        for (id, expected_region) in &regions {
            let info = router.get_routing(OrganizationId::new(*id)).expect("get routing");
            assert_eq!(info.region, *expected_region);
        }
    }
}
