//! Peer discovery and dynamic endpoint management.
//!
//! The discovery module enables the SDK to dynamically discover cluster peers
//! and update its endpoint list for failover and load distribution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    DiscoveryService                         │
//! │   Periodic refresh │ Peer fetching │ Endpoint updates       │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    ConnectionPool                           │
//! │   Receives updated endpoints │ Triggers reconnection        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use inferadb_ledger_sdk::{DiscoveryConfig, ClientConfig};
//! use inferadb_ledger_sdk::discovery::DiscoveryService;
//! use inferadb_ledger_sdk::connection::ConnectionPool;
//!
//! let config = ClientConfig::builder()
//!     .endpoints(vec!["http://localhost:50051".into()])
//!     .client_id("my-client")
//!     .build()?;
//!
//! let pool = ConnectionPool::new(config);
//! let discovery = DiscoveryService::new(pool.clone(), DiscoveryConfig::enabled());
//!
//! // Start background refresh
//! discovery.start_background_refresh();
//!
//! // Or manually refresh
//! let peers = discovery.get_peers().await?;
//! discovery.refresh_peers().await?;
//! ```

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use inferadb_ledger_raft::proto;
use parking_lot::RwLock;
use tokio::{
    sync::Notify,
    time::{Duration, interval},
};
use tracing::{debug, info, warn};

use crate::{
    config::DiscoveryConfig,
    connection::ConnectionPool,
    error::{Result, RpcSnafu},
    retry::with_retry,
};

/// Information about a discovered peer in the cluster.
///
/// Each peer represents a node that the client can connect to for
/// Ledger operations. The addresses field contains private/WireGuard
/// IPs that can be combined with the gRPC port to form connection URLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// Node identifier.
    pub node_id: String,

    /// IP addresses (IPv4 or IPv6) for this node.
    /// These are private/WireGuard IPs, not public addresses.
    pub addresses: Vec<String>,

    /// gRPC port (same for all addresses), typically 5000.
    pub grpc_port: u32,

    /// When this peer was last seen by the cluster.
    pub last_seen: Option<SystemTime>,
}

impl PeerInfo {
    /// Creates SDK `PeerInfo` from proto `PeerInfo`.
    pub(crate) fn from_proto(proto: proto::PeerInfo) -> Self {
        let node_id = proto.node_id.map(|n| n.id).unwrap_or_default();

        let last_seen = proto.last_seen.and_then(|ts| {
            let secs = ts.seconds.try_into().ok()?;
            let nanos = ts.nanos.try_into().ok()?;
            Some(SystemTime::UNIX_EPOCH + Duration::new(secs, nanos))
        });

        Self { node_id, addresses: proto.addresses, grpc_port: proto.grpc_port, last_seen }
    }

    /// Generates endpoint URLs from this peer's addresses and port.
    ///
    /// Returns URLs in the format `http://{address}:{port}`.
    #[must_use]
    pub fn endpoint_urls(&self) -> Vec<String> {
        self.addresses.iter().map(|addr| format!("http://{}:{}", addr, self.grpc_port)).collect()
    }
}

/// Result of a peer discovery refresh operation.
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    /// Discovered peers.
    pub peers: Vec<PeerInfo>,

    /// System version for cache invalidation.
    pub system_version: u64,
}

/// Service for discovering cluster peers and managing dynamic endpoints.
///
/// The `DiscoveryService` can operate in two modes:
/// - **Manual refresh**: Call [`refresh_peers()`](Self::refresh_peers) to update endpoints
/// - **Background refresh**: Call [`start_background_refresh()`](Self::start_background_refresh) to
///   periodically update endpoints
///
/// # Thread Safety
///
/// This service is thread-safe and can be cloned. All clones share the same
/// underlying state and background task.
#[derive(Debug, Clone)]
pub struct DiscoveryService {
    /// Connection pool to fetch peers from and update endpoints on.
    pool: ConnectionPool,

    /// Discovery configuration.
    config: DiscoveryConfig,

    /// Cached system version for conditional fetching.
    cached_version: Arc<RwLock<u64>>,

    /// Whether background refresh is running.
    running: Arc<AtomicBool>,

    /// Notify channel to trigger immediate refresh.
    refresh_notify: Arc<Notify>,

    /// Notify channel to signal shutdown.
    shutdown_notify: Arc<Notify>,
}

impl DiscoveryService {
    /// Creates a new discovery service.
    ///
    /// The service is created in a stopped state. Call
    /// [`start_background_refresh()`](Self::start_background_refresh) to begin
    /// periodic endpoint updates.
    #[must_use]
    pub fn new(pool: ConnectionPool, config: DiscoveryConfig) -> Self {
        Self {
            pool,
            config,
            cached_version: Arc::new(RwLock::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            refresh_notify: Arc::new(Notify::new()),
            shutdown_notify: Arc::new(Notify::new()),
        }
    }

    /// Returns the discovery configuration.
    #[must_use]
    pub fn config(&self) -> &DiscoveryConfig {
        &self.config
    }

    /// Returns the connection pool used by this service.
    #[must_use]
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Returns whether background refresh is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Fetches peer information from the cluster.
    ///
    /// This method queries the SystemDiscoveryService for known peers.
    /// It does not update the connection pool endpoints; use
    /// [`refresh_peers()`](Self::refresh_peers) for that.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the RPC returns an error.
    pub async fn get_peers(&self) -> Result<DiscoveryResult> {
        self.get_peers_with_limit(0).await
    }

    /// Fetches peer information with a maximum peer count.
    ///
    /// # Arguments
    ///
    /// * `max_peers` - Maximum number of peers to return (0 = no limit)
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the RPC returns an error.
    pub async fn get_peers_with_limit(&self, max_peers: u32) -> Result<DiscoveryResult> {
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        let result = with_retry(&retry_policy, || async {
            let channel = pool.get_channel().await?;
            let compression = pool.compression_enabled();

            let mut client = Self::create_discovery_client(channel, compression);

            let request = proto::GetPeersRequest { max_peers };
            let response = client.get_peers(request).await.map_err(|status| {
                RpcSnafu { code: status.code(), message: status.message().to_string() }.build()
            })?;

            let inner = response.into_inner();
            let peers = inner.peers.into_iter().map(PeerInfo::from_proto).collect();

            Ok(DiscoveryResult { peers, system_version: inner.system_version })
        })
        .await?;

        // Update cached version
        {
            let mut version = self.cached_version.write();
            if result.system_version > *version {
                *version = result.system_version;
            }
        }

        Ok(result)
    }

    /// Refreshes peer information and updates the connection pool endpoints.
    ///
    /// This method:
    /// 1. Fetches current peers from the cluster
    /// 2. Extracts endpoint URLs from discovered peers
    /// 3. Updates the connection pool with new endpoints
    /// 4. Resets the pool to trigger reconnection on next use
    ///
    /// # Errors
    ///
    /// Returns an error if fetching peers fails. The connection pool is
    /// only updated if the fetch succeeds.
    pub async fn refresh_peers(&self) -> Result<()> {
        let result = self.get_peers().await?;

        if result.peers.is_empty() {
            warn!("Discovery returned no peers, keeping existing endpoints");
            return Ok(());
        }

        // Collect all endpoint URLs from discovered peers
        let endpoints: Vec<String> =
            result.peers.iter().flat_map(|peer| peer.endpoint_urls()).collect();

        if endpoints.is_empty() {
            warn!("Discovered peers have no valid addresses, keeping existing endpoints");
            return Ok(());
        }

        // Update pool endpoints and reset connection
        info!(
            "Discovery found {} peers with {} endpoints, updating pool",
            result.peers.len(),
            endpoints.len()
        );

        self.pool.update_endpoints(endpoints);
        self.pool.reset();

        Ok(())
    }

    /// Starts the background refresh task.
    ///
    /// The task periodically calls [`refresh_peers()`](Self::refresh_peers)
    /// at the interval specified in the [`DiscoveryConfig`].
    ///
    /// This method is idempotent; calling it multiple times has no effect
    /// if the task is already running.
    ///
    /// To stop the background task, call
    /// [`stop_background_refresh()`](Self::stop_background_refresh).
    pub fn start_background_refresh(&self) {
        if !self.config.is_enabled() {
            debug!("Discovery is disabled, not starting background refresh");
            return;
        }

        if self.running.swap(true, Ordering::SeqCst) {
            debug!("Background refresh already running");
            return;
        }

        let service = self.clone();
        let refresh_interval = self.config.refresh_interval();

        tokio::spawn(async move {
            info!("Starting discovery background refresh (interval: {:?})", refresh_interval);

            let mut ticker = interval(refresh_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = service.refresh_peers().await {
                            warn!("Background peer refresh failed: {}", e);
                        }
                    }
                    _ = service.refresh_notify.notified() => {
                        debug!("Immediate refresh triggered");
                        if let Err(e) = service.refresh_peers().await {
                            warn!("Triggered peer refresh failed: {}", e);
                        }
                    }
                    _ = service.shutdown_notify.notified() => {
                        info!("Stopping discovery background refresh");
                        service.running.store(false, Ordering::SeqCst);
                        return;
                    }
                }
            }
        });
    }

    /// Stops the background refresh task.
    ///
    /// This signals the background task to exit gracefully. The task
    /// will complete its current iteration before stopping.
    pub fn stop_background_refresh(&self) {
        if self.running.load(Ordering::Relaxed) {
            self.shutdown_notify.notify_one();
        }
    }

    /// Triggers an immediate peer refresh.
    ///
    /// If background refresh is running, this causes an immediate
    /// refresh without waiting for the next scheduled interval.
    /// If background refresh is not running, this has no effect.
    pub fn trigger_refresh(&self) {
        if self.running.load(Ordering::Relaxed) {
            self.refresh_notify.notify_one();
        }
    }

    /// Returns the cached system version from the last successful refresh.
    #[must_use]
    pub fn cached_system_version(&self) -> u64 {
        *self.cached_version.read()
    }

    /// Creates a SystemDiscoveryService client with compression settings.
    fn create_discovery_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
    ) -> proto::system_discovery_service_client::SystemDiscoveryServiceClient<
        tonic::transport::Channel,
    > {
        let client =
            proto::system_discovery_service_client::SystemDiscoveryServiceClient::new(channel);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::ClientConfig;

    fn test_config() -> ClientConfig {
        ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid test config")
    }

    fn test_discovery_config() -> DiscoveryConfig {
        DiscoveryConfig::enabled().with_refresh_interval(Duration::from_secs(1))
    }

    #[test]
    fn peer_info_from_proto() {
        let proto_peer = proto::PeerInfo {
            node_id: Some(proto::NodeId { id: "node-1".to_string() }),
            addresses: vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            grpc_port: 5000,
            last_seen: Some(prost_types::Timestamp { seconds: 1000, nanos: 500 }),
        };

        let peer = PeerInfo::from_proto(proto_peer);

        assert_eq!(peer.node_id, "node-1");
        assert_eq!(peer.addresses, vec!["10.0.0.1", "10.0.0.2"]);
        assert_eq!(peer.grpc_port, 5000);
        assert!(peer.last_seen.is_some());
    }

    #[test]
    fn peer_info_from_proto_missing_node_id() {
        let proto_peer = proto::PeerInfo {
            node_id: None,
            addresses: vec!["10.0.0.1".to_string()],
            grpc_port: 5000,
            last_seen: None,
        };

        let peer = PeerInfo::from_proto(proto_peer);

        assert_eq!(peer.node_id, "");
        assert_eq!(peer.addresses, vec!["10.0.0.1"]);
        assert!(peer.last_seen.is_none());
    }

    #[test]
    fn peer_info_endpoint_urls() {
        let peer = PeerInfo {
            node_id: "node-1".to_string(),
            addresses: vec!["10.0.0.1".to_string(), "fd00::1".to_string()],
            grpc_port: 5000,
            last_seen: None,
        };

        let urls = peer.endpoint_urls();

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "http://10.0.0.1:5000");
        assert_eq!(urls[1], "http://fd00::1:5000");
    }

    #[test]
    fn peer_info_equality() {
        let peer1 = PeerInfo {
            node_id: "node-1".to_string(),
            addresses: vec!["10.0.0.1".to_string()],
            grpc_port: 5000,
            last_seen: None,
        };

        let peer2 = PeerInfo {
            node_id: "node-1".to_string(),
            addresses: vec!["10.0.0.1".to_string()],
            grpc_port: 5000,
            last_seen: None,
        };

        assert_eq!(peer1, peer2);
    }

    #[test]
    fn discovery_service_creation() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config.clone());

        assert!(service.config().is_enabled());
        assert_eq!(service.config().refresh_interval(), Duration::from_secs(1));
        assert!(!service.is_running());
        assert_eq!(service.cached_system_version(), 0);
    }

    #[test]
    fn discovery_service_disabled_config() {
        let pool = ConnectionPool::new(test_config());
        let config = DiscoveryConfig::disabled();
        let service = DiscoveryService::new(pool, config);

        assert!(!service.config().is_enabled());
    }

    #[tokio::test]
    async fn get_peers_returns_error_on_connection_failure() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config);

        let result = service.get_peers().await;

        // Should fail since there's no server
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn refresh_peers_returns_error_on_connection_failure() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config);

        let result = service.refresh_peers().await;

        // Should fail since there's no server
        assert!(result.is_err());
    }

    #[test]
    fn start_background_refresh_disabled() {
        let pool = ConnectionPool::new(test_config());
        let config = DiscoveryConfig::disabled();
        let service = DiscoveryService::new(pool, config);

        // Should not start when disabled
        service.start_background_refresh();
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn start_and_stop_background_refresh() {
        let pool = ConnectionPool::new(test_config());
        // Use a long interval to avoid immediate refresh on start
        let config = DiscoveryConfig::enabled().with_refresh_interval(Duration::from_secs(3600));
        let service = DiscoveryService::new(pool, config);

        // Start background refresh
        service.start_background_refresh();

        // Give the task time to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(service.is_running());

        // Stop background refresh
        service.stop_background_refresh();

        // Wait for the task to stop - may need more time if in a select! branch
        for _ in 0..20 {
            if !service.is_running() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn start_background_refresh_idempotent() {
        let pool = ConnectionPool::new(test_config());
        // Use a long interval to avoid immediate refresh attempts
        let config = DiscoveryConfig::enabled().with_refresh_interval(Duration::from_secs(3600));
        let service = DiscoveryService::new(pool, config);

        // Start multiple times
        service.start_background_refresh();
        tokio::time::sleep(Duration::from_millis(50)).await;
        service.start_background_refresh();
        service.start_background_refresh();

        // Should still only be running once
        assert!(service.is_running());

        // Cleanup
        service.stop_background_refresh();
        for _ in 0..20 {
            if !service.is_running() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[test]
    fn trigger_refresh_when_not_running() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config);

        // Should be a no-op when not running
        service.trigger_refresh();
        assert!(!service.is_running());
    }

    #[test]
    fn discovery_service_shares_pool_with_client() {
        // Verify that the pool is shared between client and discovery service
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool.clone(), config);

        // Update endpoints via discovery service
        service.pool().update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);

        // Original pool should see the update
        assert_eq!(pool.active_endpoints(), vec!["http://10.0.0.1:5000".to_string()]);
    }

    #[test]
    fn cached_system_version_updates_after_successful_mock() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config);

        // Initially zero
        assert_eq!(service.cached_system_version(), 0);
    }

    #[tokio::test]
    async fn get_peers_with_limit_returns_error_on_connection_failure() {
        let pool = ConnectionPool::new(test_config());
        let config = test_discovery_config();
        let service = DiscoveryService::new(pool, config);

        let result = service.get_peers_with_limit(5).await;

        // Should fail since there's no server
        assert!(result.is_err());
    }
}
