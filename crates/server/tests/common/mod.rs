//! Test harness for cluster integration tests.
//!
//! Provides utilities for spawning and managing multi-node test clusters.
//!
//! ## Cluster Types
//!
//! - `TestCluster`: Single-shard cluster using the standard bootstrap flow. Best for testing Raft
//!   consensus, membership changes, and basic operations.
//!
//! - `MultiShardTestCluster`: Multi-shard cluster using `MultiRaftManager`. Best for testing
//!   horizontal scaling, shard routing, and high-throughput.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{JoinClusterRequest, admin_service_client::AdminServiceClient};
use inferadb_ledger_raft::{
    LedgerTypeConfig, MultiRaftConfig, MultiRaftManager, MultiShardLedgerServer, ShardConfig,
    ShardGroup,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_test_utils::TestDir;
use openraft::Raft;
use tokio::time::timeout;

/// A test node in a cluster.
pub struct TestNode {
    /// The node ID.
    pub id: u64,
    /// The gRPC address.
    pub addr: SocketAddr,
    /// The Raft instance.
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer (internally thread-safe via inferadb-ledger-store MVCC).
    pub state: Arc<StateLayer<FileBackend>>,
    /// Temporary directory for node data.
    _temp_dir: TestDir,
    /// Server task handle for cleanup.
    _server_handle: tokio::task::JoinHandle<()>,
    /// Shutdown sender — kept alive so the server doesn't immediately exit.
    /// When dropped, the watch receiver in `serve_with_shutdown` resolves,
    /// triggering graceful shutdown.
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl TestNode {
    /// Checks if this node is the current leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
    }

    /// Returns the current leader ID if known.
    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// Returns the current term.
    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Returns the last applied log index.
    pub fn last_applied(&self) -> u64 {
        self.raft.metrics().borrow().last_applied.map_or(0, |id| id.index)
    }
}

/// A test cluster of Raft nodes.
pub struct TestCluster {
    /// The nodes in the cluster.
    nodes: Vec<TestNode>,
}

impl TestCluster {
    /// Creates a new test cluster with the given number of nodes.
    ///
    /// The first node bootstraps the cluster, and other nodes join via
    /// the AdminService's join_cluster RPC.
    /// All nodes use ephemeral ports on localhost.
    pub async fn new(size: usize) -> Self {
        assert!(size >= 1, "cluster must have at least 1 node");

        // Use wide random range to minimize port conflicts when tests run in parallel
        let base_port = 40000 + (rand::random::<u16>() % 5000);
        let mut nodes = Vec::with_capacity(size);

        // Step 1: Start the bootstrap node as a SINGLE-NODE cluster (no peers)
        // This allows it to immediately become leader, then we dynamically add nodes
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
        let temp_dir = TestDir::new();
        let data_dir = temp_dir.path().to_path_buf();

        // Bootstrap node: no discovery configured → no peers found → bootstraps.
        // Uses auto-generated Snowflake ID for realistic testing.
        let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
            .destination(data_dir.join("backups").to_string_lossy().to_string())
            .build()
            .expect("valid backup config");

        let config = inferadb_ledger_server::config::Config {
            listen_addr: addr,
            metrics_addr: None,
            data_dir: Some(data_dir.clone()),
            single: true, // Single-node mode for immediate bootstrap
            backup: Some(backup_config),
            ..inferadb_ledger_server::config::Config::default()
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(
            &config,
            &data_dir,
            inferadb_ledger_raft::HealthState::new(),
            shutdown_rx,
        )
        .await
        .expect("bootstrap node");

        // Get the auto-generated Snowflake ID from Raft metrics
        let node_id = bootstrapped.raft.metrics().borrow().id;

        let server = bootstrapped.server;
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::error!("server error: {}", e);
            }
        });

        // Wait for the gRPC server to accept TCP connections before proceeding.
        // Leader election (via in-memory Raft metrics) can complete before the TCP
        // listener is bound, causing ConnectionRefused in subsequent gRPC calls.
        let tcp_start = tokio::time::Instant::now();
        while tcp_start.elapsed() < Duration::from_secs(5) {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let leader_raft = bootstrapped.raft.clone();
        let leader_addr = addr;
        nodes.push(TestNode {
            id: node_id,
            addr,
            raft: bootstrapped.raft,
            state: bootstrapped.state,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            _shutdown_tx: shutdown_tx,
        });

        // Wait for the bootstrap node to become leader
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_secs(5);
        while start.elapsed() < timeout_duration {
            let metrics = leader_raft.metrics().borrow().clone();
            if metrics.current_leader == Some(node_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Verify leader election succeeded
        {
            let metrics = leader_raft.metrics().borrow().clone();
            if metrics.current_leader != Some(node_id) {
                panic!("Bootstrap node failed to become leader within timeout");
            }
        }

        // Step 2: Start remaining nodes and have them join the cluster dynamically
        for i in 1..size {
            let port = base_port + i as u16;
            let temp_dir = TestDir::new();
            let data_dir = temp_dir.path().to_path_buf();
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            // Joining node: use join mode (bootstrap_expect=0) for dynamic node addition.
            // This bypasses bootstrap entirely - no Raft cluster initialized, waiting
            // to be added via AdminService's JoinCluster RPC.
            // Uses auto-generated Snowflake ID for realistic testing.
            let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
                .destination(data_dir.join("backups").to_string_lossy().to_string())
                .build()
                .expect("valid backup config");

            let config = inferadb_ledger_server::config::Config {
                listen_addr: addr,
                metrics_addr: None,
                data_dir: Some(data_dir.clone()),
                join: true, // Join mode: wait to be added to existing cluster
                backup: Some(backup_config),
                ..inferadb_ledger_server::config::Config::default()
            };

            // Create the node - with bootstrap_expect=0, it won't initialize its own
            // Raft cluster. It just starts the gRPC server and waits to be added via
            // AdminService's JoinCluster RPC.
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

            let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(
                &config,
                &data_dir,
                inferadb_ledger_raft::HealthState::new(),
                shutdown_rx,
            )
            .await
            .expect("bootstrap node");

            // Get the auto-generated Snowflake ID from Raft metrics
            let node_id = bootstrapped.raft.metrics().borrow().id;

            // Start server in background
            let server = bootstrapped.server;
            let server_handle = tokio::spawn(async move {
                if let Err(e) = server.serve().await {
                    tracing::error!("server error: {}", e);
                }
            });

            // Wait for the gRPC server to accept TCP connections before joining.
            let tcp_start = tokio::time::Instant::now();
            while tcp_start.elapsed() < Duration::from_secs(5) {
                if tokio::net::TcpStream::connect(addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // Join the cluster via the leader's AdminService with retry.
            // We use more attempts with exponential backoff since membership changes
            // are serialized in OpenRaft and may take time if a previous change is
            // still in progress.
            let endpoint = format!("http://{}", leader_addr);
            let mut join_success = false;
            let mut last_error = String::new();
            let max_attempts = 10;

            for attempt in 0..max_attempts {
                let mut client = match AdminServiceClient::connect(endpoint.clone()).await {
                    Ok(c) => c,
                    Err(e) => {
                        last_error = format!("connect failed: {}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    },
                };

                let join_request = JoinClusterRequest { node_id, address: addr.to_string() };

                match client.join_cluster(join_request).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            join_success = true;
                            break;
                        } else {
                            last_error = resp.message.clone();
                            // If membership change in progress, wait longer
                            let backoff = if resp.message.contains("membership")
                                || resp.message.contains("Timeout")
                            {
                                Duration::from_millis(500 * (attempt + 1) as u64)
                            } else {
                                Duration::from_millis(100 * (attempt + 1) as u64)
                            };
                            tokio::time::sleep(backoff).await;
                        }
                    },
                    Err(e) => {
                        last_error = format!("join RPC failed: {}", e);
                        tokio::time::sleep(Duration::from_millis(200 * (attempt + 1) as u64)).await;
                    },
                }
            }

            if !join_success {
                panic!(
                    "Node {} failed to join cluster after {} attempts: {}",
                    node_id, max_attempts, last_error
                );
            }

            let new_raft = bootstrapped.raft.clone();
            nodes.push(TestNode {
                id: node_id,
                addr,
                raft: bootstrapped.raft,
                state: bootstrapped.state,
                _temp_dir: temp_dir,
                _server_handle: server_handle,
                _shutdown_tx: shutdown_tx,
            });

            // Wait for the new node to see itself as a voter and sync with the cluster
            // This is critical to ensure the membership change is fully committed
            // before we try to add another node
            let sync_start = tokio::time::Instant::now();
            let sync_timeout = Duration::from_secs(30);
            while sync_start.elapsed() < sync_timeout {
                let metrics = new_raft.metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                let is_voter = membership.voter_ids().any(|id| id == node_id);

                if is_voter && metrics.current_leader.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // CRITICAL: Wait for the LEADER's membership to exit joint consensus.
            // OpenRaft serializes membership changes - we cannot add the next node
            // until the current change is fully committed on the leader.
            // A joint config has len() > 1, uniform config has len() == 1.
            let stabilize_start = tokio::time::Instant::now();
            let stabilize_timeout = Duration::from_secs(10);
            while stabilize_start.elapsed() < stabilize_timeout {
                let leader_metrics = leader_raft.metrics().borrow().clone();
                let leader_membership = leader_metrics.membership_config.membership();
                let joint_config = leader_membership.get_joint_config();

                // Exit when leader shows single (uniform) config - change is complete
                if joint_config.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // Wait for cluster to fully stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        Self { nodes }
    }

    /// Waits for a leader to be elected AND all nodes to agree.
    ///
    /// Returns the leader's node ID.
    pub async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_agreement(Duration::from_secs(10))
            .await
            .expect("leader election timed out")
    }

    /// Waits for a leader with timeout (any node reporting a leader).
    pub async fn wait_for_leader_timeout(&self, duration: Duration) -> Option<u64> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < duration {
            for node in &self.nodes {
                if let Some(leader_id) = node.current_leader() {
                    return Some(leader_id);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        None
    }

    /// Waits for ALL nodes to agree on the same leader.
    ///
    /// This is more robust than `wait_for_leader_timeout` as it ensures
    /// leader information has propagated to all nodes.
    pub async fn wait_for_leader_agreement(&self, duration: Duration) -> Option<u64> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < duration {
            let leaders: Vec<Option<u64>> = self.nodes.iter().map(|n| n.current_leader()).collect();

            // Check if all nodes report the same leader (and it's not None)
            if let Some(first) = leaders.first().copied().flatten()
                && leaders.iter().all(|&l| l == Some(first))
            {
                return Some(first);
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        None
    }

    /// Returns the current leader node.
    ///
    /// Uses consensus from node metrics rather than relying on a single node.
    pub fn leader(&self) -> Option<&TestNode> {
        // Find consensus leader ID from node metrics
        let leader_id = self.nodes.iter().filter_map(|n| n.current_leader()).next()?;

        // Return the node with that ID
        self.nodes.iter().find(|n| n.id == leader_id)
    }

    /// Returns all follower nodes.
    pub fn followers(&self) -> Vec<&TestNode> {
        self.nodes.iter().filter(|n| !n.is_leader()).collect()
    }

    /// Returns a node by ID.
    pub fn node(&self, id: u64) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Returns all nodes.
    pub fn nodes(&self) -> &[TestNode] {
        &self.nodes
    }

    /// Waits for all nodes to have the same last applied index.
    #[allow(dead_code)]
    pub async fn wait_for_sync(&self, timeout_duration: Duration) -> bool {
        timeout(timeout_duration, async {
            loop {
                let indices: Vec<u64> = self.nodes.iter().map(|n| n.last_applied()).collect();

                if !indices.is_empty() && indices.iter().all(|&i| i == indices[0] && i > 0) {
                    return true;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or(false)
    }
}

/// Helper to create a gRPC write client for a node.
#[allow(dead_code)]
pub async fn create_write_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_proto::proto::write_service_client::WriteServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_proto::proto::write_service_client::WriteServiceClient::connect(endpoint).await
}

/// Helper to create a read client for a node.
#[allow(dead_code)]
pub async fn create_read_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_proto::proto::read_service_client::ReadServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_proto::proto::read_service_client::ReadServiceClient::connect(endpoint).await
}

/// Helper to create a health client for a node.
#[allow(dead_code)]
pub async fn create_health_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient::connect(endpoint)
        .await
}

/// Helper to create an admin client for a node.
#[allow(dead_code)]
pub async fn create_admin_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::connect(endpoint).await
}

// ============================================================================
// Multi-Shard Test Infrastructure
// ============================================================================

/// A test node with multiple shards.
///
/// Uses `MultiRaftManager` to manage independent Raft groups per shard,
/// enabling horizontal scaling and parallel consensus.
pub struct MultiShardTestNode {
    /// The node ID.
    pub id: u64,
    /// The gRPC address.
    pub addr: SocketAddr,
    /// The multi-raft manager containing all shards.
    pub manager: Arc<MultiRaftManager>,
    /// Temporary directory for node data.
    _temp_dir: TestDir,
    /// Server task handle for cleanup.
    _server_handle: tokio::task::JoinHandle<()>,
}

impl MultiShardTestNode {
    /// Returns the system shard (shard 0).
    pub fn system_shard(&self) -> Arc<ShardGroup> {
        self.manager.system_shard().expect("system shard exists")
    }

    /// Returns a data shard by ID.
    pub fn shard(&self, shard_id: inferadb_ledger_types::ShardId) -> Option<Arc<ShardGroup>> {
        self.manager.get_shard(shard_id).ok()
    }

    /// Returns all shard IDs.
    pub fn shard_ids(&self) -> Vec<inferadb_ledger_types::ShardId> {
        self.manager.list_shards()
    }

    /// Checks if this node is leader for the system shard.
    pub fn is_system_leader(&self) -> bool {
        let shard = self.system_shard();
        let metrics = shard.raft().metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
    }

    /// Returns leader ID for the system shard.
    pub fn system_leader(&self) -> Option<u64> {
        let shard = self.system_shard();
        shard.raft().metrics().borrow().current_leader
    }
}

/// A multi-shard test cluster.
///
/// Each node runs multiple independent Raft groups (shards), allowing
/// horizontal scaling of both reads and writes.
///
/// ## Architecture
///
/// ```text
/// MultiShardTestCluster
///     |
///     +-- Node 1 (MultiRaftManager)
///     |       +-- Shard 0 (system): organization/vault metadata
///     |       +-- Shard 1 (data): entity storage
///     |       +-- Shard 2 (data): entity storage
///     |
///     +-- Node 2 (MultiRaftManager)
///     |       +-- Shard 0, 1, 2 (same structure)
///     |
///     +-- Node 3 ...
/// ```
pub struct MultiShardTestCluster {
    /// The nodes in the cluster.
    nodes: Vec<MultiShardTestNode>,
    /// Number of data shards.
    num_shards: usize,
}

impl MultiShardTestCluster {
    /// Creates a new multi-shard test cluster.
    ///
    /// # Arguments
    ///
    /// * `num_nodes` - Number of nodes in the cluster (typically 1 or 3)
    /// * `num_data_shards` - Number of data shards (in addition to system shard 0)
    ///
    /// Each node will have `num_data_shards + 1` Raft groups running.
    pub async fn new(num_nodes: usize, num_data_shards: usize) -> Self {
        assert!(num_nodes >= 1, "cluster must have at least 1 node");
        assert!(num_data_shards >= 1, "must have at least 1 data shard");

        // Use wide random range (non-overlapping with TestCluster) to minimize port conflicts
        let base_port = 50000 + (rand::random::<u16>() % 5000);
        let mut nodes = Vec::with_capacity(num_nodes);

        // Build the member list for all shards
        let members: Vec<(u64, String)> = (0..num_nodes)
            .map(|i| {
                let node_id = (i + 1) as u64;
                let port = base_port + i as u16;
                let addr = format!("127.0.0.1:{}", port);
                (node_id, addr)
            })
            .collect();

        // Start each node
        for i in 0..num_nodes {
            let node_id = (i + 1) as u64;
            let port = base_port + i as u16;
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            let temp_dir = TestDir::new();

            // Create MultiRaftManager config
            let config = MultiRaftConfig::new(temp_dir.path().to_path_buf(), node_id);
            let manager = Arc::new(MultiRaftManager::new(config));

            // Start system shard (shard 0) - required first
            let system_config = ShardConfig {
                shard_id: inferadb_ledger_types::ShardId::new(0),
                initial_members: members.clone(),
                bootstrap: i == 0, // Only first node bootstraps
                enable_background_jobs: true,
            };
            manager.start_system_shard(system_config).await.expect("start system shard");

            // Start data shards (shard 1, 2, ...)
            for shard_id in 1..=num_data_shards {
                let shard_config = ShardConfig {
                    shard_id: inferadb_ledger_types::ShardId::new(shard_id as u32),
                    initial_members: members.clone(),
                    bootstrap: i == 0,
                    enable_background_jobs: true,
                };
                manager
                    .start_data_shard(shard_config)
                    .await
                    .unwrap_or_else(|_| panic!("start data shard {}", shard_id));
            }

            // Create and start the multi-shard gRPC server
            let server = MultiShardLedgerServer::builder()
                .manager(manager.clone())
                .addr(addr)
                .max_concurrent(1000)
                .timeout_secs(30)
                .build();

            let server_handle = tokio::spawn(async move {
                if let Err(e) = server.serve().await {
                    tracing::error!("multi-shard server error: {}", e);
                }
            });

            // Give server time to bind
            tokio::time::sleep(Duration::from_millis(100)).await;

            nodes.push(MultiShardTestNode {
                id: node_id,
                addr,
                manager,
                _temp_dir: temp_dir,
                _server_handle: server_handle,
            });
        }

        // Wait for all shards to elect leaders
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_secs(10);

        'outer: while start.elapsed() < timeout_duration {
            let mut all_ready = true;

            // Check each shard on the first node
            if let Some(node) = nodes.first() {
                for shard_id in 0..=num_data_shards as u32 {
                    if let Ok(shard) =
                        node.manager.get_shard(inferadb_ledger_types::ShardId::new(shard_id))
                    {
                        let metrics = shard.raft().metrics().borrow().clone();
                        if metrics.current_leader.is_none() {
                            all_ready = false;
                            break;
                        }
                    } else {
                        all_ready = false;
                        break;
                    }
                }
            }

            if all_ready {
                break 'outer;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Self { nodes, num_shards: num_data_shards }
    }

    /// Returns all nodes.
    pub fn nodes(&self) -> &[MultiShardTestNode] {
        &self.nodes
    }

    /// Returns a node by ID.
    pub fn node(&self, id: u64) -> Option<&MultiShardTestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Returns the leader node for the system shard.
    pub fn system_leader(&self) -> Option<&MultiShardTestNode> {
        self.nodes.iter().find(|n| n.is_system_leader())
    }

    /// Returns any node (for client connections).
    pub fn any_node(&self) -> &MultiShardTestNode {
        &self.nodes[0]
    }

    /// Returns the number of data shards.
    pub fn num_data_shards(&self) -> usize {
        self.num_shards
    }

    /// Returns all node addresses.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|n| n.addr).collect()
    }

    /// Waits for a leader to be elected on all shards.
    pub async fn wait_for_leaders(&self, timeout_duration: Duration) -> bool {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout_duration {
            let mut all_have_leaders = true;

            for node in &self.nodes {
                for shard_id in node.shard_ids() {
                    if let Some(shard) = node.shard(shard_id) {
                        let metrics = shard.raft().metrics().borrow().clone();
                        if metrics.current_leader.is_none() {
                            all_have_leaders = false;
                            break;
                        }
                    }
                }
                if !all_have_leaders {
                    break;
                }
            }

            if all_have_leaders {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        false
    }
}

// ============================================================================
// External Cluster Infrastructure (for integration tests against running cluster)
// ============================================================================

/// An external cluster started by a shell script.
///
/// Reads endpoints from the `LEDGER_ENDPOINTS` environment variable.
/// Returns `None` from `from_env()` if the variable is unset, allowing
/// tests to gracefully skip when no cluster is available.
pub struct ExternalCluster {
    endpoints: Vec<String>,
}

impl ExternalCluster {
    /// Reads `LEDGER_ENDPOINTS` env var (comma-separated `http://host:port`).
    ///
    /// Returns `None` if the variable is unset, allowing tests to skip.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("LEDGER_ENDPOINTS").ok()?;
        let endpoints: Vec<String> =
            raw.split(',').map(|e| e.trim().to_string()).filter(|e| !e.is_empty()).collect();

        if endpoints.is_empty() {
            return None;
        }

        Some(Self { endpoints })
    }

    /// Returns all cluster endpoints.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Returns the first endpoint (convenience for single-endpoint operations).
    pub fn any_endpoint(&self) -> &str {
        &self.endpoints[0]
    }

    /// Poll `GetClusterInfo` until a leader exists, with timeout.
    ///
    /// Returns the leader's endpoint URL.
    pub async fn wait_for_leader(&self, timeout_duration: Duration) -> String {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout_duration {
            for endpoint in &self.endpoints {
                if let Ok(info) = Self::get_cluster_info(endpoint).await
                    && info.leader_id > 0
                    && let Some(leader_ep) = Self::leader_endpoint_from_info(&info, &self.endpoints)
                {
                    return leader_ep;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        panic!("No leader elected within {:?}", timeout_duration);
    }

    /// Call `GetClusterInfo` on the given endpoint.
    pub async fn get_cluster_info(
        endpoint: &str,
    ) -> Result<
        inferadb_ledger_proto::proto::GetClusterInfoResponse,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut client =
            inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::connect(
                endpoint.to_string(),
            )
            .await?;

        let response =
            client.get_cluster_info(inferadb_ledger_proto::proto::GetClusterInfoRequest {}).await?;

        Ok(response.into_inner())
    }

    /// Finds the leader's endpoint URL by matching leader address from
    /// `GetClusterInfoResponse` against known endpoints.
    pub fn leader_endpoint_from_info(
        info: &inferadb_ledger_proto::proto::GetClusterInfoResponse,
        endpoints: &[String],
    ) -> Option<String> {
        let leader_member = info.members.iter().find(|m| m.is_leader)?;
        let leader_addr = &leader_member.address;

        // Match against endpoints: endpoint is "http://host:port", address is "host:port"
        endpoints.iter().find(|ep| ep.ends_with(leader_addr) || ep.contains(leader_addr)).cloned()
    }

    /// Finds non-leader endpoint URLs.
    pub fn non_leader_endpoints(
        info: &inferadb_ledger_proto::proto::GetClusterInfoResponse,
        endpoints: &[String],
    ) -> Vec<String> {
        let leader_member = info.members.iter().find(|m| m.is_leader);
        let leader_addr = leader_member.map(|m| m.address.as_str()).unwrap_or("");

        endpoints
            .iter()
            .filter(|ep| !ep.ends_with(leader_addr) && !ep.contains(leader_addr))
            .cloned()
            .collect()
    }
}

/// Generic polling helper for eventually-consistent checks.
///
/// Calls `f` repeatedly until it returns `Some(T)`, with the given interval
/// between attempts. Returns `None` if the timeout expires.
pub async fn poll_until<F, Fut, T>(
    timeout_duration: Duration,
    interval: Duration,
    f: F,
) -> Option<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let start = tokio::time::Instant::now();

    while start.elapsed() < timeout_duration {
        if let Some(result) = f().await {
            return Some(result);
        }
        tokio::time::sleep(interval).await;
    }

    None
}

/// Helper to create an admin client from a URL string.
pub async fn create_admin_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}

/// Helper to create a health client from a URL string.
pub async fn create_health_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}
