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

use inferadb_ledger_raft::{
    LedgerTypeConfig, MultiRaftConfig, MultiRaftManager, MultiShardLedgerServer, ShardConfig,
    ShardGroup,
    proto::{JoinClusterRequest, admin_service_client::AdminServiceClient},
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
}

impl TestNode {
    /// Check if this node is the current leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
    }

    /// Get the current leader ID if known.
    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// Get the current term.
    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Get the last applied log index.
    pub fn last_applied(&self) -> u64 {
        self.raft.metrics().borrow().last_applied.map(|id| id.index).unwrap_or(0)
    }
}

/// A test cluster of Raft nodes.
pub struct TestCluster {
    /// The nodes in the cluster.
    nodes: Vec<TestNode>,
}

impl TestCluster {
    /// Create a new test cluster with the given number of nodes.
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
        let node_id = 1u64;
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
        let temp_dir = TestDir::new();

        // Bootstrap node has NO PEERS - starts as single-node cluster
        let config = inferadb_ledger_server::config::Config {
            node_id,
            listen_addr: addr,
            metrics_addr: None,
            data_dir: temp_dir.path().to_path_buf(),
            peers: vec![], // Empty! Single-node cluster can immediately elect self as leader
            batching: inferadb_ledger_server::config::BatchConfig::default(),
            rate_limit: inferadb_ledger_server::config::RateLimitConfig::default(),
            discovery: inferadb_ledger_server::config::DiscoveryConfig::default(),
            bootstrap: true,
        };

        let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(&config)
            .await
            .expect("bootstrap node");

        let server = bootstrapped.server;
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::error!("server error: {}", e);
            }
        });

        let leader_raft = bootstrapped.raft.clone();
        let leader_addr = addr;
        nodes.push(TestNode {
            id: node_id,
            addr,
            raft: bootstrapped.raft,
            state: bootstrapped.state,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
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
            let node_id = (i + 1) as u64;
            let port = base_port + i as u16;
            let temp_dir = TestDir::new();
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            // Non-bootstrap nodes: peers list is just for networking, not initial membership
            // The leader peer is all we need to be able to connect for join RPC
            let peers = vec![inferadb_ledger_server::config::PeerConfig {
                node_id: 1, // leader
                addr: leader_addr.to_string(),
            }];

            let config = inferadb_ledger_server::config::Config {
                node_id,
                listen_addr: addr,
                metrics_addr: None,
                data_dir: temp_dir.path().to_path_buf(),
                peers,
                batching: inferadb_ledger_server::config::BatchConfig::default(),
                rate_limit: inferadb_ledger_server::config::RateLimitConfig::default(),
                discovery: inferadb_ledger_server::config::DiscoveryConfig::default(),
                bootstrap: false, // Non-bootstrap nodes join dynamically
            };

            // Create the node (doesn't join cluster yet)
            let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(&config)
                .await
                .expect("bootstrap node");

            // Start server in background
            let server = bootstrapped.server;
            let server_handle = tokio::spawn(async move {
                if let Err(e) = server.serve().await {
                    tracing::error!("server error: {}", e);
                }
            });

            // Give server time to start accepting Raft replication connections
            // The leader's add_learner call will try to replicate to this node
            tokio::time::sleep(Duration::from_millis(200)).await;

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

    /// Wait for a leader to be elected AND all nodes to agree.
    ///
    /// Returns the leader's node ID.
    pub async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_agreement(Duration::from_secs(10))
            .await
            .expect("leader election timed out")
    }

    /// Wait for a leader with timeout (any node reporting a leader).
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

    /// Wait for ALL nodes to agree on the same leader.
    ///
    /// This is more robust than `wait_for_leader_timeout` as it ensures
    /// leader information has propagated to all nodes.
    pub async fn wait_for_leader_agreement(&self, duration: Duration) -> Option<u64> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < duration {
            let leaders: Vec<Option<u64>> = self.nodes.iter().map(|n| n.current_leader()).collect();

            // Check if all nodes report the same leader (and it's not None)
            if let Some(first) = leaders.first().copied().flatten() {
                if leaders.iter().all(|&l| l == Some(first)) {
                    return Some(first);
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        None
    }

    /// Get the current leader node.
    ///
    /// Uses consensus from node metrics rather than relying on a single node.
    pub fn leader(&self) -> Option<&TestNode> {
        // Find consensus leader ID from node metrics
        let leader_id = self.nodes.iter().filter_map(|n| n.current_leader()).next()?;

        // Return the node with that ID
        self.nodes.iter().find(|n| n.id == leader_id)
    }

    /// Get all follower nodes.
    pub fn followers(&self) -> Vec<&TestNode> {
        self.nodes.iter().filter(|n| !n.is_leader()).collect()
    }

    /// Get a node by ID.
    pub fn node(&self, id: u64) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Get all nodes.
    pub fn nodes(&self) -> &[TestNode] {
        &self.nodes
    }

    /// Wait for all nodes to have the same last applied index.
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
    inferadb_ledger_raft::proto::write_service_client::WriteServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(endpoint).await
}

/// Helper to create a read client for a node.
#[allow(dead_code)]
pub async fn create_read_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_raft::proto::read_service_client::ReadServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_raft::proto::read_service_client::ReadServiceClient::connect(endpoint).await
}

/// Helper to create a health client for a node.
#[allow(dead_code)]
pub async fn create_health_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_raft::proto::health_service_client::HealthServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_raft::proto::health_service_client::HealthServiceClient::connect(endpoint).await
}

/// Helper to create an admin client for a node.
#[allow(dead_code)]
pub async fn create_admin_client(
    addr: SocketAddr,
) -> Result<
    inferadb_ledger_raft::proto::admin_service_client::AdminServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    inferadb_ledger_raft::proto::admin_service_client::AdminServiceClient::connect(endpoint).await
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
    /// Get the system shard (shard 0).
    pub fn system_shard(&self) -> Arc<ShardGroup> {
        self.manager.system_shard().expect("system shard exists")
    }

    /// Get a data shard by ID.
    pub fn shard(&self, shard_id: u32) -> Option<Arc<ShardGroup>> {
        self.manager.get_shard(shard_id).ok()
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<u32> {
        self.manager.list_shards()
    }

    /// Check if this node is leader for the system shard.
    pub fn is_system_leader(&self) -> bool {
        let shard = self.system_shard();
        let metrics = shard.raft().metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
    }

    /// Get leader ID for the system shard.
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
///     |       +-- Shard 0 (system): namespace/vault metadata
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
    /// Create a new multi-shard test cluster.
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
                shard_id: 0,
                initial_members: members.clone(),
                bootstrap: i == 0, // Only first node bootstraps
                enable_background_jobs: true,
            };
            manager.start_system_shard(system_config).await.expect("start system shard");

            // Start data shards (shard 1, 2, ...)
            for shard_id in 1..=num_data_shards {
                let shard_config = ShardConfig {
                    shard_id: shard_id as u32,
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
            let server =
                MultiShardLedgerServer::new(manager.clone(), addr).with_rate_limit(1000, 30); // High concurrency for tests

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
                    if let Ok(shard) = node.manager.get_shard(shard_id) {
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

    /// Get all nodes.
    pub fn nodes(&self) -> &[MultiShardTestNode] {
        &self.nodes
    }

    /// Get a node by ID.
    pub fn node(&self, id: u64) -> Option<&MultiShardTestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Get the leader node for the system shard.
    pub fn system_leader(&self) -> Option<&MultiShardTestNode> {
        self.nodes.iter().find(|n| n.is_system_leader())
    }

    /// Get any node (for client connections).
    pub fn any_node(&self) -> &MultiShardTestNode {
        &self.nodes[0]
    }

    /// Get the number of data shards.
    pub fn num_data_shards(&self) -> usize {
        self.num_shards
    }

    /// Get all node addresses.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|n| n.addr).collect()
    }

    /// Wait for a leader to be elected on all shards.
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
