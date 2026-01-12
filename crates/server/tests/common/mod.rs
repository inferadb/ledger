//! Test harness for cluster integration tests.
//!
//! Provides utilities for spawning and managing multi-node test clusters.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use parking_lot::RwLock;
use tempfile::TempDir;
use tokio::time::timeout;

use ledger_raft::LedgerTypeConfig;
use ledger_raft::proto::JoinClusterRequest;
use ledger_raft::proto::admin_service_client::AdminServiceClient;
use ledger_storage::StateLayer;

/// A test node in a cluster.
pub struct TestNode {
    /// The node ID.
    pub id: u64,
    /// The gRPC address.
    pub addr: SocketAddr,
    /// The Raft instance.
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    pub state: Arc<RwLock<StateLayer>>,
    /// Temporary directory for node data.
    _temp_dir: TempDir,
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
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0)
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

        let base_port = 50100 + (rand::random::<u16>() % 1000);
        let mut nodes = Vec::with_capacity(size);

        // Step 1: Start the bootstrap node as a SINGLE-NODE cluster (no peers)
        // This allows it to immediately become leader, then we dynamically add nodes
        let node_id = 1u64;
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
        let temp_dir = tempfile::tempdir().expect("create temp dir");

        // Bootstrap node has NO PEERS - starts as single-node cluster
        let config = ledger_server::config::Config {
            node_id,
            listen_addr: addr,
            metrics_addr: None,
            data_dir: temp_dir.path().to_path_buf(),
            peers: vec![], // Empty! Single-node cluster can immediately elect self as leader
            batching: ledger_server::config::BatchConfig::default(),
            rate_limit: ledger_server::config::RateLimitConfig::default(),
            discovery: ledger_server::config::DiscoveryConfig::default(),
            bootstrap: true,
        };

        let bootstrapped = ledger_server::bootstrap::bootstrap_node(&config)
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
            let temp_dir = tempfile::tempdir().expect("create temp dir");
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            // Non-bootstrap nodes: peers list is just for networking, not initial membership
            // The leader peer is all we need to be able to connect for join RPC
            let peers = vec![ledger_server::config::PeerConfig {
                node_id: 1, // leader
                addr: leader_addr.to_string(),
            }];

            let config = ledger_server::config::Config {
                node_id,
                listen_addr: addr,
                metrics_addr: None,
                data_dir: temp_dir.path().to_path_buf(),
                peers,
                batching: ledger_server::config::BatchConfig::default(),
                rate_limit: ledger_server::config::RateLimitConfig::default(),
                discovery: ledger_server::config::DiscoveryConfig::default(),
                bootstrap: false, // Non-bootstrap nodes join dynamically
            };

            // Create the node (doesn't join cluster yet)
            let bootstrapped = ledger_server::bootstrap::bootstrap_node(&config)
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
                    }
                };

                let join_request = JoinClusterRequest {
                    node_id,
                    address: addr.to_string(),
                };

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
                    }
                    Err(e) => {
                        last_error = format!("join RPC failed: {}", e);
                        tokio::time::sleep(Duration::from_millis(200 * (attempt + 1) as u64)).await;
                    }
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
        let leader_id = self
            .nodes
            .iter()
            .filter_map(|n| n.current_leader())
            .next()?;

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
    ledger_raft::proto::write_service_client::WriteServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    ledger_raft::proto::write_service_client::WriteServiceClient::connect(endpoint).await
}

/// Helper to create a read client for a node.
#[allow(dead_code)]
pub async fn create_read_client(
    addr: SocketAddr,
) -> Result<
    ledger_raft::proto::read_service_client::ReadServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    ledger_raft::proto::read_service_client::ReadServiceClient::connect(endpoint).await
}

/// Helper to create a health client for a node.
#[allow(dead_code)]
pub async fn create_health_client(
    addr: SocketAddr,
) -> Result<
    ledger_raft::proto::health_service_client::HealthServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    ledger_raft::proto::health_service_client::HealthServiceClient::connect(endpoint).await
}

/// Helper to create an admin client for a node.
#[allow(dead_code)]
pub async fn create_admin_client(
    addr: SocketAddr,
) -> Result<
    ledger_raft::proto::admin_service_client::AdminServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let endpoint = format!("http://{}", addr);
    ledger_raft::proto::admin_service_client::AdminServiceClient::connect(endpoint).await
}
