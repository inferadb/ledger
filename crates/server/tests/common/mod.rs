//! Test harness for cluster integration tests.
//!
//! Provides utilities for spawning and managing multi-node test clusters.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use parking_lot::RwLock;
use tempfile::TempDir;
use tokio::time::timeout;

use ledger_raft::LedgerTypeConfig;
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
    /// The first node bootstraps the cluster, and other nodes join.
    /// All nodes use ephemeral ports on localhost.
    pub async fn new(size: usize) -> Self {
        assert!(size >= 1, "cluster must have at least 1 node");

        let base_port = 50100 + (rand::random::<u16>() % 1000);
        let mut nodes = Vec::with_capacity(size);

        // Create all node configs first so we know peer addresses
        let configs: Vec<_> = (0..size)
            .map(|i| {
                let node_id = (i + 1) as u64;
                let port = base_port + i as u16;
                let temp_dir = tempfile::tempdir().expect("create temp dir");
                let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
                (node_id, port, addr, temp_dir)
            })
            .collect();

        // Build peer list for each node
        let all_peers: Vec<_> = configs
            .iter()
            .map(|(id, _, addr, _)| ledger_server::config::PeerConfig {
                node_id: *id,
                addr: addr.to_string(),
            })
            .collect();

        // Start each node
        for (i, (node_id, _port, addr, temp_dir)) in configs.into_iter().enumerate() {
            // Build config with peers (excluding self)
            let peers: Vec<_> = all_peers
                .iter()
                .filter(|p| p.node_id != node_id)
                .cloned()
                .collect();

            let config = ledger_server::config::Config {
                node_id,
                listen_addr: addr,
                data_dir: temp_dir.path().to_path_buf(),
                peers,
                batching: ledger_server::config::BatchConfig::default(),
                bootstrap: i == 0, // Only first node bootstraps
            };

            // Bootstrap the node
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

            nodes.push(TestNode {
                id: node_id,
                addr,
                raft: bootstrapped.raft,
                state: bootstrapped.state,
                _temp_dir: temp_dir,
                _server_handle: server_handle,
            });
        }

        // Give servers time to start accepting connections
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self { nodes }
    }

    /// Wait for a leader to be elected.
    ///
    /// Returns the leader's node ID.
    pub async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_timeout(Duration::from_secs(10))
            .await
            .expect("leader election timed out")
    }

    /// Wait for a leader with timeout.
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

    /// Get the current leader node.
    pub fn leader(&self) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.is_leader())
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
