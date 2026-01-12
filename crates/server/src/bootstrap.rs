//! Cluster bootstrap and initialization.
//!
//! Provides functions to:
//! - Bootstrap a new cluster with this node as the initial leader
//! - Join an existing cluster by contacting the leader

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use parking_lot::RwLock;
use redb::Database;
use tonic::transport::Channel;

use ledger_raft::proto::JoinClusterRequest;
use ledger_raft::proto::admin_service_client::AdminServiceClient;
use ledger_raft::{
    GrpcRaftNetworkFactory, LedgerNodeId, LedgerServer, LedgerTypeConfig, RaftLogStore,
    TtlGarbageCollector,
};
use ledger_storage::{BlockArchive, StateLayer};

use crate::config::Config;

/// Error type for bootstrap operations.
#[derive(Debug)]
pub enum BootstrapError {
    /// Failed to open database.
    Database(String),
    /// Failed to create Raft storage.
    Storage(String),
    /// Failed to create Raft instance.
    Raft(String),
    /// Failed to initialize cluster.
    Initialize(String),
    /// Failed to join existing cluster.
    #[allow(dead_code)] // Reserved for join-cluster mode
    Join(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Database(msg) => write!(f, "database error: {}", msg),
            BootstrapError::Storage(msg) => write!(f, "storage error: {}", msg),
            BootstrapError::Raft(msg) => write!(f, "raft error: {}", msg),
            BootstrapError::Initialize(msg) => write!(f, "initialization error: {}", msg),
            BootstrapError::Join(msg) => write!(f, "join error: {}", msg),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Bootstrapped node components.
pub struct BootstrappedNode {
    /// The Raft instance.
    #[allow(dead_code)]
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    #[allow(dead_code)]
    pub state: Arc<RwLock<StateLayer>>,
    /// The configured Ledger server.
    pub server: LedgerServer,
    /// TTL garbage collector background task handle.
    #[allow(dead_code)]
    pub gc_handle: tokio::task::JoinHandle<()>,
}

/// Bootstrap a new cluster or join an existing one based on configuration.
///
/// If `config.bootstrap` is true, initializes a new single-node cluster.
/// Otherwise, the node starts without initialization (ready to join).
pub async fn bootstrap_node(config: &Config) -> Result<BootstrappedNode, BootstrapError> {
    // Create data directories
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|e| BootstrapError::Database(format!("failed to create data dir: {}", e)))?;

    // Open state database
    let state_db_path = config.data_dir.join("state.redb");
    let state_db = Database::create(&state_db_path)
        .map_err(|e| BootstrapError::Database(format!("failed to create state db: {}", e)))?;
    let state_db = Arc::new(state_db);
    let state = Arc::new(RwLock::new(StateLayer::new(state_db)));

    // Open block archive for historical block storage
    let blocks_db_path = config.data_dir.join("blocks.redb");
    let blocks_db = Database::create(&blocks_db_path)
        .map_err(|e| BootstrapError::Database(format!("failed to create blocks db: {}", e)))?;
    let block_archive = Arc::new(BlockArchive::new(Arc::new(blocks_db)));

    // Open Raft log store and configure with dependencies
    let log_path = config.data_dir.join("raft.redb");
    let log_store = RaftLogStore::open(&log_path)
        .map_err(|e| BootstrapError::Storage(format!("failed to open log store: {}", e)))?
        .with_state_layer(state.clone())
        .with_block_archive(block_archive.clone())
        .with_shard_config(0, config.node_id.to_string()); // Default shard 0

    // Get accessor before log_store is consumed by Adaptor
    let applied_state_accessor = log_store.accessor();

    // Create Raft network factory
    let network = GrpcRaftNetworkFactory::new();

    // Build Raft configuration
    let raft_config = openraft::Config {
        cluster_name: "ledger".to_string(),
        heartbeat_interval: 150,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };

    // Create adaptor to split RaftStorage into log storage and state machine
    // The Adaptor provides RaftLogStorage and RaftStateMachine from our RaftStorage impl
    // Note: Adaptor takes ownership of the store
    let (log_storage, state_machine) = Adaptor::new(log_store);

    // Create Raft instance
    let raft = Raft::<LedgerTypeConfig>::new(
        config.node_id,
        Arc::new(raft_config),
        network,
        log_storage,
        state_machine,
    )
    .await
    .map_err(|e| BootstrapError::Raft(format!("failed to create raft: {}", e)))?;

    let raft = Arc::new(raft);

    // Bootstrap cluster if this is the initial node
    if config.bootstrap {
        bootstrap_cluster(&raft, config).await?;
    }

    // Create server with block archive for GetBlock/GetBlockRange
    let server = LedgerServer::with_block_archive(
        raft.clone(),
        state.clone(),
        applied_state_accessor.clone(),
        Some(block_archive),
        config.listen_addr,
    )
    .with_rate_limit(
        config.rate_limit.max_concurrent,
        config.rate_limit.timeout_secs,
    );

    // Start TTL garbage collector as background task
    let gc = TtlGarbageCollector::new(
        raft.clone(),
        config.node_id,
        state.clone(),
        applied_state_accessor,
    );
    let gc_handle = gc.start();
    tracing::info!("Started TTL garbage collector");

    Ok(BootstrappedNode {
        raft,
        state,
        server,
        gc_handle,
    })
}

/// Bootstrap a new cluster with this node as the initial member.
///
/// This should only be called once for the first node in a new cluster.
async fn bootstrap_cluster(
    raft: &Raft<LedgerTypeConfig>,
    config: &Config,
) -> Result<(), BootstrapError> {
    // Build initial membership with this node and any configured peers
    let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();

    // Add self
    members.insert(
        config.node_id,
        BasicNode {
            addr: config.listen_addr.to_string(),
        },
    );

    // Add configured peers
    for peer in &config.peers {
        members.insert(
            peer.node_id,
            BasicNode {
                addr: peer.addr.clone(),
            },
        );
    }

    // Initialize the cluster
    raft.initialize(members)
        .await
        .map_err(|e| BootstrapError::Initialize(format!("failed to initialize: {}", e)))?;

    tracing::info!(
        node_id = config.node_id,
        "Bootstrapped new cluster as initial leader"
    );

    Ok(())
}

/// Join an existing cluster by contacting a peer.
///
/// This is called when `config.bootstrap` is false and the node needs to join
/// an existing cluster. The node contacts one of the configured peers and
/// requests to be added to the cluster.
///
/// Note: This should be called after the gRPC server has started, since the
/// leader needs to be able to reach this node to replicate logs.
#[allow(dead_code)] // Reserved for join-cluster mode in main.rs
pub async fn join_cluster(config: &Config) -> Result<(), BootstrapError> {
    if config.peers.is_empty() {
        return Err(BootstrapError::Join(
            "No peers configured, cannot join cluster".to_string(),
        ));
    }

    let my_address = config.listen_addr.to_string();

    // Try each peer until one accepts our join request
    for peer in &config.peers {
        tracing::info!(
            peer_addr = %peer.addr,
            "Attempting to join cluster via peer"
        );

        // Connect to peer
        let endpoint = match Channel::from_shared(format!("http://{}", peer.addr)) {
            Ok(e) => e.connect_timeout(Duration::from_secs(5)),
            Err(e) => {
                tracing::warn!(peer_addr = %peer.addr, error = %e, "Invalid peer address");
                continue;
            }
        };

        let channel = match endpoint.connect().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(peer_addr = %peer.addr, error = %e, "Failed to connect to peer");
                continue;
            }
        };

        let mut client = AdminServiceClient::new(channel);

        // Request to join
        let request = JoinClusterRequest {
            node_id: config.node_id,
            address: my_address.clone(),
        };

        match client.join_cluster(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    tracing::info!(node_id = config.node_id, "Successfully joined cluster");
                    return Ok(());
                }

                // If not leader, try the leader address if provided
                if !resp.leader_address.is_empty() {
                    tracing::info!(
                        leader_addr = %resp.leader_address,
                        "Peer redirected to leader"
                    );

                    // Connect to leader
                    if let Ok(endpoint) =
                        Channel::from_shared(format!("http://{}", resp.leader_address))
                    {
                        if let Ok(leader_channel) = endpoint
                            .connect_timeout(Duration::from_secs(5))
                            .connect()
                            .await
                        {
                            let mut leader_client = AdminServiceClient::new(leader_channel);
                            let leader_request = JoinClusterRequest {
                                node_id: config.node_id,
                                address: my_address.clone(),
                            };

                            if let Ok(leader_response) =
                                leader_client.join_cluster(leader_request).await
                            {
                                if leader_response.into_inner().success {
                                    tracing::info!(
                                        node_id = config.node_id,
                                        "Successfully joined cluster via leader"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                    }
                }

                tracing::warn!(
                    peer_addr = %peer.addr,
                    message = %resp.message,
                    "Join request rejected"
                );
            }
            Err(e) => {
                tracing::warn!(
                    peer_addr = %peer.addr,
                    error = %e,
                    "Join request failed"
                );
            }
        }
    }

    Err(BootstrapError::Join(
        "Failed to join cluster via any configured peer".to_string(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_bootstrap_single_node() {
        let temp_dir = tempdir().expect("create temp dir");
        let config = Config::for_test(1, 50051, temp_dir.path().to_path_buf());

        let result = bootstrap_node(&config).await;
        assert!(
            result.is_ok(),
            "bootstrap should succeed: {:?}",
            result.err()
        );
    }
}
