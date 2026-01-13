//! Cluster bootstrap and initialization.
//!
//! Provides functions to:
//! - Bootstrap a new cluster with this node as the initial leader
//! - Join an existing cluster by contacting the leader

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use redb::Database;
use tonic::transport::Channel;
use tracing::info;

use ledger_raft::proto::JoinClusterRequest;
use ledger_raft::proto::admin_service_client::AdminServiceClient;
use ledger_raft::{
    AutoRecoveryJob, BlockCompactor, GrpcRaftNetworkFactory, LearnerRefreshJob, LedgerNodeId,
    LedgerServer, LedgerTypeConfig, RaftLogStore, TtlGarbageCollector,
};
use ledger_storage::{BlockArchive, SnapshotManager, StateLayer};

use crate::config::Config;
use crate::discovery::resolve_bootstrap_peers;

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
    /// The shared state layer (internally thread-safe via redb MVCC).
    #[allow(dead_code)]
    pub state: Arc<StateLayer>,
    /// The configured Ledger server.
    pub server: LedgerServer,
    /// TTL garbage collector background task handle.
    #[allow(dead_code)]
    pub gc_handle: tokio::task::JoinHandle<()>,
    /// Block compactor background task handle.
    #[allow(dead_code)]
    pub compactor_handle: tokio::task::JoinHandle<()>,
    /// Auto-recovery background task handle.
    #[allow(dead_code)]
    pub recovery_handle: tokio::task::JoinHandle<()>,
    /// Learner refresh background task handle (only active on learner nodes).
    #[allow(dead_code)]
    pub learner_refresh_handle: tokio::task::JoinHandle<()>,
}

/// Bootstrap a new cluster or join an existing one based on configuration.
///
/// If `config.bootstrap` is true, initializes a new single-node cluster.
/// Otherwise, the node starts without initialization (ready to join).
pub async fn bootstrap_node(config: &Config) -> Result<BootstrappedNode, BootstrapError> {
    // Create data directories
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|e| BootstrapError::Database(format!("failed to create data dir: {}", e)))?;

    // Open state database and initialize tables
    let state_db_path = config.data_dir.join("state.redb");
    let state_db = Database::create(&state_db_path)
        .map_err(|e| BootstrapError::Database(format!("failed to create state db: {}", e)))?;
    let state_db = Arc::new(state_db);
    // StateLayer is internally thread-safe via redb MVCC - no external lock needed
    let state = Arc::new(
        StateLayer::open(state_db)
            .map_err(|e| BootstrapError::Database(format!("failed to init state tables: {}", e)))?,
    );

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

    // Clone block_archive for background jobs before it's moved to server
    let block_archive_for_compactor = block_archive.clone();
    let block_archive_for_recovery = block_archive.clone();

    // Create snapshot manager for recovery optimization
    let snapshot_dir = config.data_dir.join("snapshots");
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_dir, 5)); // Keep last 5 snapshots

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
        applied_state_accessor.clone(),
    );
    let gc_handle = gc.start();
    tracing::info!("Started TTL garbage collector");

    // Start block compactor for COMPACTED retention mode
    let compactor = BlockCompactor::new(
        raft.clone(),
        config.node_id,
        block_archive_for_compactor,
        applied_state_accessor.clone(),
    );
    let compactor_handle = compactor.start();
    tracing::info!("Started block compactor");

    // Start auto-recovery job for detecting and recovering diverged vaults
    // Per DESIGN.md §8.2: Circuit breaker with bounded retries
    let recovery = AutoRecoveryJob::new(
        raft.clone(),
        config.node_id,
        applied_state_accessor.clone(),
        state.clone(),
    )
    .with_block_archive(block_archive_for_recovery)
    .with_snapshot_manager(snapshot_manager);
    let recovery_handle = recovery.start();
    tracing::info!("Started auto-recovery job with snapshot support");

    // Start learner refresh job for keeping learner state synchronized
    // Per DESIGN.md §9.3: Background polling of voters for fresh state
    let learner_refresh =
        LearnerRefreshJob::new(raft.clone(), config.node_id, applied_state_accessor);
    let learner_refresh_handle = learner_refresh.start();
    tracing::info!("Started learner refresh job");

    Ok(BootstrappedNode {
        raft,
        state,
        server,
        gc_handle,
        compactor_handle,
        recovery_handle,
        learner_refresh_handle,
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
/// Per DESIGN.md §3.6: Uses DNS SRV discovery to find cluster entry points,
/// combined with cached peers and static configuration.
///
/// Note: This should be called after the gRPC server has started, since the
/// leader needs to be able to reach this node to replicate logs.
#[allow(dead_code)] // Reserved for join-cluster mode in main.rs
pub async fn join_cluster(config: &Config) -> Result<(), BootstrapError> {
    // Extract static peer addresses from config
    let static_peer_addrs: Vec<String> = config.peers.iter().map(|p| p.addr.clone()).collect();

    // Resolve all bootstrap peers: cached + static + DNS SRV
    let peer_addresses = resolve_bootstrap_peers(&static_peer_addrs, &config.discovery).await;

    if peer_addresses.is_empty() {
        return Err(BootstrapError::Join(
            "No peers available (checked cache, config, and DNS SRV)".to_string(),
        ));
    }

    info!(
        peer_count = peer_addresses.len(),
        "Resolved bootstrap peers for cluster join"
    );

    let my_address = config.listen_addr.to_string();

    // Try each peer until one accepts our join request
    for peer_addr in &peer_addresses {
        if let Err(e) = try_join_via_peer(config.node_id, &my_address, *peer_addr).await {
            tracing::warn!(peer_addr = %peer_addr, error = %e, "Join attempt failed");
            continue;
        }
        return Ok(());
    }

    Err(BootstrapError::Join(
        "Failed to join cluster via any discovered peer".to_string(),
    ))
}

/// Attempt to join the cluster via a specific peer address.
async fn try_join_via_peer(
    node_id: u64,
    my_address: &str,
    peer_addr: SocketAddr,
) -> Result<(), String> {
    tracing::info!(peer_addr = %peer_addr, "Attempting to join cluster via peer");

    // Connect to peer
    let endpoint = Channel::from_shared(format!("http://{}", peer_addr))
        .map_err(|e| format!("Invalid peer address: {}", e))?
        .connect_timeout(Duration::from_secs(5));

    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    let mut client = AdminServiceClient::new(channel);

    // Request to join
    let request = JoinClusterRequest {
        node_id,
        address: my_address.to_string(),
    };

    let response = client
        .join_cluster(request)
        .await
        .map_err(|e| format!("Join RPC failed: {}", e))?;

    let resp = response.into_inner();
    if resp.success {
        tracing::info!(node_id, "Successfully joined cluster");
        return Ok(());
    }

    // If not leader, try the leader address if provided
    if !resp.leader_address.is_empty() {
        tracing::info!(leader_addr = %resp.leader_address, "Peer redirected to leader");

        let leader_addr: SocketAddr = resp
            .leader_address
            .parse()
            .map_err(|e| format!("Invalid leader address: {}", e))?;

        return try_join_via_leader(node_id, my_address, leader_addr).await;
    }

    Err(format!("Join request rejected: {}", resp.message))
}

/// Follow a redirect to join via the leader.
async fn try_join_via_leader(
    node_id: u64,
    my_address: &str,
    leader_addr: SocketAddr,
) -> Result<(), String> {
    let endpoint = Channel::from_shared(format!("http://{}", leader_addr))
        .map_err(|e| format!("Invalid leader address: {}", e))?
        .connect_timeout(Duration::from_secs(5));

    let leader_channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("Failed to connect to leader: {}", e))?;

    let mut leader_client = AdminServiceClient::new(leader_channel);
    let leader_request = JoinClusterRequest {
        node_id,
        address: my_address.to_string(),
    };

    let leader_response = leader_client
        .join_cluster(leader_request)
        .await
        .map_err(|e| format!("Leader join RPC failed: {}", e))?;

    if leader_response.into_inner().success {
        tracing::info!(node_id, "Successfully joined cluster via leader");
        return Ok(());
    }

    Err("Leader rejected join request".to_string())
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
