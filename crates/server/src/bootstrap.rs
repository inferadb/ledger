//! Cluster bootstrap and initialization.
//!
//! Provides functions to:
//! - Bootstrap a new cluster with this node as the initial leader
//! - Join an existing cluster by contacting the leader

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_proto::proto::{
    BlockAnnouncement, JoinClusterRequest, admin_service_client::AdminServiceClient,
};
use inferadb_ledger_raft::{
    AutoRecoveryJob, BackupJob, BackupManager, BlockCompactor, GrpcRaftNetworkFactory,
    LearnerRefreshJob, LedgerNodeId, LedgerServer, LedgerTypeConfig, RaftLogStore,
    ResourceMetricsCollector, RuntimeConfigHandle, TtlGarbageCollector,
};
use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::{Database, FileBackend};
use openraft::{BasicNode, Raft, storage::Adaptor};
use tokio::sync::broadcast;
use tonic::transport::Channel;
use tracing::info;

use crate::{
    config::Config,
    coordinator::{BootstrapDecision, coordinate_bootstrap},
    discovery::resolve_bootstrap_peers,
};

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
    /// Failed to join existing cluster (reserved for join-cluster mode).
    #[allow(dead_code)] // reserved for join-cluster mode
    Join(String),
    /// Failed to resolve or generate node ID.
    NodeId(String),
    /// Bootstrap coordination timed out waiting for peers.
    Timeout(String),
    /// Configuration validation failed.
    Config(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Database(msg) => write!(f, "database error: {}", msg),
            BootstrapError::Storage(msg) => write!(f, "storage error: {}", msg),
            BootstrapError::Raft(msg) => write!(f, "raft error: {}", msg),
            BootstrapError::Initialize(msg) => write!(f, "initialization error: {}", msg),
            BootstrapError::Join(msg) => write!(f, "join error: {}", msg),
            BootstrapError::NodeId(msg) => write!(f, "node id error: {}", msg),
            BootstrapError::Timeout(msg) => write!(f, "bootstrap timeout: {}", msg),
            BootstrapError::Config(msg) => write!(f, "configuration error: {}", msg),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Bootstrapped node components.
///
/// Some fields are not directly accessed but are retained for ownership semantics:
/// - `raft` and `state`: Maintain Arc reference counts for shared state
/// - Handle fields: Keep background tasks alive and allow graceful shutdown
pub struct BootstrappedNode {
    /// The Raft instance.
    #[allow(dead_code)] // retained to maintain Arc reference count for shared Raft state
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    #[allow(dead_code)] // retained to maintain Arc reference count for shared state layer
    pub state: Arc<StateLayer<FileBackend>>,
    /// The configured Ledger server.
    pub server: LedgerServer,
    /// TTL garbage collector background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub gc_handle: tokio::task::JoinHandle<()>,
    /// Block compactor background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub compactor_handle: tokio::task::JoinHandle<()>,
    /// Auto-recovery background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub recovery_handle: tokio::task::JoinHandle<()>,
    /// Learner refresh background task handle (only active on learner nodes).
    #[allow(dead_code)] // retained to keep background task alive
    pub learner_refresh_handle: tokio::task::JoinHandle<()>,
    /// Resource metrics collector background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub resource_metrics_handle: tokio::task::JoinHandle<()>,
    /// Automated backup background task handle (only active when backup is configured).
    #[allow(dead_code)] // retained to keep background task alive
    pub backup_handle: Option<tokio::task::JoinHandle<()>>,
    /// Runtime configuration handle for hot-reloadable settings.
    pub runtime_config: RuntimeConfigHandle,
}

/// Bootstrap a new cluster, join an existing one, or resume from saved state.
///
/// Behavior is determined automatically via coordinated bootstrap:
///
/// - If the node has persisted Raft state, it resumes from that state.
/// - If `bootstrap_expect=0`, waits to be added to existing cluster via AdminService.
/// - If `bootstrap_expect=1`, bootstraps immediately as a single-node cluster.
/// - Otherwise, coordinates with discovered peers using GetNodeInfo RPC:
///   - If any peer is already a cluster member, waits to join existing cluster
///   - If enough peers found, the node with lowest Snowflake ID bootstraps all members
///   - If this node doesn't have lowest ID, waits to be added by the bootstrapping node
///
/// # Errors
///
/// Returns [`BootstrapError`] if:
/// - Configuration validation fails
/// - Node ID resolution fails
/// - Database creation or opening fails
/// - Raft instance creation fails
/// - Cluster initialization or join times out
pub async fn bootstrap_node(
    config: &Config,
    data_dir: &std::path::Path,
    health_state: inferadb_ledger_raft::HealthState,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<BootstrappedNode, BootstrapError> {
    // Validate bootstrap configuration
    config.validate().map_err(|e| BootstrapError::Config(e.to_string()))?;

    // Resolve the effective node ID (manual or auto-generated Snowflake ID)
    let node_id = config.node_id(data_dir).map_err(|e| BootstrapError::NodeId(e.to_string()))?;

    std::fs::create_dir_all(data_dir)
        .map_err(|e| BootstrapError::Database(format!("failed to create data dir: {}", e)))?;

    let state_db_path = data_dir.join("state.db");
    let state_db = Arc::new(
        Database::<FileBackend>::create(&state_db_path)
            .map_err(|e| BootstrapError::Database(format!("failed to create state db: {}", e)))?,
    );
    // StateLayer is internally thread-safe via MVCC - no external lock needed
    let state = Arc::new(StateLayer::new(state_db));

    let blocks_db_path = data_dir.join("blocks.db");
    let blocks_db = Arc::new(
        Database::<FileBackend>::create(&blocks_db_path)
            .map_err(|e| BootstrapError::Database(format!("failed to create blocks db: {}", e)))?,
    );
    let block_archive = Arc::new(BlockArchive::new(blocks_db));

    // Create block announcements broadcast channel for real-time notifications.
    // Buffer size of 1024 allows for burst handling during high commit rates.
    let (block_announcements, _) = broadcast::channel::<BlockAnnouncement>(1024);

    let log_path = data_dir.join("raft.db");
    let log_store = RaftLogStore::open(&log_path)
        .map_err(|e| BootstrapError::Storage(format!("failed to open log store: {}", e)))?
        .with_state_layer(state.clone())
        .with_block_archive(block_archive.clone())
        .with_shard_config(inferadb_ledger_types::ShardId::new(0), node_id.to_string()) // Default shard 0
        .with_block_announcements(block_announcements.clone());

    // Determine bootstrap behavior before log_store is consumed by Adaptor
    let is_initialized = log_store.is_initialized();

    // Get accessor before log_store is consumed by Adaptor
    let applied_state_accessor = log_store.accessor();

    let network =
        GrpcRaftNetworkFactory::with_trace_config(config.wide_events.otel.trace_raft_rpcs);
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

    let raft = Raft::<LedgerTypeConfig>::new(
        node_id,
        Arc::new(raft_config),
        network,
        log_storage,
        state_machine,
    )
    .await
    .map_err(|e| BootstrapError::Raft(format!("failed to create raft: {}", e)))?;

    let raft = Arc::new(raft);

    // Determine whether to bootstrap based on existing state and bootstrap_expect
    if is_initialized {
        tracing::info!("Existing Raft state found, resuming");
    } else if config.is_join_mode() {
        // Join mode: wait to be added to existing cluster via AdminService
        tracing::info!(
            node_id,
            "Join mode (bootstrap_expect=0): waiting to be added via AdminService"
        );
        // Note: We intentionally do NOT bootstrap or call discovery here.
        // The calling code is responsible for adding this node to the cluster.
    } else if config.is_single_node() {
        // Single-node mode: bootstrap immediately without coordination
        tracing::info!(node_id, "Bootstrapping single-node cluster (bootstrap_expect=1)");
        bootstrap_cluster(&raft, node_id, &config.listen_addr).await?;
    } else {
        // Fresh node - use coordinated bootstrap to determine action
        let my_address = config.listen_addr.to_string();

        let decision = coordinate_bootstrap(node_id, &my_address, config)
            .await
            .map_err(|e| BootstrapError::Timeout(e.to_string()))?;

        match decision {
            BootstrapDecision::Bootstrap { initial_members } => {
                if initial_members.len() == 1 {
                    // Single-node bootstrap
                    tracing::info!(node_id, "Bootstrapping new single-node cluster");
                    bootstrap_cluster(&raft, node_id, &config.listen_addr).await?;
                } else {
                    // Multi-node coordinated bootstrap - this node has lowest ID
                    tracing::info!(
                        node_id,
                        member_count = initial_members.len(),
                        "Bootstrapping new multi-node cluster (lowest ID)"
                    );
                    bootstrap_cluster_multi(&raft, initial_members).await?;
                }
            },
            BootstrapDecision::WaitForJoin { leader_addr } => {
                // Another node has the lowest ID and will bootstrap
                tracing::info!(
                    node_id,
                    leader = %leader_addr,
                    "Waiting for cluster bootstrap by lowest-ID node"
                );
                let timeout = Duration::from_secs(config.peers_timeout_secs);
                let poll_interval = Duration::from_secs(config.peers_poll_secs);
                wait_for_cluster_join(&raft, timeout, poll_interval).await?;
            },
            BootstrapDecision::JoinExisting { via_peer } => {
                // Existing cluster found - wait to be added
                tracing::info!(
                    node_id,
                    peer = %via_peer,
                    "Found existing cluster, waiting to be added via AdminService"
                );
                let timeout = Duration::from_secs(config.peers_timeout_secs);
                let poll_interval = Duration::from_secs(config.peers_poll_secs);
                wait_for_cluster_join(&raft, timeout, poll_interval).await?;
            },
        }
    }

    let block_archive_for_compactor = block_archive.clone();
    let block_archive_for_recovery = block_archive.clone();
    let snapshot_dir = data_dir.join("snapshots");
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_dir, 5));
    let snapshot_manager_for_backup = snapshot_manager.clone();

    // Create runtime config handle for hot-reloadable settings.
    // Services read from this handle on every request via lock-free ArcSwap::load().
    let runtime_config = RuntimeConfigHandle::default();

    // Create backup manager if configured.
    // The manager creates the destination directory and is shared between
    // the admin service (on-demand RPCs) and the background job (automated backups).
    let backup_manager = config
        .backup
        .as_ref()
        .map(|backup_config| {
            BackupManager::new(backup_config).map(Arc::new).map_err(|e| {
                BootstrapError::Config(format!("failed to create backup manager: {e}"))
            })
        })
        .transpose()?;

    let server = LedgerServer::builder()
        .raft(raft.clone())
        .state(state.clone())
        .applied_state(applied_state_accessor.clone())
        .block_archive(Some(block_archive))
        .block_announcements(block_announcements)
        .addr(config.listen_addr)
        .max_concurrent(config.max_concurrent)
        .timeout_secs(config.timeout_secs)
        .health_state(health_state.clone())
        .shutdown_rx(Some(shutdown_rx))
        .runtime_config(Some(runtime_config.clone()))
        .data_dir(Some(data_dir.to_path_buf()))
        .build();
    // Wire backup support into server if configured.
    // Done post-construction because bon type-state builders don't support
    // conditional field setting (each setter changes the builder type).
    let server = if let Some(ref mgr) = backup_manager {
        server.with_backup(mgr.clone(), snapshot_manager_for_backup.clone())
    } else {
        server
    };

    // Register background jobs with the watchdog (if attached to health state).
    // Each job gets an AtomicU64 handle that it writes to on every cycle.
    // The liveness probe detects stuck jobs by checking these heartbeats.
    let watchdog = health_state.watchdog();

    let gc_handle = TtlGarbageCollector::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .state(state.clone())
        .applied_state(applied_state_accessor.clone())
        .watchdog_handle(watchdog.map(|w| w.register("ttl_gc", 60)))
        .build()
        .start();
    tracing::info!("Started TTL garbage collector");

    // Start block compactor for COMPACTED retention mode
    let compactor_handle = BlockCompactor::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .block_archive(block_archive_for_compactor)
        .applied_state(applied_state_accessor.clone())
        .watchdog_handle(watchdog.map(|w| w.register("block_compactor", 300)))
        .build()
        .start();
    tracing::info!("Started block compactor");

    // Start auto-recovery job for detecting and recovering diverged vaults
    // Per DESIGN.md §8.2: Circuit breaker with bounded retries
    let recovery_handle = AutoRecoveryJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .applied_state(applied_state_accessor.clone())
        .state(state.clone())
        .block_archive(Some(block_archive_for_recovery))
        .snapshot_manager(Some(snapshot_manager))
        .watchdog_handle(watchdog.map(|w| w.register("auto_recovery", 30)))
        .build()
        .start();
    tracing::info!("Started auto-recovery job with snapshot support");

    // Start learner refresh job for keeping learner state synchronized
    // Per DESIGN.md §9.3: Background polling of voters for fresh state
    let learner_refresh_handle = LearnerRefreshJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .applied_state(applied_state_accessor)
        .watchdog_handle(watchdog.map(|w| w.register("learner_refresh", 5)))
        .build()
        .start();
    tracing::info!("Started learner refresh job");

    // Start resource saturation metrics collector
    let snapshot_dir_for_metrics = data_dir.join("snapshots");
    let resource_metrics_handle = ResourceMetricsCollector::builder()
        .state(state.clone())
        .data_dir(data_dir.to_path_buf())
        .snapshot_dir(snapshot_dir_for_metrics)
        .watchdog_handle(watchdog.map(|w| w.register("resource_metrics", 30)))
        .build()
        .start();
    tracing::info!("Started resource metrics collector");

    // Start automated backup job if configured and enabled
    let backup_handle =
        if let (Some(backup_config), Some(mgr)) = (config.backup.as_ref(), backup_manager) {
            if backup_config.enabled {
                let job = BackupJob::builder()
                    .raft(raft.clone())
                    .node_id(node_id)
                    .snapshot_manager(snapshot_manager_for_backup)
                    .backup_manager(mgr)
                    .interval(Duration::from_secs(backup_config.interval_secs))
                    .build();
                let handle = job.start();
                tracing::info!(
                    interval_secs = backup_config.interval_secs,
                    destination = %backup_config.destination,
                    retention = backup_config.retention_count,
                    "Started automated backup job"
                );
                Some(handle)
            } else {
                tracing::info!("Backup configured but not enabled, skipping automated backup job");
                None
            }
        } else {
            None
        };

    Ok(BootstrappedNode {
        raft,
        state,
        server,
        gc_handle,
        compactor_handle,
        recovery_handle,
        learner_refresh_handle,
        resource_metrics_handle,
        backup_handle,
        runtime_config,
    })
}

/// Bootstrap a new single-node cluster with this node as the initial member.
///
/// Additional nodes join dynamically via `join_cluster()` using discovery.
async fn bootstrap_cluster(
    raft: &Raft<LedgerTypeConfig>,
    node_id: u64,
    listen_addr: &SocketAddr,
) -> Result<(), BootstrapError> {
    let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
    members.insert(node_id, BasicNode { addr: listen_addr.to_string() });

    raft.initialize(members)
        .await
        .map_err(|e| BootstrapError::Initialize(format!("failed to initialize: {}", e)))?;

    tracing::info!(node_id = node_id, "Bootstrapped new single-node cluster");

    Ok(())
}

/// Bootstrap a new cluster with multiple initial members.
///
/// This is used during coordinated bootstrap when multiple nodes start simultaneously.
/// The node with the lowest Snowflake ID calls this with all discovered members.
///
/// # Arguments
///
/// * `raft` - The Raft instance to initialize
/// * `initial_members` - List of (node_id, address) pairs for all initial members
async fn bootstrap_cluster_multi(
    raft: &Raft<LedgerTypeConfig>,
    initial_members: Vec<(u64, String)>,
) -> Result<(), BootstrapError> {
    let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
    for (node_id, addr) in &initial_members {
        members.insert(*node_id, BasicNode { addr: addr.clone() });
    }

    let member_ids: Vec<u64> = initial_members.iter().map(|(id, _)| *id).collect();
    raft.initialize(members)
        .await
        .map_err(|e| BootstrapError::Initialize(format!("failed to initialize: {}", e)))?;

    tracing::info!(members = ?member_ids, "Bootstrapped new multi-node cluster");

    Ok(())
}

/// Wait for this node to be added to the cluster by another node.
///
/// This is used during coordinated bootstrap when this node is not the lowest-ID
/// node. The lowest-ID node will bootstrap and then add other members via Raft.
///
/// # Arguments
///
/// * `raft` - The Raft instance to check for membership
/// * `timeout` - Maximum time to wait before giving up
/// * `poll_interval` - How often to check cluster membership status
///
/// # Returns
///
/// Returns `Ok(())` when the node becomes a cluster member, or `Err(Timeout)` if
/// the timeout expires before joining.
async fn wait_for_cluster_join(
    raft: &Raft<LedgerTypeConfig>,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), BootstrapError> {
    let start = Instant::now();

    loop {
        if start.elapsed() > timeout {
            return Err(BootstrapError::Timeout(format!(
                "timed out waiting to join cluster after {}s",
                timeout.as_secs()
            )));
        }

        let metrics = raft.metrics().borrow().clone();

        // Check if we're now part of a cluster (have a leader or are in voter set)
        let has_leader = metrics.current_leader.is_some();
        let is_voter = metrics.membership_config.membership().voter_ids().count() > 0;

        if has_leader || is_voter {
            tracing::info!(
                leader = ?metrics.current_leader,
                term = metrics.current_term,
                "Successfully joined cluster"
            );
            return Ok(());
        }

        tracing::debug!(
            elapsed_secs = start.elapsed().as_secs(),
            timeout_secs = timeout.as_secs(),
            "Waiting to be added to cluster"
        );
        tokio::time::sleep(poll_interval).await;
    }
}

/// Join an existing cluster by contacting a peer.
///
/// Uses discovery (DNS A records + cached peers) to find cluster entry points.
/// The node contacts discovered peers and requests to be added to the cluster.
///
/// Note: This should be called after the gRPC server has started, since the
/// leader needs to be able to reach this node to replicate logs.
///
/// # Errors
///
/// Returns [`BootstrapError`] if:
/// - Node ID resolution fails
/// - No peers are discoverable via DNS or cache
/// - All join attempts to discovered peers fail
#[allow(dead_code)] // reserved for join-cluster mode
pub async fn join_cluster(
    config: &Config,
    data_dir: &std::path::Path,
) -> Result<(), BootstrapError> {
    let node_id = config.node_id(data_dir).map_err(|e| BootstrapError::NodeId(e.to_string()))?;

    let peer_addresses = resolve_bootstrap_peers(config).await;

    if peer_addresses.is_empty() {
        return Err(BootstrapError::Join("No peers available (checked cache and DNS)".to_string()));
    }

    info!(peer_count = peer_addresses.len(), "Resolved bootstrap peers for cluster join");

    let my_address = config.listen_addr.to_string();

    for peer_addr in &peer_addresses {
        if let Err(e) = try_join_via_peer(node_id, &my_address, *peer_addr).await {
            tracing::warn!(peer_addr = %peer_addr, error = %e, "Join attempt failed");
            continue;
        }
        return Ok(());
    }

    Err(BootstrapError::Join("Failed to join cluster via any discovered peer".to_string()))
}

/// Attempt to join the cluster via a specific peer address.
async fn try_join_via_peer(
    node_id: u64,
    my_address: &str,
    peer_addr: SocketAddr,
) -> Result<(), String> {
    tracing::info!(peer_addr = %peer_addr, "Attempting to join cluster via peer");

    let endpoint = Channel::from_shared(format!("http://{}", peer_addr))
        .map_err(|e| format!("Invalid peer address: {}", e))?
        .connect_timeout(Duration::from_secs(5));

    let channel = endpoint.connect().await.map_err(|e| format!("Failed to connect: {}", e))?;

    let mut client = AdminServiceClient::new(channel);

    let request = JoinClusterRequest { node_id, address: my_address.to_string() };

    let response =
        client.join_cluster(request).await.map_err(|e| format!("Join RPC failed: {}", e))?;

    let resp = response.into_inner();
    if resp.success {
        tracing::info!(node_id, "Successfully joined cluster");
        return Ok(());
    }

    // If not leader, try the leader address if provided
    if !resp.leader_address.is_empty() {
        tracing::info!(leader_addr = %resp.leader_address, "Peer redirected to leader");

        let leader_addr: SocketAddr =
            resp.leader_address.parse().map_err(|e| format!("Invalid leader address: {}", e))?;

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

    let leader_channel =
        endpoint.connect().await.map_err(|e| format!("Failed to connect to leader: {}", e))?;

    let mut leader_client = AdminServiceClient::new(leader_channel);
    let leader_request = JoinClusterRequest { node_id, address: my_address.to_string() };

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
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_bootstrap_single_node() {
        let temp_dir = tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50051, data_dir.clone());

        let health = inferadb_ledger_raft::HealthState::new();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let result = bootstrap_node(&config, &data_dir, health, rx).await;
        assert!(result.is_ok(), "bootstrap should succeed: {:?}", result.err());
    }
}
