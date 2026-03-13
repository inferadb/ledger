//! Cluster bootstrap and initialization.
//!
//! Provides functions to:
//! - Resume from persisted Raft state (existing node restart)
//! - Bootstrap a new cluster (single-node or coordinated multi-node)

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_raft::{
    AutoRecoveryJob, BackupJob, BackupManager, BlockCompactor, EventsGarbageCollector,
    GrpcRaftNetworkFactory, HotKeyDetector, IntegrityScrubberJob, LearnerRefreshJob, LedgerNodeId,
    LedgerTypeConfig, OrganizationPurgeJob, OrphanCleanupJob, PostErasureCompactionJob,
    RaftLogStore, RaftManager, RaftManagerConfig, RateLimiter, ResourceMetricsCollector,
    RuntimeConfigHandle, SagaOrchestrator, TokenMaintenanceJob, TtlGarbageCollector,
    event_writer::{EventHandle, EventWriter},
};
use inferadb_ledger_services::LedgerServer;
use inferadb_ledger_state::{
    BlockArchive, LocalBackend, ObjectStorageBackend, SnapshotManager, StateLayer,
    TieredSnapshotManager,
};
use inferadb_ledger_store::FileBackend;
use openraft::{BasicNode, Raft, storage::Adaptor};
use tokio::sync::broadcast;

use crate::{
    config::Config,
    coordinator::{BootstrapDecision, coordinate_bootstrap},
};

/// Errors during cluster bootstrap, including database, Raft, and coordination failures.
#[derive(Debug, snafu::Snafu)]
pub enum BootstrapError {
    /// Failed to open database.
    #[snafu(display("database error: {message}"))]
    Database {
        /// Error description.
        message: String,
    },
    /// Failed to create Raft storage.
    #[snafu(display("storage error: {message}"))]
    Storage {
        /// Error description.
        message: String,
    },
    /// Failed to create Raft instance.
    #[snafu(display("raft error: {message}"))]
    Raft {
        /// Error description.
        message: String,
    },
    /// Failed to initialize cluster.
    #[snafu(display("initialization error: {message}"))]
    Initialize {
        /// Error description.
        message: String,
    },
    /// Failed to resolve or generate node ID.
    #[snafu(display("node id error: {message}"))]
    NodeId {
        /// Error description.
        message: String,
    },
    /// Bootstrap coordination timed out waiting for peers.
    #[snafu(display("bootstrap timeout: {message}"))]
    Timeout {
        /// Error description.
        message: String,
    },
    /// Configuration validation failed.
    #[snafu(display("configuration error: {message}"))]
    Config {
        /// Error description.
        message: String,
    },
    /// gRPC server failed to start.
    #[snafu(display("server error: {message}"))]
    Server {
        /// Error description.
        message: String,
    },
}

/// Holds ownership of all node resources after a successful bootstrap.
///
/// Some fields are not directly accessed but are retained for ownership semantics:
/// - `raft` and `state`: Maintain Arc reference counts for shared state
/// - Handle fields: Keep background tasks alive and allow graceful shutdown
pub struct BootstrappedNode {
    /// Raft consensus instance (retained for reference counting).
    #[allow(dead_code)] // retained to maintain Arc reference count for shared Raft state
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    #[allow(dead_code)] // retained to maintain Arc reference count for shared state layer
    pub state: Arc<StateLayer<FileBackend>>,
    /// gRPC server running in background.
    pub server_handle: tokio::task::JoinHandle<Result<(), String>>,
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
    /// Events garbage collector background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub events_gc_handle: Option<tokio::task::JoinHandle<()>>,
    /// Runtime configuration handle for hot-reloadable settings (used by UpdateConfig RPC).
    #[allow(dead_code)] // retained: handle is cloned into LedgerServer during bootstrap
    pub runtime_config: RuntimeConfigHandle,
    /// Saga orchestrator background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub saga_handle: tokio::task::JoinHandle<()>,
    /// Saga orchestrator submission handle for service handlers.
    #[allow(dead_code)]
    // retained: cloned into LedgerServer via saga_cell OnceCell during bootstrap
    pub saga_orchestrator_handle: inferadb_ledger_raft::SagaOrchestratorHandle,
    /// Orphan cleanup background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub orphan_cleanup_handle: tokio::task::JoinHandle<()>,
    /// Integrity scrubber background task handle.
    #[allow(dead_code)] // retained to keep background task alive
    pub integrity_scrub_handle: tokio::task::JoinHandle<()>,
    /// Organization purge background task handle (leader-only).
    #[allow(dead_code)] // retained to keep background task alive
    pub org_purge_handle: tokio::task::JoinHandle<()>,
    /// Token maintenance background task handle (leader-only).
    #[allow(dead_code)] // retained to keep background task alive
    pub token_maintenance_handle: tokio::task::JoinHandle<()>,
    /// Post-erasure compaction background task handle (leader-only).
    #[allow(dead_code)] // retained to keep background task alive
    pub post_erasure_compaction_handle: tokio::task::JoinHandle<()>,
    /// Snapshot demotion background task handle (only active when warm tier is configured).
    #[allow(dead_code)] // retained to keep background task alive
    pub snapshot_demotion_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Bootstraps a new cluster, joins an existing one, or resumes from saved state.
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
    key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
) -> Result<BootstrappedNode, BootstrapError> {
    // Validate bootstrap configuration
    config.validate().map_err(|e| BootstrapError::Config { message: e.to_string() })?;

    // Resolve the effective node ID (manual or auto-generated Snowflake ID)
    let node_id =
        config.node_id(data_dir).map_err(|e| BootstrapError::NodeId { message: e.to_string() })?;

    std::fs::create_dir_all(data_dir).map_err(|e| BootstrapError::Database {
        message: format!("failed to create data dir: {e}"),
    })?;

    // Detect legacy flat layout (state.db in data_dir root). Per-region layout
    // places databases under global/ and regions/{name}/.
    let storage_manager = inferadb_ledger_raft::RegionStorageManager::new(data_dir.to_path_buf());
    storage_manager
        .detect_legacy_layout()
        .map_err(|e| BootstrapError::Database { message: e.to_string() })?;

    // Open GLOBAL region databases (state.db, blocks.db, events.db under global/)
    let region_storage = storage_manager
        .open_region(inferadb_ledger_types::Region::GLOBAL)
        .map_err(|e| BootstrapError::Database { message: e.to_string() })?;

    // Wrap raw databases in domain-specific types
    // StateLayer is internally thread-safe via MVCC — no external lock needed
    let state = Arc::new(StateLayer::new(region_storage.state_db().clone()));
    let block_archive = Arc::new(BlockArchive::new(region_storage.blocks_db().clone()));
    let events_db = region_storage.events_db().clone();

    // Create block announcements broadcast channel for real-time notifications.
    // Buffer size of 1024 allows for burst handling during high commit rates.
    let (block_announcements, _) = broadcast::channel::<BlockAnnouncement>(1024);

    // Create EventWriter for apply-phase event persistence.
    // The writer filters events by scope (system/organization) based on EventConfig
    // and is called during Raft apply_to_state_machine().
    let event_writer = EventWriter::new(Arc::clone(&events_db), config.events.clone());

    let log_path = storage_manager.raft_db_path(inferadb_ledger_types::Region::GLOBAL);
    let log_store = RaftLogStore::open(&log_path)
        .map_err(|e| BootstrapError::Storage { message: format!("failed to open log store: {e}") })?
        .with_state_layer(state.clone())
        .with_block_archive(block_archive.clone())
        .with_region_config(
            inferadb_ledger_types::Region::GLOBAL,
            inferadb_ledger_types::NodeId::new(node_id.to_string()),
        )
        .with_block_announcements(block_announcements.clone())
        .with_event_writer(event_writer);

    // Determine bootstrap behavior before log_store is consumed by Adaptor
    let is_initialized = log_store.is_initialized();

    // Get accessor and commitment buffer before log_store is consumed by Adaptor
    let applied_state_accessor = log_store.accessor();
    let commitment_buffer = log_store.commitment_buffer();

    let network = GrpcRaftNetworkFactory::with_trace_config(config.logging.otel.trace_raft_rpcs);
    let raft_config = if let Some(ref rc) = config.raft {
        openraft::Config {
            cluster_name: "ledger".to_string(),
            heartbeat_interval: rc.heartbeat_interval.as_millis() as u64,
            election_timeout_min: rc.election_timeout_min.as_millis() as u64,
            election_timeout_max: rc.election_timeout_max.as_millis() as u64,
            max_payload_entries: rc.max_entries_per_rpc,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(rc.snapshot_threshold),
            ..Default::default()
        }
    } else {
        openraft::Config {
            cluster_name: "ledger".to_string(),
            heartbeat_interval: 150,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
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
    .map_err(|e| BootstrapError::Raft { message: format!("failed to create raft: {e}") })?;

    let raft = Arc::new(raft);

    let block_archive_for_compactor = block_archive.clone();
    let block_archive_for_recovery = block_archive.clone();
    let snapshot_dir = storage_manager.snapshot_dir(inferadb_ledger_types::Region::GLOBAL);
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_dir.clone(), 5));
    let snapshot_manager_for_backup = snapshot_manager.clone();

    // Construct tiered snapshot manager wrapping the local (hot) snapshot directory.
    // When warm_url is configured, old snapshots are periodically demoted to object storage.
    let tiered_config = config.tiered_storage.clone();
    let demote_interval_secs = tiered_config.demote_interval_secs;
    let tiered_manager: Option<Arc<TieredSnapshotManager>> =
        if let Some(ref warm_url) = tiered_config.warm_url {
            let hot = Box::new(LocalBackend::new(snapshot_dir.clone(), tiered_config.hot_count));
            let warm = Box::new(
                ObjectStorageBackend::with_multipart_threshold(
                    warm_url,
                    tiered_config.multipart_threshold_bytes,
                )
                .map_err(|e| BootstrapError::Config {
                    message: format!("failed to create warm storage backend: {e}"),
                })?,
            );
            Some(Arc::new(TieredSnapshotManager::new_with_warm(hot, warm, tiered_config)))
        } else {
            None
        };

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
            BackupManager::new(backup_config).map(Arc::new).map_err(|e| BootstrapError::Config {
                message: format!("failed to create backup manager: {e}"),
            })
        })
        .transpose()?;

    // Create handler-phase event handle for gRPC service denial recording.
    // Shared across WriteService, AdminService, and SagaOrchestrator via Clone.
    let event_config = Arc::new(config.events.clone());
    let event_handle = EventHandle::new(Arc::clone(&events_db), event_config, node_id);
    let event_handle_for_saga = Some(event_handle.clone());

    // Create RaftManager and register the externally-created system region.
    // This bridges the existing manual bootstrap flow with the unified LedgerServer
    // that requires a manager. Phase 4 will replace this with start_system_region().
    let raft_manager_config =
        RaftManagerConfig::new(data_dir.to_path_buf(), node_id, config.region);
    let manager = Arc::new(RaftManager::new(raft_manager_config));
    manager
        .register_external_region(
            inferadb_ledger_types::Region::GLOBAL,
            raft.clone(),
            state.clone(),
            block_archive.clone(),
            applied_state_accessor.clone(),
            block_announcements,
            commitment_buffer,
        )
        .map_err(|e| BootstrapError::Raft {
            message: format!("failed to register system region: {e}"),
        })?;

    // Create TokenServiceConfig when a key_manager is provided (enables TokenService).
    let token_service_config =
        key_manager.as_ref().map(|km| inferadb_ledger_services::server::TokenServiceConfig {
            jwt_engine: Arc::new(inferadb_ledger_services::jwt::JwtEngine::new(config.jwt.clone())),
            jwt_config: config.jwt.clone(),
            key_manager: km.clone(),
        });

    // Create rate limiter and hot key detector.
    // Uses config-provided limits if available, otherwise hardcoded defaults.
    // Runtime reconfiguration updates the atomic fields via UpdateConfig RPC.
    let rate_limiter = if let Some(ref rl) = config.rate_limit {
        Arc::new(RateLimiter::new(
            rl.client_burst,
            rl.client_rate,
            rl.organization_burst,
            rl.organization_rate,
            rl.backpressure_threshold,
            config.region.as_str(),
        ))
    } else {
        Arc::new(RateLimiter::new(100, 50.0, 1000, 500.0, 100, config.region.as_str()))
    };
    let hot_key_config = inferadb_ledger_types::config::HotKeyConfig::default();
    let hot_key_detector = Arc::new(HotKeyDetector::new(&hot_key_config));

    // Extract proposal_timeout from Raft config (otherwise LedgerServer uses its 30s default).
    let proposal_timeout =
        config.raft.as_ref().map(|r| r.proposal_timeout).unwrap_or(Duration::from_secs(30));

    // Parse email blinding key from hex if configured.
    // When absent, onboarding RPCs (email verification, registration) return FAILED_PRECONDITION.
    let email_blinding_key = config
        .email_blinding_key
        .as_deref()
        .map(|hex_str| {
            let key_bytes = parse_hex_key(hex_str).map_err(|e| BootstrapError::Config {
                message: format!("email_blinding_key: {e}"),
            })?;
            Ok::<_, BootstrapError>(Arc::new(inferadb_ledger_types::EmailBlindingKey::new(
                key_bytes, 1,
            )))
        })
        .transpose()?;

    let server = LedgerServer::builder()
        .manager(manager.clone())
        .addr(config.listen_addr)
        .max_concurrent(config.max_concurrent)
        .timeout_secs(config.timeout_secs)
        .health_state(health_state.clone())
        .shutdown_rx(Some(shutdown_rx))
        .runtime_config(Some(runtime_config.clone()))
        .organization_rate_limiter(Some(rate_limiter))
        .hot_key_detector(Some(hot_key_detector))
        .data_dir(Some(data_dir.to_path_buf()))
        .events_db(Some((*events_db).clone()))
        .event_handle(Some(event_handle))
        .region(config.region)
        .token_service(token_service_config)
        .proposal_timeout(proposal_timeout)
        .health_check_config(config.health_check.clone())
        .max_read_forward_lag(config.max_read_forward_lag)
        .email_blinding_key(email_blinding_key)
        .build();
    // Wire backup support post-construction because bon's type-state builders
    // don't support conditional field setting (each setter changes the builder type).
    let server = if let Some(ref mgr) = backup_manager {
        server.with_backup(mgr.clone(), snapshot_manager_for_backup.clone())
    } else {
        server
    };

    // Retain the saga cell so we can fill it after the saga orchestrator starts.
    // The gRPC server starts before the orchestrator — handlers return UNAVAILABLE
    // until the cell is set.
    let saga_cell = server.saga_cell();

    // Start the gRPC server BEFORE coordination so peers can reach us.
    // Services gracefully degrade on uninitialized Raft: WriteService returns
    // UNAVAILABLE, ReadService works, HealthService reports Degraded.
    let server_addr = config.listen_addr;
    let server_handle =
        tokio::spawn(async move { server.serve().await.map_err(|e| e.to_string()) });

    // Wait for TCP listener to bind before proceeding to coordination.
    let tcp_start = Instant::now();
    let mut tcp_ready = false;
    while tcp_start.elapsed() < Duration::from_secs(5) {
        if server_handle.is_finished() {
            // Server exited early — likely a bind failure
            let result =
                server_handle.await.unwrap_or_else(|e| Err(format!("server task panicked: {e}")));
            return Err(BootstrapError::Server {
                message: result.err().unwrap_or_else(|| "server exited unexpectedly".to_string()),
            });
        }
        if tokio::net::TcpStream::connect(server_addr).await.is_ok() {
            tcp_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if !tcp_ready {
        server_handle.abort();
        return Err(BootstrapError::Server {
            message: format!("server did not accept TCP connections on {server_addr} within 5s"),
        });
    }

    // Determine whether to bootstrap based on existing state and bootstrap_expect.
    // The gRPC server is already running, so peers can reach us via GetNodeInfo.
    let coordination_result: Result<(), BootstrapError> = async {
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
                .map_err(|e| BootstrapError::Timeout { message: e.to_string() })?;

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
        Ok(())
    }
    .await;

    // If coordination failed, abort the server task before returning the error.
    if let Err(e) = coordination_result {
        server_handle.abort();
        return Err(e);
    }

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
    // Circuit breaker with bounded retries
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
    // Background polling of voters for fresh state
    let learner_refresh_handle = LearnerRefreshJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .applied_state(applied_state_accessor.clone())
        .watchdog_handle(watchdog.map(|w| w.register("learner_refresh", 5)))
        .build()
        .start();
    tracing::info!("Started learner refresh job");

    // Start resource saturation metrics collector
    let snapshot_dir_for_metrics =
        storage_manager.snapshot_dir(inferadb_ledger_types::Region::GLOBAL);
    let resource_metrics_handle = ResourceMetricsCollector::builder()
        .state(state.clone())
        .data_dir(data_dir.to_path_buf())
        .snapshot_dir(snapshot_dir_for_metrics)
        .region(config.region.as_str())
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

    // Start events garbage collector for TTL-based event expiry.
    // Runs on ALL nodes (not leader-only) because events.db is node-local.
    let events_gc_handle = if config.events.enabled {
        let gc = EventsGarbageCollector::builder()
            .events_db(events_db)
            .config(config.events.clone())
            .watchdog_handle(watchdog.map(|w| w.register("events_gc", 300)))
            .build();
        let handle = gc.start();
        tracing::info!("Started events garbage collector");
        Some(handle)
    } else {
        None
    };

    // Start saga orchestrator for cross-organization operations (leader-only)
    let (saga_handle, saga_orchestrator_handle) = SagaOrchestrator::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .state(state.clone())
        .event_handle(event_handle_for_saga)
        .interval(Duration::from_secs(config.saga.poll_interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("saga_orchestrator", 60)))
        .manager(Some(manager.clone()))
        .key_manager(key_manager)
        .build()
        .start();
    tracing::info!("Started saga orchestrator");
    // Fill the saga cell so service handlers can submit sagas.
    let _ = saga_cell.set(saga_orchestrator_handle.clone());

    // Start orphan cleanup job for removing stale membership records (leader-only)
    let orphan_cleanup_handle = OrphanCleanupJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .state(state.clone())
        .applied_state(applied_state_accessor)
        .interval(Duration::from_secs(config.cleanup.interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("orphan_cleanup", 7200)))
        .build()
        .start();
    tracing::info!("Started orphan cleanup job");

    // Start integrity scrubber for background page checksum verification (all nodes)
    let integrity_scrub_handle = IntegrityScrubberJob::builder()
        .state(state.clone())
        .interval(Duration::from_secs(config.integrity.scrub_interval_secs))
        .pages_per_cycle_percent(config.integrity.pages_per_cycle_percent)
        .watchdog_handle(watchdog.map(|w| w.register("integrity_scrub", 7200)))
        .build()
        .start();
    tracing::info!("Started integrity scrubber");

    // Start organization purge job for removing soft-deleted organizations
    // after their retention cooldown expires (leader-only)
    let org_purge_handle = OrganizationPurgeJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .state(state.clone())
        .manager(Some(manager.clone()))
        .watchdog_handle(watchdog.map(|w| w.register("org_purge", 7200)))
        .build()
        .start();
    tracing::info!("Started organization purge job");

    // Start token maintenance job for expired token cleanup and signing key lifecycle
    let token_maintenance_handle = TokenMaintenanceJob::builder()
        .raft(raft.clone())
        .node_id(node_id)
        .state(state.clone())
        .interval(Duration::from_secs(config.token_maintenance_interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("token_maintenance", 600)))
        .build()
        .start();
    tracing::info!("Started token maintenance job");

    // Start post-erasure compaction job to enforce maximum Raft log retention.
    // Triggers proactive snapshots on all Raft groups (GLOBAL + regional) when
    // time since last snapshot exceeds the configured threshold, ensuring
    // encrypted PII entries are compacted within the retention window.
    let post_erasure_compaction_handle = PostErasureCompactionJob::builder()
        .node_id(node_id)
        .raft(raft.clone())
        .manager(Some(manager.clone()))
        .watchdog_handle(watchdog.map(|w| w.register("post_erasure_compaction", 600)))
        .build()
        .start();
    tracing::info!("Started post-erasure compaction job");

    // Start snapshot demotion task if warm tier is configured.
    // Periodically moves old snapshots from local SSD to object storage.
    // S3 unavailability degrades gracefully: logs the error and retries next cycle.
    let snapshot_demotion_handle = tiered_manager.map(|mgr| {
        let interval = Duration::from_secs(demote_interval_secs);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                match mgr.demote_to_warm() {
                    Ok(0) => {},
                    Ok(count) => {
                        tracing::info!(demoted = count, "Demoted snapshots from hot to warm tier");
                    },
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Snapshot demotion failed, hot tier continues operating normally"
                        );
                    },
                }
            }
        });
        tracing::info!(interval_secs = demote_interval_secs, "Started snapshot demotion task");
        handle
    });

    Ok(BootstrappedNode {
        raft,
        state,
        server_handle,
        gc_handle,
        compactor_handle,
        recovery_handle,
        learner_refresh_handle,
        resource_metrics_handle,
        backup_handle,
        events_gc_handle,
        runtime_config,
        saga_handle,
        saga_orchestrator_handle,
        orphan_cleanup_handle,
        integrity_scrub_handle,
        org_purge_handle,
        token_maintenance_handle,
        post_erasure_compaction_handle,
        snapshot_demotion_handle,
    })
}

/// Bootstraps a new single-node cluster with this node as the initial member.
///
/// Additional nodes can join dynamically via discovery.
async fn bootstrap_cluster(
    raft: &Raft<LedgerTypeConfig>,
    node_id: u64,
    listen_addr: &SocketAddr,
) -> Result<(), BootstrapError> {
    let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
    members.insert(node_id, BasicNode { addr: listen_addr.to_string() });

    raft.initialize(members).await.map_err(|e| BootstrapError::Initialize {
        message: format!("failed to initialize: {e}"),
    })?;

    tracing::info!(node_id = node_id, "Bootstrapped new single-node cluster");

    Ok(())
}

/// Bootstraps a new cluster with multiple initial members.
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
    raft.initialize(members).await.map_err(|e| BootstrapError::Initialize {
        message: format!("failed to initialize: {e}"),
    })?;

    tracing::info!(members = ?member_ids, "Bootstrapped new multi-node cluster");

    Ok(())
}

/// Waits for this node to be added to the cluster by another node.
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
            return Err(BootstrapError::Timeout {
                message: format!("timed out waiting to join cluster after {}s", timeout.as_secs()),
            });
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

/// Parses a hex-encoded 32-byte key string into a fixed-size byte array.
///
/// Returns an error if the hex string is invalid or does not decode to exactly 32 bytes.
fn parse_hex_key(hex_str: &str) -> Result<[u8; 32], String> {
    let hex_str = hex_str.trim();
    if hex_str.len() != 64 {
        return Err(format!("expected 64 hex characters (32 bytes), got {}", hex_str.len()));
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex_str[i * 2..i * 2 + 2], 16)
            .map_err(|e| format!("invalid hex at position {}: {e}", i * 2))?;
    }
    Ok(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
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
        let result = bootstrap_node(&config, &data_dir, health, rx, None).await;
        assert!(result.is_ok(), "bootstrap should succeed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_bootstrap_creates_per_region_databases() {
        let temp_dir = tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50052, data_dir.clone());

        let health = inferadb_ledger_raft::HealthState::new();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let node = bootstrap_node(&config, &data_dir, health, rx, None)
            .await
            .expect("bootstrap should succeed");

        // Per-region databases should be under global/ directory
        let global_dir = data_dir.join("global");
        assert!(global_dir.join("events.db").exists(), "events.db should be in global/");
        assert!(global_dir.join("state.db").exists(), "state.db should be in global/");
        assert!(global_dir.join("blocks.db").exists(), "blocks.db should be in global/");
        assert!(global_dir.join("raft.db").exists(), "raft.db should be in global/");

        // No databases in root (flat layout is legacy)
        assert!(!data_dir.join("state.db").exists(), "state.db should not be in root");
        assert!(!data_dir.join("events.db").exists(), "events.db should not be in root");

        // Events GC should be running (default config has events.enabled = true)
        assert!(
            node.events_gc_handle.is_some(),
            "events GC should be started when events are enabled"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_events_disabled_skips_gc() {
        let temp_dir = tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let mut config = Config::for_test(1, 50053, data_dir.clone());
        config.events.enabled = false;

        let health = inferadb_ledger_raft::HealthState::new();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let node = bootstrap_node(&config, &data_dir, health, rx, None)
            .await
            .expect("bootstrap should succeed");

        // events.db is still created (needed for snapshot restore), but GC is not started
        assert!(
            data_dir.join("global").join("events.db").exists(),
            "events.db should still be created even when disabled"
        );
        assert!(
            node.events_gc_handle.is_none(),
            "events GC should not be started when events are disabled"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_rejects_legacy_flat_layout() {
        let temp_dir = tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();

        // Simulate legacy flat layout
        std::fs::write(data_dir.join("state.db"), b"legacy").expect("create legacy file");

        let config = Config::for_test(1, 50054, data_dir.clone());
        let health = inferadb_ledger_raft::HealthState::new();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let result = bootstrap_node(&config, &data_dir, health, rx, None).await;

        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("bootstrap should fail with legacy layout"),
        };
        assert!(err.contains("Legacy flat layout"), "error should mention legacy layout: {err}");
    }
}
