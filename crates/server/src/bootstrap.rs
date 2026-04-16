//! Two-phase cluster bootstrap: start and init.
//!
//! Provides the `bootstrap_node` function which handles two distinct paths:
//!
//! - **Restart** (cluster_id file exists): loads cluster state, starts all background jobs, and
//!   marks the node as ready.
//! - **Fresh node** (no cluster_id file): starts the system region and gRPC server but does NOT
//!   start background jobs or mark ready. The node waits for an `InitCluster` RPC (or shutdown
//!   signal) to complete initialization.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_raft::{
    AutoRecoveryJob, BackupJob, BackupManager, BlockCompactor, ConsensusHandle,
    EventsGarbageCollector, HotKeyDetector, IntegrityScrubberJob, InviteMaintenanceJob,
    LearnerRefreshJob, OrganizationPurgeJob, OrphanCleanupJob, PostErasureCompactionJob,
    RaftManager, RaftManagerConfig, RateLimiter, RegionConfig, RegionStorageManager,
    ResourceMetricsCollector, RuntimeConfigHandle, SagaOrchestrator, SagaOrchestratorHandle,
    TokenMaintenanceJob, TtlGarbageCollector, event_writer::EventHandle,
    log_storage::AppliedStateAccessor,
};
use inferadb_ledger_services::LedgerServer;
use inferadb_ledger_state::{
    BlockArchive, EventsDatabase, LocalBackend, ObjectStorageBackend, SnapshotManager, StateLayer,
    TieredSnapshotManager,
};
use inferadb_ledger_store::FileBackend;

use crate::config::Config;

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
/// - `state`: Maintains Arc reference count for shared state
/// - Handle fields: Keep background tasks alive and allow graceful shutdown
pub struct BootstrappedNode {
    /// Consensus handle for background jobs and graceful shutdown.
    pub handle: Arc<ConsensusHandle>,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    #[allow(dead_code)] // retained to maintain Arc reference count for shared state layer
    pub state: Arc<StateLayer<FileBackend>>,
    /// Multi-Raft manager for region routing. Exposed so callers (e.g., test
    /// infrastructure) can start additional data regions post-bootstrap.
    #[allow(dead_code)] // used by test infrastructure
    pub manager: Arc<RaftManager>,
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
    /// Invite maintenance background task handle (leader-only).
    #[allow(dead_code)] // retained to keep background task alive
    pub invite_maintenance_handle: tokio::task::JoinHandle<()>,
    /// Post-erasure compaction background task handle (leader-only).
    #[allow(dead_code)] // retained to keep background task alive
    pub post_erasure_compaction_handle: tokio::task::JoinHandle<()>,
    /// Snapshot demotion background task handle (only active when warm tier is configured).
    #[allow(dead_code)] // retained to keep background task alive
    pub snapshot_demotion_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Bootstraps the node using the two-phase start+init pattern.
///
/// The function detects whether this node has been initialized by checking for
/// a `cluster_id` file in the data directory:
///
/// - **Restart path** (cluster_id exists): Loads the cluster ID, starts the system region with
///   persisted membership, builds the full gRPC server, starts all background jobs, and marks the
///   node as ready.
///
/// - **Fresh node path** (no cluster_id): Starts the system region and gRPC server but leaves the
///   node in an uninitialized state. Only `GetNodeInfo`, `InitCluster`, and health probes are
///   functional. All other RPCs return `UNAVAILABLE`. If `--join` seeds are configured, a
///   background task polls them via `GetNodeInfo`. The node waits for either an `InitCluster` RPC
///   or the shutdown signal.
///
/// # Errors
///
/// Returns [`BootstrapError`] if:
/// - Configuration validation fails
/// - Node ID resolution fails
/// - Database creation or opening fails
/// - Raft instance creation fails
/// - The gRPC server fails to bind
pub async fn bootstrap_node(
    config: &Config,
    data_dir: &std::path::Path,
    health_state: inferadb_ledger_raft::HealthState,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
) -> Result<BootstrappedNode, BootstrapError> {
    // Validate configuration.
    config.validate().map_err(|e| BootstrapError::Config { message: e.to_string() })?;

    // Resolve the effective node ID (manual or auto-generated Snowflake ID).
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

    // Create the RaftManager with timing parameters from config.
    let (hb_ms, el_min_ms, el_max_ms) = if let Some(ref rc) = config.raft {
        (
            rc.heartbeat_interval.as_millis() as u64,
            rc.election_timeout_min.as_millis() as u64,
            rc.election_timeout_max.as_millis() as u64,
        )
    } else {
        (150, 300, 600) // RaftManagerConfig defaults
    };
    let raft_manager_config = RaftManagerConfig::builder()
        .data_dir(data_dir.to_path_buf())
        .node_id(node_id)
        .local_region(config.region)
        .heartbeat_interval_ms(hb_ms)
        .election_timeout_min_ms(el_min_ms)
        .election_timeout_max_ms(el_max_ms)
        .build();
    // Single shared per-node connection registry. All per-region consensus
    // transports created via `start_region` clone this Arc so that they
    // reuse one channel per peer instead of opening a connection per region.
    let registry = Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new());
    let manager = Arc::new(RaftManager::new(raft_manager_config, registry));

    // Check whether this node has been initialized (cluster_id file exists).
    let cluster_id = crate::cluster_id::load_cluster_id(data_dir).map_err(|e| {
        BootstrapError::Database { message: format!("failed to load cluster_id: {e}") }
    })?;

    // Start the GLOBAL system region. On restart, persisted membership loads
    // automatically. On fresh nodes, the region starts with this node as the
    // sole member — the InitCluster handler will handle membership changes.
    let region_config = RegionConfig::builder()
        .region(inferadb_ledger_types::Region::GLOBAL)
        .initial_members(vec![(node_id, config.advertise_addr())])
        .events_config(config.events.clone())
        .build();
    let system_region = manager.start_system_region(region_config).await.map_err(|e| {
        BootstrapError::Raft { message: format!("failed to start system region: {e}") }
    })?;

    // Spawn region creation handler — processes CreateDataRegion entries applied
    // on the GLOBAL log store and starts local data regions via the RaftManager.

    // Health flag for the region handler task. If the task exits (channel
    // closed or unrecoverable panic), this flag is cleared. The health service
    // checks it to report NOT_SERVING when the handler is down.
    let region_handler_healthy = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let region_handler_healthy_clone = region_handler_healthy.clone();

    if let Some(mut region_rx) = system_region.take_region_creation_rx() {
        let mgr = manager.clone();
        let bootstrap_events_config = config.events.clone();
        let healthy_flag = region_handler_healthy_clone;
        tokio::spawn(async move {
            // Each message is processed in a child task for panic isolation.
            // If processing one message panics, tokio catches it via the
            // JoinHandle and the loop continues with the next message.
            while let Some((region, initial_members)) = region_rx.recv().await {
                let mgr_clone = mgr.clone();
                let events_clone = bootstrap_events_config.clone();
                let result = tokio::spawn(async move {
                    process_region_event(&mgr_clone, &events_clone, region, initial_members).await;
                })
                .await;

                if let Err(join_err) = result
                    && join_err.is_panic()
                {
                    tracing::error!(
                        "Region handler panicked processing event — continuing with next message. \
                         Panic: {join_err:?}"
                    );
                }
            }
            // Channel closed — mark handler as unhealthy so health service detects it.
            healthy_flag.store(false, std::sync::atomic::Ordering::Release);
            tracing::warn!("Region handler channel closed — handler task exiting");
        });
    }

    let _ = region_handler_healthy;

    // Shared peer liveness map — updated by the Raft service on every incoming
    // consensus message, read by the admin service's CheckPeerLiveness RPC and
    // the GLOBAL leader's quorum-based dead node detection task.
    let peer_liveness: Arc<
        parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>,
    > = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));

    // Background DR/liveness tasks require a fully initialized node with cluster
    // state. On fresh nodes these are deferred until after InitCluster completes
    // (the fresh path spawns them via `start_background_jobs`).
    if cluster_id.is_some() {
        // === Restart: re-discover peer addresses from --join seeds ===
        //
        // On restart, the PeerAddressMap is empty (RegisterPeerAddress entries
        // were applied in a prior run but the map is in-memory only). Seed the
        // map from --join addresses by querying GetNodeInfo from each seed to
        // learn its numeric node_id, then insert into the map. This enables
        // `reconcile_transport_channels` to register transport channels promptly
        // instead of waiting for peers to reconnect to us.
        if let Some(ref seeds) = config.join
            && !seeds.is_empty()
        {
            let addrs = crate::discovery::parse_seed_addresses(seeds);
            let peer_addresses = manager.peer_addresses().clone();
            let my_node_id = node_id;
            let ct = system_region.consensus_transport().cloned();
            tokio::spawn(async move {
                for addr in &addrs {
                    match crate::discovery::discover_node_info(*addr, Duration::from_secs(5)).await
                    {
                        Some(info) if info.node_id != my_node_id && info.node_id != 0 => {
                            let addr_str = addr.to_string();
                            tracing::info!(
                                node_id = info.node_id,
                                addr = %addr_str,
                                "Restart: discovered peer from seed"
                            );
                            peer_addresses.insert(info.node_id, addr_str.clone());

                            // Register in the consensus transport so heartbeats
                            // can flow immediately rather than waiting for the
                            // 1s reconcile_transport_channels cycle.
                            if let Some(ref transport) = ct
                                && let Err(e) =
                                    transport.set_peer_via_registry(info.node_id, &addr_str).await
                            {
                                tracing::warn!(
                                    node_id = info.node_id,
                                    error = %e,
                                    "Failed to register seed peer on restart"
                                );
                            }
                        },
                        _ => {
                            tracing::debug!(
                                seed = %addr,
                                "Restart: seed not reachable or returned no node_id"
                            );
                        },
                    }
                }
            });
        }

        // === Restart: re-start existing data regions from disk ===
        //
        // On restart, only the GLOBAL region is started eagerly. Data regions
        // whose directories exist (created by prior CreateDataRegion entries)
        // must be re-opened. The consensus Shard for each region will use its
        // persisted membership (Fix 1) so it won't spuriously self-elect.
        {
            let discovered = storage_manager.discover_existing_regions();
            if !discovered.is_empty() {
                tracing::info!(
                    count = discovered.len(),
                    "Restart: re-starting discovered data regions"
                );
            }
            for region in discovered {
                let region_config = inferadb_ledger_raft::RegionConfig::builder()
                    .region(region)
                    .initial_members(vec![(node_id, config.advertise_addr())])
                    .events_config(config.events.clone())
                    .build();
                match manager.start_data_region(region_config).await {
                    Ok(_) => {
                        tracing::info!(region = region.as_str(), "Restart: data region started");
                    },
                    Err(e) => {
                        tracing::error!(
                            region = region.as_str(),
                            error = %e,
                            "Restart: failed to start data region"
                        );
                    },
                }
            }
        }

        let bg_initial_delay = config.background_jobs.initial_delay();
        let bg_stagger_delay = config.background_jobs.stagger_delay();

        // Spawn DR checker task (1s cadence) — reactive repair for dead/decommissioning voters.
        {
            let mgr = manager.clone();
            tokio::spawn(async move {
                let mut learner_first_seen: crate::dr_scheduler::LearnerFirstSeen =
                    std::collections::HashMap::new();
                tokio::time::sleep(bg_initial_delay).await;
                loop {
                    reconcile_transport_channels(&mgr).await;
                    crate::dr_scheduler::run_checker_cycle(&mgr, &mut learner_first_seen).await;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            });
        }

        // Spawn DR scheduler task (5s cadence) — proactive growth (learner add/promote).
        {
            let mgr = manager.clone();
            tokio::spawn(async move {
                tokio::time::sleep(bg_stagger_delay).await;
                loop {
                    crate::dr_scheduler::run_scheduler_cycle(&mgr).await;
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        }

        // Spawn DR membership reporter — DR leaders propose RegionMembershipReport
        // to GLOBAL so the drain monitor can track decommission progress.
        {
            let mgr = manager.clone();
            tokio::spawn(async move {
                tokio::time::sleep(bg_stagger_delay).await;
                loop {
                    if let Ok(global) = mgr.system_region() {
                        for region in mgr.list_regions() {
                            if region == inferadb_ledger_types::Region::GLOBAL {
                                continue;
                            }
                            let Ok(group) = mgr.get_region_group(region) else { continue };
                            if !group.handle().is_leader() {
                                continue;
                            }
                            let state = group.handle().shard_state();
                            let report = inferadb_ledger_raft::types::LedgerRequest::System(
                            inferadb_ledger_raft::types::SystemRequest::RegionMembershipReport {
                                region,
                                voters: state.voters.iter().map(|n| n.0).collect(),
                                learners: state.learners.iter().map(|n| n.0).collect(),
                                conf_epoch: state.conf_epoch,
                            },
                        );
                            let _ = global
                                .handle()
                                .propose_and_wait(
                                    inferadb_ledger_raft::types::RaftPayload::system(report),
                                    std::time::Duration::from_secs(5),
                                )
                                .await;
                        }
                    }
                    // Spec says 10s for the periodic backstop, but faster cadence (2s)
                    // ensures the drain monitor sees fresh reports promptly when the
                    // event-driven reporter can't reach GLOBAL (DR leader ≠ GLOBAL leader).
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            });
        }

        // Spawn drain monitor — checks decommissioning/dead nodes and removes
        // them from GLOBAL once all DR replicas are drained.
        {
            let mgr = manager.clone();
            tokio::spawn(async move {
                tokio::time::sleep(bg_stagger_delay).await;
                loop {
                    if let Ok(global) = mgr.system_region()
                        && global.handle().is_leader()
                        && let Some(reader) = mgr.system_state_reader()
                    {
                        let statuses = reader.all_node_statuses();
                        if !statuses.is_empty() {
                            tracing::debug!(
                                count = statuses.len(),
                                "Drain monitor: scanning node statuses"
                            );
                        }
                        for &(node_id, status) in &statuses {
                            if status != inferadb_ledger_raft::types::NodeStatus::Decommissioning
                                && status != inferadb_ledger_raft::types::NodeStatus::Dead
                            {
                                continue;
                            }

                            // Check whether this node still has DR replicas.
                            // Deviation from spec: the spec says "check GLOBAL-stored DR membership
                            // reports — NOT local list_regions()." However, GLOBAL-only reports lag
                            // significantly when the DR leader ≠ GLOBAL leader (the DR leader can't
                            // propose reports to GLOBAL as a follower). The hybrid approach — local
                            // authoritative state for DRs where we're the leader, GLOBAL reports
                            // for remote DRs — provides faster drain convergence while maintaining
                            // correctness for regulated regions this node doesn't host.
                            let mut has_replicas = false;
                            let target = inferadb_ledger_consensus::types::NodeId(node_id);
                            for region in mgr.list_regions() {
                                if region == inferadb_ledger_types::Region::GLOBAL {
                                    continue;
                                }
                                if let Ok(group) = mgr.get_region_group(region)
                                    && group.handle().is_leader()
                                {
                                    let s = group.handle().shard_state();
                                    if s.voters.contains(&target) || s.learners.contains(&target) {
                                        has_replicas = true;
                                        break;
                                    }
                                }
                            }
                            // For DRs where we're not the leader: check GLOBAL reports.
                            if !has_replicas {
                                let memberships = reader.region_memberships();
                                has_replicas = memberships
                                    .iter()
                                    .any(|(_, members)| members.contains(&node_id));
                            }

                            tracing::debug!(
                                node_id,
                                has_replicas,
                                ?status,
                                "Drain monitor: checking node"
                            );
                            if !has_replicas {
                                tracing::info!(
                                    node_id,
                                    ?status,
                                    "Drain complete — removing from GLOBAL"
                                );
                                // Remove from GLOBAL Raft membership. Only one
                                // membership change at a time — skip this cycle on
                                // conflict (the next cycle will retry).
                                match global.handle().remove_node(node_id).await {
                                    Ok(()) => {
                                        // Mark as Removed in the node status store.
                                        let set_removed =
                                        inferadb_ledger_raft::types::LedgerRequest::System(
                                            inferadb_ledger_raft::types::SystemRequest::SetNodeStatus {
                                                node_id,
                                                status:
                                                    inferadb_ledger_raft::types::NodeStatus::Removed,
                                            },
                                        );
                                        let _ = global
                                            .handle()
                                            .propose_and_wait(
                                                inferadb_ledger_raft::types::RaftPayload::system(
                                                    set_removed,
                                                ),
                                                std::time::Duration::from_secs(5),
                                            )
                                            .await;
                                        // Continue to process remaining drainable nodes
                                        // in the same cycle (each remove_node awaits commit).
                                    },
                                    Err(e) => {
                                        let msg = e.to_string();
                                        if !msg.contains("no-op") {
                                            tracing::debug!(
                                                node_id,
                                                error = %e,
                                                "Drain monitor: GLOBAL remove_node deferred"
                                            );
                                        }
                                        // Conflict or already removed — skip this node.
                                    },
                                }
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            });
        }

        // Spawn dead node detection — GLOBAL leader checks peer liveness with
        // quorum confirmation before marking nodes Dead.
        {
            let mgr = manager.clone();
            let liveness = peer_liveness.clone();
            let liveness_config = inferadb_ledger_raft::LivenessConfig::default();
            tokio::spawn(async move {
                tokio::time::sleep(liveness_config.liveness_check_interval).await;
                loop {
                    if let Ok(global) = mgr.system_region()
                        && global.handle().is_leader()
                    {
                        check_peer_liveness_quorum(&mgr, &liveness, &global, &liveness_config)
                            .await;
                    }
                    tokio::time::sleep(liveness_config.liveness_check_interval).await;
                }
            });
        }
    } // end: cluster_id.is_some() guard for background tasks

    // Extract components from the system region.
    let handle = system_region.handle().clone();
    let state = system_region.state().clone();
    let block_archive = system_region.block_archive().clone();
    let applied_state_accessor = system_region.applied_state().clone();
    let consensus_transport = system_region.consensus_transport().cloned();
    let events_db = system_region.events_db().cloned().ok_or_else(|| BootstrapError::Database {
        message: "system region missing events_db".to_string(),
    })?;

    let snapshot_dir = storage_manager.snapshot_dir(inferadb_ledger_types::Region::GLOBAL);
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_dir.clone(), 5));
    let snapshot_manager_for_backup = snapshot_manager.clone();

    // Construct tiered snapshot manager wrapping the local (hot) snapshot directory.
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
    let runtime_config = RuntimeConfigHandle::default();

    // Create backup manager if configured.
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
    let event_config = Arc::new(config.events.clone());
    let event_handle = EventHandle::new(Arc::clone(&events_db), event_config, node_id);
    let event_handle_for_saga = Some(event_handle.clone());

    // Create TokenServiceConfig when a key_manager is provided (enables TokenService).
    let token_service_config =
        key_manager.as_ref().map(|km| inferadb_ledger_services::server::TokenServiceConfig {
            jwt_engine: Arc::new(inferadb_ledger_services::jwt::JwtEngine::new(config.jwt.clone())),
            jwt_config: config.jwt.clone(),
            key_manager: km.clone(),
        });

    // Create rate limiter and hot key detector.
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

    // Extract proposal_timeout from Raft config.
    let proposal_timeout =
        config.raft.as_ref().map(|r| r.proposal_timeout).unwrap_or(Duration::from_secs(30));

    // Parse email blinding key from hex if configured.
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

    // Seed the peer address map with the local node's advertise address.
    let peer_addresses = manager.peer_addresses().clone();
    peer_addresses.insert(node_id, config.advertise_addr());

    // Create init sender for fresh nodes. On restart (cluster_id exists), this
    // is None — the AdminService init_cluster handler returns AlreadyInitialized.
    // The watch channel receiver stays in bootstrap for the fresh path; the
    // sender is shared with AdminService (InitCluster RPC) and seed polling.
    let (init_tx, init_rx) = if cluster_id.is_none() {
        let (tx, rx) = tokio::sync::watch::channel::<Option<u64>>(None);
        (Some(Arc::new(tx)), Some(rx))
    } else {
        (None, None)
    };

    let server = LedgerServer::builder()
        .manager(manager.clone())
        .addr(config.listen_addr)
        .advertise_addr(config.advertise.clone())
        .max_concurrent(config.max_concurrent)
        .timeout_secs(config.timeout_secs)
        .health_state(health_state.clone())
        .shutdown_rx(Some(shutdown_rx.clone()))
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
        .enable_grpc_reflection(config.enable_grpc_reflection)
        .email_blinding_key(email_blinding_key)
        .peer_addresses(Some(peer_addresses))
        .consensus_transport(consensus_transport.clone())
        .init_sender(init_tx.clone())
        .cluster_id(cluster_id)
        .peer_liveness(Some(peer_liveness.clone()))
        .build();

    // Wire backup support post-construction.
    let server = if let Some(ref mgr) = backup_manager {
        server.with_backup(mgr.clone(), snapshot_manager_for_backup.clone())
    } else {
        server
    };

    // Retain the saga cell so we can fill it after the saga orchestrator starts.
    let saga_cell = server.saga_cell();

    // Start the gRPC server.
    let server_addr = config.listen_addr;
    let server_handle = tokio::spawn(async move {
        match server.serve().await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!(error = %e, "gRPC server exited with error");
                Err(e.to_string())
            },
        }
    });

    // Wait for TCP listener to bind before proceeding.
    wait_for_tcp_ready(&server_handle, server_addr).await?;

    if cluster_id.is_some() {
        // === Restart path ===
        // Node was previously initialized. Start all background jobs and mark ready.
        tracing::info!(
            node_id,
            cluster_id = cluster_id.unwrap_or(0),
            "Restarting initialized node"
        );

        let jobs = start_background_jobs(StartJobsInput {
            config,
            data_dir,
            node_id,
            health_state: &health_state,
            handle: &handle,
            state: &state,
            block_archive: &block_archive,
            applied_state_accessor: &applied_state_accessor,
            events_db: &events_db,
            snapshot_manager,
            snapshot_manager_for_backup,
            backup_manager,
            tiered_manager,
            demote_interval_secs,
            event_handle_for_saga,
            saga_cell,
            manager: &manager,
            key_manager,
            storage_manager: &storage_manager,
        })?;

        health_state.mark_ready();
        tracing::info!(node_id, "Node is ready");

        Ok(BootstrappedNode {
            handle,
            state,
            manager,
            server_handle,
            runtime_config,
            gc_handle: jobs.gc_handle,
            compactor_handle: jobs.compactor_handle,
            recovery_handle: jobs.recovery_handle,
            learner_refresh_handle: jobs.learner_refresh_handle,
            resource_metrics_handle: jobs.resource_metrics_handle,
            backup_handle: jobs.backup_handle,
            events_gc_handle: jobs.events_gc_handle,
            saga_handle: jobs.saga_handle,
            saga_orchestrator_handle: jobs.saga_orchestrator_handle,
            orphan_cleanup_handle: jobs.orphan_cleanup_handle,
            integrity_scrub_handle: jobs.integrity_scrub_handle,
            org_purge_handle: jobs.org_purge_handle,
            token_maintenance_handle: jobs.token_maintenance_handle,
            invite_maintenance_handle: jobs.invite_maintenance_handle,
            post_erasure_compaction_handle: jobs.post_erasure_compaction_handle,
            snapshot_demotion_handle: jobs.snapshot_demotion_handle,
        })
    } else {
        // === Fresh node path ===
        // Start the gRPC server but leave the node uninitialized. The node waits
        // for an `InitCluster` RPC (or seed discovery) to generate and persist a
        // cluster_id before starting background jobs.
        tracing::info!(node_id, "Node started in uninitialized state");

        // Extract the init channel created before the server was spawned.
        // The sender was wired into AdminService via LedgerServer; the receiver
        // stays here so we can wait for initialization.
        let (init_tx, mut init_rx) = match (init_tx, init_rx) {
            (Some(tx), Some(rx)) => (tx, rx),
            _ => {
                return Err(BootstrapError::Initialize {
                    message: "init channel not created for fresh node path".to_string(),
                });
            },
        };

        // If --join seeds are provided, spawn a background polling loop that
        // discovers an initialized cluster and joins it automatically.
        if let Some(ref seeds) = config.join
            && !seeds.is_empty()
        {
            let seeds_clone = seeds.clone();
            let signal = init_tx.clone();
            let my_addr = config.advertise_addr();
            let handle_clone = handle.clone();
            let consensus_transport_clone = consensus_transport.clone();
            let data_dir_clone = data_dir.to_path_buf();
            tokio::spawn(async move {
                seed_polling_loop(
                    seeds_clone,
                    signal,
                    node_id,
                    my_addr,
                    handle_clone,
                    consensus_transport_clone,
                    data_dir_clone,
                )
                .await;
            });
        }

        // Wait for initialization (via InitCluster RPC or seed discovery) or shutdown.
        let mut shutdown_wait = shutdown_rx.clone();
        tokio::select! {
            result = init_rx.wait_for(|v| v.is_some()) => {
                match result {
                    Ok(guard) => {
                        if let Some(cid) = *guard {
                            tracing::info!(node_id, cluster_id = cid, "Initialization complete");
                        }
                    }
                    Err(_) => {
                        // All senders dropped without initializing — treat as shutdown.
                        tracing::info!(node_id, "Init channel closed before initialization");
                        return Err(BootstrapError::Initialize {
                            message: "init channel closed before initialization".to_string(),
                        });
                    }
                }
            }
            _ = async { let _ = shutdown_wait.wait_for(|&v| v).await; } => {
                tracing::info!(node_id, "Shutdown received before initialization");
                return Err(BootstrapError::Initialize {
                    message: "shutdown received before initialization".to_string(),
                });
            }
        }

        // Now start background jobs (same as restart path).
        let jobs = start_background_jobs(StartJobsInput {
            config,
            data_dir,
            node_id,
            health_state: &health_state,
            handle: &handle,
            state: &state,
            block_archive: &block_archive,
            applied_state_accessor: &applied_state_accessor,
            events_db: &events_db,
            snapshot_manager,
            snapshot_manager_for_backup,
            backup_manager,
            tiered_manager,
            demote_interval_secs,
            event_handle_for_saga,
            saga_cell,
            manager: &manager,
            key_manager,
            storage_manager: &storage_manager,
        })?;

        health_state.mark_ready();
        tracing::info!(node_id, "Node is ready");

        Ok(BootstrappedNode {
            handle,
            state,
            manager,
            server_handle,
            runtime_config,
            gc_handle: jobs.gc_handle,
            compactor_handle: jobs.compactor_handle,
            recovery_handle: jobs.recovery_handle,
            learner_refresh_handle: jobs.learner_refresh_handle,
            resource_metrics_handle: jobs.resource_metrics_handle,
            backup_handle: jobs.backup_handle,
            events_gc_handle: jobs.events_gc_handle,
            saga_handle: jobs.saga_handle,
            saga_orchestrator_handle: jobs.saga_orchestrator_handle,
            orphan_cleanup_handle: jobs.orphan_cleanup_handle,
            integrity_scrub_handle: jobs.integrity_scrub_handle,
            org_purge_handle: jobs.org_purge_handle,
            token_maintenance_handle: jobs.token_maintenance_handle,
            invite_maintenance_handle: jobs.invite_maintenance_handle,
            post_erasure_compaction_handle: jobs.post_erasure_compaction_handle,
            snapshot_demotion_handle: jobs.snapshot_demotion_handle,
        })
    }
}

/// Polls seed nodes to discover an initialized cluster and join it.
///
/// Iterates over all seed addresses, calling `GetNodeInfo` on each. When a seed
/// with a non-zero `cluster_id` is found, sends a `JoinCluster` RPC to that seed,
/// persists the cluster ID locally, and signals bootstrap to proceed.
async fn seed_polling_loop(
    seeds: Vec<String>,
    init_tx: Arc<tokio::sync::watch::Sender<Option<u64>>>,
    my_node_id: u64,
    my_address: String,
    _handle: Arc<ConsensusHandle>,
    consensus_transport: Option<inferadb_ledger_raft::GrpcConsensusTransport>,
    data_dir: std::path::PathBuf,
) {
    use crate::discovery::parse_seed_addresses;

    let addrs = parse_seed_addresses(&seeds);
    if addrs.is_empty() {
        tracing::warn!("No valid seed addresses parsed from --join, seed polling disabled");
        return;
    }

    let rpc_timeout = Duration::from_secs(5);

    loop {
        for addr in &addrs {
            let info = match crate::discovery::discover_node_info(*addr, rpc_timeout).await {
                Some(info) if info.cluster_id != 0 => info,
                _ => continue,
            };

            tracing::info!(
                seed = %addr,
                cluster_id = info.cluster_id,
                seed_node_id = info.node_id,
                "Discovered initialized cluster, joining"
            );

            // Register a transport channel for the seed so Raft replication can reach it.
            if let Some(ref transport) = consensus_transport
                && let Err(e) =
                    transport.set_peer_via_registry(info.node_id, &addr.to_string()).await
            {
                tracing::warn!(
                    seed_node_id = info.node_id,
                    seed = %addr,
                    error = %e,
                    "Failed to register seed peer via registry"
                );
            }

            // Send JoinCluster RPC to the seed node.
            let endpoint = format!("http://{addr}");
            let channel = match tonic::transport::Channel::from_shared(endpoint) {
                Ok(ep) => match ep.connect().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        tracing::warn!(seed = %addr, error = %e, "Failed to connect for JoinCluster");
                        continue;
                    },
                },
                Err(e) => {
                    tracing::warn!(seed = %addr, error = %e, "Invalid seed endpoint");
                    continue;
                },
            };

            let mut client =
                inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::new(
                    channel,
                );
            let join_result = client
                .join_cluster(inferadb_ledger_proto::proto::JoinClusterRequest {
                    node_id: my_node_id,
                    address: my_address.clone(),
                })
                .await;

            match join_result {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    if inner.success {
                        tracing::info!(cluster_id = info.cluster_id, "Successfully joined cluster");
                    } else if inner.message.contains("already a voter") {
                        tracing::info!(
                            cluster_id = info.cluster_id,
                            "Already a member of the cluster"
                        );
                    } else {
                        tracing::warn!(
                            message = %inner.message,
                            "JoinCluster returned non-success, retrying"
                        );
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!(seed = %addr, error = %e, "JoinCluster RPC failed, retrying");
                    continue;
                },
            }

            // Persist cluster_id and signal bootstrap to proceed.
            if let Err(e) = crate::cluster_id::write_cluster_id(&data_dir, info.cluster_id) {
                tracing::error!(error = %e, "Failed to persist cluster_id");
            }
            let _ = init_tx.send(Some(info.cluster_id));
            return;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Waits for the gRPC server to accept TCP connections.
///
/// Polls the server address for up to 30 seconds. Returns an error if the
/// server exits early (bind failure) or fails to accept connections in time.
///
/// The generous timeout accounts for debug-mode builds where constructing 13+
/// gRPC services with interceptors and layers can take several seconds,
/// especially when multiple nodes start on the same machine simultaneously.
async fn wait_for_tcp_ready(
    server_handle: &tokio::task::JoinHandle<Result<(), String>>,
    server_addr: std::net::SocketAddr,
) -> Result<(), BootstrapError> {
    let tcp_start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut tcp_ready = false;
    while tcp_start.elapsed() < timeout {
        if server_handle.is_finished() {
            // Server exited early — likely a bind failure. We cannot await a
            // borrowed JoinHandle, so report the early exit without consuming it.
            return Err(BootstrapError::Server {
                message: format!("server exited before accepting connections on {server_addr}"),
            });
        }
        if tokio::net::TcpStream::connect(server_addr).await.is_ok() {
            tcp_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !tcp_ready {
        return Err(BootstrapError::Server {
            message: format!(
                "server did not accept TCP connections on {server_addr} within {}s",
                timeout.as_secs()
            ),
        });
    }
    Ok(())
}

/// Input parameters for starting all background jobs.
struct StartJobsInput<'a> {
    config: &'a Config,
    data_dir: &'a std::path::Path,
    node_id: u64,
    health_state: &'a inferadb_ledger_raft::HealthState,
    handle: &'a Arc<ConsensusHandle>,
    state: &'a Arc<StateLayer<FileBackend>>,
    block_archive: &'a Arc<BlockArchive<FileBackend>>,
    applied_state_accessor: &'a AppliedStateAccessor,
    events_db: &'a Arc<EventsDatabase<FileBackend>>,
    snapshot_manager: Arc<SnapshotManager>,
    snapshot_manager_for_backup: Arc<SnapshotManager>,
    backup_manager: Option<Arc<BackupManager>>,
    tiered_manager: Option<Arc<TieredSnapshotManager>>,
    demote_interval_secs: u64,
    event_handle_for_saga: Option<EventHandle<FileBackend>>,
    saga_cell: Arc<tokio::sync::OnceCell<SagaOrchestratorHandle>>,
    manager: &'a Arc<RaftManager>,
    key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
    storage_manager: &'a RegionStorageManager,
}

/// Output handles from starting all background jobs.
struct StartJobsOutput {
    gc_handle: tokio::task::JoinHandle<()>,
    compactor_handle: tokio::task::JoinHandle<()>,
    recovery_handle: tokio::task::JoinHandle<()>,
    learner_refresh_handle: tokio::task::JoinHandle<()>,
    resource_metrics_handle: tokio::task::JoinHandle<()>,
    backup_handle: Option<tokio::task::JoinHandle<()>>,
    events_gc_handle: Option<tokio::task::JoinHandle<()>>,
    saga_handle: tokio::task::JoinHandle<()>,
    saga_orchestrator_handle: SagaOrchestratorHandle,
    orphan_cleanup_handle: tokio::task::JoinHandle<()>,
    integrity_scrub_handle: tokio::task::JoinHandle<()>,
    org_purge_handle: tokio::task::JoinHandle<()>,
    token_maintenance_handle: tokio::task::JoinHandle<()>,
    invite_maintenance_handle: tokio::task::JoinHandle<()>,
    post_erasure_compaction_handle: tokio::task::JoinHandle<()>,
    snapshot_demotion_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Starts all background jobs for an initialized node.
///
/// This is extracted from the main bootstrap function to share between the
/// restart path and (in the future) the `InitCluster` handler.
fn start_background_jobs(input: StartJobsInput<'_>) -> Result<StartJobsOutput, BootstrapError> {
    let watchdog = input.health_state.watchdog();

    let gc_handle = TtlGarbageCollector::builder()
        .handle(input.handle.clone())
        .state(input.state.clone())
        .applied_state(input.applied_state_accessor.clone())
        .watchdog_handle(watchdog.map(|w| w.register("ttl_gc", 60)))
        .build()
        .start();
    tracing::info!("Started TTL garbage collector");

    let compactor_handle = BlockCompactor::builder()
        .handle(input.handle.clone())
        .block_archive(input.block_archive.clone())
        .applied_state(input.applied_state_accessor.clone())
        .watchdog_handle(watchdog.map(|w| w.register("block_compactor", 300)))
        .build()
        .start();
    tracing::info!("Started block compactor");

    let recovery_handle = AutoRecoveryJob::builder()
        .handle(input.handle.clone())
        .node_id(input.node_id)
        .applied_state(input.applied_state_accessor.clone())
        .state(input.state.clone())
        .block_archive(Some(input.block_archive.clone()))
        .snapshot_manager(Some(input.snapshot_manager))
        .watchdog_handle(watchdog.map(|w| w.register("auto_recovery", 30)))
        .build()
        .start();
    tracing::info!("Started auto-recovery job with snapshot support");

    let learner_refresh_handle = LearnerRefreshJob::builder()
        .handle(input.handle.clone())
        .applied_state(input.applied_state_accessor.clone())
        .peer_addresses(Some(input.manager.peer_addresses().clone()))
        .registry(input.manager.registry())
        .watchdog_handle(watchdog.map(|w| w.register("learner_refresh", 5)))
        .build()
        .start();
    tracing::info!("Started learner refresh job");

    let snapshot_dir_for_metrics =
        input.storage_manager.snapshot_dir(inferadb_ledger_types::Region::GLOBAL);
    let resource_metrics_handle = ResourceMetricsCollector::builder()
        .state(input.state.clone())
        .data_dir(input.data_dir.to_path_buf())
        .snapshot_dir(snapshot_dir_for_metrics)
        .region(input.config.region.as_str())
        .watchdog_handle(watchdog.map(|w| w.register("resource_metrics", 30)))
        .build()
        .start();
    tracing::info!("Started resource metrics collector");

    let backup_handle = if let (Some(backup_config), Some(mgr)) =
        (input.config.backup.as_ref(), input.backup_manager)
    {
        if backup_config.enabled {
            let job = BackupJob::builder()
                .handle(input.handle.clone())
                .snapshot_manager(input.snapshot_manager_for_backup)
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

    let events_gc_handle = if input.config.events.enabled {
        let gc = EventsGarbageCollector::builder()
            .events_db(input.events_db.clone())
            .config(input.config.events.clone())
            .watchdog_handle(watchdog.map(|w| w.register("events_gc", 300)))
            .build();
        let handle = gc.start();
        tracing::info!("Started events garbage collector");
        Some(handle)
    } else {
        None
    };

    let (saga_handle, saga_orchestrator_handle) = SagaOrchestrator::builder()
        .handle(input.handle.clone())
        .node_id(input.node_id)
        .state(input.state.clone())
        .event_handle(input.event_handle_for_saga)
        .interval(Duration::from_secs(input.config.saga.poll_interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("saga_orchestrator", 60)))
        .manager(Some(input.manager.clone()))
        .key_manager(input.key_manager)
        .build()
        .start();
    tracing::info!("Started saga orchestrator");
    let _ = input.saga_cell.set(saga_orchestrator_handle.clone());

    let orphan_cleanup_handle = OrphanCleanupJob::builder()
        .handle(input.handle.clone())
        .state(input.state.clone())
        .applied_state(input.applied_state_accessor.clone())
        .interval(Duration::from_secs(input.config.cleanup.interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("orphan_cleanup", 7200)))
        .build()
        .start();
    tracing::info!("Started orphan cleanup job");

    let integrity_scrub_handle = IntegrityScrubberJob::builder()
        .state(input.state.clone())
        .interval(Duration::from_secs(input.config.integrity.scrub_interval_secs))
        .pages_per_cycle_percent(input.config.integrity.pages_per_cycle_percent)
        .watchdog_handle(watchdog.map(|w| w.register("integrity_scrub", 7200)))
        .build()
        .start();
    tracing::info!("Started integrity scrubber");

    let org_purge_handle = OrganizationPurgeJob::builder()
        .handle(input.handle.clone())
        .state(input.state.clone())
        .manager(Some(input.manager.clone()))
        .watchdog_handle(watchdog.map(|w| w.register("org_purge", 7200)))
        .build()
        .start();
    tracing::info!("Started organization purge job");

    let token_maintenance_handle = TokenMaintenanceJob::builder()
        .handle(input.handle.clone())
        .state(input.state.clone())
        .interval(Duration::from_secs(input.config.token_maintenance_interval_secs))
        .watchdog_handle(watchdog.map(|w| w.register("token_maintenance", 600)))
        .build()
        .start();
    tracing::info!("Started token maintenance job");

    let invite_maintenance_handle = InviteMaintenanceJob::builder()
        .handle(input.handle.clone())
        .state(input.state.clone())
        .manager(Some(input.manager.clone()))
        .watchdog_handle(watchdog.map(|w| w.register("invite_maintenance", 600)))
        .build()
        .start();
    tracing::info!("Started invite maintenance job");

    let post_erasure_compaction_handle = PostErasureCompactionJob::builder()
        .handle(input.handle.clone())
        .manager(Some(input.manager.clone()))
        .watchdog_handle(watchdog.map(|w| w.register("post_erasure_compaction", 600)))
        .build()
        .start();
    tracing::info!("Started post-erasure compaction job");

    let snapshot_demotion_handle = input.tiered_manager.map(|mgr| {
        let interval = Duration::from_secs(input.demote_interval_secs);
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
        tracing::info!(
            interval_secs = input.demote_interval_secs,
            "Started snapshot demotion task"
        );
        handle
    });

    Ok(StartJobsOutput {
        gc_handle,
        compactor_handle,
        recovery_handle,
        learner_refresh_handle,
        resource_metrics_handle,
        backup_handle,
        events_gc_handle,
        saga_handle,
        saga_orchestrator_handle,
        orphan_cleanup_handle,
        integrity_scrub_handle,
        org_purge_handle,
        token_maintenance_handle,
        invite_maintenance_handle,
        post_erasure_compaction_handle,
        snapshot_demotion_handle,
    })
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

/// Processes a single region event from the GLOBAL apply worker channel.
///
/// Handles `CreateDataRegion` entries (starts local data regions via the
/// `RaftManager`) and transport reconciliation signals. Extracted from the
/// region handler task loop so each event can be individually isolated
/// with `catch_unwind`.
async fn process_region_event(
    mgr: &inferadb_ledger_raft::RaftManager,
    events_config: &inferadb_ledger_types::events::EventConfig,
    region: inferadb_ledger_types::Region,
    initial_members: Vec<(u64, String)>,
) {
    // Sentinel: Region::GLOBAL signals a transport reconciliation.
    if region == inferadb_ledger_types::Region::GLOBAL {
        if let Some((_, tag)) = initial_members.first()
            && tag == "reconcile_transport"
        {
            reconcile_transport_channels(mgr).await;
        }
        return;
    }

    if mgr.has_region(region) {
        return;
    }

    let region_config = inferadb_ledger_raft::RegionConfig::builder()
        .region(region)
        .initial_members(initial_members)
        .events_config(events_config.clone())
        .build();
    match mgr.start_data_region(region_config).await {
        Ok(_) => {
            tracing::info!(region = region.as_str(), "Data region created via Raft consensus");
            reconcile_transport_channels(mgr).await;
        },
        Err(e) => {
            tracing::warn!(
                region = region.as_str(),
                error = %e,
                "Failed to create data region from Raft consensus"
            );
        },
    }
}

/// Ensures all regions' consensus transports have channels to known peers.
///
/// Iterates all peer addresses and registers transport channels for any peers
/// that are missing from any region's transport. This is especially important
/// for followers that only have a channel to the leader — without channels to
/// other voters, leader transfer elections fail (VoteRequest can't reach peers).
async fn reconcile_transport_channels(manager: &inferadb_ledger_raft::RaftManager) {
    let peer_addrs = manager.peer_addresses().iter_peers();
    let regions = manager.list_regions();

    for region in regions {
        let Ok(group) = manager.get_region_group(region) else { continue };
        let Some(transport) = group.consensus_transport() else { continue };
        let known_peers = transport.peers();
        let local_node = group.handle().node_id();

        for (node_id, addr) in &peer_addrs {
            if *node_id == local_node || known_peers.contains(node_id) {
                continue;
            }
            if let Err(e) = transport.set_peer_via_registry(*node_id, addr).await {
                tracing::warn!(
                    node_id = *node_id,
                    addr = %addr,
                    error = %e,
                    "Failed to register peer via registry"
                );
            }
        }
    }
}

/// Quorum-based dead node detection. For each node that the GLOBAL leader
/// hasn't heard from within the timeout, queries other voters to confirm
/// unreachability before marking Dead.
async fn check_peer_liveness_quorum(
    manager: &inferadb_ledger_raft::RaftManager,
    local_liveness: &parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>,
    global: &inferadb_ledger_raft::RegionGroup,
    liveness_config: &inferadb_ledger_raft::LivenessConfig,
) {
    let Some(reader) = manager.system_state_reader() else { return };
    let state = global.handle().shard_state();
    let local_node_id = manager.config().node_id;
    let dead_node_timeout = liveness_config.dead_node_timeout;
    let now = std::time::Instant::now();

    let liveness_snapshot = local_liveness.read().clone();

    for voter in &state.voters {
        let node_id = voter.0;
        if node_id == local_node_id {
            continue; // Never evaluate our own liveness.
        }

        // Check if we've seen this node recently.
        let seen_recently = liveness_snapshot
            .get(&node_id)
            .is_some_and(|last_seen| now.duration_since(*last_seen) <= dead_node_timeout);
        if seen_recently {
            continue;
        }

        // Check current status — skip if already Decommissioning or Dead.
        let status = reader.node_status(node_id);
        if status != inferadb_ledger_raft::types::NodeStatus::Active {
            continue;
        }

        // Quorum confirmation: query other voters.
        let other_voters: Vec<u64> = state
            .voters
            .iter()
            .map(|n| n.0)
            .filter(|&id| id != local_node_id && id != node_id)
            .collect();

        let quorum = (state.voters.len() / 2) + 1;
        let mut unreachable_votes = 1u32; // Count ourselves.
        let mut reachable_votes = 0u32;

        // Fan out queries concurrently with a wall-clock budget.
        // Each task returns (voter_id, Option<bool>) so we can update liveness
        // for voters that responded, regardless of the target's reachability.
        let mut join_set = tokio::task::JoinSet::new();
        let target_node_id = node_id;
        let query_timeout = liveness_config.liveness_query_timeout;
        for &voter_id in &other_voters {
            if let Some(addr) = manager.peer_addresses().get(voter_id) {
                let addr = addr.clone();
                join_set.spawn(async move {
                    let result =
                        query_peer_liveness_rpc(&addr, target_node_id, query_timeout).await;
                    (voter_id, result)
                });
            }
        }

        let deadline = tokio::time::Instant::now() + liveness_config.liveness_cycle_budget;
        while let Ok(Some(result)) = tokio::time::timeout_at(deadline, join_set.join_next()).await {
            if let Ok((responding_voter_id, Some(reachable))) = result {
                if reachable {
                    reachable_votes += 1;
                } else {
                    unreachable_votes += 1;
                }
                // The voter responded — update its liveness regardless of the
                // target's reachability status. A response proves the querying
                // voter is alive from our perspective.
                local_liveness.write().insert(responding_voter_id, std::time::Instant::now());
            }
            if unreachable_votes >= quorum as u32 || reachable_votes >= quorum as u32 {
                break;
            }
        }

        if unreachable_votes >= quorum as u32 {
            tracing::warn!(
                node_id,
                unreachable_votes,
                reachable_votes,
                "Quorum confirms node unreachable — marking Dead"
            );
            let set_dead = inferadb_ledger_raft::types::LedgerRequest::System(
                inferadb_ledger_raft::types::SystemRequest::SetNodeStatus {
                    node_id,
                    status: inferadb_ledger_raft::types::NodeStatus::Dead,
                },
            );
            let _ = global
                .handle()
                .propose_and_wait(
                    inferadb_ledger_raft::types::RaftPayload::system(set_dead),
                    std::time::Duration::from_secs(5),
                )
                .await;
        } else {
            tracing::info!(
                node_id,
                unreachable_votes,
                reachable_votes,
                "Node unreachable from leader but reachable from quorum — not marking Dead"
            );
        }
    }
}

/// Queries a peer's `CheckPeerLiveness` RPC. Returns `Some(reachable)` or `None` on failure.
async fn query_peer_liveness_rpc(
    addr: &str,
    target_node_id: u64,
    timeout: std::time::Duration,
) -> Option<bool> {
    let endpoint = format!("http://{addr}");
    let channel = tonic::transport::Channel::from_shared(endpoint).ok()?.connect_lazy();
    let mut client =
        inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::new(channel);
    let response = tokio::time::timeout(
        timeout,
        client.check_peer_liveness(inferadb_ledger_proto::proto::CheckPeerLivenessRequest {
            target_node_id,
        }),
    )
    .await
    .ok()?
    .ok()?;
    Some(response.into_inner().reachable)
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

        // Write cluster_id so we take the restart path (initialized node).
        crate::cluster_id::write_cluster_id(&data_dir, 1).expect("write cluster_id");

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

        // Write cluster_id so we take the restart path.
        crate::cluster_id::write_cluster_id(&data_dir, 1).expect("write cluster_id");

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

        // Write cluster_id so we take the restart path.
        crate::cluster_id::write_cluster_id(&data_dir, 1).expect("write cluster_id");

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

    #[tokio::test]
    async fn test_bootstrap_fresh_node_waits_for_init() {
        let temp_dir = tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50055, data_dir.clone());

        // No cluster_id file — fresh node path. Without an InitCluster RPC
        // or seed discovery, the node blocks until shutdown is signaled.
        let health = inferadb_ledger_raft::HealthState::new();
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Signal shutdown after a short delay so the test completes.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = tx.send(true);
        });

        let result = bootstrap_node(&config, &data_dir, health, rx, None).await;
        // Fresh node now returns an error when shutdown fires before initialization.
        match result {
            Ok(_) => panic!("fresh node should fail when shutdown fires before init"),
            Err(err) => {
                assert!(
                    err.to_string().contains("shutdown received before initialization"),
                    "expected shutdown-before-init error, got: {err}"
                );
            },
        }
    }

    // =================================================================
    // parse_hex_key tests
    // =================================================================

    #[test]
    fn test_parse_hex_key_valid() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let result = parse_hex_key(hex).unwrap();
        assert_eq!(result[0], 0x01);
        assert_eq!(result[1], 0x23);
        assert_eq!(result[15], 0xef);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_parse_hex_key_all_zeros() {
        let hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let result = parse_hex_key(hex).unwrap();
        assert_eq!(result, [0u8; 32]);
    }

    #[test]
    fn test_parse_hex_key_all_ff() {
        let hex = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let result = parse_hex_key(hex).unwrap();
        assert_eq!(result, [0xff; 32]);
    }

    #[test]
    fn test_parse_hex_key_uppercase() {
        let hex = "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";
        let result = parse_hex_key(hex).unwrap();
        assert_eq!(result[0], 0xAB);
        assert_eq!(result[1], 0xCD);
    }

    #[test]
    fn test_parse_hex_key_trims_whitespace() {
        let hex = "  0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  ";
        let result = parse_hex_key(hex).unwrap();
        assert_eq!(result[0], 0x01);
    }

    #[test]
    fn test_parse_hex_key_too_short() {
        let hex = "0123456789abcdef";
        let err = parse_hex_key(hex).unwrap_err();
        assert!(err.contains("expected 64 hex characters"));
    }

    #[test]
    fn test_parse_hex_key_too_long() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef00";
        let err = parse_hex_key(hex).unwrap_err();
        assert!(err.contains("expected 64 hex characters"));
    }

    #[test]
    fn test_parse_hex_key_empty() {
        let err = parse_hex_key("").unwrap_err();
        assert!(err.contains("expected 64 hex characters"));
    }

    #[test]
    fn test_parse_hex_key_invalid_chars() {
        let hex = "gg23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let err = parse_hex_key(hex).unwrap_err();
        assert!(err.contains("invalid hex at position 0"));
    }

    #[test]
    fn test_parse_hex_key_invalid_char_mid_string() {
        let hex = "0123456789abcdefXX23456789abcdef0123456789abcdef0123456789abcdef";
        let err = parse_hex_key(hex).unwrap_err();
        assert!(err.contains("invalid hex at position 16"));
    }

    // =================================================================
    // BootstrapError display tests
    // =================================================================

    #[test]
    fn bootstrap_error_display_database() {
        let err = BootstrapError::Database { message: "disk full".to_string() };
        assert_eq!(err.to_string(), "database error: disk full");
    }

    #[test]
    fn bootstrap_error_display_storage() {
        let err = BootstrapError::Storage { message: "corrupt".to_string() };
        assert_eq!(err.to_string(), "storage error: corrupt");
    }

    #[test]
    fn bootstrap_error_display_raft() {
        let err = BootstrapError::Raft { message: "term mismatch".to_string() };
        assert_eq!(err.to_string(), "raft error: term mismatch");
    }

    #[test]
    fn bootstrap_error_display_initialize() {
        let err = BootstrapError::Initialize { message: "failed".to_string() };
        assert_eq!(err.to_string(), "initialization error: failed");
    }

    #[test]
    fn bootstrap_error_display_node_id() {
        let err = BootstrapError::NodeId { message: "bad id".to_string() };
        assert_eq!(err.to_string(), "node id error: bad id");
    }

    #[test]
    fn bootstrap_error_display_timeout() {
        let err = BootstrapError::Timeout { message: "30s elapsed".to_string() };
        assert_eq!(err.to_string(), "bootstrap timeout: 30s elapsed");
    }

    #[test]
    fn bootstrap_error_display_config() {
        let err = BootstrapError::Config { message: "bad cluster size".to_string() };
        assert_eq!(err.to_string(), "configuration error: bad cluster size");
    }

    #[test]
    fn bootstrap_error_display_server() {
        let err = BootstrapError::Server { message: "bind failed".to_string() };
        assert_eq!(err.to_string(), "server error: bind failed");
    }
}
