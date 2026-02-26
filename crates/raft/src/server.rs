//! gRPC server for InferaDB Ledger.
//!
//! This module provides the main server that exposes all gRPC services:
//! - ReadService: Query operations
//! - WriteService: Transaction submission
//! - AdminService: Organization and vault management
//! - HealthService: Health checks
//! - SystemDiscoveryService: Peer discovery

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BlockAnnouncement, admin_service_server::AdminServiceServer,
    events_service_server::EventsServiceServer, health_service_server::HealthServiceServer,
    raft_service_server::RaftServiceServer, read_service_server::ReadServiceServer,
    system_discovery_service_server::SystemDiscoveryServiceServer,
    write_service_server::WriteServiceServer,
};
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
use openraft::Raft;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::{
    api_version::{ApiVersionLayer, api_version_interceptor},
    batching::BatchConfig,
    graceful_shutdown::ConnectionTrackingLayer,
    idempotency::IdempotencyCache,
    log_storage::AppliedStateAccessor,
    rate_limit::RateLimiter,
    services::{
        AdminServiceImpl, DiscoveryServiceImpl, EventsServiceImpl, HealthServiceImpl,
        RaftServiceImpl, ReadServiceImpl, WriteServiceImpl,
    },
    types::LedgerTypeConfig,
};

/// The main Ledger gRPC server.
///
/// Combines all services with the Raft consensus layer and state storage.
/// Supports graceful shutdown via a `shutdown_rx` watch channel.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct LedgerServer {
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Idempotency cache for duplicate detection.
    #[builder(default = Arc::new(IdempotencyCache::new()))]
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for historical block retrieval.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Block announcement broadcast channel.
    #[builder(default = broadcast::channel(1000).0)]
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Server address.
    addr: SocketAddr,
    /// Max concurrent requests per connection.
    #[builder(default = 100)]
    max_concurrent: usize,
    /// Request timeout in seconds.
    #[builder(default = 30)]
    timeout_secs: u64,
    /// Per-organization rate limiter (optional).
    #[builder(default)]
    organization_rate_limiter: Option<Arc<RateLimiter>>,
    /// Hot key detector for identifying frequently accessed keys (optional).
    #[builder(default)]
    hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    /// Node health state for three-probe health checking.
    #[builder(default)]
    health_state: crate::graceful_shutdown::HealthState,
    /// Shutdown signal receiver. When `true` is sent, the server stops.
    #[builder(default)]
    shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    /// Maximum time to wait for Raft proposals to commit.
    ///
    /// Passed to write and admin services. If a client's gRPC deadline
    /// is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Runtime configuration handle for hot-reloadable settings.
    ///
    /// When provided, the `AdminService` exposes `UpdateConfig`/`GetConfig`
    /// RPCs that atomically swap the live config via `ArcSwap`.
    #[builder(default)]
    runtime_config: Option<crate::runtime_config::RuntimeConfigHandle>,
    /// Backup manager for `CreateBackup`/`ListBackups`/`RestoreBackup` RPCs.
    #[builder(default)]
    backup_manager: Option<Arc<crate::backup::BackupManager>>,
    /// Snapshot manager for backup creation and restore operations.
    #[builder(default)]
    snapshot_manager: Option<Arc<inferadb_ledger_state::SnapshotManager>>,
    /// Data directory for dependency health checks (disk writability).
    #[builder(default)]
    data_dir: Option<std::path::PathBuf>,
    /// Health check configuration for dependency validation.
    #[builder(default)]
    health_check_config: Option<inferadb_ledger_types::config::HealthCheckConfig>,
    /// Maximum Raft log lag before forwarding reads to the leader.
    ///
    /// When a follower's applied index trails its last log index by more than
    /// this threshold, read requests are transparently forwarded to the leader
    /// to avoid serving stale data during catch-up.
    /// Default 0: only serve reads locally when fully caught up.
    #[builder(default)]
    max_read_forward_lag: u64,
    /// Events database for the events query service (optional).
    #[builder(default)]
    events_db: Option<inferadb_ledger_state::EventsDatabase<FileBackend>>,
    /// Handler-phase event handle for recording denial and admin events.
    #[builder(default)]
    event_handle: Option<crate::event_writer::EventHandle<FileBackend>>,
}

impl LedgerServer {
    /// Starts the gRPC server.
    ///
    /// This method blocks until the server is shut down. If a `shutdown_rx`
    /// was provided via the builder, the server will stop when the signal
    /// is received. Otherwise, it blocks indefinitely.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind to the configured address
    /// or encounters a transport-level error during operation.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            max_concurrent = self.max_concurrent,
            timeout_secs = self.timeout_secs,
            "Configuring request limits"
        );

        // Configure backpressure with tower layers
        // Note: RateLimitLayer is not used because it doesn't implement Clone,
        // which tonic's serve() requires. The concurrency_limit + load_shed
        // combination provides effective backpressure protection.
        let layer = ServiceBuilder::new()
            // Limit concurrent requests per connection
            .concurrency_limit(self.max_concurrent)
            // Reject new requests when overloaded (returns 503)
            .load_shed()
            // Set timeout for requests
            .timeout(Duration::from_secs(self.timeout_secs))
            .into_inner();

        // Create service implementations
        let read_service = ReadServiceImpl::builder()
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .block_archive(self.block_archive.clone())
            .block_announcements(self.block_announcements.clone())
            .raft(Some(self.raft.clone()))
            .max_read_forward_lag(self.max_read_forward_lag)
            .build();
        // Create write service with batching enabled for high throughput.
        // Server-level batching coalesces individual Write RPCs into single Raft
        // proposals. This improves throughput when clients can't or don't use
        // BatchWrite RPC.
        let batch_config = BatchConfig::default();
        let write_service = if let Some(archive) = &self.block_archive {
            let (service, task) = WriteServiceImpl::with_block_archive_and_batching(
                self.raft.clone(),
                self.idempotency.clone(),
                archive.clone(),
                batch_config,
            );
            tokio::spawn(task);
            service
        } else {
            let (service, task) = WriteServiceImpl::new_with_batching(
                self.raft.clone(),
                self.idempotency.clone(),
                batch_config,
            );
            tokio::spawn(task);
            service
        };
        // Add applied state for sequence gap detection
        let write_service = write_service.with_applied_state(self.applied_state.clone());
        // Add per-organization rate limiting if configured
        let write_service = match &self.organization_rate_limiter {
            Some(limiter) => write_service.with_rate_limiter(limiter.clone()),
            None => write_service,
        };
        // Add hot key detection if configured
        let write_service = match &self.hot_key_detector {
            Some(detector) => write_service.with_hot_key_detector(detector.clone()),
            None => write_service,
        };
        // Wire proposal_timeout into write service
        let write_service = write_service.with_proposal_timeout(self.proposal_timeout);
        // Wire handler-phase event handle for denial event recording
        let write_service = if let Some(ref handle) = self.event_handle {
            write_service.with_event_handle(handle.clone())
        } else {
            write_service
        };
        let admin_service = AdminServiceImpl::builder()
            .raft(self.raft.clone())
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .block_archive(self.block_archive.clone())
            .listen_addr(self.addr)
            .proposal_timeout(self.proposal_timeout)
            .build();
        // Wire runtime config handle into admin service for UpdateConfig/GetConfig RPCs.
        // Pass the rate limiter and hot key detector so config changes propagate to them.
        let admin_service = if let Some(handle) = self.runtime_config {
            admin_service.with_runtime_config(
                handle,
                self.organization_rate_limiter.clone(),
                self.hot_key_detector.clone(),
            )
        } else {
            admin_service
        };
        // Wire handler-phase event handle for admin event recording
        let admin_service = if let Some(ref handle) = self.event_handle {
            admin_service.with_event_handle(handle.clone())
        } else {
            admin_service
        };
        // Wire backup support into admin service for CreateBackup/ListBackups/RestoreBackup RPCs.
        let admin_service = if let (Some(backup_mgr), Some(snap_mgr)) =
            (self.backup_manager, self.snapshot_manager)
        {
            admin_service.with_backup(backup_mgr, snap_mgr)
        } else {
            admin_service
        };
        // Extract connection tracker before health_state is moved into HealthServiceImpl
        let connection_tracker = self.health_state.connection_tracker().clone();
        let health_service = HealthServiceImpl::new(
            self.raft.clone(),
            self.state.clone(),
            self.applied_state.clone(),
            self.health_state,
        );
        // Attach dependency health checker if data_dir is provided
        let health_service = if let Some(data_dir) = self.data_dir {
            let config = self.health_check_config.unwrap_or_default();
            let checker = crate::dependency_health::DependencyHealthChecker::new(
                self.raft.clone(),
                data_dir,
                config,
            );
            health_service.with_dependency_checker(checker)
        } else {
            health_service
        };
        let discovery_service = DiscoveryServiceImpl::builder()
            .raft(self.raft.clone())
            .state(self.state.clone())
            .applied_state(self.applied_state.clone())
            .build();

        // RaftService handles inter-node Raft RPCs (Vote, AppendEntries, InstallSnapshot)
        let raft_service = RaftServiceImpl::new(self.raft.clone());

        // gRPC reflection allows tools like grpcurl to discover services
        // without requiring proto files on the client side.
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(inferadb_ledger_proto::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        tracing::info!("Starting Ledger gRPC server on {}", self.addr);

        let mut router = Server::builder()
            // Track in-flight requests for connection draining during shutdown.
            // Outermost layer so it counts every request, including those rejected
            // by concurrency limits or load shedding.
            .layer(ConnectionTrackingLayer::new(connection_tracker))
            .layer(layer)
            // API version response header on all responses
            .layer(ApiVersionLayer)
            // Client-facing services validate x-ledger-api-version request header.
            // Health, Discovery, and Raft services are exempted — they are
            // infrastructure endpoints used by probes and inter-node communication.
            .add_service(ReadServiceServer::with_interceptor(read_service, api_version_interceptor))
            .add_service(WriteServiceServer::with_interceptor(
                write_service,
                api_version_interceptor,
            ))
            .add_service(AdminServiceServer::with_interceptor(
                admin_service,
                api_version_interceptor,
            ))
            .add_service(HealthServiceServer::new(health_service))
            .add_service(SystemDiscoveryServiceServer::new(discovery_service))
            .add_service(RaftServiceServer::new(raft_service))
            .add_service(reflection_service);

        // EventsService is optional — only registered when events_db is provided.
        // The bootstrap code (Task 11) provides the EventsDatabase at server start.
        if let Some(events_db) = self.events_db {
            // Extract ingestion fields from EventHandle when available.
            // The EventHandle carries the event config and node_id needed
            // for IngestEvents validation and handler-phase event emission.
            let (event_config, node_id, ingestion_rate_limiter) =
                if let Some(ref handle) = self.event_handle {
                    let rate_limit = handle.config().ingestion.ingest_rate_limit_per_source;
                    (
                        Some(Arc::clone(handle.config_arc())),
                        Some(handle.node_id()),
                        Some(Arc::new(crate::event_writer::IngestionRateLimiter::new(rate_limit))),
                    )
                } else {
                    (None, None, None)
                };

            let events_service = EventsServiceImpl::builder()
                .events_db(events_db)
                .applied_state(self.applied_state.clone())
                .page_token_codec(crate::pagination::PageTokenCodec::with_random_key())
                .maybe_event_config(event_config)
                .maybe_node_id(node_id)
                .maybe_ingestion_rate_limiter(ingestion_rate_limiter)
                .build();
            router = router.add_service(EventsServiceServer::with_interceptor(
                events_service,
                api_version_interceptor,
            ));
        }

        if let Some(mut shutdown_rx) = self.shutdown_rx {
            router
                .serve_with_shutdown(self.addr, async move {
                    let _ = shutdown_rx.wait_for(|v| *v).await;
                    tracing::info!("Shutdown signal received, stopping gRPC server");
                })
                .await?;
        } else {
            router.serve(self.addr).await?;
        }

        Ok(())
    }

    /// Returns the Raft instance.
    #[must_use]
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Returns the idempotency cache.
    #[must_use]
    pub fn idempotency(&self) -> &Arc<IdempotencyCache> {
        &self.idempotency
    }

    /// Returns the block announcements sender (for broadcasting new blocks).
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Returns the block archive.
    #[must_use]
    pub fn block_archive(&self) -> Option<&Arc<BlockArchive<FileBackend>>> {
        self.block_archive.as_ref()
    }

    /// Attaches backup support (backup manager + snapshot manager).
    ///
    /// Enables `CreateBackup`, `ListBackups`, and `RestoreBackup` RPCs on the
    /// admin service. Done post-construction because bon type-state builders
    /// don't support conditional field setting.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<crate::backup::BackupManager>,
        snapshot_manager: Arc<inferadb_ledger_state::SnapshotManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self.snapshot_manager = Some(snapshot_manager);
        self
    }
}
