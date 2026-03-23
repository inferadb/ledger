//! gRPC server for InferaDB Ledger.
//!
//! Main server exposing all gRPC services:
//! - ReadService: Query operations
//! - WriteService: Transaction submission
//! - OrganizationService: Organization lifecycle management
//! - VaultService: Vault lifecycle management
//! - AdminService: Cluster operations, maintenance, backup/restore
//! - UserService: User lifecycle and email management
//! - AppService: Organization-scoped client application management
//! - InvitationService: Organization invitation lifecycle
//! - TokenService: JWT lifecycle (sessions, vault tokens, signing keys)
//! - EventsService: Organization-scoped event queries
//! - HealthService: Health checks
//! - SystemDiscoveryService: Peer discovery
//! - RaftService: Inter-node Raft consensus

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use inferadb_ledger_proto::proto::{
    admin_service_server::AdminServiceServer, app_service_server::AppServiceServer,
    events_service_server::EventsServiceServer, health_service_server::HealthServiceServer,
    invitation_service_server::InvitationServiceServer,
    organization_service_server::OrganizationServiceServer, raft_service_server::RaftServiceServer,
    read_service_server::ReadServiceServer,
    system_discovery_service_server::SystemDiscoveryServiceServer,
    token_service_server::TokenServiceServer, user_service_server::UserServiceServer,
    vault_service_server::VaultServiceServer, write_service_server::WriteServiceServer,
};
use inferadb_ledger_raft::{
    graceful_shutdown::ConnectionTrackingLayer, idempotency::IdempotencyCache,
    raft_manager::RaftManager, rate_limit::RateLimiter,
};
use inferadb_ledger_store::FileBackend;
use tonic::transport::Server;
use tower::ServiceBuilder;

use crate::{
    api_version::{ApiVersionLayer, api_version_interceptor},
    jwt::JwtEngine,
    services::{
        AdminService, AppService, DiscoveryService, EventsService, HealthService,
        InvitationService, OrganizationService, RaftService, ReadService, RegionResolver,
        RegionResolverService, TokenServiceImpl, UserService, VaultService, WriteService,
        service_infra::ServiceContext,
    },
};

/// Bundled configuration for the optional `TokenService`.
///
/// Replaces three separate `Option` fields on `LedgerServer` that had to be
/// provided all-or-nothing. With this struct, partial configuration is
/// impossible — either the entire token service is configured or it isn't.
pub struct TokenServiceConfig {
    /// JWT engine for token signing and validation.
    pub jwt_engine: Arc<JwtEngine>,
    /// JWT configuration (token TTLs, issuer, clock skew).
    pub jwt_config: inferadb_ledger_types::config::JwtConfig,
    /// Region key manager for signing key envelope encryption.
    pub key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
}

/// The main Ledger gRPC server.
///
/// Combines all services with the Raft consensus layer and state storage.
/// Every `LedgerServer` is multi-region capable — a single-region deployment
/// is simply a `RaftManager` with one region (GLOBAL).
///
/// Supports graceful shutdown via a `shutdown_rx` watch channel.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct LedgerServer {
    /// The Raft manager containing all region groups.
    ///
    /// Routes requests to the correct region based on organization assignment.
    /// A single-region deployment has one region (GLOBAL).
    manager: Arc<RaftManager>,
    /// Idempotency cache for duplicate detection.
    #[builder(default = Arc::new(IdempotencyCache::new()))]
    idempotency: Arc<IdempotencyCache>,
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
    hot_key_detector: Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    /// Node health state for three-probe health checking.
    #[builder(default)]
    health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
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
    runtime_config: Option<inferadb_ledger_raft::runtime_config::RuntimeConfigHandle>,
    /// Backup manager for `CreateBackup`/`ListBackups`/`RestoreBackup` RPCs.
    #[builder(default)]
    backup_manager: Option<Arc<inferadb_ledger_raft::backup::BackupManager>>,
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
    event_handle: Option<inferadb_ledger_raft::event_writer::EventHandle<FileBackend>>,
    /// Geographic region this node belongs to.
    ///
    /// Included in discovery responses so peers know this node's region.
    #[builder(default = inferadb_ledger_types::Region::GLOBAL)]
    region: inferadb_ledger_types::Region,
    /// Token service configuration (JWT engine, config, key manager).
    ///
    /// When provided, the `TokenService` is registered and exposes
    /// session/vault token lifecycle RPCs.
    #[builder(default)]
    token_service: Option<TokenServiceConfig>,
    /// Maximum concurrent `WatchBlocks` streams across all connections.
    ///
    /// Prevents resource exhaustion from too many open server-streaming RPCs.
    /// Each `WatchBlocks` call increments a shared atomic counter; the stream
    /// is rejected with `RESOURCE_EXHAUSTED` when the limit is reached.
    #[builder(default = 1000)]
    max_watch_streams: usize,
    /// Enables gRPC server reflection.
    ///
    /// When true, tools like `grpcurl` can discover services without
    /// requiring proto files on the client side. Disabled by default
    /// in production to reduce the attack surface.
    #[builder(default = false)]
    enable_grpc_reflection: bool,
    /// HMAC key for privacy-preserving email uniqueness enforcement.
    ///
    /// When present, onboarding RPCs (email verification, registration) are
    /// enabled. When absent, those RPCs return `FAILED_PRECONDITION`.
    #[builder(default)]
    email_blinding_key: Option<Arc<inferadb_ledger_types::EmailBlindingKey>>,
    /// Saga orchestrator handle for submitting cross-region sagas.
    ///
    /// Wrapped in `OnceCell` — set after the server starts when the orchestrator
    /// is ready. Returned to bootstrap via `saga_cell()` for deferred initialization.
    #[builder(default = Arc::new(tokio::sync::OnceCell::new()))]
    saga_handle: Arc<tokio::sync::OnceCell<inferadb_ledger_raft::SagaOrchestratorHandle>>,
}

impl LedgerServer {
    /// Returns a clone of the saga cell for deferred initialization.
    ///
    /// Bootstrap calls this before `serve()` to retain a handle to the
    /// `OnceCell`. After the saga orchestrator starts, bootstrap sets the
    /// cell value so service handlers can submit sagas.
    pub fn saga_cell(
        &self,
    ) -> Arc<tokio::sync::OnceCell<inferadb_ledger_raft::SagaOrchestratorHandle>> {
        self.saga_handle.clone()
    }

    /// Starts the gRPC server.
    ///
    /// Blocks until the server is shut down. If a `shutdown_rx`
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

        // Extract system region for services that need direct Raft/state access
        // (admin, health, discovery operate on the system region).
        let system = self.manager.system_region().map_err(|e| {
            Box::new(std::io::Error::other(format!("System region not available: {e}")))
                as Box<dyn std::error::Error>
        })?;

        // Build region resolver from the manager — routes requests to the
        // correct region based on organization assignment.
        let resolver: Arc<dyn RegionResolver> =
            Arc::new(RegionResolverService::new(self.manager.clone()));

        // Shared counter for active WatchBlocks streams
        let active_watch_streams = Arc::new(AtomicUsize::new(0));

        // Create service implementations
        let read_service = ReadService::builder()
            .resolver(resolver.clone())
            .manager(Some(self.manager.clone()))
            .max_read_forward_lag(self.max_read_forward_lag)
            .active_streams(active_watch_streams)
            .max_streams(self.max_watch_streams)
            .build();

        // Create write service using the resolver. Batch writers are per-region
        // (created by RaftManager::start_region), not constructed here.
        let mut write_service = WriteService::builder()
            .resolver(resolver.clone())
            .manager(Some(self.manager.clone()))
            .idempotency(self.idempotency.clone())
            .proposal_timeout(self.proposal_timeout)
            .build()
            .with_health_state(self.health_state.clone());
        // Wire optional features via builder methods
        if let Some(ref limiter) = self.organization_rate_limiter {
            write_service = write_service.with_rate_limiter(limiter.clone());
        }
        if let Some(ref detector) = self.hot_key_detector {
            write_service = write_service.with_hot_key_detector(detector.clone());
        }
        if let Some(ref handle) = self.event_handle {
            write_service = write_service.with_event_handle(handle.clone());
        }

        let admin_service = AdminService::builder()
            .raft(system.raft().clone())
            .state(system.state().clone())
            .applied_state(system.applied_state().clone())
            .block_archive(Some(system.block_archive().clone()))
            .listen_addr(self.addr)
            .proposal_timeout(self.proposal_timeout)
            .build()
            .with_raft_manager(self.manager.clone());
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
        // Wire health state for drain-phase write rejection
        let admin_service = admin_service.with_health_state(self.health_state.clone());

        // Build shared service context for Organization, Vault, User, and App services.
        // All four share the same Raft, state, applied_state, and config — ServiceContext
        // consolidates these into a single clonable struct.
        let svc_ctx = ServiceContext {
            raft: system.raft().clone(),
            state: system.state().clone(),
            applied_state: system.applied_state().clone(),
            sampler: None,
            node_id: self.event_handle.as_ref().map(|h| h.node_id()),
            validation_config: std::sync::Arc::new(
                inferadb_ledger_types::config::ValidationConfig::default(),
            ),
            proposal_timeout: self.proposal_timeout,
            event_handle: self.event_handle.clone(),
            health_state: Some(self.health_state.clone()),
            manager: Some(self.manager.clone()),
            email_blinding_key: self.email_blinding_key.clone(),
            jwt_engine: self.token_service.as_ref().map(|ts| ts.jwt_engine.clone()),
            jwt_config: self.token_service.as_ref().map(|ts| ts.jwt_config.clone()),
            key_manager: self.token_service.as_ref().map(|ts| ts.key_manager.clone()),
            saga_handle: self.saga_handle.clone(),
        };

        let organization_service = OrganizationService::new(svc_ctx.clone());
        let vault_service = VaultService::new(svc_ctx.clone());
        let user_service = UserService::new(svc_ctx.clone());

        // TokenService is optional — registered when token_service config is provided.
        let token_service = self.token_service.map(|ts| {
            let mut svc = TokenServiceImpl::new(
                svc_ctx.clone(),
                ts.jwt_engine,
                ts.jwt_config,
                ts.key_manager,
            );
            if let Some(ref limiter) = self.organization_rate_limiter {
                svc = svc.with_rate_limiter(limiter.clone());
            }
            svc
        });

        let invitation_service = InvitationService::new(svc_ctx.clone());
        let app_service = AppService::new(svc_ctx);

        // Extract connection tracker before health_state is moved into HealthService
        let connection_tracker = self.health_state.connection_tracker().clone();
        let health_service = HealthService::new(
            system.raft().clone(),
            system.state().clone(),
            system.applied_state().clone(),
            self.health_state,
        )
        .with_manager(self.manager.clone());
        // Attach dependency health checker if data_dir is provided
        let health_service = if let Some(data_dir) = self.data_dir {
            let config = self.health_check_config.unwrap_or_default();
            let checker = inferadb_ledger_raft::dependency_health::DependencyHealthChecker::new(
                system.raft().clone(),
                data_dir,
                config,
            );
            health_service.with_dependency_checker(checker)
        } else {
            health_service
        };

        let discovery_service = DiscoveryService::builder()
            .raft(system.raft().clone())
            .state(system.state().clone())
            .applied_state(system.applied_state().clone())
            .region(self.region)
            .build();

        // RaftService routes inter-node Raft RPCs to the correct region.
        let raft_service = RaftService::new(self.manager.clone());

        tracing::info!("Starting Ledger gRPC server on {}", self.addr);

        let mut router = Server::builder()
            // HTTP/2 and TCP keepalive for long-lived connections
            .http2_keepalive_interval(Some(Duration::from_secs(60)))
            .http2_keepalive_timeout(Some(Duration::from_secs(20)))
            .tcp_keepalive(Some(Duration::from_secs(60)))
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
            .add_service(OrganizationServiceServer::with_interceptor(
                organization_service,
                api_version_interceptor,
            ))
            .add_service(VaultServiceServer::with_interceptor(
                vault_service,
                api_version_interceptor,
            ))
            .add_service(UserServiceServer::with_interceptor(user_service, api_version_interceptor))
            .add_service(AppServiceServer::with_interceptor(app_service, api_version_interceptor))
            .add_service(InvitationServiceServer::with_interceptor(
                invitation_service,
                api_version_interceptor,
            ))
            .add_service(HealthServiceServer::new(health_service))
            .add_service(SystemDiscoveryServiceServer::new(discovery_service))
            .add_service(RaftServiceServer::new(raft_service));

        // gRPC reflection allows tools like grpcurl to discover services
        // without requiring proto files on the client side.
        // Disabled by default to reduce attack surface in production.
        if self.enable_grpc_reflection {
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(inferadb_ledger_proto::FILE_DESCRIPTOR_SET)
                .build_v1()?;
            router = router.add_service(reflection_service);
        }

        // Register TokenService if JWT support is configured
        if let Some(token_svc) = token_service {
            router = router.add_service(TokenServiceServer::with_interceptor(
                token_svc,
                api_version_interceptor,
            ));
        }

        // EventsService is optional — only registered when events_db is provided.
        if let Some(events_db) = self.events_db {
            // Extract ingestion fields from EventHandle when available.
            // The EventHandle carries the event config and node_id needed
            // for IngestEvents validation and handler-phase event emission.
            let (event_config, node_id, ingestion_rate_limiter) = if let Some(ref handle) =
                self.event_handle
            {
                let rate_limit = handle.config().ingestion.ingest_rate_limit_per_source;
                (
                    Some(Arc::clone(handle.config_arc())),
                    Some(handle.node_id()),
                    Some(Arc::new(inferadb_ledger_raft::event_writer::IngestionRateLimiter::new(
                        rate_limit,
                    ))),
                )
            } else {
                (None, None, None)
            };

            let events_service = EventsService::builder()
                .events_db(events_db)
                .applied_state(system.applied_state().clone())
                .page_token_codec(
                    inferadb_ledger_raft::pagination::PageTokenCodec::with_random_key(),
                )
                .maybe_event_config(event_config)
                .maybe_node_id(node_id)
                .maybe_ingestion_rate_limiter(ingestion_rate_limiter)
                .build();
            router = router.add_service(EventsServiceServer::with_interceptor(
                events_service,
                api_version_interceptor,
            ));
        }

        // Start periodic cleanup of stale rate limiter buckets to prevent
        // unbounded HashMap growth from departed clients/organizations.
        let _cleanup_handle = self.organization_rate_limiter.as_ref().map(|limiter| {
            limiter.start_cleanup_task(
                Duration::from_secs(60),  // run cleanup every 60 seconds
                Duration::from_secs(300), // remove buckets idle for 5 minutes
            )
        });

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

    /// Returns the multi-region Raft manager.
    #[must_use]
    pub fn manager(&self) -> &Arc<RaftManager> {
        &self.manager
    }

    /// Returns the idempotency cache.
    #[must_use]
    pub fn idempotency(&self) -> &Arc<IdempotencyCache> {
        &self.idempotency
    }

    /// Attaches backup support (backup manager + snapshot manager).
    ///
    /// Enables `CreateBackup`, `ListBackups`, and `RestoreBackup` RPCs on the
    /// admin service.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<inferadb_ledger_raft::backup::BackupManager>,
        snapshot_manager: Arc<inferadb_ledger_state::SnapshotManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self.snapshot_manager = Some(snapshot_manager);
        self
    }
}
