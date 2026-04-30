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
//!
//! Inter-node Raft consensus RPCs are handled by
//! [`inferadb_ledger_raft::wire_consensus_transport::server_handler::WireRaftServerHandler`],
//! not by a service in this crate.

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use inferadb_ledger_raft::{
    idempotency::IdempotencyCache, raft_manager::RaftManager, rate_limit::RateLimiter,
    wire_consensus_transport::server_handler::WireRaftServerHandler,
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_wire_transport::{WireServer, auth::AuthVerifier, tls as wire_tls};

use crate::{
    jwt::JwtEngine,
    proposal::RaftProposalService,
    services::{
        AdminService, AppService, DiscoveryService, EventsService, HealthService,
        InvitationService, OrganizationService, ReadService, RegionResolver, RegionResolverService,
        SchemaServiceStub, TokenServiceImpl, UserService, VaultService, WriteService,
        service_infra::ServiceContext,
    },
    wire_server::LedgerWireDispatcher,
};

/// Selects which client-facing transport [`LedgerServer::serve`] binds.
///
/// Post-F.1.f.2 the wire transport is the only client-facing surface.
/// The enum is preserved as a single-variant placeholder so call sites
/// configuring `transport_kind = TransportKind::Wire` continue to compile;
/// future stages may collapse it into a unit type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransportKind {
    /// In-house QUIC wire transport. The bind requires a
    /// `LedgerServer::wire_tls_config`.
    #[default]
    Wire,
}

/// Bundled configuration for the optional `TokenService`.
///
/// Replaces three separate `Option` fields on `LedgerServer` that had to be
/// provided all-or-nothing. With this struct, partial configuration is
/// impossible — either the entire token service is configured or it isn't.
///
/// `Clone` is cheap — every field is either `Arc<_>` or already `Clone`.
/// Cloning is needed because `LedgerServer::build_services` reads the
/// config out of `&self` rather than consuming it.
#[derive(Clone)]
pub struct TokenServiceConfig {
    /// JWT engine for token signing and validation.
    pub jwt_engine: Arc<JwtEngine>,
    /// JWT configuration (token TTLs, issuer, clock skew).
    pub jwt_config: inferadb_ledger_types::config::JwtConfig,
    /// Region key manager for signing key envelope encryption.
    pub key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
}

/// Constructed bundle of all 13 service implementations + the schema stub.
///
/// Produced by `LedgerServer::build_services` and consumed by the tonic
/// transport in [`LedgerServer::serve`]. Each field holds an `Arc<_>` so a
/// future parallel transport bind (the in-house wire transport) can clone the
/// same handles into [`crate::wire_server::LedgerWireDispatcher`] without
/// duplicating the construction graph or competing for ownership.
///
/// Generic over `B` because [`EventsService<B>`] is the only service that
/// carries a storage-backend type parameter; every other service is
/// concrete. Production callers always instantiate `ServiceBundle<FileBackend>`.
///
/// `events` and `token` are `Option` because both services require runtime
/// configuration (`events_db` / `event_handle` for events; `token_service`
/// config for token) that may legitimately be absent — for example, a node
/// running without local events.db materialisation, or a deployment that
/// disables JWT issuance. The remaining services are unconditionally
/// constructed because their dependency graph (state layer + Raft manager)
/// is mandatory for any node that serves traffic.
pub struct ServiceBundle<B: inferadb_ledger_store::StorageBackend + Send + Sync + 'static> {
    /// `ReadService` — query operations.
    pub read: Arc<ReadService>,
    /// `WriteService` — transaction submission.
    pub write: Arc<WriteService>,
    /// `AdminService` — cluster ops, maintenance, backup/restore.
    pub admin: Arc<AdminService>,
    /// `OrganizationService` — organization lifecycle.
    pub organization: Arc<OrganizationService>,
    /// `VaultService` — vault lifecycle.
    pub vault: Arc<VaultService>,
    /// `UserService` — user lifecycle and email management.
    pub user: Arc<UserService>,
    /// `AppService` — organization-scoped client app management.
    pub app: Arc<AppService>,
    /// `InvitationService` — organization invitation lifecycle.
    pub invitation: Arc<InvitationService>,
    /// `HealthService` — gRPC health checks plus dependency probes.
    pub health: Arc<HealthService>,
    /// `DiscoveryService` — peer discovery + leader resolution.
    pub discovery: Arc<DiscoveryService>,
    /// `RegionResolverService` — region routing backing the
    /// [`crate::services::RegionResolver`] trait used by `Read`/`Write`.
    /// Held on the bundle so other transports can reach the same resolver
    /// without rebuilding it.
    pub region_resolver: Arc<RegionResolverService>,
    /// `SchemaServiceStub` — wire-only stub for `SchemaService`. Not
    /// registered with the tonic router (no proto traffic for schema
    /// management); held here so the wire transport can mount it.
    pub schema: Arc<SchemaServiceStub>,
    /// `TokenServiceImpl` — JWT lifecycle. `None` when JWT support is
    /// not configured (no `token_service` on the builder).
    pub token: Option<Arc<TokenServiceImpl>>,
    /// `EventsService<B>` — events query + ingestion. `None` when no
    /// `events_db` is configured.
    pub events: Option<Arc<EventsService<B>>>,
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
#[allow(dead_code)]
pub struct LedgerServer {
    /// The Raft manager containing all region groups.
    ///
    /// Routes requests to the correct region based on organization assignment.
    /// A single-region deployment has one region (GLOBAL).
    manager: Arc<RaftManager>,
    /// Idempotency cache for duplicate detection.
    #[builder(default = Arc::new(IdempotencyCache::new()))]
    idempotency: Arc<IdempotencyCache>,
    /// Server bind address (TCP).
    ///
    /// When `None`, no TCP listener is created. At least one of `addr` or
    /// `socket` must be set.
    #[builder(default)]
    addr: Option<SocketAddr>,
    /// Address other nodes should use to reach this node.
    ///
    /// Falls back to `addr` when not set. Used in `GetNodeInfo` and
    /// data region initial_members.
    #[builder(default)]
    advertise_addr: Option<String>,
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
    /// Snapshot manager for the internal Raft snapshot-install path.
    ///
    /// Note: this is **no longer wired into `AdminService`** — the
    /// operator-facing backup path moved to multi-DB archive format and
    /// no longer routes through the logical-snapshot pipeline. The
    /// field remains so callers that build snapshots elsewhere (e.g.
    /// peer replication) can hold onto a manager via the bootstrap
    /// path.
    #[builder(default)]
    snapshot_manager: Option<Arc<inferadb_ledger_state::SnapshotManager>>,
    /// Region key manager — supplies the local node's RMK fingerprint
    /// for stamping outgoing backup archives and validating incoming
    /// archives at restore-stage time.
    #[builder(default)]
    key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
    /// Root directory backup archives are written to and restored from.
    /// `{backups_dir}/backup-{id}.tar.zst`.
    #[builder(default)]
    backups_dir: Option<std::path::PathBuf>,
    /// Data directory for dependency health checks (disk writability).
    #[builder(default)]
    data_dir: Option<std::path::PathBuf>,
    /// Health check configuration for dependency validation.
    #[builder(default)]
    health_check_config: Option<inferadb_ledger_types::config::HealthCheckConfig>,
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
    /// Shared peer address map (node ID → network address).
    ///
    /// Passed to services that need to resolve peer addresses for forwarding
    /// requests (read, write, discovery, admin, health). Updated dynamically
    /// via `announce_peer` RPCs.
    #[builder(default)]
    peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
    /// GLOBAL consensus transport for dynamic peer channel management.
    ///
    /// Passed to `AdminService` so that `JoinCluster`/`LeaveCluster` can
    /// register and unregister gRPC channels for Raft replication.
    #[builder(default)]
    consensus_transport: Option<inferadb_ledger_raft::raft_manager::ConsensusTransportImpl>,
    /// Initialization signal sender for fresh (uninitialized) nodes.
    ///
    /// When present, `AdminService.init_cluster()` generates a cluster ID,
    /// persists it, and sends `Some(cluster_id)` through this channel to unblock
    /// bootstrap. On restart (node already initialized), this is `None` and the
    /// init handler returns `already_initialized = true`.
    #[builder(default)]
    init_sender: Option<Arc<tokio::sync::watch::Sender<Option<u64>>>>,
    /// Static cluster ID for already-initialized nodes.
    ///
    /// Set during bootstrap on the restart path. AdminService uses this to
    /// return `already_initialized` from `InitCluster` and to report the
    /// cluster ID in `GetNodeInfo`.
    #[builder(default)]
    cluster_id: Option<u64>,
    /// Per-node liveness timestamps for quorum-based dead node detection.
    ///
    /// Shared between the admin service (CheckPeerLiveness RPC), raft service
    /// (updates on incoming messages), and bootstrap liveness checker.
    #[builder(default)]
    peer_liveness:
        Option<Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>>,
    /// Path to a Unix domain socket for gRPC connections.
    ///
    /// When set without `addr`, the server listens on the Unix socket only.
    /// When both `addr` and `socket` are set, the server binds both TCP and
    /// UDS simultaneously.
    #[builder(default)]
    socket: Option<std::path::PathBuf>,
    /// HTTP/2 transport tuning (flow-control windows, max concurrent streams).
    ///
    /// Applied to `tonic::transport::Server::builder()` at startup so the
    /// values bind for the lifetime of the process. Defaults raise the
    /// per-stream window to 2 MiB and the per-connection window to 8 MiB —
    /// the tonic / hyper defaults of 64 KiB stall multiplexed traffic on
    /// loopback once 256+ concurrent streams share a connection.
    #[builder(default)]
    http2_config: inferadb_ledger_types::config::Http2Config,
    /// Selects which transport [`Self::serve`] binds. Defaults to
    /// [`TransportKind::Grpc`] for backwards compatibility — flipping to
    /// [`TransportKind::Wire`] also requires supplying [`Self::wire_tls_config`].
    /// See [`TransportKind`] for migration context.
    #[builder(default)]
    transport_kind: TransportKind,
    /// Pre-built rustls server config used when `transport_kind ==
    /// TransportKind::Wire`. The wire transport drives QUIC on a UDP
    /// socket bound to [`Self::addr`]; rustls / TLS 1.3 is non-optional
    /// (QUIC requires it). Bootstrap is responsible for constructing this
    /// from the same cert / key pair the legacy tonic path consumes — F.1.b
    /// itself ships the runtime plumbing, not the cert-rotation policy.
    /// `None` is fine in `Grpc` mode; in `Wire` mode it is a runtime error.
    #[builder(default)]
    wire_tls_config: Option<rustls::ServerConfig>,
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

    /// Constructs every service implementation needed by both the tonic
    /// transport (`serve`) and any future parallel transport bind (the
    /// in-house wire transport).
    ///
    /// Reads from `&self` rather than consuming so the surrounding
    /// transport plumbing in `serve` (HTTP/2 keepalive, listener bind,
    /// shutdown wiring, rate-limiter cleanup task) can keep working
    /// against the original `LedgerServer` fields without a second
    /// destructure. Services hold `Arc<_>` clones internally; the bundle
    /// itself wraps each service in `Arc<_>` so a future wire transport
    /// can hand the same service handles to
    /// [`crate::wire_server::LedgerWireDispatcher`] without rebuilding the
    /// graph.
    ///
    /// Construction order is fixed by the dependency arrows:
    ///
    /// 1. `RegionResolverService` — depended on by `Read`/`Write`.
    /// 2. `ProposalService` — depended on by `Write` plus the four `ServiceContext`-using services
    ///    (`Organization`/`Vault`/`User`/`App`).
    /// 3. `Read`, `Write` — independent of `ServiceContext`.
    /// 4. `Admin` — depends on the system region's handle/state/applied_state.
    /// 5. `ServiceContext` — composed once and cloned into the four org-scoped services.
    /// 6. `Organization`/`Vault`/`User`/`App`, `Token` (optional), `Invitation` — `ServiceContext`
    ///    consumers.
    /// 7. `Health`, `Discovery`, `Raft` — system-region-only services.
    /// 8. `Events` (optional) — depends on the proposer captured before `ServiceContext` is moved
    ///    into `App`.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` when the `RaftManager` has not started its
    /// system region yet (every service in the bundle needs the system
    /// region's `state` / `applied_state` / `handle`).
    fn build_services(&self) -> Result<ServiceBundle<FileBackend>, Box<dyn std::error::Error>> {
        // Extract system region for services that need direct Raft/state access
        // (admin, health, discovery operate on the system region).
        let system = self.manager.system_region().map_err(|e| {
            Box::new(std::io::Error::other(format!("System region not available: {e}")))
                as Box<dyn std::error::Error>
        })?;

        // Build region resolver from the manager — routes requests to the
        // correct region based on organization assignment.
        let region_resolver = Arc::new(RegionResolverService::new(self.manager.clone()));
        let resolver: Arc<dyn RegionResolver> = region_resolver.clone();

        // Shared counter for active WatchBlocks streams. Owned by the
        // ReadService — the bundle does not surface it because it's an
        // implementation detail of the read-streaming hot path.
        let active_watch_streams = Arc::new(AtomicUsize::new(0));

        // ReadService — independent of ServiceContext.
        let read_service = ReadService::builder()
            .resolver(resolver.clone())
            .manager(Some(self.manager.clone()))
            .active_streams(active_watch_streams)
            .max_streams(self.max_watch_streams)
            .peer_addresses(self.peer_addresses.clone())
            .registry(Some(self.manager.registry()))
            .build();

        // Build the shared `ProposalService` early so both `WriteService` and
        // the downstream `ServiceContext` can hold the same `Arc`. Both paths
        // route per-vault proposals through `propose_organization_request_to_vault`.
        let proposer: Arc<dyn crate::proposal::ProposalService> =
            Arc::new(RaftProposalService::new(system.handle().clone(), Some(self.manager.clone())));

        // WriteService — uses resolver + proposer. Batch writers are per-region
        // (created by RaftManager::start_region), not constructed here.
        let mut write_service = WriteService::builder()
            .resolver(resolver.clone())
            .manager(Some(self.manager.clone()))
            .proposal_service(proposer.clone())
            .idempotency(self.idempotency.clone())
            .proposal_timeout(self.proposal_timeout)
            .peer_addresses(self.peer_addresses.clone())
            .build()
            .with_health_state(self.health_state.clone());
        if let Some(ref limiter) = self.organization_rate_limiter {
            write_service = write_service.with_rate_limiter(limiter.clone());
        }
        if let Some(ref detector) = self.hot_key_detector {
            write_service = write_service.with_hot_key_detector(detector.clone());
        }
        if let Some(ref handle) = self.event_handle {
            write_service = write_service.with_event_handle(handle.clone());
        }

        // AdminService — operates on the system region.
        let mut admin_service = AdminService::builder()
            .handle(system.handle().clone())
            .state(system.state().clone())
            .applied_state(system.applied_state().clone())
            .block_archive(Some(system.block_archive().clone()))
            .advertise_addr(self.advertise_addr.clone().unwrap_or_else(|| {
                self.socket
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned())
                    .or_else(|| self.addr.map(|a| a.to_string()))
                    .unwrap_or_default()
            }))
            .proposal_timeout(self.proposal_timeout)
            .peer_addresses(self.peer_addresses.clone())
            .build()
            .with_raft_manager(self.manager.clone());
        if let Some(ref transport) = self.consensus_transport {
            admin_service = admin_service.with_consensus_transport(transport.clone());
        }
        // Wire runtime config handle into admin service for UpdateConfig/GetConfig RPCs.
        // Pass the rate limiter and hot key detector so config changes propagate to them.
        let admin_service = if let Some(ref handle) = self.runtime_config {
            admin_service.with_runtime_config(
                handle.clone(),
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
        // The snapshot-based logical-backup path was removed — the new
        // archive path consumes only the BackupManager, plus the
        // RegionKeyManager and backups directory wired via
        // `with_key_manager` / `with_backups_dir` below.
        let admin_service = if let Some(ref backup_mgr) = self.backup_manager {
            admin_service.with_backup(backup_mgr.clone())
        } else {
            admin_service
        };
        let admin_service = if let Some(ref km) = self.key_manager {
            admin_service.with_key_manager(km.clone())
        } else {
            admin_service
        };
        let admin_service = if let Some(ref dir) = self.backups_dir {
            admin_service.with_backups_dir(dir.clone())
        } else {
            admin_service
        };
        // Wire health state for drain-phase write rejection
        let admin_service = admin_service.with_health_state(self.health_state.clone());
        // Wire init sender and data_dir for fresh-node initialization via InitCluster RPC.
        let admin_service = if let Some(ref sender) = self.init_sender {
            admin_service.with_init_sender(sender.clone(), self.data_dir.clone())
        } else {
            admin_service
        };
        // Wire static cluster_id for already-initialized nodes (restart path).
        let admin_service = if let Some(cid) = self.cluster_id {
            admin_service.with_cluster_id(cid)
        } else {
            admin_service
        };
        // Wire peer liveness map for CheckPeerLiveness RPC (quorum-based dead node detection).
        let admin_service = if let Some(ref liveness) = self.peer_liveness {
            admin_service.with_peer_liveness(liveness.clone())
        } else {
            admin_service
        };

        // Build shared service context for Organization, Vault, User, and App services.
        // All four share the same proposal path, state, applied_state, and config —
        // ServiceContext consolidates these into a single clonable struct.
        let svc_ctx = ServiceContext {
            proposer: proposer.clone(),
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
            email_blinding_key: self.email_blinding_key.clone(),
            jwt_engine: self.token_service.as_ref().map(|ts| ts.jwt_engine.clone()),
            jwt_config: self.token_service.as_ref().map(|ts| ts.jwt_config.clone()),
            key_manager: self.token_service.as_ref().map(|ts| ts.key_manager.clone()),
            manager: Some(self.manager.clone()),
            saga_handle: self.saga_handle.clone(),
            signing_key_cache: self.manager.signing_key_cache(),
        };

        let organization_service = OrganizationService::new(svc_ctx.clone());
        let vault_service = VaultService::new(svc_ctx.clone());
        let user_service = UserService::new(svc_ctx.clone());

        // TokenService is optional — only built when JWT support is configured.
        // `TokenServiceConfig` derives Clone (its three fields are all
        // `Arc<_>` / `Clone`) so we can read it out of `&self` rather than
        // consuming.
        let token_service = self.token_service.as_ref().map(|ts| {
            let mut svc = TokenServiceImpl::new(
                svc_ctx.clone(),
                ts.jwt_engine.clone(),
                ts.jwt_config.clone(),
                ts.key_manager.clone(),
            );
            if let Some(ref limiter) = self.organization_rate_limiter {
                svc = svc.with_rate_limiter(limiter.clone());
            }
            svc
        });

        let invitation_service = InvitationService::new(svc_ctx.clone());
        // Capture a proposer handle for EventsService before moving svc_ctx.
        let events_proposer = svc_ctx.proposer.clone();
        let app_service = AppService::new(svc_ctx);

        // HealthService — system-region-only.
        let health_service = HealthService::new(
            system.handle().clone(),
            system.state().clone(),
            system.applied_state().clone(),
            self.health_state.clone(),
        )
        .with_manager(self.manager.clone());
        // Attach dependency health checker if data_dir is provided
        let health_service = if let Some(ref data_dir) = self.data_dir {
            let config = self.health_check_config.clone().unwrap_or_default();
            let mut checker = inferadb_ledger_raft::dependency_health::DependencyHealthChecker::new(
                system.handle().clone(),
                data_dir.clone(),
                config,
            );
            // Peer reachability now reads the in-process `peer_liveness`
            // map populated by the Raft request path on every successful
            // inbound consensus envelope. Transport-agnostic — works for
            // both the legacy gRPC `RaftService` and the wire-protocol
            // `WireRaftServerHandler`. The legacy out-of-band TCP probe
            // was unsuited to the wire transport (QUIC/UDP) and has been
            // removed.
            if let Some(ref peer_liveness) = self.peer_liveness {
                checker = checker.with_peer_liveness(peer_liveness.clone());
            }
            health_service.with_dependency_checker(checker)
        } else {
            health_service
        };

        let discovery_service = DiscoveryService::builder()
            .handle(system.handle().clone())
            .state(system.state().clone())
            .applied_state(system.applied_state().clone())
            .region(self.region)
            .raft_manager(Some(self.manager.clone()))
            .peer_addresses(self.peer_addresses.clone())
            .build();

        // EventsService is optional — only registered when events_db is provided.
        let events_service = self.events_db.as_ref().map(|events_db| {
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

            // Resolver for per-vault events stores. `ListEvents` fans out
            // across the org-level store and every per-vault store
            // registered for the target organization. The resolver walks
            // the manager's vault-group registry and returns the local
            // node's events.db handle for each `(region, org_id, *)`
            // triple — vault groups not hosted on this node are filtered
            // out implicitly because they are absent from `vault_groups`.
            let manager_for_sources = self.manager.clone();
            let vault_event_sources: crate::services::VaultEventSources<FileBackend> =
                Arc::new(move |target_org| {
                    let mut out = Vec::new();
                    for (region, org_id, vault_id) in manager_for_sources.list_vault_groups() {
                        if org_id != target_org {
                            continue;
                        }
                        let Ok(group) =
                            manager_for_sources.get_vault_group(region, org_id, vault_id)
                        else {
                            continue;
                        };
                        let Some(writer) = group.event_writer() else {
                            continue;
                        };
                        out.push(crate::services::VaultEventSource {
                            vault_id,
                            events_db: (**writer.events_db()).clone(),
                        });
                    }
                    out
                });

            EventsService::builder()
                .events_db(events_db.clone())
                .applied_state(system.applied_state().clone())
                .page_token_codec(
                    inferadb_ledger_raft::pagination::PageTokenCodec::with_random_key(),
                )
                .maybe_event_config(event_config)
                .maybe_node_id(node_id)
                .maybe_ingestion_rate_limiter(ingestion_rate_limiter)
                // Route IngestEvents through REGIONAL Raft.
                .proposer(events_proposer.clone())
                .manager(self.manager.clone())
                .vault_event_sources(vault_event_sources)
                .proposal_timeout(self.proposal_timeout)
                .build()
        });

        Ok(ServiceBundle {
            read: Arc::new(read_service),
            write: Arc::new(write_service),
            admin: Arc::new(admin_service),
            organization: Arc::new(organization_service),
            vault: Arc::new(vault_service),
            user: Arc::new(user_service),
            app: Arc::new(app_service),
            invitation: Arc::new(invitation_service),
            health: Arc::new(health_service),
            discovery: Arc::new(discovery_service),
            region_resolver,
            schema: Arc::new(SchemaServiceStub),
            token: token_service.map(Arc::new),
            events: events_service.map(Arc::new),
        })
    }

    /// Construct a [`LedgerWireDispatcher`] from a [`ServiceBundle`].
    ///
    /// Mirrors the service-graph composition in [`Self::serve`]'s tonic
    /// branch: the same 13 service `Arc`s the tonic router registers also
    /// hand off to the wire dispatcher, and the dispatcher picks up a
    /// [`WireRaftServerHandler`] so inter-node Raft frames route through
    /// the same `RaftManager`. F.1.b uses this method to bring up the wire
    /// transport alongside tonic; F.1.d will keep it as the only path.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` when the bundle is missing the `token` or
    /// `events` service. The wire dispatcher accepts those as required
    /// `Arc<_>` fields (no `Option`); production deployments that disable
    /// JWT or events on the tonic side cannot currently bind the wire
    /// transport. Callers that genuinely need the wire transport without
    /// one of these services should defer to F.1.c, which threads
    /// optionality through the dispatcher.
    pub fn build_wire_dispatcher(
        bundle: &ServiceBundle<FileBackend>,
        raft_manager: Arc<RaftManager>,
        peer_liveness: Option<
            Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
        >,
    ) -> Result<LedgerWireDispatcher<FileBackend>, Box<dyn std::error::Error>> {
        let mut raft_handler = WireRaftServerHandler::new(raft_manager);
        if let Some(liveness) = peer_liveness {
            raft_handler = raft_handler.with_peer_liveness(liveness);
        }
        let raft_handler = Arc::new(raft_handler);
        let token = bundle.token.as_ref().cloned().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "token service required for wire transport (build_wire_dispatcher)",
            )) as Box<dyn std::error::Error>
        })?;
        let events = bundle.events.as_ref().cloned().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "events service required for wire transport (build_wire_dispatcher)",
            )) as Box<dyn std::error::Error>
        })?;
        Ok(LedgerWireDispatcher::new(
            bundle.health.clone(),
            bundle.write.clone(),
            bundle.read.clone(),
            bundle.vault.clone(),
            bundle.organization.clone(),
            bundle.user.clone(),
            bundle.app.clone(),
            bundle.invitation.clone(),
            token,
            events,
            bundle.admin.clone(),
            bundle.discovery.clone(),
        )
        .with_raft_handler(raft_handler))
    }

    /// Starts the configured transport.
    ///
    /// Blocks until the server is shut down. If a `shutdown_rx`
    /// was provided via the builder, the server will stop when the signal
    /// is received. Otherwise, it blocks indefinitely.
    ///
    /// Branches on [`Self::transport_kind`]: the default
    /// [`TransportKind::Grpc`] preserves the existing tonic + HTTP/2
    /// behaviour; [`TransportKind::Wire`] binds the in-house QUIC wire
    /// transport instead. The wire path requires a [`Self::wire_tls_config`]
    /// and (currently, F.1.b) the `permissive-wire-auth` feature for the
    /// stub verifier — F.1.c replaces the verifier with the production
    /// `JwtAuthVerifier`.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind to the configured address
    /// or encounters a transport-level error during operation.
    #[allow(clippy::disallowed_methods)] // tokio::try_join! macro internally uses .unwrap()
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        self.serve_wire().await
    }

    /// In-house QUIC wire transport bind path.
    ///
    /// Composes the [`ServiceBundle`], hands it to
    /// [`Self::build_wire_dispatcher`] to produce a [`LedgerWireDispatcher`]
    /// (wired with a [`WireRaftServerHandler`]), and binds a [`WireServer`]
    /// to the configured TCP `addr` (re-used as a UDP bind target for QUIC).
    ///
    /// The `socket` (Unix domain) and `http2_config` settings are
    /// tonic-specific and ignored in wire mode — the wire transport is
    /// QUIC-over-UDP only. The `concurrency_limit` / `load_shed` /
    /// `timeout` tower layers live inside the tonic stack and are not
    /// translated; per-connection backpressure on the wire path comes
    /// from QUIC stream caps in
    /// [`inferadb_ledger_wire_transport::tls`] (1024 concurrent bidi
    /// streams per connection, 0 uni — the symmetric server-side cap).
    ///
    /// # Verifier selection
    ///
    /// - Under `cfg(test)` or the `permissive-wire-auth` feature, the
    ///   [`crate::services::permissive_verifier::PermissiveVerifier`] stub accepts every credential
    ///   — regardless of whether [`Self::token_service`] is configured. This lets test fixtures
    ///   wire `token_service` (so `LedgerWireDispatcher` accepts the bundle) while still accepting
    ///   empty `auth_payload`s on the test `WireClient` — the production `JwtAuthVerifier` would
    ///   otherwise reject them and hang the RPC. Production builds DO NOT enable the feature, so
    ///   the production path below is the only reachable arm.
    /// - In production builds (no `permissive-wire-auth`, not `cfg(test)`): when
    ///   [`Self::token_service`] is `Some(_)`, F.1.c's production
    ///   [`crate::services::JwtAuthVerifier`] wraps the configured [`crate::jwt::JwtEngine`]. When
    ///   `token_service` is `None`, bind fails with a runtime error rather than silently accepting
    ///   traffic.
    async fn serve_wire(self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting Ledger wire-transport server (QUIC)");

        let addr = self.addr.ok_or_else(|| {
            Box::new(std::io::Error::other(
                "wire transport requires --listen <addr>; UDS is not supported on the wire path",
            )) as Box<dyn std::error::Error>
        })?;

        let crypto = self.wire_tls_config.clone().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "wire transport requires a rustls ServerConfig; \
                 set LedgerServer::wire_tls_config before calling serve()",
            )) as Box<dyn std::error::Error>
        })?;

        // Test / dev fast path: when `permissive-wire-auth` is enabled
        // (or under `cfg(test)`), accept every credential unconditionally.
        // Tests that wire `token_service` for the dispatcher's sake
        // would otherwise be rejected by `JwtAuthVerifier`'s empty-token
        // check; the permissive arm below decouples dispatcher
        // configuration from test client auth.
        #[cfg(any(test, feature = "permissive-wire-auth"))]
        {
            if self.token_service.is_some() {
                tracing::warn!(
                    "wire transport starting with PermissiveVerifier (test/permissive-wire-auth \
                     feature override) even though token_service IS configured. Production builds \
                     do NOT enable this feature — JwtAuthVerifier wins on those builds."
                );
            } else {
                tracing::warn!(
                    "wire transport starting with PermissiveVerifier — token_service is not \
                     configured. This is acceptable for tests / local dev only."
                );
            }
            let verifier =
                Arc::new(crate::services::permissive_verifier::PermissiveVerifier::new());
            // `return` is required here so the production `cfg` arm below stays
            // unreachable under `cfg(test)` / `permissive-wire-auth`.
            #[allow(clippy::needless_return)]
            return Self::run_wire_server(addr, crypto, verifier, self).await;
        }

        // Production path. Compiler warns about unreachable arms when
        // both `cfg(test)` and `permissive-wire-auth` are off — the
        // early-return above eats every code path under the feature.
        #[cfg(not(any(test, feature = "permissive-wire-auth")))]
        {
            if let Some(ref token) = self.token_service {
                let verifier =
                    Arc::new(crate::services::JwtAuthVerifier::new(token.jwt_engine.clone()));
                Self::run_wire_server(addr, crypto, verifier, self).await
            } else {
                Err(Box::new(std::io::Error::other(
                    "wire transport requires `token_service` to be configured for the production \
                     JwtAuthVerifier; or build with the `permissive-wire-auth` feature for the \
                     test-only stub",
                )))
            }
        }
    }

    /// Inner wire-server run loop. Generic over the [`AuthVerifier`] impl
    /// so both the production [`crate::services::JwtAuthVerifier`] and
    /// the (cfg-gated) [`crate::services::permissive_verifier::PermissiveVerifier`]
    /// share a single bind/shutdown path.
    async fn run_wire_server<V>(
        addr: SocketAddr,
        crypto: rustls::ServerConfig,
        verifier: Arc<V>,
        server: Self,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        V: AuthVerifier + 'static,
    {
        let manager = server.manager.clone();
        let shutdown_rx = server.shutdown_rx.clone();
        let peer_liveness = server.peer_liveness.clone();
        let bundle = server.build_services()?;
        let dispatcher = Arc::new(Self::build_wire_dispatcher(&bundle, manager, peer_liveness)?);
        let quic_config = wire_tls::server_config(crypto);

        let wire_server = WireServer::bind(addr, quic_config, verifier, dispatcher)
            .map_err(|e| Box::new(std::io::Error::other(format!("wire bind failed: {e}"))))?;
        let bound = wire_server.local_addr().ok();
        tracing::info!(
            requested = %addr,
            bound = ?bound,
            "wire-transport server listening (QUIC over UDP)",
        );

        if let Some(mut rx) = shutdown_rx {
            let _ = rx.wait_for(|v| *v).await;
            tracing::info!("wire-transport shutdown signalled, draining connections");
            let outcome = wire_server
                .shutdown(Duration::from_secs(30), 0u32.into(), b"graceful shutdown")
                .await;
            tracing::info!(?outcome, "wire-transport shutdown complete");
        } else {
            // No shutdown signal wired in. Park the task; `WireServer`'s
            // accept loop runs in the background until the process exits.
            std::future::pending::<()>().await;
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
    /// admin service. The snapshot manager is held so the internal
    /// Raft snapshot-install path keeps working; it is no longer used
    /// by the operator-facing backup RPCs.
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

    /// Attaches the region key manager — required so the
    /// multi-DB-archive backup path can stamp outgoing archives with
    /// the local RMK fingerprint and pre-flight incoming archives at
    /// restore-stage time.
    #[must_use]
    pub fn with_key_manager(
        mut self,
        key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    ) -> Self {
        self.key_manager = Some(key_manager);
        self
    }

    /// Attaches the root directory backup archives live in.
    ///
    /// Mirrors the path the [`BackupManager`](inferadb_ledger_raft::backup::BackupManager)
    /// writes through; held here so `restore_backup` can resolve a
    /// `backup_id` to an archive path without round-tripping through
    /// the manager.
    #[must_use]
    pub fn with_backups_dir(mut self, backups_dir: std::path::PathBuf) -> Self {
        self.backups_dir = Some(backups_dir);
        self
    }

    /// Attaches the rustls server config used by the wire transport.
    ///
    /// Required when [`Self::transport_kind`] is [`TransportKind::Wire`];
    /// ignored otherwise. The cert / key the caller bakes into the rustls
    /// config is the wire transport's TLS identity — separate from the
    /// (currently empty) tonic-side TLS surface, but operators are expected
    /// to derive both from the same on-disk PEM material in production.
    #[must_use]
    pub fn with_wire_tls_config(mut self, crypto: rustls::ServerConfig) -> Self {
        self.wire_tls_config = Some(crypto);
        self
    }

    /// Returns the configured transport for inspection / smoke tests.
    #[must_use]
    pub fn transport_kind(&self) -> TransportKind {
        self.transport_kind
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    //! F.1.b unit tests focus on the dispatcher composition + transport
    //! selection wiring. Standing up a full `LedgerServer` (real
    //! `RaftManager`, real `StateLayer`, full bundle) is heavy; the
    //! dispatcher-from-bundle code path is exercised by the type system
    //! plus the small subset of fields the build_wire_dispatcher helper
    //! actually reaches for.
    //!
    //! End-to-end coverage (real wire bind, real RPC round-trip) lands in
    //! F.1.d's default-flip CI run, which exercises this branch through
    //! TestCluster-backed integration tests.
    use super::*;

    #[test]
    fn transport_kind_default_is_wire() {
        // Post-F.1.f.2: wire is the only transport.
        assert_eq!(TransportKind::default(), TransportKind::Wire);
    }

    /// `LedgerServer::build_wire_dispatcher` is generic over `B = FileBackend`
    /// concretely; this is a compile-only proof that the signature lines up
    /// with the F.1.b spec. Real construction needs the full service graph
    /// and is deferred to F.1.d.
    #[test]
    fn build_wire_dispatcher_signature_matches_spec() {
        // We can't call the function here without standing up a real
        // `ServiceBundle` and `RaftManager`. The bound check below is the
        // strongest static guarantee: it forces the compiler to confirm
        // the signature shape exactly.
        fn _assert_signature(
            bundle: &ServiceBundle<FileBackend>,
            mgr: Arc<RaftManager>,
            peer_liveness: Option<
                Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
            >,
        ) -> Result<LedgerWireDispatcher<FileBackend>, Box<dyn std::error::Error>> {
            LedgerServer::build_wire_dispatcher(bundle, mgr, peer_liveness)
        }
        let _ = _assert_signature
            as fn(
                &ServiceBundle<FileBackend>,
                Arc<RaftManager>,
                Option<Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>>,
            )
                -> Result<LedgerWireDispatcher<FileBackend>, Box<dyn std::error::Error>>;
    }

    /// TLS bridge smoke test: feed a self-signed cert through the same
    /// `tls::server_config` path `serve_wire` uses and assert the
    /// `quinn::ServerConfig` builds without panicking. This exercises the
    /// rustls-provider install + ALPN-override + QUIC-config conversion
    /// at the same call site `serve_wire` does, just without the
    /// surrounding `LedgerServer` graph.
    #[cfg(any(test, feature = "permissive-wire-auth"))]
    #[test]
    fn wire_tls_bridge_builds_quic_config() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("rcgen self-signed");
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        let crypto = inferadb_ledger_wire_transport::tls::rustls_server_crypto(chain, key)
            .expect("rustls_server_crypto");

        // The exact call shape used in `serve_wire`:
        let _quic_cfg = inferadb_ledger_wire_transport::tls::server_config(crypto);
    }
}
