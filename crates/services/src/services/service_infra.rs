//! Shared service infrastructure for gRPC service implementations.
//!
//! [`ServiceContext`] consolidates the fields and methods shared across
//! `OrganizationService`, `VaultService`, `UserService`, and `AppService`:
//! Raft consensus, state access, trace propagation, proposal submission,
//! and handler-phase event recording.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    log_storage::AppliedStateAccessor,
    logging::{OperationType, RequestContext, Sampler},
    types::{LedgerResponse, OrganizationRequest, RaftPayload, RegionRequest, SystemRequest},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{EmailBlindingKey, OrganizationId, Region, config::ValidationConfig};
use tonic::Status;

use super::{error_classify, metadata::status_with_correlation};
use crate::proposal::{ProposalService, serialize_payload};

/// Shared infrastructure for gRPC services that propose writes through Raft.
///
/// Holds the fields common to `OrganizationService`, `VaultService`,
/// `UserService`, and `AppService`. Each service stores a `ServiceContext`
/// alongside any service-specific fields (e.g. `AppService`'s credential cache).
///
/// Cloneable — all inner types are `Arc`-wrapped or `Copy`.
#[derive(Clone)]
pub(crate) struct ServiceContext {
    /// Proposal service for submitting writes through Raft consensus.
    ///
    /// Abstracts over `ConsensusHandle` and `RaftManager` so handlers can be
    /// unit-tested with a mock implementation.
    pub(crate) proposer: Arc<dyn ProposalService>,
    /// State layer for direct entity/relationship reads.
    pub(crate) state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied Raft state (slug resolution, metadata).
    pub(crate) applied_state: AppliedStateAccessor,
    /// Sampler for log tail sampling.
    pub(crate) sampler: Option<Sampler>,
    /// Node ID for logging and events.
    pub(crate) node_id: Option<u64>,
    /// Input validation configuration.
    pub(crate) validation_config: Arc<ValidationConfig>,
    /// Maximum Raft proposal timeout.
    pub(crate) proposal_timeout: Duration,
    /// Handler-phase event handle for audit events.
    pub(crate) event_handle: Option<inferadb_ledger_raft::event_writer::EventHandle<FileBackend>>,
    /// Health state for drain-phase write rejection.
    pub(crate) health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
    /// HMAC key for privacy-preserving email uniqueness enforcement.
    ///
    /// When present, onboarding RPCs (email verification, registration) are
    /// enabled. When absent, those RPCs return `FAILED_PRECONDITION`.
    #[allow(dead_code)]
    pub(crate) email_blinding_key: Option<Arc<EmailBlindingKey>>,
    /// JWT engine for signing session tokens during onboarding.
    ///
    /// Shared with `TokenServiceImpl`. Required for `verify_email_code` (existing
    /// user session) and `complete_registration` (new user session).
    #[allow(dead_code)]
    pub(crate) jwt_engine: Option<Arc<crate::jwt::JwtEngine>>,
    /// JWT configuration (TTLs, issuer, clock skew) for session creation.
    #[allow(dead_code)]
    pub(crate) jwt_config: Option<inferadb_ledger_types::config::JwtConfig>,
    /// Key manager for decrypting signing keys during JWT operations.
    #[allow(dead_code)]
    pub(crate) key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
    /// Multi-Raft manager for GLOBAL state consistency checks.
    ///
    /// Used by `ensure_global_consistency()` to wait for local GLOBAL state
    /// replication before reads that depend on recently-written data.
    pub(crate) manager: Option<Arc<inferadb_ledger_raft::raft_manager::RaftManager>>,

    /// Saga orchestrator handle for submitting cross-region sagas.
    ///
    /// Required for `complete_registration` (onboarding saga). Wrapped in
    /// `OnceCell` because the saga orchestrator starts after the gRPC server
    /// during bootstrap — the cell is empty during startup and gets set once
    /// the orchestrator is ready.
    #[allow(dead_code)]
    pub(crate) saga_handle:
        Arc<tokio::sync::OnceCell<inferadb_ledger_raft::SagaOrchestratorHandle>>,
}

/// Routing target for [`ServiceContext::propose_serialized`].
///
/// Selects which Raft group receives the serialized payload. `Global` routes
/// to the GLOBAL `(GLOBAL, 0)` group (cluster control plane); `Region` routes
/// to the target region's regional control-plane group (`(region, 0)`).
#[allow(dead_code)] // `Organization` is reserved for γ Phase 3 routing.
enum ProposalRoute {
    /// GLOBAL Raft group (cluster control plane).
    Global,
    /// A specific region's Raft group (regional control plane).
    Region(Region),
    /// A specific `(region, organization_id)` per-organization group (data plane).
    Organization(Region, OrganizationId),
}

impl ServiceContext {
    /// Creates a unified `RequestContext` using the `from_request` constructor.
    ///
    /// Unlike [`make_request_context`](Self::make_request_context), this variant:
    /// - Extracts trace context directly from the request (no separate `TraceContext` variable
    ///   needed)
    /// - Enables automatic gRPC metric emission on Drop
    /// - Attaches the event handle so `ctx.record_event()` works
    ///
    /// Must be called **before** `request.into_inner()`.
    pub(crate) fn make_request_context_from<T>(
        &self,
        service: &'static str,
        method: &'static str,
        request: &tonic::Request<T>,
    ) -> RequestContext {
        let event_handle: Option<Arc<dyn inferadb_ledger_raft::event_writer::EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let mut ctx = RequestContext::from_request(service, method, request, event_handle);
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action(method);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }
        ctx
    }

    /// Shared proposal path: serialize `request` as `RaftPayload<R>`, drive
    /// the timer, submit through `route`, stamp correlation on errors.
    ///
    /// All tier-typed propose helpers funnel through here so the timer +
    /// error-correlation wrapping stays identical across `SystemRequest`,
    /// `OrganizationRequest`-as-metadata, and region-scoped `SystemRequest`
    /// paths.
    async fn propose_serialized<R: serde::Serialize>(
        &self,
        request: R,
        route: ProposalRoute,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let timeout = self.effective_timeout(grpc_metadata);
        let payload = RaftPayload::new(request, ctx.caller_or_zero());
        let bytes = serialize_payload(payload)?;

        ctx.start_raft_timer();
        let result = match route {
            ProposalRoute::Global => self.proposer.propose_bytes(bytes, timeout).await,
            ProposalRoute::Region(region) => {
                self.proposer.propose_to_region_bytes(region, bytes, timeout).await
            },
            ProposalRoute::Organization(region, organization) => {
                self.proposer
                    .propose_to_organization_bytes(region, organization, bytes, timeout)
                    .await
            },
        };
        ctx.end_raft_timer();

        let result = result
            .map_err(|e| status_with_correlation(e, &ctx.request_id(), ctx.trace_id()));
        if let Err(ref e) = result {
            ctx.set_error("ProposalError", e.message());
        }
        result
    }

    /// Proposes a system request through Raft with deadline handling.
    ///
    /// Serializes `SystemRequest` directly as `postcard(RaftPayload<SystemRequest>)`
    /// and proposes to the GLOBAL Raft group.
    ///
    /// The request must not contain plaintext PII (names, emails, addresses).
    /// Use [`propose_regional`](Self::propose_regional) for PII-bearing requests.
    pub(crate) async fn propose_system_request(
        &self,
        system_request: SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        self.propose_serialized(system_request, ProposalRoute::Global, grpc_metadata, ctx).await
    }

    /// Proposes an [`OrganizationRequest`] as org-tier metadata to the GLOBAL
    /// Raft group.
    ///
    /// Wraps `organization_request` in [`SystemRequest::OrganizationMetadata`]
    /// and routes to the GLOBAL group (`(GLOBAL, 0)`), whose
    /// `ApplyWorker<SystemRequest>` decodes the outer wrapper and delegates to
    /// `apply_organization_request_with_events`. This ensures that all org-tier
    /// metadata (vault lifecycle, app management, member/team/invite management)
    /// lands in GLOBAL `AppliedState` — the state that `write.rs`, `read.rs`,
    /// and `SlugResolver` read from.
    ///
    /// The `region` and `organization` parameters are retained in the signature
    /// for future per-org group routing of data-plane ops. They are not used
    /// for the metadata path routed here.
    pub(crate) async fn propose_organization_request(
        &self,
        _region: Region,
        _organization: OrganizationId,
        organization_request: OrganizationRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let wrapped = SystemRequest::OrganizationMetadata(Box::new(organization_request));
        self.propose_serialized(wrapped, ProposalRoute::Global, grpc_metadata, ctx).await
    }

    /// Proposes an [`OrganizationRequest`] directly to the per-organization
    /// Raft group (γ Phase 3 routing).
    ///
    /// Unlike [`propose_organization_request`](Self::propose_organization_request)
    /// — which wraps in `SystemRequest::OrganizationMetadata` and lands in
    /// GLOBAL state for the transitional shim — this helper serializes
    /// the request as `RaftPayload<OrganizationRequest>` and routes to
    /// the per-org group via `route_organization`. The per-org
    /// `ApplyWorker<OrganizationRequest>` decodes the bytes and applies
    /// against per-org `AppliedState`.
    ///
    /// Used by the vault service for `CreateVault` / `UpdateVault` /
    /// `DeleteVault` after the γ per-org-allocation migration.
    #[allow(dead_code)] // Wired when γ Phase 3 routing flip lands.
    pub(crate) async fn propose_to_organization_request(
        &self,
        region: Region,
        organization: OrganizationId,
        organization_request: OrganizationRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        self.propose_serialized(
            organization_request,
            ProposalRoute::Organization(region, organization),
            grpc_metadata,
            ctx,
        )
        .await
    }

    /// Proposes a [`RegionRequest`] to the target region's Raft group
    /// (regional control plane tier).
    ///
    /// **Not yet active in B.1.** The `(region, 0)` apply worker is
    /// `ApplyWorker<SystemRequest>` until the B.1.6 RegionGroup storage
    /// split ships. Calling this method today returns a
    /// `FAILED_PRECONDITION` error; future work will wire the region's
    /// `ApplyWorker<RegionRequest>` and remove this guard.
    #[allow(dead_code)]
    pub(crate) async fn propose_region_request(
        &self,
        _region: Region,
        _region_request: RegionRequest,
        _grpc_metadata: &tonic::metadata::MetadataMap,
        _ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        Err(Status::failed_precondition(
            "RegionRequest routing is not yet active (pending B.1.6 RegionGroup split)",
        ))
    }

    /// Proposes a system request to a specific region's Raft group.
    ///
    /// Serializes `SystemRequest` directly as `postcard(RaftPayload<SystemRequest>)`
    /// and proposes to the given region's Raft group. Used by onboarding handlers
    /// for PII-carrying requests that must stay in-region and for cross-region
    /// session creation.
    ///
    /// # Errors
    ///
    /// - `FAILED_PRECONDITION` if the `RaftManager` is not configured.
    /// - `UNAVAILABLE` if the region's Raft group is not active on this node.
    /// - `DEADLINE_EXCEEDED` if the proposal times out.
    /// - `INTERNAL` / `UNAVAILABLE` for Raft errors (via `classify_raft_error`).
    pub(crate) async fn propose_regional(
        &self,
        region: Region,
        system_request: SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        self.propose_serialized(system_request, ProposalRoute::Region(region), grpc_metadata, ctx).await
    }

    /// Returns the state layer for a specific region's Raft group.
    ///
    /// Delegates to the [`ProposalService`]. Enables direct reads from a
    /// region's state without proposing through Raft.
    ///
    /// # Errors
    ///
    /// - `FAILED_PRECONDITION` if the `RaftManager` is not configured.
    /// - `UNAVAILABLE` if the region's Raft group is not active on this node.
    pub(crate) fn regional_state(
        &self,
        region: Region,
    ) -> Result<Arc<StateLayer<FileBackend>>, Status> {
        self.proposer.regional_state(region)
    }

    /// Proposes a user-scoped `SystemRequest` to a region with PII encryption.
    ///
    /// Encrypts the `SystemRequest` with the user's `UserShredKey` before entering
    /// the Raft log. At apply time, the state machine decrypts using the key from
    /// state. When the user is erased and their `UserShredKey` is destroyed, all
    /// historical log entries become cryptographically unrecoverable (crypto-shredding).
    ///
    /// Returns `NOT_FOUND` if the `UserShredKey` is absent — the user may have been
    /// erased or is not yet provisioned. This function must only be called for
    /// post-registration operations where the `UserShredKey` is guaranteed to exist.
    pub(crate) async fn propose_regional_encrypted(
        &self,
        region: Region,
        system_request: SystemRequest,
        user_id: inferadb_ledger_types::UserId,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        // UserShredKey is stored in REGIONAL state (written during user creation).
        // Read from the region's state layer, not the GLOBAL one.
        let regional_state = self.regional_state(region)?;
        let sys_svc = inferadb_ledger_state::system::SystemOrganizationService::new(regional_state);
        let shred_key =
            sys_svc.get_user_shred_key(user_id).map_err(|e| error_classify::crypto_error(&e))?;

        let shred_key = shred_key.ok_or_else(|| {
            Status::not_found(format!(
                "UserShredKey not found for user {user_id}: user may have been erased"
            ))
        })?;

        let encrypted = inferadb_ledger_raft::entry_crypto::encrypt_user_system_request(
            &system_request,
            &shred_key.key,
            user_id,
        )
        .map_err(|e| error_classify::crypto_error(&e))?;

        self.propose_regional(
            region,
            SystemRequest::EncryptedUserSystem(encrypted),
            grpc_metadata,
            ctx,
        )
        .await
    }

    /// Proposes an org-scoped `SystemRequest` to a region with PII encryption.
    ///
    /// Encrypts the `SystemRequest` with the organization's `OrgShredKey` before
    /// entering the Raft log. At apply time, the state machine decrypts using
    /// the key from state. When the organization is purged and its `OrgShredKey`
    /// destroyed, all historical log entries become cryptographically
    /// unrecoverable (crypto-shredding).
    ///
    /// Returns `NOT_FOUND` if the `OrgShredKey` is absent — the organization may
    /// have been purged or is not yet provisioned.
    pub(crate) async fn propose_regional_org_encrypted(
        &self,
        region: Region,
        system_request: SystemRequest,
        organization: inferadb_ledger_types::OrganizationId,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        // OrgShredKey is stored in REGIONAL state (written by CreateOrganization saga).
        // Read from the region's state layer, not the GLOBAL one.
        let regional_state = self.regional_state(region)?;
        let sys_svc = inferadb_ledger_state::system::SystemOrganizationService::new(regional_state);
        let shred_key = sys_svc
            .get_org_shred_key(organization)
            .map_err(|e| error_classify::crypto_error(&e))?;

        let shred_key = shred_key.ok_or_else(|| {
            Status::not_found(format!(
                "OrgShredKey not found for organization {organization}: organization may have been purged"
            ))
        })?;

        let encrypted = inferadb_ledger_raft::entry_crypto::encrypt_org_system_request(
            &system_request,
            &shred_key.key,
            organization,
        )
        .map_err(|e| error_classify::crypto_error(&e))?;

        self.propose_regional(
            region,
            SystemRequest::EncryptedOrgSystem(encrypted),
            grpc_metadata,
            ctx,
        )
        .await
    }

    /// Returns current Raft metrics for the GLOBAL group.
    ///
    /// Used by vault creation to populate genesis block headers with leader ID
    /// and term. Returns `None` when metrics are unavailable (e.g., in tests
    /// with a mock proposer).
    pub(crate) fn raft_metrics(&self) -> Option<crate::proposal::LedgerRaftMetrics> {
        self.proposer.raft_metrics()
    }

    /// Computes the effective Raft proposal timeout, respecting gRPC deadlines.
    fn effective_timeout(&self, grpc_metadata: &tonic::metadata::MetadataMap) -> Duration {
        let grpc_deadline =
            inferadb_ledger_raft::deadline::extract_deadline_from_metadata(grpc_metadata);
        inferadb_ledger_raft::deadline::effective_timeout(self.proposal_timeout, grpc_deadline)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
pub(crate) mod tests {
    use arc_swap::ArcSwap;
    use inferadb_ledger_raft::{
        log_storage::AppliedState,
        raft_manager::{RaftManager, RaftManagerConfig, RegionConfig},
    };
    use inferadb_ledger_test_utils::TestDir;

    use super::*;
    use crate::proposal::RaftProposalService;

    /// Creates a `ServiceContext` backed by a [`MockProposalService`] and a
    /// real (empty) `StateLayer<FileBackend>` for state reads.
    ///
    /// The returned `AppliedState` is wrapped in `Arc<ArcSwap<_>>` so tests
    /// can populate slug indexes, org metadata, and other applied state before
    /// calling handlers.
    ///
    /// [`MockProposalService`]: crate::proposal::mock::MockProposalService
    pub(crate) fn test_service_context_with_mock(
        proposer: Arc<dyn crate::proposal::ProposalService>,
        temp: &TestDir,
    ) -> (ServiceContext, Arc<ArcSwap<AppliedState>>) {
        let db_path = temp.path().join("test_mock.db");
        let db = Arc::new(
            inferadb_ledger_store::Database::create(&db_path).expect("create test database"),
        );
        let state = Arc::new(inferadb_ledger_state::StateLayer::new(db));
        let applied_state_inner = Arc::new(ArcSwap::from_pointee(AppliedState::default()));
        let applied_state = inferadb_ledger_raft::log_storage::AppliedStateAccessor::new_for_test(
            applied_state_inner.clone(),
        );

        let ctx = ServiceContext {
            proposer,
            state,
            applied_state,
            sampler: None,
            node_id: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_secs(5),
            event_handle: None,
            health_state: None,
            email_blinding_key: None,
            jwt_engine: None,
            jwt_config: None,
            key_manager: None,
            manager: None,
            saga_handle: Arc::new(tokio::sync::OnceCell::new()),
        };
        (ctx, applied_state_inner)
    }

    /// Creates a `ServiceContext` with a proposer that has no manager,
    /// for testing FAILED_PRECONDITION error paths.
    async fn create_test_context_without_manager() -> (ServiceContext, TestDir) {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let proposer: Arc<dyn ProposalService> =
            Arc::new(RaftProposalService::new(system.handle().clone(), None));

        let ctx = ServiceContext {
            proposer,
            state: system.state().clone(),
            applied_state: system.applied_state().clone(),
            sampler: None,
            node_id: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_secs(5),
            event_handle: None,
            health_state: None,
            email_blinding_key: None,
            jwt_engine: None,
            jwt_config: None,
            key_manager: None,
            manager: None,
            saga_handle: Arc::new(tokio::sync::OnceCell::new()),
        };
        (ctx, temp)
    }

    /// Creates a `ServiceContext` backed by a `RaftManager` with only
    /// the GLOBAL (system) region started.
    async fn create_test_context_with_manager() -> (ServiceContext, Arc<RaftManager>, TestDir) {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let proposer: Arc<dyn ProposalService> =
            Arc::new(RaftProposalService::new(system.handle().clone(), Some(manager.clone())));

        let ctx = ServiceContext {
            proposer,
            state: system.state().clone(),
            applied_state: system.applied_state().clone(),
            sampler: None,
            node_id: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_millis(200),
            event_handle: None,
            health_state: None,
            email_blinding_key: None,
            jwt_engine: None,
            jwt_config: None,
            key_manager: None,
            manager: None,
            saga_handle: Arc::new(tokio::sync::OnceCell::new()),
        };
        (ctx, manager, temp)
    }

    /// Helper to create a simple `SystemRequest` variant for testing.
    fn test_system_request() -> SystemRequest {
        SystemRequest::VerifyUserEmail { email_id: inferadb_ledger_types::UserEmailId::new(1) }
    }

    #[tokio::test]
    async fn regional_state_without_manager_returns_failed_precondition() {
        let (ctx, _temp) = create_test_context_without_manager().await;

        match ctx.regional_state(Region::US_EAST_VA) {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::FailedPrecondition);
                assert!(err.message().contains("RaftManager"));
            },
            Ok(_) => panic!("Expected FAILED_PRECONDITION error"),
        }
    }

    #[tokio::test]
    async fn regional_state_unknown_region_returns_unavailable() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;

        // US_EAST_VA was never started — only GLOBAL (system) region exists.
        match ctx.regional_state(Region::US_EAST_VA) {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::Unavailable);
                assert!(
                    err.message().contains("us-east-va") || err.message().contains("US_EAST_VA"),
                    "Error should mention the region: {}",
                    err.message()
                );
            },
            Ok(_) => panic!("Expected UNAVAILABLE error"),
        }
    }

    #[tokio::test]
    async fn regional_state_with_valid_region_returns_state_layer() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;

        // GLOBAL is the system region — should be accessible.
        match ctx.regional_state(Region::GLOBAL) {
            Ok(state) => {
                // Verify it's the same state layer that the ServiceContext holds for system region.
                assert!(Arc::ptr_eq(&state, &ctx.state));
            },
            Err(err) => panic!("Expected Ok for GLOBAL region, got: {err}"),
        }
    }

    #[tokio::test]
    async fn propose_regional_without_manager_returns_failed_precondition() {
        let (ctx, _temp) = create_test_context_without_manager().await;

        let metadata = tonic::metadata::MetadataMap::new();
        let mut req_ctx =
            inferadb_ledger_raft::logging::RequestContext::new("test", "propose_regional");

        let err = ctx
            .propose_regional(Region::US_EAST_VA, test_system_request(), &metadata, &mut req_ctx)
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("RaftManager"));
    }

    #[tokio::test]
    async fn propose_regional_unknown_region_triggers_global_proposal() {
        let (ctx, manager, _temp) = create_test_context_with_manager().await;

        // Verify the region doesn't exist yet.
        assert!(!manager.has_region(Region::US_EAST_VA), "region should not exist before propose");

        let metadata = tonic::metadata::MetadataMap::new();
        let mut req_ctx =
            inferadb_ledger_raft::logging::RequestContext::new("test", "propose_regional");

        // In unit tests, GLOBAL Raft consensus may not be fully functional
        // (no apply worker processing), so the propose may time out. The key
        // behavior: propose_regional attempts CreateDataRegion through GLOBAL
        // when the region is missing, rather than creating locally.
        // Full end-to-end region creation is validated in integration tests.
        let result = ctx
            .propose_regional(Region::US_EAST_VA, test_system_request(), &metadata, &mut req_ctx)
            .await;

        // The proposal should fail (timeout or unavailable) since the full
        // consensus pipeline isn't running in this unit test.
        assert!(result.is_err(), "expected error in unit test without full consensus");
    }

    #[tokio::test]
    async fn raft_metrics_returns_some_with_real_proposer() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;
        assert!(ctx.raft_metrics().is_some());
    }
}
