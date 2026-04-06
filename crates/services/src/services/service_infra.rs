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
    trace_context,
    types::{LedgerRequest, LedgerResponse, SystemRequest},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    EmailBlindingKey, Region, config::ValidationConfig, events::EventEntry,
};
use tonic::Status;

use crate::proposal::ProposalService;

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
    /// Abstracts over `openraft::Raft` and `RaftManager` so handlers can be
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

impl ServiceContext {
    /// Creates a `RequestContext` for a service method.
    ///
    /// Populates operation type, transport metadata, trace context, and node ID.
    /// The caller should use `extract_caller` separately to set the user slug
    /// from the proto request's `UserSlug caller` field.
    pub(crate) fn make_request_context(
        &self,
        service: &'static str,
        method: &'static str,
        grpc_metadata: &tonic::metadata::MetadataMap,
        trace_ctx: &trace_context::TraceContext,
    ) -> RequestContext {
        let mut ctx = RequestContext::new(service, method);
        ctx.set_admin_action(method);
        self.fill_context(&mut ctx, grpc_metadata, trace_ctx);
        ctx
    }

    /// Fills common fields on a `RequestContext` from gRPC metadata and trace context.
    pub(crate) fn fill_context(
        &self,
        ctx: &mut RequestContext,
        grpc_metadata: &tonic::metadata::MetadataMap,
        trace_ctx: &trace_context::TraceContext,
    ) {
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(grpc_metadata);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );
    }

    /// Proposes a `LedgerRequest` through Raft with deadline handling.
    ///
    /// Delegates to the [`ProposalService`] and records timing on the
    /// `RequestContext`. Error classification (leadership → `UNAVAILABLE`,
    /// timeout → `DEADLINE_EXCEEDED`) is handled by the proposer.
    pub(crate) async fn propose_request(
        &self,
        request: LedgerRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let timeout = self.effective_timeout(grpc_metadata);

        ctx.start_raft_timer();
        let result = self.proposer.propose(request, ctx.caller_or_zero(), timeout).await;
        ctx.end_raft_timer();

        if let Err(ref e) = result {
            ctx.set_error("ProposalError", e.message());
        }
        result
    }

    /// Proposes a system request through Raft with deadline handling.
    ///
    /// Wraps the `SystemRequest` as `LedgerRequest::System` and delegates
    /// to `propose_request`. Proposals go to the GLOBAL Raft group.
    ///
    /// The request must not contain plaintext PII (names, emails, addresses).
    /// Use [`propose_regional`](Self::propose_regional) for PII-bearing requests.
    pub(crate) async fn propose_system_request(
        &self,
        system_request: SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        self.propose_request(LedgerRequest::System(system_request), grpc_metadata, ctx).await
    }

    /// Proposes a system request to a specific region's Raft group.
    ///
    /// Wraps the `SystemRequest` as `LedgerRequest::System` and delegates to
    /// the [`ProposalService`]. Used by onboarding handlers for PII-carrying
    /// requests that must stay in-region and for cross-region session creation.
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
        let timeout = self.effective_timeout(grpc_metadata);
        let request = LedgerRequest::System(system_request);

        ctx.start_raft_timer();
        let result =
            self.proposer.propose_to_region(region, request, ctx.caller_or_zero(), timeout).await;
        ctx.end_raft_timer();

        if let Err(ref e) = result {
            ctx.set_error("ProposalError", e.message());
        }
        result
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

    /// Proposes a top-level `LedgerRequest` to a specific region's Raft group.
    ///
    /// Unlike [`propose_regional`](Self::propose_regional) which wraps a
    /// `SystemRequest` into `LedgerRequest::System`, this method accepts any
    /// `LedgerRequest` variant directly. Used for `CreateRefreshToken` proposals
    /// during cross-region session creation (e.g., existing user login via onboarding).
    #[allow(dead_code)]
    pub(crate) async fn propose_regional_ledger_request(
        &self,
        region: Region,
        request: LedgerRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let timeout = self.effective_timeout(grpc_metadata);

        ctx.start_raft_timer();
        let result =
            self.proposer.propose_to_region(region, request, ctx.caller_or_zero(), timeout).await;
        ctx.end_raft_timer();

        if let Err(ref e) = result {
            ctx.set_error("ProposalError", e.message());
        }
        result
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
        let shred_key = sys_svc.get_user_shred_key(user_id).map_err(|e| {
            tracing::error!(error = %e, user_id = %user_id, "Failed to read UserShredKey");
            Status::internal("Internal error")
        })?;

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
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to encrypt Raft entry");
            Status::internal("Internal error")
        })?;

        self.propose_regional_ledger_request(
            region,
            LedgerRequest::EncryptedUserSystem(encrypted),
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
        let shred_key = sys_svc.get_org_shred_key(organization).map_err(|e| {
            tracing::error!(error = %e, organization = %organization, "Failed to read OrgShredKey");
            Status::internal("Internal error")
        })?;

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
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to encrypt Raft entry");
            Status::internal("Internal error")
        })?;

        self.propose_regional_ledger_request(
            region,
            LedgerRequest::EncryptedOrgSystem(encrypted),
            grpc_metadata,
            ctx,
        )
        .await
    }

    /// Records a handler-phase audit event if the event handle is configured.
    pub(crate) fn record_handler_event(&self, entry: EventEntry) {
        if let Some(ref handle) = self.event_handle {
            handle.record_handler_event(entry);
        }
    }

    /// Returns the configured TTL for handler-phase events, defaulting to 90 days.
    pub(crate) fn default_ttl_days(&self) -> u32 {
        self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)
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
    use inferadb_ledger_raft::{
        log_storage::AppliedState,
        raft_manager::{RaftManager, RaftManagerConfig, RegionConfig},
    };
    use inferadb_ledger_test_utils::TestDir;
    use parking_lot::RwLock;

    use super::*;
    use crate::proposal::RaftProposalService;

    /// Creates a `ServiceContext` backed by a [`MockProposalService`] and a
    /// real (empty) `StateLayer<FileBackend>` for state reads.
    ///
    /// The returned `AppliedState` is wrapped in `Arc<RwLock<_>>` so tests
    /// can populate slug indexes, org metadata, and other applied state before
    /// calling handlers.
    ///
    /// [`MockProposalService`]: crate::proposal::mock::MockProposalService
    pub(crate) fn test_service_context_with_mock(
        proposer: Arc<dyn crate::proposal::ProposalService>,
        temp: &TestDir,
    ) -> (ServiceContext, Arc<RwLock<AppliedState>>) {
        let db_path = temp.path().join("test_mock.db");
        let db = Arc::new(
            inferadb_ledger_store::Database::create(&db_path).expect("create test database"),
        );
        let state = Arc::new(inferadb_ledger_state::StateLayer::new(db));
        let applied_state_inner = Arc::new(RwLock::new(AppliedState::default()));
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
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let proposer: Arc<dyn ProposalService> =
            Arc::new(RaftProposalService::new(system.raft().clone(), None));

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
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let proposer: Arc<dyn ProposalService> =
            Arc::new(RaftProposalService::new(system.raft().clone(), Some(manager.clone())));

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
    async fn propose_regional_unknown_region_returns_unavailable() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;

        let metadata = tonic::metadata::MetadataMap::new();
        let mut req_ctx =
            inferadb_ledger_raft::logging::RequestContext::new("test", "propose_regional");

        let err = ctx
            .propose_regional(Region::US_EAST_VA, test_system_request(), &metadata, &mut req_ctx)
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn raft_metrics_returns_some_with_real_proposer() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;
        assert!(ctx.raft_metrics().is_some());
    }
}
