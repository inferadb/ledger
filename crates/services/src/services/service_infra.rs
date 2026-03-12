//! Shared service infrastructure for gRPC service implementations.
//!
//! [`ServiceContext`] consolidates the fields and methods shared across
//! `OrganizationService`, `VaultService`, `UserService`, and `AppService`:
//! Raft consensus, state access, trace propagation, proposal submission,
//! and handler-phase event recording.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    error::classify_raft_error,
    log_storage::AppliedStateAccessor,
    logging::{OperationType, RequestContext, Sampler},
    raft_manager::RaftManager,
    trace_context,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig, RaftPayload, SystemRequest},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    EmailBlindingKey, Region, config::ValidationConfig, events::EventEntry,
};
use openraft::Raft;
use tonic::Status;

/// Shared infrastructure for gRPC services that propose writes through Raft.
///
/// Holds the fields common to `OrganizationService`, `VaultService`,
/// `UserService`, and `AppService`. Each service stores a `ServiceContext`
/// alongside any service-specific fields (e.g. `AppService`'s credential cache).
///
/// Cloneable — all inner types are `Arc`-wrapped or `Copy`.
#[derive(Clone)]
pub(crate) struct ServiceContext {
    /// Raft consensus handle for proposing write operations.
    pub(crate) raft: Arc<Raft<LedgerTypeConfig>>,
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
    /// Multi-region Raft manager for proposing to regional Raft groups.
    ///
    /// Required for onboarding handlers and any operation that proposes to
    /// a specific region's Raft group (e.g., PII writes, verification codes).
    /// `None` when the service does not need regional proposals (tests, single-region).
    ///
    /// Used by `propose_regional` and `regional_state` for onboarding
    /// handlers and PII data residency enforcement.
    #[allow(dead_code)]
    pub(crate) manager: Option<Arc<RaftManager>>,
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
    /// Creates a `RequestContext` for a service method and fills in common fields.
    ///
    /// Replaces the per-service `org_ctx!`/`vault_ctx!`/`user_ctx!`/`app_ctx!` macros.
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
    /// Maps Raft errors via `classify_raft_error`: leadership errors become
    /// `UNAVAILABLE` (retryable), other Raft errors become `INTERNAL`.
    pub(crate) async fn propose_request(
        &self,
        request: LedgerRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let timeout = self.effective_timeout(grpc_metadata);
        let payload = RaftPayload::new(request);

        ctx.start_raft_timer();
        let result = tokio::time::timeout(timeout, self.raft.client_write(payload)).await;
        ctx.end_raft_timer();

        match result {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => {
                ctx.set_error("RaftError", &e.to_string());
                Err(classify_raft_error(&e.to_string()))
            },
            Err(_elapsed) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                ctx.set_error("Timeout", "Raft proposal timed out");
                Err(Status::deadline_exceeded(format!(
                    "Raft proposal timed out after {}ms",
                    timeout.as_millis()
                )))
            },
        }
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
    /// Resolves `region` to a [`RegionGroup`] via the [`RaftManager`], then
    /// proposes the request through that group's Raft instance. This is the
    /// service-layer equivalent of the saga orchestrator's `target_region()`
    /// routing.
    ///
    /// Used by onboarding handlers for PII-carrying requests that must stay
    /// in-region (verification codes, user profiles) and for cross-region
    /// session creation (proposing `CreateRefreshToken` to a user's actual
    /// region).
    ///
    /// # Errors
    ///
    /// - `FAILED_PRECONDITION` if the `RaftManager` is not configured.
    /// - `UNAVAILABLE` if the region's Raft group is not active on this node.
    /// - `DEADLINE_EXCEEDED` if the proposal times out.
    /// - `INTERNAL` / `UNAVAILABLE` for Raft errors (via `classify_raft_error`).
    #[allow(dead_code)]
    pub(crate) async fn propose_regional(
        &self,
        region: Region,
        system_request: SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional proposals require RaftManager configuration")
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            Status::unavailable(format!("Region {region} is not active on this node: {e}"))
        })?;

        let timeout = self.effective_timeout(grpc_metadata);
        let request = LedgerRequest::System(system_request);
        let payload = RaftPayload::new(request);

        ctx.start_raft_timer();
        let result = tokio::time::timeout(timeout, region_group.raft().client_write(payload)).await;
        ctx.end_raft_timer();

        match result {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => {
                ctx.set_error("RaftError", &e.to_string());
                Err(classify_raft_error(&e.to_string()))
            },
            Err(_elapsed) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                ctx.set_error("Timeout", "Regional Raft proposal timed out");
                Err(Status::deadline_exceeded(format!(
                    "Regional Raft proposal timed out after {}ms (region: {region})",
                    timeout.as_millis()
                )))
            },
        }
    }

    /// Returns the state layer for a specific region's Raft group.
    ///
    /// Enables direct reads from a region's state without proposing through
    /// Raft. Used by service handlers that need to read onboarding accounts,
    /// user profiles, or other regional data before or after proposals.
    ///
    /// # Errors
    ///
    /// - `FAILED_PRECONDITION` if the `RaftManager` is not configured.
    /// - `UNAVAILABLE` if the region's Raft group is not active on this node.
    #[allow(dead_code)]
    pub(crate) fn regional_state(
        &self,
        region: Region,
    ) -> Result<Arc<StateLayer<FileBackend>>, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional state access requires RaftManager configuration")
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            Status::unavailable(format!("Region {region} is not active on this node: {e}"))
        })?;

        Ok(region_group.state().clone())
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
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional proposals require RaftManager configuration")
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            Status::unavailable(format!("Region {region} is not active on this node: {e}"))
        })?;

        let timeout = self.effective_timeout(grpc_metadata);
        let payload = RaftPayload::new(request);

        ctx.start_raft_timer();
        let result = tokio::time::timeout(timeout, region_group.raft().client_write(payload)).await;
        ctx.end_raft_timer();

        match result {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => {
                ctx.set_error("RaftError", &e.to_string());
                Err(classify_raft_error(&e.to_string()))
            },
            Err(_elapsed) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                ctx.set_error("Timeout", "Regional Raft proposal timed out");
                Err(Status::deadline_exceeded(format!(
                    "Regional Raft proposal timed out after {}ms (region: {region})",
                    timeout.as_millis()
                )))
            },
        }
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

    /// Computes the effective Raft proposal timeout, respecting gRPC deadlines.
    fn effective_timeout(&self, grpc_metadata: &tonic::metadata::MetadataMap) -> Duration {
        let grpc_deadline =
            inferadb_ledger_raft::deadline::extract_deadline_from_metadata(grpc_metadata);
        inferadb_ledger_raft::deadline::effective_timeout(self.proposal_timeout, grpc_deadline)
    }

    /// Returns a reference to the Raft manager, if configured.
    #[allow(dead_code)]
    pub(crate) fn manager(&self) -> Option<&Arc<RaftManager>> {
        self.manager.as_ref()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_raft::raft_manager::{RaftManagerConfig, RegionConfig};
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    /// Creates a `ServiceContext` with `manager: None` for testing error paths.
    ///
    /// Requires a started system region to populate the Raft/state/applied_state
    /// fields (these are mandatory), but sets `manager` to `None` so that
    /// `propose_regional` and `regional_state` hit the FAILED_PRECONDITION path.
    async fn create_test_context_without_manager() -> (ServiceContext, TestDir) {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let ctx = ServiceContext {
            raft: system.raft().clone(),
            state: system.state().clone(),
            applied_state: system.applied_state().clone(),
            sampler: None,
            node_id: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_secs(5),
            event_handle: None,
            health_state: None,
            manager: None,
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

        let ctx = ServiceContext {
            raft: system.raft().clone(),
            state: system.state().clone(),
            applied_state: system.applied_state().clone(),
            sampler: None,
            node_id: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_secs(5),
            event_handle: None,
            health_state: None,
            manager: Some(manager.clone()),
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
    async fn manager_accessor_returns_none_when_unconfigured() {
        let (ctx, _temp) = create_test_context_without_manager().await;
        assert!(ctx.manager().is_none());
    }

    #[tokio::test]
    async fn manager_accessor_returns_some_when_configured() {
        let (ctx, _manager, _temp) = create_test_context_with_manager().await;
        assert!(ctx.manager().is_some());
    }
}
