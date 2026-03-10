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
    trace_context,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig, RaftPayload},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{config::ValidationConfig, events::EventEntry};
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
    /// to `propose_request`.
    pub(crate) async fn propose_system_request(
        &self,
        system_request: inferadb_ledger_raft::types::SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        self.propose_request(LedgerRequest::System(system_request), grpc_metadata, ctx).await
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
}
