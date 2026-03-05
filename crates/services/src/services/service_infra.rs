//! Shared service infrastructure for gRPC service implementations.
//!
//! Consolidates common patterns used across `OrganizationService`, `VaultService`,
//! and `UserService`: trace context propagation, transport metadata extraction,
//! and Raft proposal submission with deadline handling.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    error::ServiceError,
    logging::{OperationType, RequestContext, Sampler},
    trace_context,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig, RaftPayload},
};
use openraft::Raft;
use tonic::Status;

/// Fills common fields on a `RequestContext` from gRPC metadata and trace context.
///
/// Shared by `OrganizationService`, `VaultService`, and `UserService` to avoid
/// duplicating transport metadata extraction and trace context propagation.
pub(crate) fn fill_context(
    ctx: &mut RequestContext,
    grpc_metadata: &tonic::metadata::MetadataMap,
    trace_ctx: &trace_context::TraceContext,
    sampler: Option<&Sampler>,
    node_id: Option<u64>,
) {
    ctx.set_operation_type(OperationType::Admin);
    ctx.extract_transport_metadata(grpc_metadata);
    if let Some(sampler) = sampler {
        ctx.set_sampler(sampler.clone());
    }
    if let Some(node_id) = node_id {
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
/// Extracts the gRPC deadline from metadata, computes an effective timeout,
/// and submits the request as a Raft payload. Maps Raft errors via
/// `ServiceError::raft()` for uniform gRPC status conversion.
///
/// Shared by `OrganizationService` and `VaultService`. `UserService` has
/// its own variant (`propose_system_request`) that uses `classify_raft_error`
/// for different error mapping.
pub(crate) async fn propose_request(
    raft: &Arc<Raft<LedgerTypeConfig>>,
    request: LedgerRequest,
    grpc_metadata: &tonic::metadata::MetadataMap,
    proposal_timeout: Duration,
    ctx: &mut RequestContext,
) -> Result<LedgerResponse, Status> {
    let grpc_deadline =
        inferadb_ledger_raft::deadline::extract_deadline_from_metadata(grpc_metadata);
    let timeout =
        inferadb_ledger_raft::deadline::effective_timeout(proposal_timeout, grpc_deadline);

    let payload = RaftPayload::new(request);

    ctx.start_raft_timer();
    let result = tokio::time::timeout(timeout, raft.client_write(payload)).await;
    ctx.end_raft_timer();

    match result {
        Ok(Ok(resp)) => Ok(resp.data),
        Ok(Err(e)) => {
            ctx.set_error("RaftError", &e.to_string());
            Err(ServiceError::raft(e).into())
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
