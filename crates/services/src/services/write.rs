//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.
//!
//! Uses application-level batching to coalesce multiple write requests into
//! single Raft proposals, improving throughput by reducing consensus
//! round-trips.

use std::{fmt::Write, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
    WriteRequest, WriteResponse, WriteSuccess,
};
use inferadb_ledger_raft::{
    batching::BatchError,
    error::classify_raft_error,
    event_writer::EventEmitter,
    idempotency::IdempotencyCache,
    logging::{OperationType, RequestContext, Sampler},
    metrics,
    proof::{self, ProofError},
    raft_manager::RaftManager,
    rate_limit::RateLimiter,
    types::{LedgerResponse, RaftPayload, LedgerRequest, OrganizationRequest},
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, SetCondition, VaultId, config::ValidationConfig};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};
use uuid::Uuid;

pub(crate) use super::metadata::{response_with_correlation, status_with_correlation};
use super::{
    error_classify,
    region_resolver::{RegionContext, RegionResolver, ResolveResult},
    slug_resolver::SlugResolver,
};

/// Classifies a batch writer error into the appropriate `tonic::Status`.
///
/// `BatchError::RaftError` may contain leadership-related messages that
/// should map to `UNAVAILABLE` (retryable) instead of `INTERNAL`.
fn classify_batch_error(err: &BatchError) -> Status {
    match err {
        BatchError::RaftError(msg) => classify_raft_error(msg),
        BatchError::Dropped => Status::unavailable("Batch writer dropped request"),
        BatchError::Internal(msg) => error_classify::raft_error(&msg),
    }
}

/// gRPC handler for transaction submission.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct WriteService {
    /// Region resolver for routing requests to the correct region.
    resolver: Arc<dyn RegionResolver>,
    /// Raft manager for creating forward clients when needed.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Per-organization rate limiter.
    #[builder(default)]
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Sampler for log tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Node ID for logging system context.
    #[builder(default)]
    node_id: Option<u64>,
    /// Hot key detector for identifying frequently accessed keys.
    #[builder(default)]
    hot_key_detector: Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Handler-phase event handle for recording denial events.
    #[builder(default)]
    event_handle: Option<inferadb_ledger_raft::event_writer::EventHandle<FileBackend>>,
    /// Health state for drain-phase write rejection.
    #[builder(default)]
    health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
    /// Shared peer address map for resolving peer endpoints in `NotLeader`
    /// hint responses returned to clients on follower nodes.
    #[builder(default)]
    peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
}

#[allow(clippy::result_large_err)]
impl WriteService {
    /// Attaches per-organization rate limiting to an existing service.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Attaches hot key detector for identifying frequently accessed keys.
    #[must_use]
    pub fn with_hot_key_detector(
        mut self,
        detector: Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>,
    ) -> Self {
        self.hot_key_detector = Some(detector);
        self
    }

    /// Attaches input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Sets the maximum time to wait for a Raft proposal to commit.
    #[must_use]
    pub fn with_proposal_timeout(mut self, timeout: Duration) -> Self {
        self.proposal_timeout = timeout;
        self
    }

    /// Attaches the handler-phase event handle for recording denial events.
    #[must_use]
    pub fn with_event_handle(
        mut self,
        handle: inferadb_ledger_raft::event_writer::EventHandle<FileBackend>,
    ) -> Self {
        self.event_handle = Some(handle);
        self
    }

    /// Attaches health state for drain-phase write rejection.
    #[must_use]
    pub fn with_health_state(
        mut self,
        health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    ) -> Self {
        self.health_state = Some(health_state);
        self
    }

    /// Rejects writes to organizations undergoing migration.
    ///
    /// Returns `Ok(())` if the organization is not migrating (or doesn't exist yet).
    /// Returns `Err(Status::FailedPrecondition)` with structured error details
    /// if the organization is actively migrating to another region.
    fn check_not_migrating(
        &self,
        system: &RegionContext,
        organization_id: OrganizationId,
    ) -> Result<(), Status> {
        if let Some(org_meta) = system.applied_state.get_organization(organization_id)
            && org_meta.status == inferadb_ledger_state::system::OrganizationStatus::Migrating
        {
            let mut context = std::collections::HashMap::new();
            context.insert("organization".to_string(), organization_id.value().to_string());
            if let Some(pending) = org_meta.pending_region {
                context.insert("target_region".to_string(), pending.as_str().to_string());
            }
            let details = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating.as_u16(),
                true,
                Some(30_000),
                context,
                Some(
                    inferadb_ledger_types::DiagnosticCode::AppOrganizationMigrating
                        .suggested_action(),
                ),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            return Err(Status::with_details(
                tonic::Code::FailedPrecondition,
                "Organization is being migrated to another region; writes are temporarily blocked",
                encoded.into(),
            ));
        }
        Ok(())
    }

    /// Validates all operations in a proto operation list.
    fn validate_operations(
        &self,
        operations: &[inferadb_ledger_proto::proto::Operation],
    ) -> Result<(), Status> {
        super::helpers::validate_operations(operations, &self.validation_config)
    }

    /// Checks all rate limit tiers (backpressure, organization, client).
    fn check_rate_limit(
        &self,
        client_id: &str,
        organization: OrganizationId,
    ) -> Result<(), Status> {
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), client_id, organization)
    }

    /// Records key accesses from operations for hot key detection.
    fn record_hot_keys(
        &self,
        vault: VaultId,
        operations: &[inferadb_ledger_proto::proto::Operation],
    ) {
        super::helpers::record_hot_keys(self.hot_key_detector.as_ref(), vault, operations);
    }

    /// Maps a failed SetCondition to the appropriate WriteErrorCode.
    ///
    /// Maps per proto WriteErrorCode:
    /// - MustNotExist failed → KEY_EXISTS (key already exists)
    /// - MustExist failed → KEY_NOT_FOUND (key doesn't exist)
    /// - VersionEquals failed with existing key → VERSION_MISMATCH
    /// - VersionEquals failed with missing key → KEY_NOT_FOUND
    /// - ValueEquals failed with existing key → VALUE_MISMATCH
    /// - ValueEquals failed with missing key → KEY_NOT_FOUND
    fn map_condition_to_error_code(
        condition: Option<&SetCondition>,
        key_exists: bool,
    ) -> WriteErrorCode {
        match condition {
            Some(SetCondition::MustNotExist) => {
                // MustNotExist failed means the key exists
                WriteErrorCode::KeyExists
            },
            Some(SetCondition::MustExist) => {
                // MustExist failed means the key doesn't exist
                WriteErrorCode::KeyNotFound
            },
            Some(SetCondition::VersionEquals(_)) => {
                if key_exists {
                    WriteErrorCode::VersionMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            },
            Some(SetCondition::ValueEquals(_)) => {
                if key_exists {
                    WriteErrorCode::ValueMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            },
            None => {
                // No condition - shouldn't reach here for PreconditionFailed
                WriteErrorCode::Unspecified
            },
        }
    }

    /// Converts bytes to hex string for request logging.
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Extracts operation type names from proto operations for request logging.
    fn extract_operation_types(
        operations: &[inferadb_ledger_proto::proto::Operation],
    ) -> Vec<&'static str> {
        use inferadb_ledger_proto::proto::operation::Op;

        operations
            .iter()
            .filter_map(|op| op.op.as_ref())
            .map(|op| match op {
                Op::CreateRelationship(_) => "create_relationship",
                Op::DeleteRelationship(_) => "delete_relationship",
                Op::SetEntity(_) => "set_entity",
                Op::DeleteEntity(_) => "delete_entity",
                Op::ExpireEntity(_) => "expire_entity",
            })
            .collect()
    }

    /// Estimates the total payload bytes across all operations.
    ///
    /// Sums key and value sizes for entity operations, and resource/relation/subject
    /// lengths for relationship operations.
    fn estimate_operations_bytes(operations: &[inferadb_ledger_proto::proto::Operation]) -> usize {
        use inferadb_ledger_proto::proto::operation::Op;

        operations
            .iter()
            .filter_map(|op| op.op.as_ref())
            .map(|op| match op {
                Op::CreateRelationship(cr) => {
                    cr.resource.len() + cr.relation.len() + cr.subject.len()
                },
                Op::DeleteRelationship(dr) => {
                    dr.resource.len() + dr.relation.len() + dr.subject.len()
                },
                Op::SetEntity(se) => se.key.len() + se.value.len(),
                Op::DeleteEntity(de) => de.key.len(),
                Op::ExpireEntity(ee) => ee.key.len(),
            })
            .sum()
    }

    /// Generates block header and transaction proof for a committed write.
    ///
    /// Uses the region resolver to obtain the block archive and applied state
    /// for the organization's region.
    ///
    /// Returns `(block_header, tx_proof, proof_error_reason)`.
    /// On success, the third element is `None`.
    /// On failure, `block_header` and `tx_proof` are `None` and `proof_error_reason`
    /// carries a stable label describing why proof generation failed. The write
    /// itself committed; the proof is a post-commit enrichment step.
    fn generate_write_proof(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> (
        Option<inferadb_ledger_proto::proto::BlockHeader>,
        Option<inferadb_ledger_proto::proto::MerkleProof>,
        Option<&'static str>,
    ) {
        let ctx = match self.resolver.resolve(organization) {
            Ok(ctx) => ctx,
            Err(_) => {
                let reason = "region_unavailable";
                metrics::record_proof_generation_failure(reason);
                warn!(
                    organization_id = organization.value(),
                    vault_id = vault.value(),
                    vault_height,
                    reason,
                    "Proof generation failed: region resolver returned error"
                );
                return (None, None, Some(reason));
            },
        };

        // Resolve internal IDs to slugs for response construction
        let org_slug = ctx.applied_state.resolve_id_to_slug(organization);
        let vault_slug = ctx.applied_state.resolve_vault_id_to_slug(vault);

        // Use the proof module's SNAFU-based implementation
        match proof::generate_write_proof(
            &ctx.block_archive,
            organization,
            org_slug,
            vault,
            vault_slug,
            vault_height,
            0,
        ) {
            Ok(write_proof) => (Some(write_proof.block_header), Some(write_proof.tx_proof), None),
            Err(e) => {
                // Classify the failure reason for metrics and response metadata
                let reason: &'static str = match &e {
                    ProofError::BlockNotFound { .. } => "block_not_found",
                    ProofError::NoTransactions => "no_transactions",
                    _ => "internal_error",
                };

                // Log with appropriate severity based on error type
                match &e {
                    ProofError::BlockNotFound { .. } | ProofError::NoTransactions => {
                        // Timing issue: block not yet written to archive
                        debug!(
                            error = %e,
                            organization_id = organization.value(),
                            vault_id = vault.value(),
                            vault_height,
                            reason,
                            "Proof generation skipped"
                        );
                    },
                    _ => {
                        warn!(
                            error = %e,
                            organization_id = organization.value(),
                            vault_id = vault.value(),
                            vault_height,
                            reason,
                            "Proof generation failed"
                        );
                    },
                }

                metrics::record_proof_generation_failure(reason);

                (None, None, Some(reason))
            },
        }
    }

    /// Converts proto operations to LedgerRequest.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0 here;
    /// the actual sequence will be assigned by the Raft state machine at apply time.
    fn operations_to_request(
        &self,
        organization: OrganizationId,
        vault: Option<VaultId>,
        operations: &[inferadb_ledger_proto::proto::Operation],
        client_id: &str,
        idempotency_key: [u8; 16],
        request_hash: u64,
    ) -> Result<LedgerRequest, Status> {
        // Convert proto operations to internal Operations
        let internal_ops: Vec<inferadb_ledger_types::Operation> = operations
            .iter()
            .map(inferadb_ledger_types::Operation::try_from)
            .collect::<Result<Vec<_>, Status>>()?;

        // Create a single transaction with all operations
        // Server-assigned sequences: sequence=0 is a placeholder; actual sequence
        // is assigned at Raft apply time for deterministic replay.
        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: inferadb_ledger_types::ClientId::new(client_id),
            sequence: 0, // Server-assigned at apply time
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
        };

        Ok(LedgerRequest::Organization(OrganizationRequest::Write {
            organization,
            vault: vault.unwrap_or(VaultId::new(0)),
            transactions: vec![transaction],
            idempotency_key,
            request_hash,
        }))
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::write_service_server::WriteService for WriteService {
    /// Processes a single write transaction containing entity and relationship operations.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    /// In multi-region deployments, requests are routed to the correct region
    /// based on organization, with cross-region forwarding when needed.
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        // Build unified request context before consuming the request body.
        // from_request extracts transport metadata and trace context from gRPC headers.
        let event_handle: Option<Arc<dyn EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let grpc_metadata = request.metadata().clone();
        let mut ctx = RequestContext::from_request("WriteService", "write", &request, event_handle);
        ctx.set_operation_type(OperationType::Write);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let req = request.into_inner();

        // Extract client ID
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();

        // Resolve organization slug → internal ID via system region
        let system = self.resolver.system_region()?;
        let organization_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve(&req.organization)?;

        // Set organization slug on context for event emission in early-exit paths.
        ctx.set_organization(req.organization.as_ref().map_or(0, |n| n.slug));

        // Reject writes to organizations undergoing migration
        self.check_not_migrating(&system, organization_id)?;

        // Check for cross-region forwarding — if the organization is on a remote
        // region, run pre-flight checks and forward the raw request.
        if self.resolver.supports_forwarding()
            && let ResolveResult::Redirect(remote) =
                self.resolver.resolve_with_redirect(organization_id)?
        {
            // Pre-flight: validation on originating node
            if let Err(status) = self.validate_operations(&req.operations) {
                ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: status.message().to_string(),
                    },
                    &[],
                );
                return Err(status);
            }

            // Pre-flight: rate limit on originating node
            if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
                ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestRateLimited,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "rate_limited".to_string(),
                    },
                    &[],
                );
                return Err(status_with_correlation(status, &ctx.request_id(), ctx.trace_id()));
            }

            // Redirect cross-region writes — clients reconnect against the remote
            // region's leader using the hint attached to the NotLeader status.
            let source_region =
                self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
            debug!(
                organization_id = organization_id.value(),
                target_region = remote.region.as_str(),
                source_region,
                "Redirecting write to remote region"
            );
            return Err(status_with_correlation(
                super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Ensure GLOBAL state is replicated before resolving vault slugs.
        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;

        // Local processing: resolve vault slug via GLOBAL applied state.
        // Vault slug indexes are maintained in the GLOBAL Raft group (CreateVault
        // is a GLOBAL operation), not in the data region's applied state.
        let region = self.resolver.resolve(organization_id)?;
        let vault_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve_vault(&req.vault)?;

        // Reject on followers — clients use the NotLeader hint to retry
        // against the within-region leader directly.
        if !region.handle.is_leader() {
            return Err(status_with_correlation(
                super::metadata::not_leader_status_from_handle(
                    region.handle.as_ref(),
                    self.peer_addresses.as_ref(),
                    "Not the leader for this region",
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Extract caller identity for canonical log line
        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Validate all operations before any processing
        if let Err(status) = self.validate_operations(&req.operations) {
            ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: status.message().to_string(),
                },
                &[],
            );
            return Err(status);
        }

        // Compute request hash for payload comparison (detects key reuse with different payload)
        let request_hash = seahash::hash(&super::helpers::hash_operations(&req.operations));

        // Populate logging context with request metadata
        ctx.set_client_info(&client_id, 0);
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Populate write operation fields
        let operation_types = Self::extract_operation_types(&req.operations);
        ctx.set_write_operation(req.operations.len(), operation_types, req.include_tx_proof);
        ctx.set_bytes_written(Self::estimate_operations_bytes(&req.operations));

        // Serialize concurrent requests with the same idempotency key.
        //
        // Without this, during leader failover the moka cache is cold and two
        // concurrent requests can both pass the replicated-state check before
        // either commits, resulting in duplicate Raft proposals. The in-flight
        // guard ensures only one request proceeds; others wait and re-check.
        use inferadb_ledger_raft::idempotency::{
            IdempotencyCheckResult, IdempotencyKey, InFlightStatus,
        };
        let _inflight_guard = loop {
            let inflight_key =
                IdempotencyKey::new(organization_id, vault_id, client_id.as_str(), idempotency_key);
            match self.idempotency.try_acquire_inflight(inflight_key) {
                InFlightStatus::Acquired(guard) => break guard,
                InFlightStatus::Waiting(notify) => {
                    // Register the waiter synchronously before any yield point.
                    // tokio::sync::Notify::notify_waiters() does not store a permit
                    // for future .notified() calls; a waiter that registers after
                    // notify_waiters() fires would miss the wakeup. The acquirer
                    // inserts the cached result BEFORE releasing, so a late waiter's
                    // cache re-check is always safe.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();

                    // Short-circuit: if notify_waiters() already fired before we
                    // registered, the cache is populated and we can return immediately.
                    match self.idempotency.check(
                        organization_id,
                        vault_id,
                        &client_id,
                        idempotency_key,
                        request_hash,
                    ) {
                        IdempotencyCheckResult::Duplicate(cached) => {
                            ctx.set_idempotency_hit(true);
                            ctx.set_cached();
                            if let Some(ref header) = cached.block_header
                                && let Some(ref state_root) = header.state_root
                            {
                                ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                            }
                            ctx.set_block_height(cached.block_height);
                            metrics::record_idempotency_operation("coalesced_fast");
                            metrics::record_idempotency_operation("hit");
                            return Ok(response_with_correlation(
                                WriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::write_response::Result::Success(
                                            cached,
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        IdempotencyCheckResult::KeyReused => {
                            ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload",
                            );
                            return Ok(response_with_correlation(
                                WriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::IdempotencyKeyReused.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message:
                                                    "Idempotency key was already used with a different request payload"
                                                        .to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: None,
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        IdempotencyCheckResult::NewRequest => {
                            // Acquirer is still in-flight (or crashed mid-insert).
                            // Wait on the notification, then re-check.
                            notified.await;
                            metrics::record_idempotency_operation("coalesced");

                            match self.idempotency.check(
                                organization_id,
                                vault_id,
                                &client_id,
                                idempotency_key,
                                request_hash,
                            ) {
                                IdempotencyCheckResult::Duplicate(cached) => {
                                    ctx.set_idempotency_hit(true);
                                    ctx.set_cached();
                                    if let Some(ref header) = cached.block_header
                                        && let Some(ref state_root) = header.state_root
                                    {
                                        ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                                    }
                                    ctx.set_block_height(cached.block_height);
                                    metrics::record_idempotency_operation("hit");
                                    return Ok(response_with_correlation(
                                        WriteResponse {
                                            result: Some(
                                                inferadb_ledger_proto::proto::write_response::Result::Success(
                                                    cached,
                                                ),
                                            ),
                                        },
                                        &ctx.request_id(),
                                        ctx.trace_id(),
                                    ));
                                },
                                IdempotencyCheckResult::KeyReused => {
                                    ctx.set_error(
                                        "IdempotencyKeyReused",
                                        "Idempotency key reused with different payload",
                                    );
                                    return Ok(response_with_correlation(
                                        WriteResponse {
                                            result: Some(
                                                inferadb_ledger_proto::proto::write_response::Result::Error(
                                                    WriteError {
                                                        code: WriteErrorCode::IdempotencyKeyReused
                                                            .into(),
                                                        key: String::new(),
                                                        current_version: None,
                                                        current_value: None,
                                                        message:
                                                            "Idempotency key was already used with a different request payload"
                                                                .to_string(),
                                                        committed_tx_id: None,
                                                        committed_block_height: None,
                                                        assigned_sequence: None,
                                                    },
                                                ),
                                            ),
                                        },
                                        &ctx.request_id(),
                                        ctx.trace_id(),
                                    ));
                                },
                                IdempotencyCheckResult::NewRequest => {
                                    // Acquirer released its guard without inserting
                                    // (crash path). Retry the outer loop.
                                    continue;
                                },
                            }
                        },
                    }
                },
            }
        };

        // Check idempotency cache for duplicate (fast path — moka hit)
        match self.idempotency.check(
            organization_id,
            vault_id,
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                ctx.set_idempotency_hit(true);
                ctx.set_cached();
                if let Some(ref header) = cached.block_header
                    && let Some(ref state_root) = header.state_root
                {
                    ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                }
                ctx.set_block_height(cached.block_height);
                metrics::record_idempotency_operation("hit");
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(cached),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(inferadb_ledger_proto::proto::write_response::Result::Error(WriteError {
                            code: WriteErrorCode::IdempotencyKeyReused.into(),
                            key: String::new(),
                            current_version: None,
                            current_value: None,
                            message:
                                "Idempotency key was already used with a different request payload"
                                    .to_string(),
                            committed_tx_id: None,
                            committed_block_height: None,
                            assigned_sequence: None,
                        })),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ));
            },
            IdempotencyCheckResult::NewRequest => {
                // Moka miss — check replicated state for cross-failover dedup
                ctx.set_idempotency_hit(false);
                {
                    use inferadb_ledger_raft::log_storage::IdempotencyCheckResult as ReplicatedCheck;
                    match region.applied_state.client_idempotency_check(
                        organization_id,
                        vault_id,
                        &client_id,
                        &idempotency_key,
                        request_hash,
                    ) {
                        ReplicatedCheck::AlreadyCommitted { sequence } => {
                            metrics::record_idempotency_operation("hit");
                            return Ok(response_with_correlation(
                                WriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::AlreadyCommitted.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message: "Request already committed (cross-failover dedup)".to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: Some(sequence),
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        ReplicatedCheck::KeyReused => {
                            ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload (cross-failover)",
                            );
                            return Ok(response_with_correlation(
                                WriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::IdempotencyKeyReused.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message: "Idempotency key was already used with a different request payload".to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: None,
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        ReplicatedCheck::Miss => {},
                    }
                }
                metrics::record_idempotency_operation("miss");
            },
        }

        // Check rate limits (backpressure, organization, client)
        if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
            ctx.set_rate_limited();
            ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestRateLimited,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "rate_limited".to_string(),
                },
                &[],
            );
            return Err(status_with_correlation(status, &ctx.request_id(), ctx.trace_id()));
        }

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request = self.operations_to_request(
            organization_id,
            Some(vault_id),
            &req.operations,
            &client_id,
            idempotency_key,
            request_hash,
        )?;

        // Re-validate vault slug before proposal submission.
        // Between the initial resolution and now (leader check, rate limiting,
        // idempotency check), the GLOBAL state may have changed (e.g., vault
        // migrated to a different shard during region migration). A mismatch
        // means the request would be routed to a stale shard.
        let revalidated_vault_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve_vault(&req.vault)?;
        if revalidated_vault_id != vault_id {
            warn!(
                vault_id = vault_id.value(),
                revalidated_vault_id = revalidated_vault_id.value(),
                "Vault routing changed during request processing"
            );
            return Err(status_with_correlation(
                super::helpers::error_code_to_status(
                    inferadb_ledger_types::ErrorCode::StaleRouting,
                    "Stale routing: vault routing changed during request processing. Retry."
                        .to_string(),
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline =
            inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout =
            inferadb_ledger_raft::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft via batch writer (if available for this region) or direct proposal
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let response = if let Some(ref batch_handle) = region.batch_handle {
            ctx.set_batch_info(true, 1);
            // Submit through batch writer for coalesced proposals
            let rx = batch_handle.submit(ledger_request);
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(Ok(resp))) => resp,
                Ok(Ok(Err(batch_err))) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchError", &batch_err.to_string());
                    return Err(status_with_correlation(
                        classify_batch_error(&batch_err),
                        &ctx.request_id(),
                        ctx.trace_id(),
                    ));
                },
                Ok(Err(_recv_err)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchChannelClosed", "Batch writer channel closed");
                    return Err(status_with_correlation(
                        Status::internal("Batch writer unavailable"),
                        &ctx.request_id(),
                        ctx.trace_id(),
                    ));
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                    metrics::record_raft_proposal_timeout();
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &ctx.request_id(),
                        ctx.trace_id(),
                    ));
                },
            }
        } else {
            ctx.set_batch_info(false, 1);
            // Direct Raft proposal (OpenRaft serializes internally)
            let commitments = region
                .commitment_buffer
                .as_ref()
                .map(|buf| std::mem::take(&mut *buf.lock().unwrap_or_else(|e| e.into_inner())))
                .unwrap_or_default();
            let payload = RaftPayload {
                request: ledger_request,
                proposed_at: chrono::Utc::now(),
                caller: ctx.caller_or_zero(),
                state_root_commitments: commitments,
            };
            match region.handle.propose_and_wait(payload, timeout).await {
                Ok(result) => result,
                Err(inferadb_ledger_raft::HandleError::Timeout { .. }) => {
                    ctx.end_raft_timer();
                    ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                    metrics::record_raft_proposal_timeout();
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &ctx.request_id(),
                        ctx.trace_id(),
                    ));
                },
                Err(e) => {
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    return Err(status_with_correlation(
                        classify_raft_error(&e.to_string()),
                        &ctx.request_id(),
                        ctx.trace_id(),
                    ));
                },
            }
        };
        ctx.end_raft_timer();

        match response {
            LedgerResponse::Write { block_height, block_hash, assigned_sequence } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof, proof_error_reason) = if req.include_tx_proof {
                    self.generate_write_proof(organization_id, vault_id, block_height)
                } else {
                    (None, None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header: block_header.clone(),
                    tx_proof,
                    assigned_sequence,
                };

                // Cache the result for idempotency
                self.idempotency.insert(
                    organization_id,
                    vault_id,
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    success.clone(),
                );
                metrics::set_idempotency_cache_size(self.idempotency.len());

                // Set success outcome with block info
                ctx.set_success();
                ctx.set_block_height(block_height);
                ctx.set_block_hash(&Self::bytes_to_hex(&block_hash));
                if let Some(ref header) = block_header
                    && let Some(ref state_root) = header.state_root
                {
                    ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                }

                let elapsed = ctx.elapsed_secs();
                metrics::record_organization_latency(organization_id, "write", elapsed);

                let mut response = response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(success),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                );
                if let Some(reason) = proof_error_reason
                    && let Ok(val) = tonic::metadata::MetadataValue::try_from(reason)
                {
                    response.metadata_mut().insert("x-proof-unavailable-reason", val);
                }
                Ok(response)
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);

                Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(inferadb_ledger_proto::proto::write_response::Result::Error(
                            WriteError {
                                code: WriteErrorCode::Unspecified.into(),
                                key: String::new(),
                                current_version: None,
                                current_value: None,
                                message,
                                committed_tx_id: None,
                                committed_block_height: None,
                                assigned_sequence: None,
                            },
                        )),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was
                // last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                ctx.set_precondition_failed(Some(&key));

                Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(inferadb_ledger_proto::proto::write_response::Result::Error(
                            WriteError {
                                code: error_code.into(),
                                key,
                                current_version,
                                current_value,
                                message: "Precondition failed".to_string(),
                                committed_tx_id: None,
                                committed_block_height: None,
                                assigned_sequence: None,
                            },
                        )),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
        }
    }

    /// Processes multiple write transactions atomically as a single batch.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    /// In multi-region deployments, requests are routed to the correct region
    /// based on organization, with cross-region forwarding when needed.
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        // Build unified request context before consuming the request body.
        // from_request extracts transport metadata and trace context from gRPC headers.
        let event_handle: Option<Arc<dyn EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let grpc_metadata = request.metadata().clone();
        let mut ctx =
            RequestContext::from_request("WriteService", "batch_write", &request, event_handle);
        ctx.set_operation_type(OperationType::Write);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let req = request.into_inner();

        // Extract client ID
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();

        // Resolve organization slug → internal ID via system region
        let system = self.resolver.system_region()?;
        let organization_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve(&req.organization)?;

        // Set organization slug on context for event emission in early-exit paths.
        ctx.set_organization(req.organization.as_ref().map_or(0, |n| n.slug));

        // Reject writes to organizations undergoing migration
        self.check_not_migrating(&system, organization_id)?;

        // Check for cross-region forwarding — if the organization is on a remote
        // region, run pre-flight checks and forward the raw request.
        if self.resolver.supports_forwarding()
            && let ResolveResult::Redirect(remote) =
                self.resolver.resolve_with_redirect(organization_id)?
        {
            // Flatten operations for pre-flight validation
            let all_operations: Vec<inferadb_ledger_proto::proto::Operation> =
                req.operations.iter().flat_map(|group| group.operations.clone()).collect();

            // Pre-flight: validation on originating node
            if let Err(status) = self.validate_operations(&all_operations) {
                ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: status.message().to_string(),
                    },
                    &[],
                );
                return Err(status);
            }

            // Pre-flight: rate limit on originating node
            if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
                ctx.record_event(
                    inferadb_ledger_types::events::EventAction::RequestRateLimited,
                    inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "rate_limited".to_string(),
                    },
                    &[],
                );
                return Err(status_with_correlation(status, &ctx.request_id(), ctx.trace_id()));
            }

            // Redirect cross-region batch writes — clients reconnect against the
            // remote region's leader using the hint attached to the NotLeader status.
            let source_region =
                self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
            debug!(
                organization_id = organization_id.value(),
                target_region = remote.region.as_str(),
                source_region,
                "Redirecting batch_write to remote region"
            );
            return Err(status_with_correlation(
                super::metadata::not_leader_remote_region(
                    &remote,
                    "Organization hosted by a remote region; reconnect to that region",
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Ensure GLOBAL state is replicated before resolving vault slugs.
        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;

        // Local processing: resolve vault slug via GLOBAL applied state.
        // Vault slug indexes are maintained in the GLOBAL Raft group (CreateVault
        // is a GLOBAL operation), not in the data region's applied state.
        let region = self.resolver.resolve(organization_id)?;
        let vault_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve_vault(&req.vault)?;

        // Reject on followers — clients use the NotLeader hint to retry
        // against the within-region leader directly.
        if !region.handle.is_leader() {
            return Err(status_with_correlation(
                super::metadata::not_leader_status_from_handle(
                    region.handle.as_ref(),
                    self.peer_addresses.as_ref(),
                    "Not the leader for this region",
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Extract caller identity for canonical log line
        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Populate logging context with request metadata
        ctx.set_client_info(&client_id, 0);
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Flatten all operations from all groups
        let all_operations: Vec<inferadb_ledger_proto::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

        // Validate all operations before any processing
        if let Err(status) = self.validate_operations(&all_operations) {
            ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: status.message().to_string(),
                },
                &[],
            );
            return Err(status);
        }

        let batch_size = all_operations.len();

        // Compute request hash for payload comparison (detects key reuse with different payload)
        let request_hash = seahash::hash(&super::helpers::hash_operations(&all_operations));

        // Populate write operation fields
        let operation_types = Self::extract_operation_types(&all_operations);
        ctx.set_write_operation(batch_size, operation_types, req.include_tx_proofs);
        ctx.set_batch_info(false, batch_size);
        ctx.set_bytes_written(Self::estimate_operations_bytes(&all_operations));

        // Serialize concurrent requests with the same idempotency key.
        // See the write() handler for detailed rationale.
        use inferadb_ledger_raft::idempotency::{
            IdempotencyCheckResult, IdempotencyKey, InFlightStatus,
        };
        let _inflight_guard = loop {
            let inflight_key =
                IdempotencyKey::new(organization_id, vault_id, client_id.as_str(), idempotency_key);
            match self.idempotency.try_acquire_inflight(inflight_key) {
                InFlightStatus::Acquired(guard) => break guard,
                InFlightStatus::Waiting(notify) => {
                    // Register the waiter synchronously before any yield point.
                    // tokio::sync::Notify::notify_waiters() does not store a permit
                    // for future .notified() calls; a waiter that registers after
                    // notify_waiters() fires would miss the wakeup. The acquirer
                    // inserts the cached result BEFORE releasing, so a late waiter's
                    // cache re-check is always safe.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();

                    // Short-circuit: if notify_waiters() already fired before we
                    // registered, the cache is populated and we can return immediately.
                    match self.idempotency.check(
                        organization_id,
                        vault_id,
                        &client_id,
                        idempotency_key,
                        request_hash,
                    ) {
                        IdempotencyCheckResult::Duplicate(cached) => {
                            ctx.set_idempotency_hit(true);
                            ctx.set_cached();
                            if let Some(ref header) = cached.block_header
                                && let Some(ref state_root) = header.state_root
                            {
                                ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                            }
                            ctx.set_block_height(cached.block_height);
                            metrics::record_idempotency_operation("coalesced_fast");
                            metrics::record_idempotency_operation("hit");
                            return Ok(response_with_correlation(
                                BatchWriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                            BatchWriteSuccess {
                                                tx_id: cached.tx_id,
                                                block_height: cached.block_height,
                                                block_header: cached.block_header,
                                                tx_proof: cached.tx_proof,
                                                assigned_sequence: cached.assigned_sequence,
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        IdempotencyCheckResult::KeyReused => {
                            ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload",
                            );
                            return Ok(response_with_correlation(
                                BatchWriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::IdempotencyKeyReused.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message:
                                                    "Idempotency key was already used with a different request payload"
                                                        .to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: None,
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        IdempotencyCheckResult::NewRequest => {
                            // Acquirer is still in-flight (or crashed mid-insert).
                            // Wait on the notification, then re-check.
                            notified.await;
                            metrics::record_idempotency_operation("coalesced");

                            match self.idempotency.check(
                                organization_id,
                                vault_id,
                                &client_id,
                                idempotency_key,
                                request_hash,
                            ) {
                                IdempotencyCheckResult::Duplicate(cached) => {
                                    ctx.set_idempotency_hit(true);
                                    ctx.set_cached();
                                    if let Some(ref header) = cached.block_header
                                        && let Some(ref state_root) = header.state_root
                                    {
                                        ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                                    }
                                    ctx.set_block_height(cached.block_height);
                                    metrics::record_idempotency_operation("hit");
                                    return Ok(response_with_correlation(
                                        BatchWriteResponse {
                                            result: Some(
                                                inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                                    BatchWriteSuccess {
                                                        tx_id: cached.tx_id,
                                                        block_height: cached.block_height,
                                                        block_header: cached.block_header,
                                                        tx_proof: cached.tx_proof,
                                                        assigned_sequence: cached.assigned_sequence,
                                                    },
                                                ),
                                            ),
                                        },
                                        &ctx.request_id(),
                                        ctx.trace_id(),
                                    ));
                                },
                                IdempotencyCheckResult::KeyReused => {
                                    ctx.set_error(
                                        "IdempotencyKeyReused",
                                        "Idempotency key reused with different payload",
                                    );
                                    return Ok(response_with_correlation(
                                        BatchWriteResponse {
                                            result: Some(
                                                inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                                    WriteError {
                                                        code: WriteErrorCode::IdempotencyKeyReused
                                                            .into(),
                                                        key: String::new(),
                                                        current_version: None,
                                                        current_value: None,
                                                        message:
                                                            "Idempotency key was already used with a different request payload"
                                                                .to_string(),
                                                        committed_tx_id: None,
                                                        committed_block_height: None,
                                                        assigned_sequence: None,
                                                    },
                                                ),
                                            ),
                                        },
                                        &ctx.request_id(),
                                        ctx.trace_id(),
                                    ));
                                },
                                IdempotencyCheckResult::NewRequest => {
                                    // Acquirer released its guard without inserting
                                    // (crash path). Retry the outer loop.
                                    continue;
                                },
                            }
                        },
                    }
                },
            }
        };

        // Check idempotency cache for duplicate (fast path — moka hit)
        match self.idempotency.check(
            organization_id,
            vault_id,
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                ctx.set_idempotency_hit(true);
                ctx.set_cached();
                if let Some(ref header) = cached.block_header
                    && let Some(ref state_root) = header.state_root
                {
                    ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                }
                ctx.set_block_height(cached.block_height);
                metrics::record_idempotency_operation("hit");
                return Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                BatchWriteSuccess {
                                    tx_id: cached.tx_id,
                                    block_height: cached.block_height,
                                    block_header: cached.block_header,
                                    tx_proof: cached.tx_proof,
                                    assigned_sequence: cached.assigned_sequence,
                                },
                            ),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                return Ok(response_with_correlation(BatchWriteResponse {
                    result: Some(inferadb_ledger_proto::proto::batch_write_response::Result::Error(WriteError {
                        code: WriteErrorCode::IdempotencyKeyReused.into(),
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message:
                            "Idempotency key was already used with a different request payload"
                                .to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }, &ctx.request_id(), ctx.trace_id()));
            },
            IdempotencyCheckResult::NewRequest => {
                // Moka miss — check replicated state for cross-failover dedup
                ctx.set_idempotency_hit(false);
                {
                    use inferadb_ledger_raft::log_storage::IdempotencyCheckResult as ReplicatedCheck;
                    match region.applied_state.client_idempotency_check(
                        organization_id,
                        vault_id,
                        &client_id,
                        &idempotency_key,
                        request_hash,
                    ) {
                        ReplicatedCheck::AlreadyCommitted { sequence } => {
                            metrics::record_idempotency_operation("hit");
                            return Ok(response_with_correlation(
                                BatchWriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::AlreadyCommitted.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message: "Request already committed (cross-failover dedup)".to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: Some(sequence),
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        ReplicatedCheck::KeyReused => {
                            ctx.set_error(
                                "IdempotencyKeyReused",
                                "Idempotency key reused with different payload (cross-failover)",
                            );
                            return Ok(response_with_correlation(
                                BatchWriteResponse {
                                    result: Some(
                                        inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                            WriteError {
                                                code: WriteErrorCode::IdempotencyKeyReused.into(),
                                                key: String::new(),
                                                current_version: None,
                                                current_value: None,
                                                message: "Idempotency key was already used with a different request payload".to_string(),
                                                committed_tx_id: None,
                                                committed_block_height: None,
                                                assigned_sequence: None,
                                            },
                                        ),
                                    ),
                                },
                                &ctx.request_id(),
                                ctx.trace_id(),
                            ));
                        },
                        ReplicatedCheck::Miss => {},
                    }
                }
                metrics::record_idempotency_operation("miss");
            },
        }

        // Check rate limits (backpressure, organization, client)
        if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
            ctx.set_rate_limited();
            ctx.record_event(
                inferadb_ledger_types::events::EventAction::RequestRateLimited,
                inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "rate_limited".to_string(),
                },
                &[],
            );
            return Err(status_with_correlation(status, &ctx.request_id(), ctx.trace_id()));
        }

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &all_operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request = self.operations_to_request(
            organization_id,
            Some(vault_id),
            &all_operations,
            &client_id,
            idempotency_key,
            request_hash,
        )?;

        // Re-validate vault slug before proposal submission.
        // Between the initial resolution and now (leader check, rate limiting,
        // idempotency check), the GLOBAL state may have changed (e.g., vault
        // migrated to a different shard during region migration). A mismatch
        // means the request would be routed to a stale shard.
        let revalidated_vault_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve_vault(&req.vault)?;
        if revalidated_vault_id != vault_id {
            warn!(
                vault_id = vault_id.value(),
                revalidated_vault_id = revalidated_vault_id.value(),
                "Vault routing changed during batch_write processing"
            );
            return Err(status_with_correlation(
                super::helpers::error_code_to_status(
                    inferadb_ledger_types::ErrorCode::StaleRouting,
                    "Stale routing: vault routing changed during request processing. Retry."
                        .to_string(),
                ),
                &ctx.request_id(),
                ctx.trace_id(),
            ));
        }

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline =
            inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout =
            inferadb_ledger_raft::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft — batch_write always uses direct proposal (not batch writer)
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let commitments = region
            .commitment_buffer
            .as_ref()
            .map(|buf| std::mem::take(&mut *buf.lock().unwrap_or_else(|e| e.into_inner())))
            .unwrap_or_default();
        let payload = RaftPayload {
            request: ledger_request,
            proposed_at: chrono::Utc::now(),
            caller: ctx.caller_or_zero(),
            state_root_commitments: commitments,
        };
        let response = match region.handle.propose_and_wait(payload, timeout).await {
            Ok(result) => result,
            Err(inferadb_ledger_raft::HandleError::Timeout { .. }) => {
                ctx.end_raft_timer();
                ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                metrics::record_raft_proposal_timeout();
                return Err(status_with_correlation(
                    Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )),
                    &ctx.request_id(),
                    ctx.trace_id(),
                ));
            },
            Err(e) => {
                ctx.end_raft_timer();
                ctx.set_error("RaftError", &e.to_string());
                return Err(status_with_correlation(
                    classify_raft_error(&e.to_string()),
                    &ctx.request_id(),
                    ctx.trace_id(),
                ));
            },
        };
        ctx.end_raft_timer();

        match response {
            LedgerResponse::Write { block_height, block_hash, assigned_sequence } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof, proof_error_reason) = if req.include_tx_proofs {
                    self.generate_write_proof(organization_id, vault_id, block_height)
                } else {
                    (None, None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header: block_header.clone(),
                    tx_proof,
                    assigned_sequence,
                };

                // Cache the result for idempotency
                self.idempotency.insert(
                    organization_id,
                    vault_id,
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    success.clone(),
                );
                metrics::set_idempotency_cache_size(self.idempotency.len());

                // Set success outcome with block info
                ctx.set_success();
                ctx.set_block_height(block_height);
                ctx.set_block_hash(&Self::bytes_to_hex(&block_hash));
                if let Some(ref header) = block_header
                    && let Some(ref state_root) = header.state_root
                {
                    ctx.set_state_root(&Self::bytes_to_hex(&state_root.value));
                }

                let elapsed = ctx.elapsed_secs();
                metrics::record_organization_latency(organization_id, "write", elapsed);

                let mut response = response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                BatchWriteSuccess {
                                    tx_id: success.tx_id,
                                    block_height: success.block_height,
                                    block_header: success.block_header,
                                    tx_proof: success.tx_proof,
                                    assigned_sequence: success.assigned_sequence,
                                },
                            ),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                );
                if let Some(reason) = proof_error_reason
                    && let Ok(val) = tonic::metadata::MetadataValue::try_from(reason)
                {
                    response.metadata_mut().insert("x-proof-unavailable-reason", val);
                }
                Ok(response)
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);

                Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                WriteError {
                                    code: WriteErrorCode::Unspecified.into(),
                                    key: String::new(),
                                    current_version: None,
                                    current_value: None,
                                    message,
                                    committed_tx_id: None,
                                    committed_block_height: None,
                                    assigned_sequence: None,
                                },
                            ),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was
                // last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                ctx.set_precondition_failed(Some(&key));

                Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Error(
                                WriteError {
                                    code: error_code.into(),
                                    key,
                                    current_version,
                                    current_value,
                                    message: "Precondition failed".to_string(),
                                    committed_tx_id: None,
                                    committed_block_height: None,
                                    assigned_sequence: None,
                                },
                            ),
                        ),
                    },
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &ctx.request_id(),
                    ctx.trace_id(),
                ))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_proto::proto::{self, WriteErrorCode};
    use inferadb_ledger_raft::batching::BatchError;
    use inferadb_ledger_types::{config::ValidationConfig, validation};
    use tonic::Status;

    use super::{WriteService, classify_batch_error};

    // =========================================================================
    // classify_batch_error
    // =========================================================================

    #[test]
    fn classify_batch_error_dropped_returns_unavailable() {
        let status = classify_batch_error(&BatchError::Dropped);
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn classify_batch_error_internal_returns_internal() {
        let status = classify_batch_error(&BatchError::Internal("disk full".into()));
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn classify_batch_error_raft_leadership_returns_unavailable() {
        let status = classify_batch_error(&BatchError::RaftError("not leader".into()));
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn classify_batch_error_raft_generic_returns_internal() {
        let status = classify_batch_error(&BatchError::RaftError("storage failure".into()));
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    /// Helper: run the same validate-and-map-to-Status logic the service uses.
    fn validate_proto_operations(
        operations: &[proto::Operation],
        config: &ValidationConfig,
    ) -> Result<(), Status> {
        validation::validate_operations_count(operations.len(), config)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut total_bytes: usize = 0;
        for proto_op in operations {
            let Some(ref op) = proto_op.op else {
                return Err(Status::invalid_argument("Operation missing op field"));
            };
            match op {
                proto::operation::Op::SetEntity(se) => {
                    validation::validate_key(&se.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_value(&se.value, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += se.key.len() + se.value.len();
                },
                proto::operation::Op::DeleteEntity(de) => {
                    validation::validate_key(&de.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += de.key.len();
                },
                proto::operation::Op::ExpireEntity(ee) => {
                    validation::validate_key(&ee.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += ee.key.len();
                },
                proto::operation::Op::CreateRelationship(cr) => {
                    validation::validate_relationship_string(&cr.resource, "resource", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&cr.relation, "relation", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&cr.subject, "subject", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += cr.resource.len() + cr.relation.len() + cr.subject.len();
                },
                proto::operation::Op::DeleteRelationship(dr) => {
                    validation::validate_relationship_string(&dr.resource, "resource", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&dr.relation, "relation", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&dr.subject, "subject", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += dr.resource.len() + dr.relation.len() + dr.subject.len();
                },
            }
        }

        validation::validate_batch_payload_bytes(total_bytes, config)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        Ok(())
    }

    fn make_set_entity(key: &str, value: &[u8]) -> proto::Operation {
        proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: key.to_string(),
                value: value.to_vec(),
                expires_at: None,
                condition: None,
            })),
        }
    }

    fn make_delete_entity(key: &str) -> proto::Operation {
        proto::Operation {
            op: Some(proto::operation::Op::DeleteEntity(proto::DeleteEntity {
                key: key.to_string(),
            })),
        }
    }

    fn make_create_relationship(resource: &str, relation: &str, subject: &str) -> proto::Operation {
        proto::Operation {
            op: Some(proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })),
        }
    }

    // =========================================================================
    // Validation → gRPC Status mapping tests
    // =========================================================================

    #[test]
    fn valid_set_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user:123", b"data")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn valid_delete_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_delete_entity("user:123")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn valid_relationship_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc:456", "viewer", "user:123")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn empty_key_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("", b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("key"), "Error should mention key: {}", err.message());
    }

    #[test]
    fn key_with_invalid_chars_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user 123", b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn key_exceeding_max_size_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_key_bytes(10).build().unwrap();
        let ops = vec![make_set_entity(&"a".repeat(11), b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("key"), "Error should mention key: {}", err.message());
    }

    #[test]
    fn value_exceeding_max_size_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_value_bytes(4).build().unwrap();
        let ops = vec![make_set_entity("key", &[0u8; 5])];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("value"), "Error should mention value: {}", err.message());
    }

    #[test]
    fn test_too_many_operations_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops = vec![
            make_set_entity("a", b"1"),
            make_set_entity("b", b"2"),
            make_set_entity("c", b"3"),
        ];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("operations"),
            "Error should mention operations: {}",
            err.message()
        );
    }

    #[test]
    fn test_zero_operations_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops: Vec<proto::Operation> = vec![];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_payload_exceeding_max_bytes_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_batch_payload_bytes(10).build().unwrap();
        let ops = vec![make_set_entity("key", &[0u8; 11])];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("payload"),
            "Error should mention payload: {}",
            err.message()
        );
    }

    #[test]
    fn test_missing_op_field_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![proto::Operation { op: None }];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("missing"),
            "Error should mention missing: {}",
            err.message()
        );
    }

    #[test]
    fn test_relationship_invalid_chars_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc 456", "viewer", "user:123")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_relationship_empty_field_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc:456", "", "user:123")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_key_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_key_bytes(5).build().unwrap();
        let ops = vec![make_set_entity("abcde", b"v")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_value_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_value_bytes(5).build().unwrap();
        let ops = vec![make_set_entity("k", &[0u8; 5])];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_operations_at_exact_limit_passes() {
        let config = ValidationConfig::builder().max_operations_per_write(2).build().unwrap();
        let ops = vec![make_set_entity("a", b"1"), make_set_entity("b", b"2")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    // =========================================================================
    // map_condition_to_error_code
    // =========================================================================

    #[test]
    fn map_condition_must_not_exist_failed_returns_key_exists() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::MustNotExist),
            true,
        );
        assert_eq!(code, WriteErrorCode::KeyExists);
    }

    #[test]
    fn map_condition_must_exist_failed_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::MustExist),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_version_equals_key_exists_returns_version_mismatch() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::VersionEquals(42)),
            true,
        );
        assert_eq!(code, WriteErrorCode::VersionMismatch);
    }

    #[test]
    fn map_condition_version_equals_key_missing_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::VersionEquals(42)),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_value_equals_key_exists_returns_value_mismatch() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::ValueEquals(b"expected".to_vec())),
            true,
        );
        assert_eq!(code, WriteErrorCode::ValueMismatch);
    }

    #[test]
    fn map_condition_value_equals_key_missing_returns_key_not_found() {
        let code = WriteService::map_condition_to_error_code(
            Some(&inferadb_ledger_types::SetCondition::ValueEquals(b"expected".to_vec())),
            false,
        );
        assert_eq!(code, WriteErrorCode::KeyNotFound);
    }

    #[test]
    fn map_condition_none_returns_unspecified() {
        let code = WriteService::map_condition_to_error_code(None, true);
        assert_eq!(code, WriteErrorCode::Unspecified);
    }

    // =========================================================================
    // bytes_to_hex
    // =========================================================================

    #[test]
    fn bytes_to_hex_empty() {
        assert_eq!(WriteService::bytes_to_hex(&[]), "");
    }

    #[test]
    fn bytes_to_hex_single_byte() {
        assert_eq!(WriteService::bytes_to_hex(&[0xff]), "ff");
    }

    #[test]
    fn bytes_to_hex_zero_padded() {
        assert_eq!(WriteService::bytes_to_hex(&[0x0a]), "0a");
    }

    #[test]
    fn bytes_to_hex_multiple_bytes() {
        assert_eq!(WriteService::bytes_to_hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn bytes_to_hex_all_zeros() {
        assert_eq!(WriteService::bytes_to_hex(&[0, 0, 0]), "000000");
    }

    // =========================================================================
    // extract_operation_types
    // =========================================================================

    #[test]
    fn extract_operation_types_empty() {
        let ops: Vec<proto::Operation> = vec![];
        let types = WriteService::extract_operation_types(&ops);
        assert!(types.is_empty());
    }

    #[test]
    fn extract_operation_types_all_variants() {
        let ops = vec![
            make_set_entity("k", b"v"),
            make_delete_entity("k"),
            proto::Operation {
                op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
                    key: "k".to_string(),
                    expired_at: 100,
                })),
            },
            make_create_relationship("r", "rel", "s"),
            proto::Operation {
                op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: "r".to_string(),
                    relation: "rel".to_string(),
                    subject: "s".to_string(),
                })),
            },
        ];
        let types = WriteService::extract_operation_types(&ops);
        assert_eq!(
            types,
            vec![
                "set_entity",
                "delete_entity",
                "expire_entity",
                "create_relationship",
                "delete_relationship",
            ]
        );
    }

    #[test]
    fn extract_operation_types_skips_none_op() {
        let ops = vec![proto::Operation { op: None }, make_set_entity("k", b"v")];
        let types = WriteService::extract_operation_types(&ops);
        assert_eq!(types, vec!["set_entity"]);
    }

    // =========================================================================
    // estimate_operations_bytes
    // =========================================================================

    #[test]
    fn estimate_operations_bytes_empty() {
        let ops: Vec<proto::Operation> = vec![];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 0);
    }

    #[test]
    fn estimate_operations_bytes_set_entity() {
        let ops = vec![make_set_entity("key", b"value")];
        // "key" = 3 bytes, "value" = 5 bytes
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 8);
    }

    #[test]
    fn estimate_operations_bytes_delete_entity() {
        let ops = vec![make_delete_entity("entity:1")];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 8);
    }

    #[test]
    fn estimate_operations_bytes_expire_entity() {
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::ExpireEntity(proto::ExpireEntity {
                key: "abc".to_string(),
                expired_at: 100,
            })),
        }];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 3);
    }

    #[test]
    fn estimate_operations_bytes_create_relationship() {
        let ops = vec![make_create_relationship("doc:1", "viewer", "user:1")];
        // "doc:1" = 5, "viewer" = 6, "user:1" = 6
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 17);
    }

    #[test]
    fn estimate_operations_bytes_delete_relationship() {
        let ops = vec![proto::Operation {
            op: Some(proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:1".to_string(),
            })),
        }];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 17);
    }

    #[test]
    fn estimate_operations_bytes_mixed_operations() {
        let ops = vec![
            make_set_entity("k", b"v"),              // 1 + 1 = 2
            make_delete_entity("dk"),                // 2
            make_create_relationship("r", "x", "s"), // 1 + 1 + 1 = 3
        ];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 7);
    }

    #[test]
    fn estimate_operations_bytes_skips_none_op() {
        let ops = vec![proto::Operation { op: None }, make_set_entity("k", b"v")];
        assert_eq!(WriteService::estimate_operations_bytes(&ops), 2);
    }
}
