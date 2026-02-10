//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.
//!
//! Per DESIGN.md §6.3: Uses application-level batching to coalesce multiple
//! write requests into single Raft proposals, improving throughput by reducing
//! consensus round-trips.

use std::{fmt::Write, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
    WriteRequest, WriteResponse, WriteSuccess, write_service_server::WriteService,
};
use inferadb_ledger_state::BlockArchive;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    NamespaceId, SetCondition, VaultId,
    audit::{AuditAction, AuditEvent, AuditOutcome, AuditResource},
    config::ValidationConfig,
};
use openraft::Raft;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};
use uuid::Uuid;

pub(crate) use super::metadata::{response_with_correlation, status_with_correlation};
use crate::{
    batching::{BatchConfig, BatchError, BatchWriter, BatchWriterHandle},
    error::classify_raft_error,
    idempotency::IdempotencyCache,
    log_storage::AppliedStateAccessor,
    metrics,
    proof::{self, ProofError},
    rate_limit::RateLimiter,
    trace_context,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig},
    wide_events::{OperationType, RequestContext, Sampler},
};

/// Classifies a batch writer error into the appropriate `tonic::Status`.
///
/// `BatchError::RaftError` may contain leadership-related messages that
/// should map to `UNAVAILABLE` (retryable) instead of `INTERNAL`.
fn classify_batch_error(err: &BatchError) -> Status {
    match err {
        BatchError::RaftError(msg) => classify_raft_error(msg),
        BatchError::Dropped => Status::unavailable("Batch writer dropped request"),
        BatchError::Internal(msg) => Status::internal(format!("Batch error: {}", msg)),
    }
}

/// Writes service implementation.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct WriteServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for proof generation (optional).
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Per-namespace rate limiter.
    #[builder(default)]
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Accessor for applied state (client sequences for gap detection).
    #[builder(default)]
    applied_state: Option<AppliedStateAccessor>,
    /// Handles for submitting writes to the batch writer.
    ///
    /// Per DESIGN.md §6.3: Writes are coalesced into batches and submitted
    /// as single Raft proposals for improved throughput.
    #[builder(default)]
    batch_handle: Option<BatchWriterHandle>,
    /// Mutex to serialize Raft proposals (fallback when batching disabled).
    ///
    /// Used for BatchWriteRequest which bypasses the batch writer, or
    /// when batch_handle is None.
    #[builder(default = Arc::new(Mutex::new(())))]
    proposal_mutex: Arc<Mutex<()>>,
    /// Sampler for wide events tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Node ID for wide events system context.
    #[builder(default)]
    node_id: Option<u64>,
    /// Audit logger for compliance-ready event tracking.
    #[builder(default)]
    audit_logger: Option<Arc<dyn crate::audit::AuditLogger>>,
    /// Hot key detector for identifying frequently accessed keys.
    #[builder(default)]
    hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Per-namespace resource quota checker.
    #[builder(default)]
    quota_checker: Option<crate::quota::QuotaChecker>,
}

#[allow(clippy::result_large_err)]
impl WriteServiceImpl {
    /// Creates a write service with batching enabled.
    ///
    /// Per DESIGN.md §6.3: Batching coalesces multiple writes into single
    /// Raft proposals for improved throughput.
    ///
    /// Returns the service and a task handle for the batch writer background loop.
    /// The caller must spawn or await the returned task.
    pub fn new_with_batching(
        raft: Arc<Raft<LedgerTypeConfig>>,
        idempotency: Arc<IdempotencyCache>,
        config: BatchConfig,
    ) -> (Self, impl std::future::Future<Output = ()> + Send + 'static) {
        let raft_clone = raft.clone();
        let proposal_mutex = Arc::new(Mutex::new(()));
        let mutex_clone = proposal_mutex.clone();

        // Create the submit function that wraps requests into BatchWrite
        let submit_fn = move |requests: Vec<LedgerRequest>| {
            let raft = raft_clone.clone();
            let mutex = mutex_clone.clone();

            Box::pin(async move {
                // Wrap multiple requests into a single BatchWrite
                let batch_request = LedgerRequest::BatchWrite { requests };

                // Acquire mutex and submit to Raft
                let _guard = mutex.lock().await;
                let result = raft.client_write(batch_request).await;
                drop(_guard);

                match result {
                    Ok(response) => {
                        // Unwrap the BatchWrite response
                        match response.data {
                            LedgerResponse::BatchWrite { responses } => Ok(responses),
                            other => {
                                // Single request case - shouldn't happen but handle it
                                Ok(vec![other])
                            },
                        }
                    },
                    Err(e) => Err(format!("Raft error: {}", e)),
                }
            })
                as futures::future::BoxFuture<'static, Result<Vec<LedgerResponse>, String>>
        };

        let writer = BatchWriter::new(config, submit_fn);
        let handle = writer.handle();
        let run_future = writer.run();

        let service = Self {
            raft,
            idempotency,
            block_archive: None,
            rate_limiter: None,
            applied_state: None,
            batch_handle: Some(handle),
            proposal_mutex,
            sampler: None,
            node_id: None,
            audit_logger: None,
            hot_key_detector: None,
            validation_config: Arc::new(ValidationConfig::default()),
            proposal_timeout: Duration::from_secs(30),
            quota_checker: None,
        };

        (service, run_future)
    }

    /// Creates a write service with block archive and batching enabled.
    pub fn with_block_archive_and_batching(
        raft: Arc<Raft<LedgerTypeConfig>>,
        idempotency: Arc<IdempotencyCache>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        config: BatchConfig,
    ) -> (Self, impl std::future::Future<Output = ()> + Send + 'static) {
        let (mut service, run_future) = Self::new_with_batching(raft, idempotency, config);
        service.block_archive = Some(block_archive);
        (service, run_future)
    }

    /// Adds per-namespace rate limiting to an existing service.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Adds applied state accessor for sequence gap detection.
    #[must_use]
    pub fn with_applied_state(mut self, applied_state: AppliedStateAccessor) -> Self {
        self.applied_state = Some(applied_state);
        self
    }

    /// Adds audit logger for compliance event tracking.
    #[must_use]
    pub fn with_audit_logger(mut self, logger: Arc<dyn crate::audit::AuditLogger>) -> Self {
        self.audit_logger = Some(logger);
        self
    }

    /// Adds hot key detector for identifying frequently accessed keys.
    #[must_use]
    pub fn with_hot_key_detector(
        mut self,
        detector: Arc<crate::hot_key_detector::HotKeyDetector>,
    ) -> Self {
        self.hot_key_detector = Some(detector);
        self
    }

    /// Sets input validation configuration for request field limits.
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

    /// Sets the per-namespace resource quota checker.
    #[must_use]
    pub fn with_quota_checker(mut self, checker: crate::quota::QuotaChecker) -> Self {
        self.quota_checker = Some(checker);
        self
    }

    /// Validates all operations in a proto operation list.
    fn validate_operations(
        &self,
        operations: &[inferadb_ledger_proto::proto::Operation],
    ) -> Result<(), Status> {
        super::helpers::validate_operations(operations, &self.validation_config)
    }

    /// Emits an audit event and record the corresponding Prometheus metric.
    fn emit_audit_event(&self, event: &AuditEvent) {
        super::helpers::emit_audit_event(self.audit_logger.as_ref(), event);
    }

    /// Builds an audit event for a write-path operation.
    fn build_audit_event(
        &self,
        action: AuditAction,
        principal: &str,
        resource: AuditResource,
        outcome: AuditOutcome,
        trace_id: Option<&str>,
        operations_count: Option<usize>,
    ) -> AuditEvent {
        super::helpers::build_audit_event(
            action,
            principal,
            resource,
            outcome,
            self.node_id,
            trace_id,
            operations_count,
        )
    }

    /// Checks all rate limit tiers (backpressure, namespace, client).
    fn check_rate_limit(&self, client_id: &str, namespace_id: NamespaceId) -> Result<(), Status> {
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), client_id, namespace_id)
    }

    /// Records key accesses from operations for hot key detection.
    fn record_hot_keys(
        &self,
        vault_id: VaultId,
        operations: &[inferadb_ledger_proto::proto::Operation],
    ) {
        super::helpers::record_hot_keys(self.hot_key_detector.as_ref(), vault_id, operations);
    }

    /// Maps a failed SetCondition to the appropriate WriteErrorCode.
    ///
    /// Per DESIGN.md §6.1 and proto WriteErrorCode:
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

    /// Converts bytes to hex string for wide event logging.
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Extracts operation type names from proto operations for wide event logging.
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

    /// Computes a hash of operations for idempotency payload comparison.
    fn hash_operations(operations: &[inferadb_ledger_proto::proto::Operation]) -> Vec<u8> {
        super::helpers::hash_operations(operations)
    }

    /// Generates block header and transaction proof for a committed write.
    ///
    /// Returns (block_header, tx_proof) if successful, (None, None) on error.
    /// Errors are logged with context for debugging.
    fn generate_write_proof(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> (
        Option<inferadb_ledger_proto::proto::BlockHeader>,
        Option<inferadb_ledger_proto::proto::MerkleProof>,
    ) {
        let Some(archive) = &self.block_archive else {
            debug!("Block archive not available for proof generation");
            return (None, None);
        };

        // Use the proof module's SNAFU-based implementation
        match proof::generate_write_proof(archive, namespace_id, vault_id, vault_height, 0) {
            Ok(write_proof) => (Some(write_proof.block_header), Some(write_proof.tx_proof)),
            Err(e) => {
                // Log with appropriate severity based on error type
                match &e {
                    ProofError::BlockNotFound { .. } | ProofError::NoTransactions => {
                        // These may be timing issues (block not yet written to archive)
                        debug!(error = %e, "Proof generation skipped");
                    },
                    _ => {
                        warn!(error = %e, "Proof generation failed");
                    },
                }
                (None, None)
            },
        }
    }

    /// Converts proto operations to LedgerRequest.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0 here;
    /// the actual sequence will be assigned by the Raft state machine at apply time.
    fn operations_to_request(
        &self,
        namespace_id: NamespaceId,
        vault_id: Option<VaultId>,
        operations: &[inferadb_ledger_proto::proto::Operation],
        client_id: &str,
    ) -> Result<LedgerRequest, Status> {
        // Convert proto operations to internal Operations
        // Time the proto → internal type conversion
        let convert_start = std::time::Instant::now();
        let internal_ops: Vec<inferadb_ledger_types::Operation> = operations
            .iter()
            .map(inferadb_ledger_types::Operation::try_from)
            .collect::<Result<Vec<_>, Status>>()?;
        let convert_secs = convert_start.elapsed().as_secs_f64();

        // Record proto decode metrics (per operation average for consistency)
        if !operations.is_empty() {
            metrics::record_proto_decode(convert_secs / operations.len() as f64, "write_operation");
        }

        // Create a single transaction with all operations
        // Server-assigned sequences: sequence=0 is a placeholder; actual sequence
        // is assigned at Raft apply time for deterministic replay.
        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: client_id.to_string(),
            sequence: 0, // Server-assigned at apply time
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
            actor: "system".to_string(), // Actor is set by upstream Engine/Control
        };

        Ok(LedgerRequest::Write {
            namespace_id,
            vault_id: vault_id.unwrap_or(VaultId::new(0)),
            transactions: vec![transaction],
        })
    }
}

#[tonic::async_trait]
impl WriteService for WriteServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this request
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_operation_type(OperationType::Write);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Extract client ID and idempotency key
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();

        // Extract namespace and vault IDs (needed for idempotency key)
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );

        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Validate all operations before any processing
        self.validate_operations(&req.operations)?;

        // Compute request hash for payload comparison (detects key reuse with different payload)
        let request_hash = seahash::hash(&Self::hash_operations(&req.operations));

        // Populate wide event context with request metadata
        ctx.set_client_info(&client_id, 0, None);
        ctx.set_target(namespace_id.value(), vault_id.value());

        // Populate write operation fields
        let operation_types = Self::extract_operation_types(&req.operations);
        ctx.set_write_operation(req.operations.len(), operation_types, req.include_tx_proof);
        ctx.set_bytes_written(Self::estimate_operations_bytes(&req.operations));

        // Check idempotency cache for duplicate
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            namespace_id.value(),
            vault_id.value(),
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
                metrics::record_idempotency_hit();
                metrics::record_write(true, ctx.elapsed_secs());
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(cached),
                        ),
                    },
                    &ctx.request_id(),
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                metrics::record_write(false, ctx.elapsed_secs());
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
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::NewRequest => {
                // Proceed with new request
                ctx.set_idempotency_hit(false);
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limits (backpressure, namespace, client)
        if let Err(status) = self.check_rate_limit(&client_id, namespace_id) {
            ctx.set_rate_limited();
            self.emit_audit_event(&self.build_audit_event(
                AuditAction::Write,
                &client_id,
                AuditResource::vault(namespace_id, vault_id),
                AuditOutcome::Denied { reason: "rate_limited".to_string() },
                Some(&trace_ctx.trace_id),
                Some(req.operations.len()),
            ));
            return Err(status_with_correlation(status, &ctx.request_id(), &trace_ctx.trace_id));
        }

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&req.operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            namespace_id,
            estimated_bytes,
        )
        .map_err(|status| {
            status_with_correlation(status, &ctx.request_id(), &trace_ctx.trace_id)
        })?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request =
            self.operations_to_request(namespace_id, Some(vault_id), &req.operations, &client_id)?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft via batch writer (if enabled) or direct proposal
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let response = if let Some(ref batch_handle) = self.batch_handle {
            ctx.set_batch_info(true, 1);
            // Submit through batch writer for coalesced proposals
            let rx = batch_handle.submit(ledger_request);
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(Ok(resp))) => resp,
                Ok(Ok(Err(batch_err))) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchError", &batch_err.to_string());
                    metrics::record_write(false, ctx.elapsed_secs());
                    return Err(status_with_correlation(
                        classify_batch_error(&batch_err),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
                Ok(Err(_recv_err)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchChannelClosed", "Batch writer channel closed");
                    metrics::record_write(false, ctx.elapsed_secs());
                    return Err(status_with_correlation(
                        Status::internal("Batch writer unavailable"),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                    metrics::record_write(false, ctx.elapsed_secs());
                    metrics::record_raft_proposal_timeout();
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
            }
        } else {
            ctx.set_batch_info(false, 1);
            // Fallback: direct Raft proposal with mutex serialization
            let _guard = self.proposal_mutex.lock().await;
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    drop(_guard);
                    result.data
                },
                Ok(Err(e)) => {
                    drop(_guard);
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    metrics::record_write(false, ctx.elapsed_secs());
                    return Err(status_with_correlation(
                        classify_raft_error(&e.to_string()),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
                Err(_elapsed) => {
                    drop(_guard);
                    ctx.end_raft_timer();
                    ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                    metrics::record_write(false, ctx.elapsed_secs());
                    metrics::record_raft_proposal_timeout();
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
            }
        };
        ctx.end_raft_timer();

        match response {
            LedgerResponse::Write { block_height, block_hash, assigned_sequence } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof) = if req.include_tx_proof {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
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
                    namespace_id.value(),
                    vault_id.value(),
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
                metrics::record_write(true, elapsed);
                metrics::record_namespace_latency(namespace_id.value(), "write", elapsed);

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::Write,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                    Some(req.operations.len()),
                ));

                Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(success),
                        ),
                    },
                    &ctx.request_id(),
                    &trace_ctx.trace_id,
                ))
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                metrics::record_write(false, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::Write,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Failed {
                        code: "Unspecified".to_string(),
                        detail: message.clone(),
                    },
                    Some(&trace_ctx.trace_id),
                    Some(req.operations.len()),
                ));

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
                    &trace_ctx.trace_id,
                ))
            },
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Per DESIGN.md §6.1: Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was
                // last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                ctx.set_precondition_failed(Some(&key));
                metrics::record_write(false, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::Write,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Failed {
                        code: "PreconditionFailed".to_string(),
                        detail: format!("key: {key}"),
                    },
                    Some(&trace_ctx.trace_id),
                    Some(req.operations.len()),
                ));

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
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                metrics::record_write(false, ctx.elapsed_secs());
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &ctx.request_id(),
                    &trace_ctx.trace_id,
                ))
            },
        }
    }

    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this request
        let mut ctx = RequestContext::new("WriteService", "batch_write");
        ctx.set_operation_type(OperationType::Write);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Extract client ID and idempotency key
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();

        // Extract namespace and vault IDs (needed for idempotency key)
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );

        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Populate wide event context with request metadata
        ctx.set_client_info(&client_id, 0, None);
        ctx.set_target(namespace_id.value(), vault_id.value());

        // Flatten all operations from all groups
        let all_operations: Vec<inferadb_ledger_proto::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

        // Validate all operations before any processing
        self.validate_operations(&all_operations)?;

        let batch_size = all_operations.len();

        // Compute request hash for payload comparison (detects key reuse with different payload)
        let request_hash = seahash::hash(&Self::hash_operations(&all_operations));

        // Populate write operation fields
        let operation_types = Self::extract_operation_types(&all_operations);
        ctx.set_write_operation(batch_size, operation_types, req.include_tx_proofs);
        ctx.set_batch_info(false, batch_size);
        ctx.set_bytes_written(Self::estimate_operations_bytes(&all_operations));

        // Check idempotency cache for duplicate
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            namespace_id.value(),
            vault_id.value(),
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
                metrics::record_idempotency_hit();
                metrics::record_batch_write(true, 0, ctx.elapsed_secs());
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
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
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
                }, &ctx.request_id(), &trace_ctx.trace_id));
            },
            IdempotencyCheckResult::NewRequest => {
                // Proceed with new request
                ctx.set_idempotency_hit(false);
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limits (backpressure, namespace, client)
        if let Err(status) = self.check_rate_limit(&client_id, namespace_id) {
            ctx.set_rate_limited();
            self.emit_audit_event(&self.build_audit_event(
                AuditAction::BatchWrite,
                &client_id,
                AuditResource::vault(namespace_id, vault_id),
                AuditOutcome::Denied { reason: "rate_limited".to_string() },
                Some(&trace_ctx.trace_id),
                Some(batch_size),
            ));
            return Err(status_with_correlation(status, &ctx.request_id(), &trace_ctx.trace_id));
        }

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&all_operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            namespace_id,
            estimated_bytes,
        )
        .map_err(|status| {
            status_with_correlation(status, &ctx.request_id(), &trace_ctx.trace_id)
        })?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &all_operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request =
            self.operations_to_request(namespace_id, Some(vault_id), &all_operations, &client_id)?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft (serialized to prevent concurrent proposal race condition)
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let _guard = self.proposal_mutex.lock().await;
        let response =
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    drop(_guard);
                    result.data
                },
                Ok(Err(e)) => {
                    drop(_guard);
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
                    return Err(status_with_correlation(
                        classify_raft_error(&e.to_string()),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
                Err(_elapsed) => {
                    drop(_guard);
                    ctx.end_raft_timer();
                    ctx.set_error("ProposalTimeout", "Raft proposal timed out");
                    metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
                    metrics::record_raft_proposal_timeout();
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &ctx.request_id(),
                        &trace_ctx.trace_id,
                    ));
                },
            };
        ctx.end_raft_timer();

        match response {
            LedgerResponse::Write { block_height, block_hash, assigned_sequence } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof) = if req.include_tx_proofs {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
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
                    namespace_id.value(),
                    vault_id.value(),
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
                metrics::record_batch_write(true, batch_size, elapsed);
                metrics::record_namespace_latency(namespace_id.value(), "write", elapsed);

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::BatchWrite,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                    Some(batch_size),
                ));

                Ok(response_with_correlation(
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
                    &trace_ctx.trace_id,
                ))
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::BatchWrite,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Failed {
                        code: "Unspecified".to_string(),
                        detail: message.clone(),
                    },
                    Some(&trace_ctx.trace_id),
                    Some(batch_size),
                ));

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
                    &trace_ctx.trace_id,
                ))
            },
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Per DESIGN.md §6.1: Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was
                // last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                ctx.set_precondition_failed(Some(&key));
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::BatchWrite,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Failed {
                        code: "PreconditionFailed".to_string(),
                        detail: format!("key: {key}"),
                    },
                    Some(&trace_ctx.trace_id),
                    Some(batch_size),
                ));

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
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &ctx.request_id(),
                    &trace_ctx.trace_id,
                ))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_proto::proto;
    use inferadb_ledger_types::{config::ValidationConfig, validation};
    use tonic::Status;

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
    fn test_valid_set_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user:123", b"data")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_valid_delete_entity_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_delete_entity("user:123")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_valid_relationship_passes_validation() {
        let config = ValidationConfig::default();
        let ops = vec![make_create_relationship("doc:456", "viewer", "user:123")];
        assert!(validate_proto_operations(&ops, &config).is_ok());
    }

    #[test]
    fn test_empty_key_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("", b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("key"), "Error should mention key: {}", err.message());
    }

    #[test]
    fn test_key_with_invalid_chars_returns_invalid_argument() {
        let config = ValidationConfig::default();
        let ops = vec![make_set_entity("user 123", b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_key_exceeding_max_size_returns_invalid_argument() {
        let config = ValidationConfig::builder().max_key_bytes(10).build().unwrap();
        let ops = vec![make_set_entity(&"a".repeat(11), b"data")];
        let err = validate_proto_operations(&ops, &config).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("key"), "Error should mention key: {}", err.message());
    }

    #[test]
    fn test_value_exceeding_max_size_returns_invalid_argument() {
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
}
