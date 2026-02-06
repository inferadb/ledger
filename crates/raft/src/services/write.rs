//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.
//!
//! Per DESIGN.md §6.3: Uses application-level batching to coalesce multiple
//! write requests into single Raft proposals, improving throughput by reducing
//! consensus round-trips.

use std::{fmt::Write, sync::Arc};

use inferadb_ledger_state::BlockArchive;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    NamespaceId, SetCondition, VaultId,
    audit::{AuditAction, AuditEvent, AuditOutcome, AuditResource},
};
use openraft::Raft;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    IdempotencyCache,
    batching::{BatchConfig, BatchWriter, BatchWriterHandle},
    log_storage::AppliedStateAccessor,
    metrics,
    proof::{self, ProofError},
    proto::{
        BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
        WriteRequest, WriteResponse, WriteSuccess, write_service_server::WriteService,
    },
    rate_limit::RateLimiter,
    trace_context,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig},
    wide_events::{OperationType, RequestContext, Sampler},
};

/// Write service implementation.
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
    /// Handle for submitting writes to the batch writer.
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
}

#[allow(clippy::result_large_err)]
impl WriteServiceImpl {
    /// Create a write service with batching enabled.
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
        };

        (service, run_future)
    }

    /// Create a write service with block archive and batching enabled.
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

    /// Add per-namespace rate limiting to an existing service.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Add applied state accessor for sequence gap detection.
    #[must_use]
    pub fn with_applied_state(mut self, applied_state: AppliedStateAccessor) -> Self {
        self.applied_state = Some(applied_state);
        self
    }

    /// Add audit logger for compliance event tracking.
    #[must_use]
    pub fn with_audit_logger(mut self, logger: Arc<dyn crate::audit::AuditLogger>) -> Self {
        self.audit_logger = Some(logger);
        self
    }

    /// Add hot key detector for identifying frequently accessed keys.
    #[must_use]
    pub fn with_hot_key_detector(
        mut self,
        detector: Arc<crate::hot_key_detector::HotKeyDetector>,
    ) -> Self {
        self.hot_key_detector = Some(detector);
        self
    }

    /// Emit an audit event and record the corresponding Prometheus metric.
    ///
    /// If the audit logger is not configured, this is a no-op (only metrics recorded).
    /// If the audit log write fails, logs a warning but does not propagate the error —
    /// the primary operation's durability takes precedence over audit logging.
    fn emit_audit_event(&self, event: &AuditEvent) {
        let outcome_str = match &event.outcome {
            AuditOutcome::Success => "success",
            AuditOutcome::Failed { .. } => "failed",
            AuditOutcome::Denied { .. } => "denied",
        };
        metrics::record_audit_event(event.action.as_str(), outcome_str);

        if let Some(ref logger) = self.audit_logger
            && let Err(e) = logger.log(event)
        {
            warn!(
                event_id = %event.event_id,
                action = %event.action.as_str(),
                error = %e,
                "Failed to write audit log event"
            );
        }
    }

    /// Build an audit event for a write-path operation.
    fn build_audit_event(
        &self,
        action: AuditAction,
        principal: &str,
        resource: AuditResource,
        outcome: AuditOutcome,
        trace_id: Option<&str>,
        operations_count: Option<usize>,
    ) -> AuditEvent {
        AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_id: Uuid::new_v4().to_string(),
            principal: principal.to_string(),
            action,
            resource,
            outcome,
            node_id: self.node_id,
            trace_id: trace_id.map(String::from),
            operations_count,
        }
    }

    /// Check all rate limit tiers (backpressure, namespace, client).
    ///
    /// Returns `Status::resource_exhausted` with `retry-after-ms` metadata if rejected.
    fn check_rate_limit(&self, client_id: &str, namespace_id: NamespaceId) -> Result<(), Status> {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.check(client_id, namespace_id).map_err(|rejection| {
                metrics::record_rate_limit_rejected(
                    rejection.level.as_str(),
                    rejection.reason.as_str(),
                );
                let mut status = Status::resource_exhausted(rejection.to_string());
                // Attach retry-after-ms as gRPC trailing metadata
                if let Ok(val) =
                    tonic::metadata::MetadataValue::try_from(rejection.retry_after_ms.to_string())
                {
                    status.metadata_mut().insert("retry-after-ms", val);
                }
                status
            })?;
        }
        Ok(())
    }

    /// Record key accesses from operations for hot key detection.
    ///
    /// Extracts entity/relationship keys from each operation and feeds them
    /// to the hot key detector. This runs after rate limiting but before
    /// Raft proposal, tracking all non-duplicate, non-rate-limited accesses.
    fn record_hot_keys(&self, vault_id: VaultId, operations: &[crate::proto::Operation]) {
        if let Some(ref detector) = self.hot_key_detector {
            use crate::proto::operation::Op;
            for op in operations {
                if let Some(ref inner) = op.op {
                    let key = match inner {
                        Op::SetEntity(set) => &set.key,
                        Op::DeleteEntity(del) => &del.key,
                        Op::CreateRelationship(rel) => &rel.resource,
                        Op::DeleteRelationship(rel) => &rel.resource,
                        Op::ExpireEntity(exp) => &exp.key,
                    };
                    detector.record_access(vault_id, key);
                }
            }
        }
    }

    /// Map a failed SetCondition to the appropriate WriteErrorCode.
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

    /// Convert bytes to hex string for wide event logging.
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Extract operation type names from proto operations for wide event logging.
    fn extract_operation_types(operations: &[crate::proto::Operation]) -> Vec<&'static str> {
        use crate::proto::operation::Op;

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
    fn estimate_operations_bytes(operations: &[crate::proto::Operation]) -> usize {
        use crate::proto::operation::Op;

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

    /// Compute a hash of operations for idempotency payload comparison.
    ///
    /// Uses a simple concatenation of operation fields to create a deterministic
    /// byte sequence that can be hashed with seahash.
    fn hash_operations(operations: &[crate::proto::Operation]) -> Vec<u8> {
        use crate::proto::operation::Op;

        let mut bytes = Vec::new();
        for op in operations {
            if let Some(inner) = &op.op {
                match inner {
                    Op::CreateRelationship(cr) => {
                        bytes.extend_from_slice(b"CR");
                        bytes.extend_from_slice(cr.resource.as_bytes());
                        bytes.extend_from_slice(cr.relation.as_bytes());
                        bytes.extend_from_slice(cr.subject.as_bytes());
                    },
                    Op::DeleteRelationship(dr) => {
                        bytes.extend_from_slice(b"DR");
                        bytes.extend_from_slice(dr.resource.as_bytes());
                        bytes.extend_from_slice(dr.relation.as_bytes());
                        bytes.extend_from_slice(dr.subject.as_bytes());
                    },
                    Op::SetEntity(se) => {
                        bytes.extend_from_slice(b"SE");
                        bytes.extend_from_slice(se.key.as_bytes());
                        bytes.extend_from_slice(&se.value);
                        if let Some(cond) = &se.condition {
                            bytes.extend_from_slice(&format!("{:?}", cond).into_bytes());
                        }
                        if let Some(exp) = se.expires_at {
                            bytes.extend_from_slice(&exp.to_le_bytes());
                        }
                    },
                    Op::DeleteEntity(de) => {
                        bytes.extend_from_slice(b"DE");
                        bytes.extend_from_slice(de.key.as_bytes());
                    },
                    Op::ExpireEntity(ee) => {
                        bytes.extend_from_slice(b"EE");
                        bytes.extend_from_slice(ee.key.as_bytes());
                        bytes.extend_from_slice(&ee.expired_at.to_le_bytes());
                    },
                }
            }
        }
        bytes
    }

    /// Convert a proto SetCondition to internal SetCondition.
    fn convert_set_condition(
        proto_condition: &crate::proto::SetCondition,
    ) -> Option<inferadb_ledger_types::SetCondition> {
        use crate::proto::set_condition::Condition;

        proto_condition.condition.as_ref().map(|c| match c {
            Condition::NotExists(true) => inferadb_ledger_types::SetCondition::MustNotExist,
            Condition::NotExists(false) => inferadb_ledger_types::SetCondition::MustExist,
            Condition::MustExists(true) => inferadb_ledger_types::SetCondition::MustExist,
            Condition::MustExists(false) => inferadb_ledger_types::SetCondition::MustNotExist,
            Condition::Version(v) => inferadb_ledger_types::SetCondition::VersionEquals(*v),
            Condition::ValueEquals(v) => {
                inferadb_ledger_types::SetCondition::ValueEquals(v.clone())
            },
        })
    }

    /// Convert a proto Operation to internal Operation.
    fn convert_operation(
        proto_op: &crate::proto::Operation,
    ) -> Result<inferadb_ledger_types::Operation, Status> {
        use crate::proto::operation::Op;

        let op = proto_op
            .op
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Operation missing op field"))?;

        match op {
            Op::CreateRelationship(cr) => {
                Ok(inferadb_ledger_types::Operation::CreateRelationship {
                    resource: cr.resource.clone(),
                    relation: cr.relation.clone(),
                    subject: cr.subject.clone(),
                })
            },
            Op::DeleteRelationship(dr) => {
                Ok(inferadb_ledger_types::Operation::DeleteRelationship {
                    resource: dr.resource.clone(),
                    relation: dr.relation.clone(),
                    subject: dr.subject.clone(),
                })
            },
            Op::SetEntity(se) => {
                let condition = se.condition.as_ref().and_then(Self::convert_set_condition);

                Ok(inferadb_ledger_types::Operation::SetEntity {
                    key: se.key.clone(),
                    value: se.value.clone(),
                    condition,
                    expires_at: se.expires_at,
                })
            },
            Op::DeleteEntity(de) => {
                Ok(inferadb_ledger_types::Operation::DeleteEntity { key: de.key.clone() })
            },
            Op::ExpireEntity(ee) => Ok(inferadb_ledger_types::Operation::ExpireEntity {
                key: ee.key.clone(),
                expired_at: ee.expired_at,
            }),
        }
    }

    /// Generate block header and transaction proof for a committed write.
    ///
    /// Returns (block_header, tx_proof) if successful, (None, None) on error.
    /// Errors are logged with context for debugging.
    fn generate_write_proof(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> (Option<crate::proto::BlockHeader>, Option<crate::proto::MerkleProof>) {
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

    /// Convert proto operations to LedgerRequest.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0 here;
    /// the actual sequence will be assigned by the Raft state machine at apply time.
    fn operations_to_request(
        &self,
        namespace_id: NamespaceId,
        vault_id: Option<VaultId>,
        operations: &[crate::proto::Operation],
        client_id: &str,
    ) -> Result<LedgerRequest, Status> {
        // Convert proto operations to internal Operations
        // Time the proto → internal type conversion
        let convert_start = std::time::Instant::now();
        let internal_ops: Vec<inferadb_ledger_types::Operation> =
            operations.iter().map(Self::convert_operation).collect::<Result<Vec<_>, Status>>()?;
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
                return Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Success(cached)),
                }));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                metrics::record_write(false, ctx.elapsed_secs());
                return Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Error(WriteError {
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
                }));
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
            return Err(status);
        }

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request =
            self.operations_to_request(namespace_id, Some(vault_id), &req.operations, &client_id)?;

        // Submit to Raft via batch writer (if enabled) or direct proposal
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let response = if let Some(ref batch_handle) = self.batch_handle {
            ctx.set_batch_info(true, 1);
            // Submit through batch writer for coalesced proposals
            let rx = batch_handle.submit(ledger_request);
            match rx.await {
                Ok(Ok(resp)) => resp,
                Ok(Err(batch_err)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchError", &batch_err.to_string());
                    metrics::record_write(false, ctx.elapsed_secs());
                    return Err(Status::internal(format!("Batch error: {}", batch_err)));
                },
                Err(_recv_err) => {
                    ctx.end_raft_timer();
                    ctx.set_error("BatchChannelClosed", "Batch writer channel closed");
                    metrics::record_write(false, ctx.elapsed_secs());
                    return Err(Status::internal("Batch writer unavailable"));
                },
            }
        } else {
            ctx.set_batch_info(false, 1);
            // Fallback: direct Raft proposal with mutex serialization
            let _guard = self.proposal_mutex.lock().await;
            let result = self.raft.client_write(ledger_request).await.map_err(|e| {
                ctx.end_raft_timer();
                ctx.set_error("RaftError", &e.to_string());
                metrics::record_write(false, ctx.elapsed_secs());
                Status::internal(format!("Raft error: {}", e))
            })?;
            drop(_guard);
            result.data
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

                metrics::record_write(true, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::Write,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                    Some(req.operations.len()),
                ));

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Success(success)),
                }))
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

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Error(WriteError {
                        code: WriteErrorCode::Unspecified.into(),
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message,
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }))
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

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Error(WriteError {
                        code: error_code.into(),
                        key,
                        current_version,
                        current_value,
                        message: "Precondition failed".to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                metrics::record_write(false, ctx.elapsed_secs());
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
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
        let all_operations: Vec<crate::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

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
                return Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Success(
                        BatchWriteSuccess {
                            tx_id: cached.tx_id,
                            block_height: cached.block_height,
                            block_header: cached.block_header,
                            tx_proof: cached.tx_proof,
                            assigned_sequence: cached.assigned_sequence,
                        },
                    )),
                }));
            },
            IdempotencyCheckResult::KeyReused => {
                ctx.set_error(
                    "IdempotencyKeyReused",
                    "Idempotency key reused with different payload",
                );
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
                return Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Error(WriteError {
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
                }));
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
            return Err(status);
        }

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &all_operations);

        // Server-assigned sequences: no gap check needed

        // Convert to internal request
        let ledger_request =
            self.operations_to_request(namespace_id, Some(vault_id), &all_operations, &client_id)?;

        // Submit to Raft (serialized to prevent concurrent proposal race condition)
        metrics::record_raft_proposal();
        ctx.start_raft_timer();
        let _guard = self.proposal_mutex.lock().await;
        let result = self.raft.client_write(ledger_request).await.map_err(|e| {
            ctx.end_raft_timer();
            ctx.set_error("RaftError", &e.to_string());
            metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
            Status::internal(format!("Raft error: {}", e))
        })?;
        drop(_guard);
        ctx.end_raft_timer();

        // Extract response
        let response = result.data;

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

                metrics::record_batch_write(true, batch_size, ctx.elapsed_secs());

                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::BatchWrite,
                    &client_id,
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                    Some(batch_size),
                ));

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Success(
                        BatchWriteSuccess {
                            tx_id: success.tx_id,
                            block_height: success.block_height,
                            block_header: success.block_header,
                            tx_proof: success.tx_proof,
                            assigned_sequence: success.assigned_sequence,
                        },
                    )),
                }))
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

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Error(WriteError {
                        code: WriteErrorCode::Unspecified.into(),
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message,
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }))
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

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Error(WriteError {
                        code: error_code.into(),
                        key,
                        current_version,
                        current_value,
                        message: "Precondition failed".to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                metrics::record_batch_write(false, batch_size, ctx.elapsed_secs());
                Err(Status::internal("Unexpected response type"))
            },
        }
    }
}
