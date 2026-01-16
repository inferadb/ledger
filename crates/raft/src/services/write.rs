//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.
//!
//! Per DESIGN.md §6.3: Uses application-level batching to coalesce multiple
//! write requests into single Raft proposals, improving throughput by reducing
//! consensus round-trips.

use std::sync::Arc;
use std::time::Instant;

use openraft::Raft;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::IdempotencyCache;
use crate::batching::{BatchConfig, BatchWriter, BatchWriterHandle};
use crate::log_storage::AppliedStateAccessor;
use crate::metrics;
use crate::proof::{self, ProofError};
use crate::proto::write_service_server::WriteService;
use crate::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
    WriteRequest, WriteResponse, WriteSuccess,
};
use crate::rate_limit::NamespaceRateLimiter;
use crate::types::{LedgerRequest, LedgerResponse, LedgerTypeConfig};

use inferadb_ledger_state::BlockArchive;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::SetCondition;

/// Write service implementation.
pub struct WriteServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for proof generation (optional).
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Per-namespace rate limiter.
    rate_limiter: Option<Arc<NamespaceRateLimiter>>,
    /// Accessor for applied state (client sequences for gap detection).
    applied_state: Option<AppliedStateAccessor>,
    /// Handle for submitting writes to the batch writer.
    ///
    /// Per DESIGN.md §6.3: Writes are coalesced into batches and submitted
    /// as single Raft proposals for improved throughput.
    batch_handle: Option<BatchWriterHandle>,
    /// Mutex to serialize Raft proposals (fallback when batching disabled).
    ///
    /// Used for BatchWriteRequest which bypasses the batch writer, or
    /// when batch_handle is None.
    proposal_mutex: Arc<Mutex<()>>,
}

#[allow(clippy::result_large_err)]
impl WriteServiceImpl {
    /// Create a new write service (without batching).
    ///
    /// For high-throughput scenarios, use `new_with_batching` instead.
    pub fn new(raft: Arc<Raft<LedgerTypeConfig>>, idempotency: Arc<IdempotencyCache>) -> Self {
        Self {
            raft,
            idempotency,
            block_archive: None,
            rate_limiter: None,
            applied_state: None,
            batch_handle: None,
            proposal_mutex: Arc::new(Mutex::new(())),
        }
    }

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
                            }
                        }
                    }
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
        };

        (service, run_future)
    }

    /// Create a write service with block archive access for proof generation.
    pub fn with_block_archive(
        raft: Arc<Raft<LedgerTypeConfig>>,
        idempotency: Arc<IdempotencyCache>,
        block_archive: Arc<BlockArchive<FileBackend>>,
    ) -> Self {
        Self {
            raft,
            idempotency,
            block_archive: Some(block_archive),
            rate_limiter: None,
            applied_state: None,
            batch_handle: None,
            proposal_mutex: Arc::new(Mutex::new(())),
        }
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

    /// Create a write service with per-namespace rate limiting.
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<NamespaceRateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Set the applied state accessor for sequence gap detection.
    pub fn with_applied_state(mut self, applied_state: AppliedStateAccessor) -> Self {
        self.applied_state = Some(applied_state);
        self
    }

    /// Check for sequence gaps before submitting to Raft.
    ///
    /// Per DESIGN.md §5.3: If `sequence > last_committed + 1`, the client
    /// has a gap indicating missed writes. Returns SEQUENCE_GAP error
    /// with the last committed sequence so clients can retry correctly.
    fn check_sequence_gap(
        &self,
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        sequence: u64,
    ) -> Result<(), WriteError> {
        // Sequence 0 is a special case (no sequence tracking)
        if sequence == 0 || client_id.is_empty() {
            return Ok(());
        }

        let Some(ref applied_state) = self.applied_state else {
            // No applied state accessor - skip gap detection
            return Ok(());
        };

        let last_committed = applied_state.client_sequence(namespace_id, vault_id, client_id);

        // Allow sequence == last_committed + 1 (normal case)
        // or sequence <= last_committed (idempotency cache handles duplicates)
        // Only reject if sequence > last_committed + 1 (gap detected)
        if sequence > last_committed + 1 {
            return Err(WriteError {
                code: WriteErrorCode::SequenceGap.into(),
                key: String::new(),
                current_version: None,
                current_value: None,
                message: format!(
                    "Sequence gap detected: received {}, expected {} (last committed: {})",
                    sequence,
                    last_committed + 1,
                    last_committed
                ),
                committed_tx_id: None,
                committed_block_height: None,
                last_committed_sequence: Some(last_committed),
            });
        }

        Ok(())
    }

    /// Check rate limit for a namespace. Returns Status::resource_exhausted if limit exceeded.
    fn check_rate_limit(&self, namespace_id: i64) -> Result<(), Status> {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.check(namespace_id).map_err(|e| {
                metrics::record_rate_limit_exceeded(namespace_id);
                Status::resource_exhausted(e.to_string())
            })?;
        }
        Ok(())
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
            }
            Some(SetCondition::MustExist) => {
                // MustExist failed means the key doesn't exist
                WriteErrorCode::KeyNotFound
            }
            Some(SetCondition::VersionEquals(_)) => {
                if key_exists {
                    WriteErrorCode::VersionMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            }
            Some(SetCondition::ValueEquals(_)) => {
                if key_exists {
                    WriteErrorCode::ValueMismatch
                } else {
                    WriteErrorCode::KeyNotFound
                }
            }
            None => {
                // No condition - shouldn't reach here for PreconditionFailed
                WriteErrorCode::Unspecified
            }
        }
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
            }
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
            }
            Op::DeleteRelationship(dr) => {
                Ok(inferadb_ledger_types::Operation::DeleteRelationship {
                    resource: dr.resource.clone(),
                    relation: dr.relation.clone(),
                    subject: dr.subject.clone(),
                })
            }
            Op::SetEntity(se) => {
                let condition = se.condition.as_ref().and_then(Self::convert_set_condition);

                Ok(inferadb_ledger_types::Operation::SetEntity {
                    key: se.key.clone(),
                    value: se.value.clone(),
                    condition,
                    expires_at: se.expires_at,
                })
            }
            Op::DeleteEntity(de) => Ok(inferadb_ledger_types::Operation::DeleteEntity {
                key: de.key.clone(),
            }),
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
        namespace_id: i64,
        vault_id: i64,
        vault_height: u64,
    ) -> (
        Option<crate::proto::BlockHeader>,
        Option<crate::proto::MerkleProof>,
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
                    }
                    _ => {
                        warn!(error = %e, "Proof generation failed");
                    }
                }
                (None, None)
            }
        }
    }

    /// Convert proto operations to LedgerRequest.
    fn operations_to_request(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        operations: &[crate::proto::Operation],
        client_id: &str,
        sequence: u64,
    ) -> Result<LedgerRequest, Status> {
        // Convert proto operations to internal Operations
        // Time the proto → internal type conversion
        let convert_start = std::time::Instant::now();
        let internal_ops: Vec<inferadb_ledger_types::Operation> = operations
            .iter()
            .map(Self::convert_operation)
            .collect::<Result<Vec<_>, Status>>()?;
        let convert_secs = convert_start.elapsed().as_secs_f64();

        // Record proto decode metrics (per operation average for consistency)
        if !operations.is_empty() {
            metrics::record_proto_decode(convert_secs / operations.len() as f64, "write_operation");
        }

        // Create a single transaction with all operations
        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: client_id.to_string(),
            sequence,
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
            actor: "system".to_string(), // Actor is set by upstream Engine/Control
        };

        Ok(LedgerRequest::Write {
            namespace_id,
            vault_id: vault_id.unwrap_or(0),
            transactions: vec![transaction],
        })
    }
}

#[tonic::async_trait]
impl WriteService for WriteServiceImpl {
    #[instrument(
        skip(self, request),
        fields(
            client_id = tracing::field::Empty,
            namespace_id = tracing::field::Empty,
            vault_id = tracing::field::Empty,
            sequence = tracing::field::Empty,
        )
    )]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract client ID and sequence for idempotency
        let client_id = req
            .client_id
            .as_ref()
            .map(|c| c.id.clone())
            .unwrap_or_default();
        let sequence = req.sequence;

        // Extract namespace and vault IDs (needed for idempotency key)
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Check idempotency cache for duplicate (keyed by namespace_id, vault_id, client_id)
        // Per DESIGN.md §5.3: Sequence tracking is per (namespace_id, vault_id, client_id)
        if let Some(cached) = self
            .idempotency
            .check(namespace_id, vault_id, &client_id, sequence)
        {
            debug!("Returning cached result for duplicate request");
            metrics::record_idempotency_hit();
            metrics::record_write(true, start.elapsed().as_secs_f64());
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Success(cached)),
            }));
        }
        metrics::record_idempotency_miss();

        // Check per-namespace rate limit
        self.check_rate_limit(namespace_id)?;

        // Check for sequence gaps (per DESIGN.md §5.3)
        if let Err(gap_error) =
            self.check_sequence_gap(namespace_id, vault_id, &client_id, sequence)
        {
            warn!(
                client_id = %client_id,
                sequence,
                "Sequence gap detected"
            );
            metrics::record_write(false, start.elapsed().as_secs_f64());
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Error(gap_error)),
            }));
        }

        // Convert to internal request
        let ledger_request = self.operations_to_request(
            namespace_id,
            Some(vault_id),
            &req.operations,
            &client_id,
            sequence,
        )?;

        // Submit to Raft via batch writer (if enabled) or direct proposal
        metrics::record_raft_proposal();
        let response = if let Some(ref batch_handle) = self.batch_handle {
            // Submit through batch writer for coalesced proposals
            let rx = batch_handle.submit(ledger_request);
            match rx.await {
                Ok(Ok(resp)) => resp,
                Ok(Err(batch_err)) => {
                    warn!(error = %batch_err, "Batch write failed");
                    metrics::record_write(false, start.elapsed().as_secs_f64());
                    return Err(Status::internal(format!("Batch error: {}", batch_err)));
                }
                Err(_recv_err) => {
                    warn!("Batch writer channel closed");
                    metrics::record_write(false, start.elapsed().as_secs_f64());
                    return Err(Status::internal("Batch writer unavailable"));
                }
            }
        } else {
            // Fallback: direct Raft proposal with mutex serialization
            let _guard = self.proposal_mutex.lock().await;
            let result = self.raft.client_write(ledger_request).await.map_err(|e| {
                warn!(error = %e, "Raft write failed");
                metrics::record_write(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Raft error: {}", e))
            })?;
            drop(_guard);
            result.data
        };
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write {
                block_height,
                block_hash: _,
            } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof) = if req.include_tx_proof {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId {
                        id: Uuid::new_v4().as_bytes().to_vec(),
                    }),
                    block_height,
                    block_header,
                    tx_proof,
                };

                // Cache the result for idempotency (keyed by namespace_id, vault_id, client_id)
                self.idempotency.insert(
                    namespace_id,
                    vault_id,
                    client_id.clone(),
                    sequence,
                    success.clone(),
                );
                metrics::set_idempotency_cache_size(self.idempotency.len());

                info!(
                    block_height,
                    latency_ms = latency * 1000.0,
                    "Write committed"
                );
                metrics::record_write(true, latency);

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Success(success)),
                }))
            }
            LedgerResponse::Error { message } => {
                warn!(error = %message, "Write failed");
                metrics::record_write(false, latency);

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Error(WriteError {
                        code: WriteErrorCode::Unspecified.into(),
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message,
                        committed_tx_id: None,
                        committed_block_height: None,
                        last_committed_sequence: None,
                    })),
                }))
            }
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Per DESIGN.md §6.1: Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                warn!(key = %key, error_code = ?error_code, "Write failed: precondition failed");
                metrics::record_write(false, latency);

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Error(WriteError {
                        code: error_code.into(),
                        key,
                        current_version,
                        current_value,
                        message: "Precondition failed".to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        last_committed_sequence: None,
                    })),
                }))
            }
            _ => {
                metrics::record_write(false, latency);
                Err(Status::internal("Unexpected response type"))
            }
        }
    }

    #[instrument(
        skip(self, request),
        fields(
            client_id = tracing::field::Empty,
            namespace_id = tracing::field::Empty,
            vault_id = tracing::field::Empty,
            sequence = tracing::field::Empty,
            batch_size = tracing::field::Empty,
        )
    )]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract client ID and sequence for idempotency
        let client_id = req
            .client_id
            .as_ref()
            .map(|c| c.id.clone())
            .unwrap_or_default();
        let sequence = req.sequence;

        // Extract namespace and vault IDs (needed for idempotency key)
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Check idempotency cache for duplicate (keyed by namespace_id, vault_id, client_id)
        // Per DESIGN.md §5.3: Sequence tracking is per (namespace_id, vault_id, client_id)
        if let Some(cached) = self
            .idempotency
            .check(namespace_id, vault_id, &client_id, sequence)
        {
            debug!("Returning cached result for duplicate batch request");
            metrics::record_idempotency_hit();
            metrics::record_batch_write(true, 0, start.elapsed().as_secs_f64());
            return Ok(Response::new(BatchWriteResponse {
                result: Some(crate::proto::batch_write_response::Result::Success(
                    BatchWriteSuccess {
                        tx_id: cached.tx_id,
                        block_height: cached.block_height,
                        block_header: cached.block_header,
                        tx_proof: cached.tx_proof,
                    },
                )),
            }));
        }
        metrics::record_idempotency_miss();

        // Check per-namespace rate limit
        self.check_rate_limit(namespace_id)?;

        // Check for sequence gaps (per DESIGN.md §5.3)
        if let Err(gap_error) =
            self.check_sequence_gap(namespace_id, vault_id, &client_id, sequence)
        {
            warn!(
                client_id = %client_id,
                sequence,
                "Sequence gap detected in batch write"
            );
            metrics::record_batch_write(false, 0, start.elapsed().as_secs_f64());
            return Ok(Response::new(BatchWriteResponse {
                result: Some(crate::proto::batch_write_response::Result::Error(gap_error)),
            }));
        }

        // Flatten all operations from all groups
        let all_operations: Vec<crate::proto::Operation> = req
            .operations
            .iter()
            .flat_map(|group| group.operations.clone())
            .collect();

        let batch_size = all_operations.len();
        tracing::Span::current().record("batch_size", batch_size);

        // Convert to internal request
        let ledger_request = self.operations_to_request(
            namespace_id,
            Some(vault_id),
            &all_operations,
            &client_id,
            sequence,
        )?;

        // Submit to Raft (serialized to prevent concurrent proposal race condition)
        metrics::record_raft_proposal();
        let _guard = self.proposal_mutex.lock().await;
        let result = self.raft.client_write(ledger_request).await.map_err(|e| {
            warn!(error = %e, "Raft batch write failed");
            metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
            Status::internal(format!("Raft error: {}", e))
        })?;
        drop(_guard);

        // Extract response
        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write {
                block_height,
                block_hash: _,
            } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof) = if req.include_tx_proofs {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId {
                        id: Uuid::new_v4().as_bytes().to_vec(),
                    }),
                    block_height,
                    block_header,
                    tx_proof,
                };

                // Cache the result for idempotency (keyed by namespace_id, vault_id, client_id)
                self.idempotency.insert(
                    namespace_id,
                    vault_id,
                    client_id.clone(),
                    sequence,
                    success.clone(),
                );
                metrics::set_idempotency_cache_size(self.idempotency.len());

                info!(
                    block_height,
                    batch_size,
                    latency_ms = latency * 1000.0,
                    "Batch write committed"
                );
                metrics::record_batch_write(true, batch_size, latency);

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Success(
                        BatchWriteSuccess {
                            tx_id: success.tx_id,
                            block_height: success.block_height,
                            block_header: success.block_header,
                            tx_proof: success.tx_proof,
                        },
                    )),
                }))
            }
            LedgerResponse::Error { message } => {
                warn!(error = %message, batch_size, "Batch write failed");
                metrics::record_batch_write(false, batch_size, latency);

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Error(
                        WriteError {
                            code: WriteErrorCode::Unspecified.into(),
                            key: String::new(),
                            current_version: None,
                            current_value: None,
                            message,
                            committed_tx_id: None,
                            committed_block_height: None,
                            last_committed_sequence: None,
                        },
                    )),
                }))
            }
            LedgerResponse::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            } => {
                // Per DESIGN.md §6.1: Return current state for client-side conflict resolution
                // Key exists if we have a current_version (which is the version when entity was last modified)
                let key_exists = current_version.is_some();
                let error_code =
                    Self::map_condition_to_error_code(failed_condition.as_ref(), key_exists);

                warn!(key = %key, error_code = ?error_code, batch_size, "Batch write failed: precondition failed");
                metrics::record_batch_write(false, batch_size, latency);

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Error(
                        WriteError {
                            code: error_code.into(),
                            key,
                            current_version,
                            current_value,
                            message: "Precondition failed".to_string(),
                            committed_tx_id: None,
                            committed_block_height: None,
                            last_committed_sequence: None,
                        },
                    )),
                }))
            }
            _ => {
                metrics::record_batch_write(false, batch_size, latency);
                Err(Status::internal("Unexpected response type"))
            }
        }
    }
}
