//! Multi-shard write service implementation.
//!
//! Routes write requests to the appropriate shard based on namespace.
//! Per DESIGN.md §4.6: Each namespace is assigned to a shard, and requests
//! are routed to the Raft instance for that shard.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_types::config::ValidationConfig;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    error::classify_raft_error,
    idempotency::IdempotencyCache,
    metrics,
    proof::{self, ProofError},
    proto::{
        BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, Operation, TxId, WriteError,
        WriteErrorCode, WriteRequest, WriteResponse, WriteSuccess,
        write_service_server::WriteService,
    },
    rate_limit::RateLimiter,
    services::{
        metadata::{response_with_correlation, status_with_correlation},
        shard_resolver::ShardResolver,
    },
    trace_context,
    types::{LedgerRequest, LedgerResponse},
};

// Note: SetCondition conversion is internal to convert_set_condition

/// Multi-shard write service implementation.
///
/// Routes write requests to the correct shard based on namespace_id.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct MultiShardWriteService {
    /// Shard resolver for routing requests.
    resolver: Arc<dyn ShardResolver>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Per-namespace rate limiter (optional).
    #[builder(default)]
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Hot key detector for identifying frequently accessed keys (optional).
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
impl MultiShardWriteService {
    /// Check all rate limit tiers (backpressure, namespace, client).
    fn check_rate_limit(
        &self,
        client_id: &str,
        namespace_id: inferadb_ledger_types::NamespaceId,
    ) -> Result<(), Status> {
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), client_id, namespace_id)
    }

    /// Set input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Validate all operations in a proto operation list.
    fn validate_operations(&self, operations: &[Operation]) -> Result<(), Status> {
        super::helpers::validate_operations(operations, &self.validation_config)
    }

    /// Record key accesses from operations for hot key detection.
    fn record_hot_keys(&self, vault_id: inferadb_ledger_types::VaultId, operations: &[Operation]) {
        super::helpers::record_hot_keys(self.hot_key_detector.as_ref(), vault_id, operations);
    }

    /// Compute a hash of operations for idempotency payload comparison.
    fn hash_operations(operations: &[Operation]) -> Vec<u8> {
        super::helpers::hash_operations(operations)
    }

    /// Generate inclusion proof for a write.
    fn generate_write_proof(
        &self,
        namespace_id: inferadb_ledger_types::NamespaceId,
        vault_id: inferadb_ledger_types::VaultId,
        vault_height: u64,
    ) -> (Option<crate::proto::BlockHeader>, Option<crate::proto::MerkleProof>) {
        let ctx = match self.resolver.resolve(namespace_id) {
            Ok(ctx) => ctx,
            Err(_) => return (None, None),
        };

        // Use the proof module's implementation
        match proof::generate_write_proof(
            &ctx.block_archive,
            namespace_id,
            vault_id,
            vault_height,
            0,
        ) {
            Ok(write_proof) => (Some(write_proof.block_header), Some(write_proof.tx_proof)),
            Err(e) => {
                match &e {
                    ProofError::BlockNotFound { .. } | ProofError::NoTransactions => {
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

    /// Build a ledger request from operations.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0 here;
    /// the actual sequence will be assigned by the Raft state machine at apply time.
    fn build_request(
        &self,
        operations: &[Operation],
        namespace_id: inferadb_ledger_types::NamespaceId,
        vault_id: inferadb_ledger_types::VaultId,
        client_id: &str,
        actor: &str,
    ) -> Result<LedgerRequest, Status> {
        let internal_ops: Vec<inferadb_ledger_types::Operation> = operations
            .iter()
            .map(inferadb_ledger_types::Operation::try_from)
            .collect::<Result<Vec<_>, Status>>()?;

        if internal_ops.is_empty() {
            return Err(Status::invalid_argument("No operations provided"));
        }

        // Server-assigned sequences: sequence=0 is a placeholder; actual sequence
        // is assigned at Raft apply time for deterministic replay.
        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: client_id.to_string(),
            sequence: 0, // Server-assigned at apply time
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
            actor: actor.to_string(),
        };

        Ok(LedgerRequest::Write { namespace_id, vault_id, transactions: vec![transaction] })
    }
}

#[tonic::async_trait]
impl WriteService for MultiShardWriteService {
    #[instrument(skip(self, request), fields(client_id, namespace_id, vault_id))]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        let start = Instant::now();
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let request_id = Uuid::new_v4();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let namespace_id = inferadb_ledger_types::NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id =
            inferadb_ledger_types::VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));
        // Actor is set by upstream Engine/Control services - Ledger uses "system" internally
        let actor = "system".to_string();

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Validate all operations before any processing
        self.validate_operations(&req.operations)?;

        // Compute request hash for payload comparison
        let request_hash = seahash::hash(&Self::hash_operations(&req.operations));

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());

        // Check idempotency cache
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            namespace_id.value(),
            vault_id.value(),
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                debug!("Returning cached result for duplicate request");
                metrics::record_idempotency_hit();
                metrics::record_write(true, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(crate::proto::write_response::Result::Success(cached)),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                metrics::record_write(false, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    WriteResponse {
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
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::NewRequest => {
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limit
        self.check_rate_limit(&client_id, namespace_id)
            .map_err(|status| status_with_correlation(status, &request_id, &trace_ctx.trace_id))?;

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&req.operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            namespace_id,
            estimated_bytes,
        )
        .map_err(|status| status_with_correlation(status, &request_id, &trace_ctx.trace_id))?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // Server-assigned sequences: no gap check needed

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Build request
        let ledger_request =
            self.build_request(&req.operations, namespace_id, vault_id, &client_id, &actor)?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to the resolved shard's Raft
        metrics::record_raft_proposal();
        let result =
            match tokio::time::timeout(timeout, ctx.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    warn!(error = %e, "Raft write failed");
                    metrics::record_write(false, start.elapsed().as_secs_f64());
                    return Err(status_with_correlation(
                        classify_raft_error(&e.to_string()),
                        &request_id,
                        &trace_ctx.trace_id,
                    ));
                },
                Err(_elapsed) => {
                    metrics::record_raft_proposal_timeout();
                    metrics::record_write(false, start.elapsed().as_secs_f64());
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &request_id,
                        &trace_ctx.trace_id,
                    ));
                },
            };

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _, assigned_sequence } => {
                // Generate proof if requested
                let (block_header, tx_proof) = if req.include_tx_proof {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header,
                    tx_proof,
                    assigned_sequence,
                };

                // Cache the successful result
                self.idempotency.insert(
                    namespace_id.value(),
                    vault_id.value(),
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    success.clone(),
                );

                metrics::record_write(true, latency);
                metrics::record_namespace_operation(namespace_id.value(), "write");
                metrics::record_namespace_latency(namespace_id.value(), "write", latency);
                info!(
                    trace_id = %trace_ctx.trace_id,
                    block_height,
                    assigned_sequence,
                    latency_ms = latency * 1000.0,
                    "Write committed"
                );

                Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(crate::proto::write_response::Result::Success(success)),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                warn!("Unexpected Raft response for write");
                metrics::record_write(false, latency);
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
        }
    }

    #[instrument(
        skip(self, request),
        fields(client_id, sequence, namespace_id, vault_id, batch_size)
    )]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        let start = Instant::now();
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let request_id = Uuid::new_v4();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let namespace_id = inferadb_ledger_types::NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id =
            inferadb_ledger_types::VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Flatten all operations from all groups
        let all_operations: Vec<crate::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

        // Validate all operations before any processing
        self.validate_operations(&all_operations)?;

        let batch_size = all_operations.len();

        // Compute request hash for payload comparison
        let request_hash = seahash::hash(&Self::hash_operations(&all_operations));

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("batch_size", batch_size);

        // Check idempotency cache
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            namespace_id.value(),
            vault_id.value(),
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                debug!("Returning cached result for duplicate batch request");
                metrics::record_idempotency_hit();
                metrics::record_batch_write(true, 0, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(crate::proto::batch_write_response::Result::Success(
                            BatchWriteSuccess {
                                tx_id: cached.tx_id,
                                block_height: cached.block_height,
                                block_header: cached.block_header,
                                tx_proof: cached.tx_proof,
                                assigned_sequence: cached.assigned_sequence,
                            },
                        )),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(BatchWriteResponse {
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
                }, &request_id, &trace_ctx.trace_id));
            },
            IdempotencyCheckResult::NewRequest => {
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limit
        self.check_rate_limit(&client_id, namespace_id)
            .map_err(|status| status_with_correlation(status, &request_id, &trace_ctx.trace_id))?;

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&all_operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            namespace_id,
            estimated_bytes,
        )
        .map_err(|status| status_with_correlation(status, &request_id, &trace_ctx.trace_id))?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &all_operations);

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Build request with flattened operations
        let ledger_request =
            self.build_request(&all_operations, namespace_id, vault_id, &client_id, "system")?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft
        metrics::record_raft_proposal();
        let result =
            match tokio::time::timeout(timeout, ctx.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    warn!(error = %e, "Raft batch write failed");
                    metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                    return Err(status_with_correlation(
                        classify_raft_error(&e.to_string()),
                        &request_id,
                        &trace_ctx.trace_id,
                    ));
                },
                Err(_elapsed) => {
                    metrics::record_raft_proposal_timeout();
                    metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                    return Err(status_with_correlation(
                        Status::deadline_exceeded(format!(
                            "Raft proposal timed out after {}ms",
                            timeout.as_millis()
                        )),
                        &request_id,
                        &trace_ctx.trace_id,
                    ));
                },
            };

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _, assigned_sequence } => {
                // Generate proof if requested
                let (block_header, tx_proof) = if req.include_tx_proofs {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                // Create WriteSuccess for idempotency cache
                let write_success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header: block_header.clone(),
                    tx_proof: tx_proof.clone(),
                    assigned_sequence,
                };

                // Cache the successful result
                self.idempotency.insert(
                    namespace_id.value(),
                    vault_id.value(),
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    write_success,
                );

                let success = BatchWriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header,
                    tx_proof,
                    assigned_sequence,
                };

                metrics::record_batch_write(true, batch_size, latency);
                metrics::record_namespace_operation(namespace_id.value(), "write");
                metrics::record_namespace_latency(namespace_id.value(), "write", latency);
                info!(
                    trace_id = %trace_ctx.trace_id,
                    block_height,
                    assigned_sequence,
                    batch_size,
                    latency_ms = latency * 1000.0,
                    "Batch write committed"
                );

                Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(crate::proto::batch_write_response::Result::Success(success)),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                warn!("Unexpected Raft response for batch write");
                metrics::record_batch_write(false, batch_size, latency);
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_multi_shard_write_service_creation() {
        // Basic struct test - full testing requires Raft setup
    }

    // Proto↔domain conversion tests (set_condition, operation variants) are in
    // proto_convert.rs which owns the centralized From/TryFrom implementations.
}
