//! Multi-shard write service implementation.
//!
//! Routes write requests to the appropriate shard based on namespace.
//! Per DESIGN.md §4.6: Each namespace is assigned to a shard, and requests
//! are routed to the Raft instance for that shard.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_types::{config::ValidationConfig, validation};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    IdempotencyCache,
    error::classify_raft_error,
    metrics,
    proof::{self, ProofError},
    proto::{
        BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, Operation, TxId, WriteError,
        WriteErrorCode, WriteRequest, WriteResponse, WriteSuccess,
        write_service_server::WriteService,
    },
    rate_limit::RateLimiter,
    services::{
        shard_resolver::ShardResolver,
        write::{response_with_correlation, status_with_correlation},
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
}

#[allow(clippy::result_large_err)]
impl MultiShardWriteService {
    /// Check all rate limit tiers (backpressure, namespace, client).
    fn check_rate_limit(
        &self,
        client_id: &str,
        namespace_id: inferadb_ledger_types::NamespaceId,
    ) -> Result<(), Status> {
        if let Some(limiter) = &self.rate_limiter {
            limiter.check(client_id, namespace_id).map_err(|rejection| {
                warn!(
                    namespace_id = namespace_id.value(),
                    level = rejection.level.as_str(),
                    reason = rejection.reason.as_str(),
                    "Rate limit exceeded"
                );
                crate::metrics::record_rate_limit_rejected(
                    rejection.level.as_str(),
                    rejection.reason.as_str(),
                );
                let mut status = Status::resource_exhausted(rejection.to_string());
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

    /// Set input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Validate all operations in a proto operation list.
    ///
    /// Checks per-operation field limits (key/value size, character whitelist)
    /// and aggregate limits (operations count, total payload size).
    fn validate_operations(&self, operations: &[Operation]) -> Result<(), Status> {
        let config = &self.validation_config;

        validation::validate_operations_count(operations.len(), config)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut total_bytes: usize = 0;
        for proto_op in operations {
            let Some(ref op) = proto_op.op else {
                return Err(Status::invalid_argument("Operation missing op field"));
            };
            match op {
                crate::proto::operation::Op::SetEntity(se) => {
                    validation::validate_key(&se.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_value(&se.value, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += se.key.len() + se.value.len();
                },
                crate::proto::operation::Op::DeleteEntity(de) => {
                    validation::validate_key(&de.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += de.key.len();
                },
                crate::proto::operation::Op::ExpireEntity(ee) => {
                    validation::validate_key(&ee.key, config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += ee.key.len();
                },
                crate::proto::operation::Op::CreateRelationship(cr) => {
                    validation::validate_relationship_string(&cr.resource, "resource", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&cr.relation, "relation", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    validation::validate_relationship_string(&cr.subject, "subject", config)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    total_bytes += cr.resource.len() + cr.relation.len() + cr.subject.len();
                },
                crate::proto::operation::Op::DeleteRelationship(dr) => {
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

    /// Record key accesses from operations for hot key detection.
    fn record_hot_keys(&self, vault_id: inferadb_ledger_types::VaultId, operations: &[Operation]) {
        if let Some(ref detector) = self.hot_key_detector {
            use crate::proto::operation::Op;
            for op in operations {
                if let Some(ref inner) = op.op {
                    let key = match inner {
                        Op::SetEntity(se) => &se.key,
                        Op::DeleteEntity(de) => &de.key,
                        Op::CreateRelationship(cr) => &cr.resource,
                        Op::DeleteRelationship(dr) => &dr.resource,
                        Op::ExpireEntity(ee) => &ee.key,
                    };
                    detector.record_access(vault_id, key);
                }
            }
        }
    }

    /// Compute a hash of operations for idempotency payload comparison.
    fn hash_operations(operations: &[Operation]) -> Vec<u8> {
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
