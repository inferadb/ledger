//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.

use std::sync::Arc;
use std::time::Instant;

use openraft::Raft;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::IdempotencyCache;
use crate::metrics;
use crate::proof::{self, ProofError};
use crate::proto::write_service_server::WriteService;
use crate::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
    WriteRequest, WriteResponse, WriteSuccess,
};
use crate::types::{LedgerRequest, LedgerResponse, LedgerTypeConfig};

use ledger_storage::BlockArchive;

/// Write service implementation.
pub struct WriteServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Block archive for proof generation (optional).
    block_archive: Option<Arc<BlockArchive>>,
}

#[allow(clippy::result_large_err)]
impl WriteServiceImpl {
    /// Create a new write service.
    pub fn new(raft: Arc<Raft<LedgerTypeConfig>>, idempotency: Arc<IdempotencyCache>) -> Self {
        Self {
            raft,
            idempotency,
            block_archive: None,
        }
    }

    /// Create a write service with block archive access for proof generation.
    pub fn with_block_archive(
        raft: Arc<Raft<LedgerTypeConfig>>,
        idempotency: Arc<IdempotencyCache>,
        block_archive: Arc<BlockArchive>,
    ) -> Self {
        Self {
            raft,
            idempotency,
            block_archive: Some(block_archive),
        }
    }

    /// Convert a proto SetCondition to internal SetCondition.
    fn convert_set_condition(
        proto_condition: &crate::proto::SetCondition,
    ) -> Option<ledger_types::SetCondition> {
        use crate::proto::set_condition::Condition;

        proto_condition.condition.as_ref().map(|c| match c {
            Condition::NotExists(true) => ledger_types::SetCondition::MustNotExist,
            Condition::NotExists(false) => ledger_types::SetCondition::MustExist,
            Condition::Version(v) => ledger_types::SetCondition::VersionEquals(*v),
            Condition::ValueEquals(v) => ledger_types::SetCondition::ValueEquals(v.clone()),
        })
    }

    /// Convert a proto Operation to internal Operation.
    fn convert_operation(
        proto_op: &crate::proto::Operation,
    ) -> Result<ledger_types::Operation, Status> {
        use crate::proto::operation::Op;

        let op = proto_op
            .op
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Operation missing op field"))?;

        match op {
            Op::CreateRelationship(cr) => Ok(ledger_types::Operation::CreateRelationship {
                resource: cr.resource.clone(),
                relation: cr.relation.clone(),
                subject: cr.subject.clone(),
            }),
            Op::DeleteRelationship(dr) => Ok(ledger_types::Operation::DeleteRelationship {
                resource: dr.resource.clone(),
                relation: dr.relation.clone(),
                subject: dr.subject.clone(),
            }),
            Op::SetEntity(se) => {
                let condition = se.condition.as_ref().and_then(Self::convert_set_condition);

                Ok(ledger_types::Operation::SetEntity {
                    key: se.key.clone(),
                    value: se.value.clone(),
                    condition,
                    expires_at: se.expires_at,
                })
            }
            Op::DeleteEntity(de) => Ok(ledger_types::Operation::DeleteEntity {
                key: de.key.clone(),
            }),
            Op::ExpireEntity(ee) => Ok(ledger_types::Operation::ExpireEntity {
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
        let internal_ops: Vec<ledger_types::Operation> = operations
            .iter()
            .map(Self::convert_operation)
            .collect::<Result<Vec<_>, Status>>()?;

        // Create a single transaction with all operations
        let transaction = ledger_types::Transaction {
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

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);

        // Check idempotency cache for duplicate
        if let Some(cached) = self.idempotency.check(&client_id, sequence) {
            debug!("Returning cached result for duplicate request");
            metrics::record_idempotency_hit();
            metrics::record_write(true, start.elapsed().as_secs_f64());
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Success(cached)),
            }));
        }
        metrics::record_idempotency_miss();

        // Extract namespace and vault IDs
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        // Record span fields
        tracing::Span::current().record("namespace_id", namespace_id);
        if let Some(vid) = vault_id {
            tracing::Span::current().record("vault_id", vid);
        }

        // Convert to internal request
        let ledger_request = self.operations_to_request(
            namespace_id,
            vault_id,
            &req.operations,
            &client_id,
            sequence,
        )?;

        // Submit to Raft
        metrics::record_raft_proposal();
        let result = self.raft.client_write(ledger_request).await.map_err(|e| {
            warn!(error = %e, "Raft write failed");
            metrics::record_write(false, start.elapsed().as_secs_f64());
            Status::internal(format!("Raft error: {}", e))
        })?;

        // Extract response
        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write {
                block_height,
                block_hash: _,
            } => {
                // Generate proof and block header if requested
                let (block_header, tx_proof) = if req.include_tx_proof {
                    self.generate_write_proof(namespace_id, vault_id.unwrap_or(0), block_height)
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

                // Cache the result for idempotency
                self.idempotency
                    .insert(client_id.clone(), sequence, success.clone());
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

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);

        // Check idempotency cache for duplicate
        if let Some(cached) = self.idempotency.check(&client_id, sequence) {
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

        // Extract namespace and vault IDs
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        // Record span fields
        tracing::Span::current().record("namespace_id", namespace_id);
        if let Some(vid) = vault_id {
            tracing::Span::current().record("vault_id", vid);
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
            vault_id,
            &all_operations,
            &client_id,
            sequence,
        )?;

        // Submit to Raft
        metrics::record_raft_proposal();
        let result = self.raft.client_write(ledger_request).await.map_err(|e| {
            warn!(error = %e, "Raft batch write failed");
            metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
            Status::internal(format!("Raft error: {}", e))
        })?;

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
                    self.generate_write_proof(namespace_id, vault_id.unwrap_or(0), block_height)
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

                // Cache the result for idempotency
                self.idempotency
                    .insert(client_id.clone(), sequence, success.clone());
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
            _ => {
                metrics::record_batch_write(false, batch_size, latency);
                Err(Status::internal("Unexpected response type"))
            }
        }
    }
}
