//! Multi-shard write service implementation.
//!
//! Routes write requests to the appropriate shard based on namespace.
//! Per DESIGN.md §4.6: Each namespace is assigned to a shard, and requests
//! are routed to the Raft instance for that shard.

use std::{sync::Arc, time::Instant};

use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    IdempotencyCache, metrics,
    proof::{self, ProofError},
    proto::{
        BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, Operation, TxId, WriteError,
        WriteErrorCode, WriteRequest, WriteResponse, WriteSuccess,
        write_service_server::WriteService,
    },
    rate_limit::NamespaceRateLimiter,
    services::shard_resolver::ShardResolver,
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
    rate_limiter: Option<Arc<NamespaceRateLimiter>>,
}

#[allow(clippy::result_large_err)]
impl MultiShardWriteService {
    /// Check per-namespace rate limit.
    fn check_rate_limit(&self, namespace_id: i64) -> Result<(), Status> {
        if let Some(limiter) = &self.rate_limiter
            && let Err(e) = limiter.check(namespace_id)
        {
            warn!(namespace_id, "Rate limit exceeded");
            return Err(Status::resource_exhausted(e.to_string()));
        }
        Ok(())
    }

    /// Check for sequence gaps before submitting to Raft.
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

        // Resolve the shard to get applied_state
        let ctx = match self.resolver.resolve(namespace_id) {
            Ok(ctx) => ctx,
            Err(_) => return Ok(()), // Skip check if shard unavailable
        };

        let last_committed = ctx.applied_state.client_sequence(namespace_id, vault_id, client_id);

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

    /// Generate inclusion proof for a write.
    fn generate_write_proof(
        &self,
        namespace_id: i64,
        vault_id: i64,
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

    /// Convert a proto operation to internal operation.
    fn convert_operation(op: &Operation) -> Result<inferadb_ledger_types::Operation, Status> {
        use crate::proto::operation::Op;

        let inner_op =
            op.op.as_ref().ok_or_else(|| Status::invalid_argument("Operation missing op field"))?;

        match inner_op {
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

    /// Build a ledger request from operations.
    fn build_request(
        &self,
        operations: &[Operation],
        namespace_id: i64,
        vault_id: i64,
        client_id: &str,
        sequence: u64,
        actor: &str,
    ) -> Result<LedgerRequest, Status> {
        let internal_ops: Vec<inferadb_ledger_types::Operation> =
            operations.iter().map(Self::convert_operation).collect::<Result<Vec<_>, Status>>()?;

        if internal_ops.is_empty() {
            return Err(Status::invalid_argument("No operations provided"));
        }

        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: client_id.to_string(),
            sequence,
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
            actor: actor.to_string(),
        };

        Ok(LedgerRequest::Write { namespace_id, vault_id, transactions: vec![transaction] })
    }
}

#[tonic::async_trait]
impl WriteService for MultiShardWriteService {
    #[instrument(skip(self, request), fields(client_id, sequence, namespace_id, vault_id))]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let sequence = req.sequence;
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        // Actor is set by upstream Engine/Control services - Ledger uses "system" internally
        let actor = "system".to_string();

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Check idempotency cache
        if let Some(cached) = self.idempotency.check(namespace_id, vault_id, &client_id, sequence) {
            debug!("Returning cached result for duplicate request");
            metrics::record_idempotency_hit();
            metrics::record_write(true, start.elapsed().as_secs_f64());
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Success(cached)),
            }));
        }
        metrics::record_idempotency_miss();

        // Check rate limit
        self.check_rate_limit(namespace_id)?;

        // Check sequence gaps
        if let Err(gap_error) =
            self.check_sequence_gap(namespace_id, vault_id, &client_id, sequence)
        {
            warn!(client_id = %client_id, sequence, "Sequence gap detected");
            metrics::record_write(false, start.elapsed().as_secs_f64());
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Error(gap_error)),
            }));
        }

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Build request
        let ledger_request = self.build_request(
            &req.operations,
            namespace_id,
            vault_id,
            &client_id,
            sequence,
            &actor,
        )?;

        // Submit to the resolved shard's Raft
        metrics::record_raft_proposal();
        let result = ctx.raft.client_write(ledger_request).await.map_err(|e| {
            warn!(error = %e, "Raft write failed");
            metrics::record_write(false, start.elapsed().as_secs_f64());
            Status::internal(format!("Raft error: {}", e))
        })?;

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _ } => {
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
                };

                // Cache the successful result
                self.idempotency.insert(
                    namespace_id,
                    vault_id,
                    client_id.clone(),
                    sequence,
                    success.clone(),
                );

                metrics::record_write(true, latency);
                info!(block_height, latency_ms = latency * 1000.0, "Write committed");

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Success(success)),
                }))
            },
            _ => {
                warn!("Unexpected Raft response for write");
                metrics::record_write(false, latency);
                Err(Status::internal("Unexpected response type"))
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
        let start = Instant::now();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let sequence = req.sequence;
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Flatten all operations from all groups
        let all_operations: Vec<crate::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

        let batch_size = all_operations.len();

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("sequence", sequence);
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("batch_size", batch_size);

        // Check rate limit
        self.check_rate_limit(namespace_id)?;

        // Check sequence gaps
        if let Err(gap_error) =
            self.check_sequence_gap(namespace_id, vault_id, &client_id, sequence)
        {
            warn!(client_id = %client_id, sequence, "Sequence gap detected");
            metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
            return Ok(Response::new(BatchWriteResponse {
                result: Some(crate::proto::batch_write_response::Result::Error(gap_error)),
            }));
        }

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Build request with flattened operations
        let ledger_request = self.build_request(
            &all_operations,
            namespace_id,
            vault_id,
            &client_id,
            sequence,
            "system",
        )?;

        // Submit to Raft
        metrics::record_raft_proposal();
        let result = ctx.raft.client_write(ledger_request).await.map_err(|e| {
            warn!(error = %e, "Raft batch write failed");
            metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
            Status::internal(format!("Raft error: {}", e))
        })?;

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _ } => {
                // Generate proof if requested
                let (block_header, tx_proof) = if req.include_tx_proofs {
                    self.generate_write_proof(namespace_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                let success = BatchWriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header,
                    tx_proof,
                };

                metrics::record_batch_write(true, batch_size, latency);
                info!(
                    block_height,
                    batch_size,
                    latency_ms = latency * 1000.0,
                    "Batch write committed"
                );

                Ok(Response::new(BatchWriteResponse {
                    result: Some(crate::proto::batch_write_response::Result::Success(success)),
                }))
            },
            _ => {
                warn!("Unexpected Raft response for batch write");
                metrics::record_batch_write(false, batch_size, latency);
                Err(Status::internal("Unexpected response type"))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_shard_write_service_creation() {
        // Basic struct test - full testing requires Raft setup
    }

    #[test]
    fn test_convert_set_condition_not_exists() {
        use crate::proto::{SetCondition, set_condition::Condition};

        let proto_condition = SetCondition { condition: Some(Condition::NotExists(true)) };

        let result = MultiShardWriteService::convert_set_condition(&proto_condition);
        assert!(matches!(result, Some(inferadb_ledger_types::SetCondition::MustNotExist)));
    }

    #[test]
    fn test_convert_set_condition_must_exists() {
        use crate::proto::{SetCondition, set_condition::Condition};

        let proto_condition = SetCondition { condition: Some(Condition::MustExists(true)) };

        let result = MultiShardWriteService::convert_set_condition(&proto_condition);
        assert!(matches!(result, Some(inferadb_ledger_types::SetCondition::MustExist)));
    }

    #[test]
    fn test_convert_set_condition_version() {
        use crate::proto::{SetCondition, set_condition::Condition};

        let proto_condition = SetCondition { condition: Some(Condition::Version(42)) };

        let result = MultiShardWriteService::convert_set_condition(&proto_condition);
        assert!(matches!(result, Some(inferadb_ledger_types::SetCondition::VersionEquals(42))));
    }

    #[test]
    fn test_convert_set_condition_value_equals() {
        use crate::proto::{SetCondition, set_condition::Condition};

        let proto_condition =
            SetCondition { condition: Some(Condition::ValueEquals(b"test_value".to_vec())) };

        let result = MultiShardWriteService::convert_set_condition(&proto_condition);
        match result {
            Some(inferadb_ledger_types::SetCondition::ValueEquals(v)) => {
                assert_eq!(v, b"test_value");
            },
            _ => unreachable!("Expected ValueEquals condition"),
        }
    }

    #[test]
    fn test_convert_set_condition_none() {
        use crate::proto::SetCondition;

        let proto_condition = SetCondition { condition: None };

        let result = MultiShardWriteService::convert_set_condition(&proto_condition);
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_operation_create_relationship() {
        use crate::proto::{CreateRelationship, Operation, operation::Op};

        let op = Operation {
            op: Some(Op::CreateRelationship(CreateRelationship {
                resource: "document:123".to_string(),
                relation: "viewer".to_string(),
                subject: "user:456".to_string(),
            })),
        };

        let result = MultiShardWriteService::convert_operation(&op).unwrap();
        match result {
            inferadb_ledger_types::Operation::CreateRelationship {
                resource,
                relation,
                subject,
            } => {
                assert_eq!(resource, "document:123");
                assert_eq!(relation, "viewer");
                assert_eq!(subject, "user:456");
            },
            _ => unreachable!("Expected CreateRelationship operation"),
        }
    }

    #[test]
    fn test_convert_operation_delete_relationship() {
        use crate::proto::{DeleteRelationship, Operation, operation::Op};

        let op = Operation {
            op: Some(Op::DeleteRelationship(DeleteRelationship {
                resource: "document:123".to_string(),
                relation: "viewer".to_string(),
                subject: "user:456".to_string(),
            })),
        };

        let result = MultiShardWriteService::convert_operation(&op).unwrap();
        match result {
            inferadb_ledger_types::Operation::DeleteRelationship {
                resource,
                relation,
                subject,
            } => {
                assert_eq!(resource, "document:123");
                assert_eq!(relation, "viewer");
                assert_eq!(subject, "user:456");
            },
            _ => unreachable!("Expected DeleteRelationship operation"),
        }
    }

    #[test]
    fn test_convert_operation_set_entity() {
        use crate::proto::{Operation, SetEntity, operation::Op};

        let op = Operation {
            op: Some(Op::SetEntity(SetEntity {
                key: "user:123".to_string(),
                value: b"test_data".to_vec(),
                condition: None,
                expires_at: Some(1000),
            })),
        };

        let result = MultiShardWriteService::convert_operation(&op).unwrap();
        match result {
            inferadb_ledger_types::Operation::SetEntity { key, value, condition, expires_at } => {
                assert_eq!(key, "user:123");
                assert_eq!(value, b"test_data");
                assert!(condition.is_none());
                assert_eq!(expires_at, Some(1000));
            },
            _ => unreachable!("Expected SetEntity operation"),
        }
    }

    #[test]
    fn test_convert_operation_delete_entity() {
        use crate::proto::{DeleteEntity, Operation, operation::Op};

        let op =
            Operation { op: Some(Op::DeleteEntity(DeleteEntity { key: "user:123".to_string() })) };

        let result = MultiShardWriteService::convert_operation(&op).unwrap();
        match result {
            inferadb_ledger_types::Operation::DeleteEntity { key } => {
                assert_eq!(key, "user:123");
            },
            _ => unreachable!("Expected DeleteEntity operation"),
        }
    }

    #[test]
    fn test_convert_operation_expire_entity() {
        use crate::proto::{ExpireEntity, Operation, operation::Op};

        let op = Operation {
            op: Some(Op::ExpireEntity(ExpireEntity {
                key: "user:123".to_string(),
                expired_at: 2000,
            })),
        };

        let result = MultiShardWriteService::convert_operation(&op).unwrap();
        match result {
            inferadb_ledger_types::Operation::ExpireEntity { key, expired_at } => {
                assert_eq!(key, "user:123");
                assert_eq!(expired_at, 2000);
            },
            _ => unreachable!("Expected ExpireEntity operation"),
        }
    }

    #[test]
    fn test_convert_operation_missing_op() {
        use crate::proto::Operation;

        let op = Operation { op: None };

        let result = MultiShardWriteService::convert_operation(&op);
        assert!(result.is_err());
    }
}
