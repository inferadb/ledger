//! Write service implementation.
//!
//! Handles transaction submission through Raft consensus.

use std::sync::Arc;

use openraft::Raft;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::write_service_server::WriteService;
use crate::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, TxId, WriteError, WriteErrorCode,
    WriteRequest, WriteResponse, WriteSuccess,
};
use crate::types::{LedgerRequest, LedgerResponse, LedgerTypeConfig};
use crate::{IdempotencyCache, RaftLogStore};

/// Write service implementation.
pub struct WriteServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The log store for state machine access.
    #[allow(dead_code)]
    log_store: Arc<RaftLogStore>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
}

impl WriteServiceImpl {
    /// Create a new write service.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        log_store: Arc<RaftLogStore>,
        idempotency: Arc<IdempotencyCache>,
    ) -> Self {
        Self {
            raft,
            log_store,
            idempotency,
        }
    }

    /// Convert operations to LedgerRequest.
    fn operations_to_request(
        &self,
        namespace_id: i64,
        vault_id: Option<i64>,
        operations: &[crate::proto::Operation],
        client_id: &str,
        sequence: u64,
    ) -> Result<LedgerRequest, Status> {
        // Convert proto operations to internal Transaction type
        let transactions = operations
            .iter()
            .map(|_op| {
                // TODO: Proper conversion from proto operations to Transaction
                ledger_types::Transaction {
                    id: *Uuid::new_v4().as_bytes(),
                    client_id: client_id.to_string(),
                    sequence,
                    operations: vec![], // TODO: Convert operations
                    timestamp: chrono::Utc::now(),
                    actor: "system".to_string(),
                }
            })
            .collect();

        Ok(LedgerRequest::Write {
            namespace_id: namespace_id.try_into().map_err(|_| {
                Status::invalid_argument("Invalid namespace_id")
            })?,
            vault_id: vault_id.unwrap_or(0).try_into().map_err(|_| {
                Status::invalid_argument("Invalid vault_id")
            })?,
            transactions,
        })
    }
}

#[tonic::async_trait]
impl WriteService for WriteServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();

        // Extract client ID and sequence for idempotency
        let client_id = req
            .client_id
            .as_ref()
            .map(|c| c.id.clone())
            .unwrap_or_default();
        let sequence = req.sequence;

        // Check idempotency cache for duplicate
        if let Some(cached) = self.idempotency.check(&client_id, sequence) {
            return Ok(Response::new(WriteResponse {
                result: Some(crate::proto::write_response::Result::Success(cached)),
            }));
        }

        // Extract namespace and vault IDs
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        // Convert to internal request
        let ledger_request = self.operations_to_request(namespace_id, vault_id, &req.operations, &client_id, sequence)?;

        // Submit to Raft
        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        // Extract response
        let response = result.data;

        match response {
            LedgerResponse::Write {
                block_height,
                block_hash: _,
            } => {
                let success = WriteSuccess {
                    tx_id: Some(TxId {
                        id: Uuid::new_v4().as_bytes().to_vec(),
                    }),
                    block_height,
                    block_header: None, // TODO: Include if include_tx_proof is set
                    tx_proof: None,
                };

                // Cache the result for idempotency
                self.idempotency
                    .insert(client_id, sequence, success.clone());

                Ok(Response::new(WriteResponse {
                    result: Some(crate::proto::write_response::Result::Success(success)),
                }))
            }
            LedgerResponse::Error { message } => Ok(Response::new(WriteResponse {
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
            })),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        let req = request.into_inner();

        // Extract client ID and sequence for idempotency
        let client_id = req
            .client_id
            .as_ref()
            .map(|c| c.id.clone())
            .unwrap_or_default();
        let sequence = req.sequence;

        // Check idempotency cache for duplicate
        if let Some(cached) = self.idempotency.check(&client_id, sequence) {
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

        // Extract namespace and vault IDs
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        // Flatten all operations from all groups
        let all_operations: Vec<crate::proto::Operation> = req
            .operations
            .iter()
            .flat_map(|group| group.operations.clone())
            .collect();

        // Convert to internal request
        let ledger_request = self.operations_to_request(namespace_id, vault_id, &all_operations, &client_id, sequence)?;

        // Submit to Raft
        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        // Extract response
        let response = result.data;

        match response {
            LedgerResponse::Write {
                block_height,
                block_hash: _,
            } => {
                let success = WriteSuccess {
                    tx_id: Some(TxId {
                        id: Uuid::new_v4().as_bytes().to_vec(),
                    }),
                    block_height,
                    block_header: None,
                    tx_proof: None,
                };

                // Cache the result for idempotency
                self.idempotency
                    .insert(client_id, sequence, success.clone());

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
            LedgerResponse::Error { message } => Ok(Response::new(BatchWriteResponse {
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
            })),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }
}
