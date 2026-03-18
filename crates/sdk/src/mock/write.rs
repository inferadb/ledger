use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockWriteService {
    pub(super) state: Arc<MockState>,
}

impl MockWriteService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn generate_tx_id() -> Vec<u8> {
        // Generate a simple incrementing tx_id for testing
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        now.to_le_bytes().to_vec()
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::write_service_server::WriteService for MockWriteService {
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, Status> {
        self.state.check_injection().await?;

        self.state.write_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();
        let idempotency_key = req.idempotency_key;

        // Check idempotency cache
        let cache_key = (organization, vault, client_id.clone(), idempotency_key.clone());
        {
            let cache = self.state.idempotency_cache.read();
            if let Some((tx_id, block_height, assigned_sequence)) = cache.get(&cache_key) {
                // Already committed - return cached result
                return Ok(Response::new(proto::WriteResponse {
                    result: Some(proto::write_response::Result::Error(proto::WriteError {
                        code: proto::WriteErrorCode::AlreadyCommitted as i32,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message: "Already committed".to_string(),
                        committed_tx_id: Some(proto::TxId { id: tx_id.clone() }),
                        committed_block_height: Some(*block_height),
                        assigned_sequence: Some(*assigned_sequence),
                    })),
                }));
            }
        }

        // Apply operations
        let block_height = self.state.block_height.fetch_add(1, Ordering::SeqCst);
        {
            let mut entities = self.state.entities.write();
            for op in req.operations {
                if let Some(op_inner) = op.op {
                    match op_inner {
                        proto::operation::Op::SetEntity(set) => {
                            entities.insert(
                                (organization, vault, set.key),
                                (set.value, block_height, set.expires_at),
                            );
                        },
                        proto::operation::Op::DeleteEntity(del) => {
                            entities.remove(&(organization, vault, del.key));
                        },
                        proto::operation::Op::CreateRelationship(rel) => {
                            let mut relationships = self.state.relationships.write();
                            let entry = relationships.entry((organization, vault)).or_default();
                            entry.push(proto::Relationship {
                                resource: rel.resource,
                                relation: rel.relation,
                                subject: rel.subject,
                            });
                        },
                        proto::operation::Op::DeleteRelationship(del) => {
                            let mut relationships = self.state.relationships.write();
                            if let Some(rels) = relationships.get_mut(&(organization, vault)) {
                                rels.retain(|r| {
                                    r.resource != del.resource
                                        || r.relation != del.relation
                                        || r.subject != del.subject
                                });
                            }
                        },
                        proto::operation::Op::ExpireEntity(expire) => {
                            entities.remove(&(organization, vault, expire.key));
                        },
                    }
                }
            }
        }

        // Assign server sequence and update client state
        let client_key = (organization, vault, client_id);
        let assigned_sequence = {
            let mut sequences = self.state.client_sequences.write();
            let next_seq = sequences.get(&client_key).copied().unwrap_or(0) + 1;
            sequences.insert(client_key, next_seq);
            next_seq
        };

        // Cache the result for idempotency
        let tx_id = Self::generate_tx_id();
        {
            let mut cache = self.state.idempotency_cache.write();
            cache.insert(cache_key, (tx_id.clone(), block_height, assigned_sequence));
        }

        Ok(Response::new(proto::WriteResponse {
            result: Some(proto::write_response::Result::Success(proto::WriteSuccess {
                tx_id: Some(proto::TxId { id: tx_id }),
                block_height,
                block_header: None,
                tx_proof: None,
                assigned_sequence,
            })),
        }))
    }

    async fn batch_write(
        &self,
        request: Request<proto::BatchWriteRequest>,
    ) -> Result<Response<proto::BatchWriteResponse>, Status> {
        self.state.check_injection().await?;

        self.state.write_count.fetch_add(1, Ordering::SeqCst);

        let req = request.into_inner();
        let organization = OrganizationSlug::new(req.organization.map_or(0, |n| n.slug));
        let vault = VaultSlug::new(req.vault.map_or(0, |v| v.slug));
        let client_id = req.client_id.map(|c| c.id).unwrap_or_default();
        let idempotency_key = req.idempotency_key;

        // Check idempotency cache
        let cache_key = (organization, vault, client_id.clone(), idempotency_key.clone());
        {
            let cache = self.state.idempotency_cache.read();
            if let Some((tx_id, block_height, assigned_sequence)) = cache.get(&cache_key) {
                // Already committed - return cached result
                return Ok(Response::new(proto::BatchWriteResponse {
                    result: Some(proto::batch_write_response::Result::Error(proto::WriteError {
                        code: proto::WriteErrorCode::AlreadyCommitted as i32,
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message: "Already committed".to_string(),
                        committed_tx_id: Some(proto::TxId { id: tx_id.clone() }),
                        committed_block_height: Some(*block_height),
                        assigned_sequence: Some(*assigned_sequence),
                    })),
                }));
            }
        }

        // Apply all operations from all batches
        let block_height = self.state.block_height.fetch_add(1, Ordering::SeqCst);
        {
            let mut entities = self.state.entities.write();
            for batch in req.operations {
                for op in batch.operations {
                    if let Some(op_inner) = op.op {
                        match op_inner {
                            proto::operation::Op::SetEntity(set) => {
                                entities.insert(
                                    (organization, vault, set.key),
                                    (set.value, block_height, set.expires_at),
                                );
                            },
                            proto::operation::Op::DeleteEntity(del) => {
                                entities.remove(&(organization, vault, del.key));
                            },
                            proto::operation::Op::CreateRelationship(rel) => {
                                let mut relationships = self.state.relationships.write();
                                let entry = relationships.entry((organization, vault)).or_default();
                                entry.push(proto::Relationship {
                                    resource: rel.resource,
                                    relation: rel.relation,
                                    subject: rel.subject,
                                });
                            },
                            proto::operation::Op::DeleteRelationship(del) => {
                                let mut relationships = self.state.relationships.write();
                                if let Some(rels) = relationships.get_mut(&(organization, vault)) {
                                    rels.retain(|r| {
                                        r.resource != del.resource
                                            || r.relation != del.relation
                                            || r.subject != del.subject
                                    });
                                }
                            },
                            proto::operation::Op::ExpireEntity(expire) => {
                                entities.remove(&(organization, vault, expire.key));
                            },
                        }
                    }
                }
            }
        }

        // Assign server sequence and update client state
        let client_key = (organization, vault, client_id);
        let assigned_sequence = {
            let mut sequences = self.state.client_sequences.write();
            let next_seq = sequences.get(&client_key).copied().unwrap_or(0) + 1;
            sequences.insert(client_key, next_seq);
            next_seq
        };

        // Cache the result for idempotency
        let tx_id = Self::generate_tx_id();
        {
            let mut cache = self.state.idempotency_cache.write();
            cache.insert(cache_key, (tx_id.clone(), block_height, assigned_sequence));
        }

        Ok(Response::new(proto::BatchWriteResponse {
            result: Some(proto::batch_write_response::Result::Success(proto::BatchWriteSuccess {
                tx_id: Some(proto::TxId { id: tx_id }),
                block_height,
                block_header: None,
                tx_proof: None,
                assigned_sequence,
            })),
        }))
    }
}
