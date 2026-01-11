//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};

use crate::metrics;
use crate::proto::read_service_server::ReadService;
use crate::proto::{
    BlockAnnouncement, GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest,
    GetBlockResponse, GetClientStateRequest, GetClientStateResponse, GetTipRequest, GetTipResponse,
    HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest, ListEntitiesResponse,
    ListRelationshipsRequest, ListRelationshipsResponse, ListResourcesRequest,
    ListResourcesResponse, ReadRequest, ReadResponse, VerifiedReadRequest, VerifiedReadResponse,
    WatchBlocksRequest,
};

use ledger_storage::StateLayer;

use crate::log_storage::AppliedStateAccessor;

/// Read service implementation.
pub struct ReadServiceImpl {
    /// The state layer for reading data.
    state: Arc<RwLock<StateLayer>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
}

impl ReadServiceImpl {
    /// Create a new read service.
    pub fn new(
        state: Arc<RwLock<StateLayer>>,
        applied_state: AppliedStateAccessor,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        Self {
            state,
            applied_state,
            block_announcements,
        }
    }
}

#[tonic::async_trait]
impl ReadService for ReadServiceImpl {
    #[instrument(
        skip(self, request),
        fields(vault_id = tracing::field::Empty, key = tracing::field::Empty)
    )]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract vault ID (0 for namespace-level reads)
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Read from state layer
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id as i64, req.key.as_bytes())
            .map_err(|e| {
                warn!(error = %e, "Read failed");
                metrics::record_read(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Storage error: {}", e))
            })?;

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(found, latency_ms = latency * 1000.0, "Read completed");
        metrics::record_read(true, latency);

        // Get current block height for this vault
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let block_height = self
            .applied_state
            .vault_height(namespace_id, vault_id as i64);

        Ok(Response::new(ReadResponse {
            value: entity.map(|e| e.value),
            block_height,
        }))
    }

    #[instrument(
        skip(self, request),
        fields(vault_id = tracing::field::Empty, key = tracing::field::Empty)
    )]
    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract vault ID
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Read from state layer
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id as i64, req.key.as_bytes())
            .map_err(|e| {
                warn!(error = %e, "Verified read failed");
                metrics::record_verified_read(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Storage error: {}", e))
            })?;

        // TODO: Generate real merkle proof
        let merkle_proof = crate::proto::MerkleProof {
            leaf_hash: None,
            siblings: vec![],
        };

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(
            found,
            latency_ms = latency * 1000.0,
            "Verified read completed"
        );
        metrics::record_verified_read(true, latency);

        // Get current block height for this vault
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let block_height = self
            .applied_state
            .vault_height(namespace_id, vault_id as i64);

        Ok(Response::new(VerifiedReadResponse {
            value: entity.map(|e| e.value),
            block_height,
            block_header: None, // TODO: Include block header from block archive
            merkle_proof: Some(merkle_proof),
            chain_proof: None,
        }))
    }

    async fn historical_read(
        &self,
        request: Request<HistoricalReadRequest>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        let req = request.into_inner();

        // Historical read requires at_height
        if req.at_height == 0 {
            return Err(Status::invalid_argument(
                "at_height is required for historical reads",
            ));
        }

        // Extract vault ID
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // TODO: Implement historical reads - for now, just read current state
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id as i64, req.key.as_bytes())
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        Ok(Response::new(HistoricalReadResponse {
            value: entity.map(|e| e.value),
            block_height: req.at_height,
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        }))
    }

    type WatchBlocksStream =
        Pin<Box<dyn Stream<Item = Result<BlockAnnouncement, Status>> + Send + 'static>>;

    async fn watch_blocks(
        &self,
        request: Request<WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        let req = request.into_inner();

        // Subscribe to block announcements
        let receiver = self.block_announcements.subscribe();

        // Filter by namespace and vault
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id);
        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        let stream =
            tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |result| {
                let namespace_id = namespace_id;
                let vault_id = vault_id;
                async move {
                    match result {
                        Ok(announcement) => {
                            // Filter by namespace
                            if let Some(ns_id) = namespace_id {
                                if announcement
                                    .namespace_id
                                    .as_ref()
                                    .map(|n| n.id)
                                    .unwrap_or(0)
                                    != ns_id
                                {
                                    return None;
                                }
                            }
                            // Filter by vault
                            if let Some(v_id) = vault_id {
                                if announcement.vault_id.as_ref().map(|v| v.id).unwrap_or(0) != v_id
                                {
                                    return None;
                                }
                            }
                            Some(Ok(announcement))
                        }
                        Err(_) => Some(Err(Status::internal("Stream error"))),
                    }
                }
            });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let _req = request.into_inner();

        // TODO: Implement block retrieval from block archive
        // For now, return empty response
        Ok(Response::new(GetBlockResponse { block: None }))
    }

    async fn get_block_range(
        &self,
        request: Request<GetBlockRangeRequest>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        let _req = request.into_inner();

        // TODO: Implement block range retrieval from block archive
        Ok(Response::new(GetBlockRangeResponse {
            blocks: vec![],
            current_tip: 0,
        }))
    }

    async fn get_tip(
        &self,
        request: Request<GetTipRequest>,
    ) -> Result<Response<GetTipResponse>, Status> {
        let req = request.into_inner();

        // Get the vault specified in the request, or return the global max height
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        let height = if vault_id != 0 {
            // Specific vault requested
            self.applied_state.vault_height(namespace_id, vault_id)
        } else if namespace_id != 0 {
            // Namespace requested - return max height across all vaults in namespace
            self.applied_state
                .all_vault_heights()
                .iter()
                .filter(|((ns, _), _)| *ns == namespace_id)
                .map(|(_, h)| *h)
                .max()
                .unwrap_or(0)
        } else {
            // No filter - return max height across all vaults
            self.applied_state
                .all_vault_heights()
                .values()
                .copied()
                .max()
                .unwrap_or(0)
        };

        // TODO: Include block_hash and state_root from block archive
        Ok(Response::new(GetTipResponse {
            height,
            block_hash: None,
            state_root: None,
        }))
    }

    async fn get_client_state(
        &self,
        request: Request<GetClientStateRequest>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        let _req = request.into_inner();

        // TODO: Look up client state from _system namespace
        Ok(Response::new(GetClientStateResponse {
            last_committed_sequence: 0,
        }))
    }

    async fn list_relationships(
        &self,
        request: Request<ListRelationshipsRequest>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        let req = request.into_inner();

        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };
        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token.as_str())
        };

        let state = self.state.read();

        // Determine which method to use based on filters
        let relationships: Vec<crate::proto::Relationship> =
            if let (Some(resource), Some(relation)) = (&req.resource, &req.relation) {
                // Optimized path: use index lookup for resource+relation
                let subjects = state
                    .list_subjects(vault_id, resource, relation)
                    .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

                subjects
                    .into_iter()
                    .take(limit)
                    .map(|subject| crate::proto::Relationship {
                        resource: resource.clone(),
                        relation: relation.clone(),
                        subject,
                    })
                    .collect()
            } else if let Some(subject) = &req.subject {
                // Use reverse index for subject lookup
                let resources = state
                    .list_resources_for_subject(vault_id, subject)
                    .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

                resources
                    .into_iter()
                    .take(limit)
                    .map(|(resource, relation)| crate::proto::Relationship {
                        resource,
                        relation,
                        subject: subject.clone(),
                    })
                    .collect()
            } else {
                // Full scan with optional resource filter
                let raw_rels = state
                    .list_relationships(vault_id, page_token, limit)
                    .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

                raw_rels
                    .into_iter()
                    .filter(|r| req.resource.as_ref().is_none_or(|res| r.resource == *res))
                    .filter(|r| req.relation.as_ref().is_none_or(|rel| r.relation == *rel))
                    .map(|r| crate::proto::Relationship {
                        resource: r.resource,
                        relation: r.relation,
                        subject: r.subject,
                    })
                    .collect()
            };

        // Create pagination token from last relationship
        let next_page_token = if relationships.len() >= limit {
            relationships
                .last()
                .map(|r| format!("{}#{}@{}", r.resource, r.relation, r.subject))
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Get current block height for this vault
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let block_height = self.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(ListRelationshipsResponse {
            relationships,
            next_page_token,
            block_height,
        }))
    }

    async fn list_resources(
        &self,
        request: Request<ListResourcesRequest>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };
        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token.as_str())
        };

        // Get current block height for this vault
        let block_height = self.applied_state.vault_height(namespace_id, vault_id);

        // List relationships and extract unique resources matching the type prefix
        let state = self.state.read();
        let relationships = state
            .list_relationships(vault_id, page_token, limit * 10) // Over-fetch to filter
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // Extract unique resource IDs matching the type prefix
        let mut resources: Vec<String> = relationships
            .into_iter()
            .filter(|r| req.resource_type.is_empty() || r.resource.starts_with(&req.resource_type))
            .map(|r| r.resource)
            .collect();

        // Deduplicate and limit
        resources.sort();
        resources.dedup();
        resources.truncate(limit);

        // Create pagination token from last resource if there are more
        let next_page_token = if resources.len() >= limit {
            resources.last().cloned().unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(ListResourcesResponse {
            resources,
            next_page_token,
            block_height,
        }))
    }

    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };
        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token.as_str())
        };
        let prefix = if req.key_prefix.is_empty() {
            None
        } else {
            Some(req.key_prefix.as_str())
        };

        // Entities are namespace-level (stored in vault_id=0 by convention)
        let vault_id = 0i64;

        // Get entities from state layer
        let state = self.state.read();
        let raw_entities = state
            .list_entities(vault_id, prefix, page_token, limit + 1)
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // Filter expired entities if not requested
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let filtered: Vec<_> = raw_entities
            .into_iter()
            .filter(|e| {
                // expires_at == 0 means never expires
                req.include_expired || e.expires_at == 0 || e.expires_at > now
            })
            .take(limit)
            .collect();

        // Convert to proto entities
        let entities: Vec<crate::proto::Entity> = filtered
            .iter()
            .map(|e| crate::proto::Entity {
                key: String::from_utf8_lossy(&e.key).to_string(),
                value: e.value.clone(),
                version: e.version,
                // Convert 0 (never expires) to None
                expires_at: if e.expires_at == 0 {
                    None
                } else {
                    Some(e.expires_at)
                },
            })
            .collect();

        // Create pagination token from last key if there are more
        let next_page_token = if entities.len() >= limit {
            filtered
                .last()
                .map(|e| String::from_utf8_lossy(&e.key).to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Entities are namespace-level, so get max height across all vaults in namespace
        let block_height = self
            .applied_state
            .all_vault_heights()
            .iter()
            .filter(|((ns, _), _)| *ns == namespace_id)
            .map(|(_, h)| *h)
            .max()
            .unwrap_or(0);

        Ok(Response::new(ListEntitiesResponse {
            entities,
            next_page_token,
            block_height,
        }))
    }
}
