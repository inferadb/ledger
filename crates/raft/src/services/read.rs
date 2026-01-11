//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::proto::read_service_server::ReadService;
use crate::proto::{
    BlockAnnouncement, GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest,
    GetBlockResponse, GetClientStateRequest, GetClientStateResponse, GetTipRequest,
    GetTipResponse, HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest,
    ListEntitiesResponse, ListRelationshipsRequest, ListRelationshipsResponse,
    ListResourcesRequest, ListResourcesResponse, ReadRequest, ReadResponse, VerifiedReadRequest,
    VerifiedReadResponse, WatchBlocksRequest,
};

use ledger_storage::StateLayer;

/// Read service implementation.
pub struct ReadServiceImpl {
    /// The state layer for reading data.
    state: Arc<RwLock<StateLayer>>,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
}

impl ReadServiceImpl {
    /// Create a new read service.
    pub fn new(
        state: Arc<RwLock<StateLayer>>,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        Self {
            state,
            block_announcements,
        }
    }
}

#[tonic::async_trait]
impl ReadService for ReadServiceImpl {
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        // Extract vault ID (0 for namespace-level reads)
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Read from state layer
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id as i64, req.key.as_bytes())
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        Ok(Response::new(ReadResponse {
            value: entity.map(|e| e.value),
            block_height: 0, // TODO: Track block heights per vault
        }))
    }

    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        let req = request.into_inner();

        // Extract vault ID
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Read from state layer
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id as i64, req.key.as_bytes())
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // TODO: Generate real merkle proof
        let merkle_proof = crate::proto::MerkleProof {
            leaf_hash: None,
            siblings: vec![],
        };

        Ok(Response::new(VerifiedReadResponse {
            value: entity.map(|e| e.value),
            block_height: 0,
            block_header: None, // TODO: Include block header
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

        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(
            move |result| {
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
                                if announcement
                                    .vault_id
                                    .as_ref()
                                    .map(|v| v.id)
                                    .unwrap_or(0)
                                    != v_id
                                {
                                    return None;
                                }
                            }
                            Some(Ok(announcement))
                        }
                        Err(_) => Some(Err(Status::internal("Stream error"))),
                    }
                }
            },
        );

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
        let _req = request.into_inner();

        // TODO: Get actual tip from state layer
        Ok(Response::new(GetTipResponse {
            height: 0,
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

        let vault_id = req.vault_id.as_ref().map(|v| v.id as i64).unwrap_or(0);

        // Use StateLayer's list_subjects if resource and relation are provided
        let state = self.state.read();
        let relationships = if let (Some(resource), Some(relation)) = (&req.resource, &req.relation)
        {
            let subjects = state
                .list_subjects(vault_id, resource, relation)
                .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

            subjects
                .into_iter()
                .map(|subject| crate::proto::Relationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject,
                })
                .collect()
        } else {
            // TODO: Implement full relationship listing
            vec![]
        };

        Ok(Response::new(ListRelationshipsResponse {
            relationships,
            next_page_token: String::new(),
            block_height: 0,
        }))
    }

    async fn list_resources(
        &self,
        _request: Request<ListResourcesRequest>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        // TODO: Implement resource listing
        Ok(Response::new(ListResourcesResponse {
            resources: vec![],
            next_page_token: String::new(),
            block_height: 0,
        }))
    }

    async fn list_entities(
        &self,
        _request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        // TODO: Implement entity listing
        Ok(Response::new(ListEntitiesResponse {
            entities: vec![],
            next_page_token: String::new(),
            block_height: 0,
        }))
    }
}
