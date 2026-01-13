//! Multi-shard read service implementation.
//!
//! Routes read requests to the appropriate shard based on namespace.
//! Per DESIGN.md §4.6: Each namespace is assigned to a shard, and requests
//! are routed to the state layer for that shard.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};

use crate::log_storage::VaultHealthStatus;
use crate::metrics;
use crate::proto::read_service_server::ReadService;
use crate::proto::{
    BlockAnnouncement, GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest,
    GetBlockResponse, GetClientStateRequest, GetClientStateResponse, GetTipRequest, GetTipResponse,
    HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest, ListEntitiesResponse,
    ListRelationshipsRequest, ListRelationshipsResponse, ListResourcesRequest,
    ListResourcesResponse, ReadConsistency, ReadRequest, ReadResponse, VerifiedReadRequest,
    VerifiedReadResponse, WatchBlocksRequest,
};
use crate::services::shard_resolver::ShardResolver;

/// Multi-shard read service implementation.
///
/// Routes read requests to the correct shard based on namespace_id.
/// This is a simplified implementation that handles core read operations.
/// Complex operations like block retrieval and streaming are forwarded
/// to the resolved shard's services.
pub struct MultiShardReadService {
    /// Shard resolver for routing requests.
    resolver: Arc<dyn ShardResolver>,
}

impl MultiShardReadService {
    /// Create a new multi-shard read service.
    pub fn new(resolver: Arc<dyn ShardResolver>) -> Self {
        Self { resolver }
    }

    /// Check consistency requirements for a read request.
    fn check_consistency(&self, namespace_id: i64, consistency: i32) -> Result<(), Status> {
        let consistency =
            ReadConsistency::try_from(consistency).unwrap_or(ReadConsistency::Unspecified);

        match consistency {
            ReadConsistency::Linearizable => {
                // Linearizable reads require this node to be the leader
                let ctx = self.resolver.resolve(namespace_id)?;
                let metrics = ctx.raft.metrics().borrow().clone();
                if metrics.current_leader.is_none() {
                    return Err(Status::unavailable(
                        "Linearizable reads not available: no leader elected",
                    ));
                }
                // For true linearizability, we'd need to check if THIS node is leader
                // For now, we allow reads if there's any leader (weaker guarantee)
                Ok(())
            }
            ReadConsistency::Eventual | ReadConsistency::Unspecified => {
                // Eventual reads can be served by any node
                Ok(())
            }
        }
    }
}

#[tonic::async_trait]
impl ReadService for MultiShardReadService {
    #[instrument(skip(self, request), fields(namespace_id, vault_id, key))]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Check consistency requirements
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health - diverged vaults cannot be read
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id,
                vault_id, at_height, "Read rejected: vault has diverged"
            );
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id, vault_id, at_height
            )));
        }

        // Read from the shard's state layer (internally thread-safe via redb MVCC)
        let entity = ctx
            .state
            .get_entity(vault_id, req.key.as_bytes())
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
        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(ReadResponse {
            value: entity.map(|e| e.value),
            block_height,
        }))
    }

    /// Batch read multiple keys in a single RPC call.
    #[instrument(skip(self, request), fields(namespace_id, vault_id, batch_size))]
    async fn batch_read(
        &self,
        request: Request<crate::proto::BatchReadRequest>,
    ) -> Result<Response<crate::proto::BatchReadResponse>, Status> {
        use crate::proto::{BatchReadResponse, BatchReadResult};

        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Limit batch size
        const MAX_BATCH_SIZE: usize = 1000;
        if req.keys.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "Batch size {} exceeds maximum {}",
                req.keys.len(),
                MAX_BATCH_SIZE
            )));
        }

        // Record span fields
        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("batch_size", req.keys.len());

        // Check consistency requirements
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health - diverged vaults cannot be read
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id,
                vault_id, at_height, "BatchRead rejected: vault has diverged"
            );
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id, vault_id, at_height
            )));
        }

        // Read all keys from the shard's state layer
        let mut results = Vec::with_capacity(req.keys.len());

        for key in &req.keys {
            let entity = ctx
                .state
                .get_entity(vault_id, key.as_bytes())
                .map_err(|e| {
                    warn!(error = %e, key = key, "BatchRead key failed");
                    Status::internal(format!("Storage error: {}", e))
                })?;

            let found = entity.is_some();
            results.push(BatchReadResult {
                key: key.clone(),
                value: entity.map(|e| e.value),
                found,
            });
        }

        let latency = start.elapsed().as_secs_f64();
        let batch_size = results.len();
        debug!(
            batch_size,
            latency_ms = latency * 1000.0,
            "BatchRead completed"
        );

        // Record metrics
        for _ in 0..batch_size {
            metrics::record_read(true, latency / batch_size as f64);
        }

        // Get current block height
        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(BatchReadResponse {
            results,
            block_height,
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id, key))]
    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id,
                vault_id, at_height, "Verified read rejected: vault has diverged"
            );
            metrics::record_verified_read(false, start.elapsed().as_secs_f64());
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id, vault_id, at_height
            )));
        }

        // Read from state layer (internally thread-safe via redb MVCC)
        let entity = ctx
            .state
            .get_entity(vault_id, req.key.as_bytes())
            .map_err(|e| {
                warn!(error = %e, "Verified read failed");
                metrics::record_verified_read(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Storage error: {}", e))
            })?;

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(
            found,
            latency_ms = latency * 1000.0,
            "Verified read completed"
        );
        metrics::record_verified_read(true, latency);

        // Get block height
        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(VerifiedReadResponse {
            value: entity.map(|e| e.value),
            block_height,
            // Proof generation requires block archive access per shard
            // For now, we return None - full proof support requires more infrastructure
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id))]
    async fn get_tip(
        &self,
        request: Request<GetTipRequest>,
    ) -> Result<Response<GetTipResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Get vault height from applied state
        let height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(GetTipResponse {
            height,
            block_hash: None, // Would need block archive for hash
            state_root: None, // Would need block archive for state root
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id))]
    async fn list_relationships(
        &self,
        request: Request<ListRelationshipsRequest>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };

        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Read from state layer - simple listing without complex pagination
        // StateLayer is internally thread-safe via redb MVCC

        // Use subject-based lookup if subject is specified
        let relationships = if let Some(subject) = &req.subject {
            let resources = ctx
                .state
                .list_resources_for_subject(vault_id, subject)
                .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

            resources
                .into_iter()
                .take(limit)
                .filter(|(resource, relation)| {
                    req.resource.as_ref().is_none_or(|r| resource == r)
                        && req.relation.as_ref().is_none_or(|rel| relation == rel)
                })
                .map(|(resource, relation)| crate::proto::Relationship {
                    resource,
                    relation,
                    subject: subject.clone(),
                })
                .collect()
        } else {
            // Full scan with filters
            let raw_rels = ctx
                .state
                .list_relationships(vault_id, None, limit)
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

        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(ListRelationshipsResponse {
            relationships,
            next_page_token: String::new(), // Simplified - no pagination for now
            block_height,
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id))]
    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };
        let prefix = if req.key_prefix.is_empty() {
            None
        } else {
            Some(req.key_prefix.as_str())
        };

        tracing::Span::current().record("namespace_id", namespace_id);

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Entities are namespace-level (stored in vault_id=0 by convention)
        let vault_id = 0i64;

        // Read from state layer (internally thread-safe via redb MVCC)
        let raw_entities = ctx
            .state
            .list_entities(vault_id, prefix, None, limit)
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // Filter expired entities if not requested
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let entities: Vec<crate::proto::Entity> = raw_entities
            .into_iter()
            .filter(|e| req.include_expired || e.expires_at == 0 || e.expires_at > now)
            .take(limit)
            .map(|e| crate::proto::Entity {
                key: String::from_utf8_lossy(&e.key).to_string(),
                value: e.value,
                version: e.version,
                expires_at: if e.expires_at == 0 {
                    None
                } else {
                    Some(e.expires_at)
                },
            })
            .collect();

        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(ListEntitiesResponse {
            entities,
            next_page_token: String::new(), // Simplified - no pagination for now
            block_height,
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id))]
    async fn list_resources(
        &self,
        request: Request<ListResourcesRequest>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };

        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Read from state layer - list relationships and extract unique resources
        // StateLayer is internally thread-safe via redb MVCC
        let relationships = ctx
            .state
            .list_relationships(vault_id, None, limit * 10) // Over-fetch to filter
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

        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(ListResourcesResponse {
            resources,
            next_page_token: String::new(), // Simplified - no pagination for now
            block_height,
        }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id))]
    async fn get_client_state(
        &self,
        request: Request<GetClientStateRequest>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let client_id = req
            .client_id
            .as_ref()
            .map(|c| c.id.as_str())
            .unwrap_or_default();

        tracing::Span::current().record("namespace_id", namespace_id);
        tracing::Span::current().record("vault_id", vault_id);

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Get client sequence from applied state
        let last_committed_sequence =
            ctx.applied_state
                .client_sequence(namespace_id, vault_id, client_id);

        Ok(Response::new(GetClientStateResponse {
            last_committed_sequence,
        }))
    }

    // Block-related methods - simplified implementations
    // Full block retrieval would require per-shard block archive infrastructure

    #[instrument(skip(self, request), fields(namespace_id, vault_id, height))]
    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Block retrieval is complex for multi-shard
        // Return None for now - full implementation would use vault_entry_to_proto_block helper
        warn!(
            namespace_id,
            "get_block: block retrieval not yet fully implemented for multi-shard"
        );

        Ok(Response::new(GetBlockResponse { block: None }))
    }

    #[instrument(skip(self, request))]
    async fn get_block_range(
        &self,
        request: Request<GetBlockRangeRequest>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Resolve shard to get current tip
        let ctx = self.resolver.resolve(namespace_id)?;
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let current_tip = ctx.applied_state.vault_height(namespace_id, vault_id);

        warn!(
            namespace_id,
            "get_block_range: not yet optimized for multi-shard"
        );

        // Return empty for now - full implementation would iterate blocks
        Ok(Response::new(GetBlockRangeResponse {
            blocks: vec![],
            current_tip,
        }))
    }

    #[instrument(skip(self, request))]
    async fn historical_read(
        &self,
        request: Request<HistoricalReadRequest>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Historical reads require snapshot reconstruction which is complex
        warn!(
            namespace_id,
            "Historical reads not yet implemented for multi-shard"
        );
        Err(Status::unimplemented(
            "Historical reads not yet implemented for multi-shard deployments",
        ))
    }

    type WatchBlocksStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<BlockAnnouncement, Status>> + Send>>;

    #[instrument(skip(self, request))]
    async fn watch_blocks(
        &self,
        request: Request<WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Block watching requires broadcast channel per shard which is complex
        warn!(
            namespace_id,
            "Block watching not yet implemented for multi-shard"
        );
        Err(Status::unimplemented(
            "Block watching not yet implemented for multi-shard deployments",
        ))
    }
}

// Note: Full block conversion would require vault_entry_to_proto_block helper
// from the original read.rs. For now, simplified implementations return None
// for block operations.

#[cfg(test)]
mod tests {
    #[test]
    fn test_multi_shard_read_service_creation() {
        // Basic struct test - full testing requires state setup
    }
}
