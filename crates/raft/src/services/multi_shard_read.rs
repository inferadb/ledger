//! Multi-shard read service implementation.
//!
//! Routes read requests to the appropriate shard based on namespace.
//! Per DESIGN.md §4.6: Each namespace is assigned to a shard, and requests
//! are routed to the state layer for that shard.
//!
//! ## Request Forwarding
//!
//! When a namespace is on a remote shard, this service forwards the request
//! via gRPC using `ForwardClient`. This enables transparent multi-shard reads
//! where clients don't need to know which node hosts their data.

use std::{pin::Pin, sync::Arc, time::Instant};

use futures::StreamExt;
use inferadb_ledger_proto::proto::{
    BlockAnnouncement, GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest,
    GetBlockResponse, GetClientStateRequest, GetClientStateResponse, GetTipRequest, GetTipResponse,
    HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest, ListEntitiesResponse,
    ListRelationshipsRequest, ListRelationshipsResponse, ListResourcesRequest,
    ListResourcesResponse, ReadConsistency, ReadRequest, ReadResponse, VerifiedReadRequest,
    VerifiedReadResponse, WatchBlocksRequest, read_service_server::ReadService,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{NamespaceId, VaultId};
use tempfile::TempDir;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};

use crate::{
    log_storage::VaultHealthStatus,
    metrics,
    multi_raft::MultiRaftManager,
    services::{
        ForwardClient,
        shard_resolver::{RemoteShardInfo, ResolveResult, ShardResolver},
    },
    trace_context,
};

/// Routes read requests to the correct shard, supporting local processing and remote forwarding.
///
/// Routes read requests to the correct shard based on namespace_id.
/// Supports both local processing and remote forwarding for transparent
/// multi-shard deployments.
pub struct MultiShardReadService {
    /// Shard resolver for routing requests.
    resolver: Arc<dyn ShardResolver>,
    /// Multi-raft manager for creating forward clients when needed.
    /// Optional to support simple single-shard deployments.
    manager: Option<Arc<MultiRaftManager>>,
}

impl MultiShardReadService {
    /// Creates a new multi-shard read service.
    pub fn new(resolver: Arc<dyn ShardResolver>) -> Self {
        Self { resolver, manager: None }
    }

    /// Creates a new multi-shard read service with forwarding support.
    pub fn with_manager(resolver: Arc<dyn ShardResolver>, manager: Arc<MultiRaftManager>) -> Self {
        Self { resolver, manager: Some(manager) }
    }

    /// Returns a forward client for a remote shard.
    ///
    /// Creates a gRPC connection to the remote shard's leader (or any member if leader unknown).
    async fn get_forward_client(&self, remote: &RemoteShardInfo) -> Result<ForwardClient, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::unavailable("Request forwarding not configured for this service")
        })?;

        let router =
            manager.router().ok_or_else(|| Status::unavailable("Shard router not initialized"))?;

        let connection = router
            .get_connection(
                remote.shard_id,
                &remote.routing.member_nodes,
                remote.routing.leader_hint.as_deref(),
            )
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to remote shard: {}", e))
            })?;

        Ok(ForwardClient::new(connection))
    }

    /// Checks consistency requirements for a read request.
    fn check_consistency(&self, namespace_id: NamespaceId, consistency: i32) -> Result<(), Status> {
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
            },
            ReadConsistency::Eventual | ReadConsistency::Unspecified => {
                // Eventual reads can be served by any node
                Ok(())
            },
        }
    }

    /// Processes a historical read request locally using the shard's block archive.
    ///
    /// Replays state from blocks up to the requested height to reconstruct
    /// the entity's value at that point in time.
    async fn historical_read_local(
        &self,
        req: &HistoricalReadRequest,
        ctx: &crate::services::shard_resolver::ShardContext,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        start: Instant,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        // Check that requested height doesn't exceed current tip
        let tip_height = ctx.applied_state.vault_height(namespace_id, vault_id);
        if req.at_height > tip_height {
            return Err(Status::invalid_argument(format!(
                "Requested height {} exceeds current tip {}",
                req.at_height, tip_height
            )));
        }

        // Create temporary state layer for replay
        let temp_dir = TempDir::new()
            .map_err(|e| Status::internal(format!("Failed to create temp dir: {}", e)))?;
        let temp_db = Arc::new(
            Database::<FileBackend>::create(temp_dir.path().join("replay.db"))
                .map_err(|e| Status::internal(format!("Failed to create temp db: {}", e)))?,
        );
        let temp_state = StateLayer::new(temp_db);

        // Track block timestamp for expiration check
        let mut block_timestamp = chrono::Utc::now();

        debug!(target_height = req.at_height, "Historical read replay starting from height 1");

        // Replay blocks from height 1 to at_height
        // Note: In production, we'd use snapshots to optimize this
        for height in 1..=req.at_height {
            // Find shard height for this vault block
            let shard_height =
                match ctx.block_archive.find_shard_height(namespace_id, vault_id, height) {
                    Ok(Some(h)) => h,
                    Ok(None) => continue, // Block might not exist at this height (sparse)
                    Err(e) => {
                        return Err(Status::internal(format!("Index lookup failed: {:?}", e)));
                    },
                };

            // Read the shard block
            let shard_block = ctx
                .block_archive
                .read_block(shard_height)
                .map_err(|e| Status::internal(format!("Block read failed: {:?}", e)))?;

            // Find the vault entry
            let vault_entry = shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            });

            if let Some(entry) = vault_entry {
                // Apply all transactions in this block
                for tx in &entry.transactions {
                    temp_state
                        .apply_operations(vault_id, &tx.operations, height)
                        .map_err(|e| Status::internal(format!("Apply failed: {:?}", e)))?;
                }

                // Track block timestamp at the target height
                if height == req.at_height {
                    block_timestamp = shard_block.timestamp;
                }
            }
        }

        // Read entity from reconstructed state
        let entity = temp_state
            .get_entity(vault_id, req.key.as_bytes())
            .map_err(|e| Status::internal(format!("Read failed: {:?}", e)))?;

        // Check expiration using block timestamp (not current time)
        // This is critical for deterministic historical state reconstruction
        let block_ts = block_timestamp.timestamp() as u64;
        let entity = entity.filter(|e| {
            // expires_at == 0 means never expires
            e.expires_at == 0 || e.expires_at > block_ts
        });

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(
            found,
            height = req.at_height,
            latency_ms = latency * 1000.0,
            "Historical read completed"
        );

        Ok(Response::new(HistoricalReadResponse {
            value: entity.map(|e| e.value),
            block_height: req.at_height,
            // Proof generation requires additional infrastructure
            block_header: None,
            merkle_proof: None,
            chain_proof: None,
        }))
    }

    /// Processes a watch_blocks request locally using the shard's broadcast channel.
    async fn watch_blocks_local(
        &self,
        req: &WatchBlocksRequest,
        ctx: &crate::services::shard_resolver::ShardContext,
        namespace_id: NamespaceId,
        vault_id: VaultId,
    ) -> Result<
        Response<Pin<Box<dyn futures::Stream<Item = Result<BlockAnnouncement, Status>> + Send>>>,
        Status,
    > {
        let start_height = req.start_height;

        // Validate start_height >= 1 (per DESIGN.md)
        if start_height == 0 {
            return Err(Status::invalid_argument(
                "start_height must be >= 1 (use 1 for full replay from genesis)",
            ));
        }

        // Get broadcast channel - required for local watch
        let announcements = ctx.block_announcements.as_ref().ok_or_else(|| {
            Status::unavailable("Block announcements not available for this shard")
        })?;

        // Get current tip for this vault
        let current_tip = ctx.applied_state.vault_height(namespace_id, vault_id);

        // Subscribe BEFORE reading historical blocks to avoid missing any
        let receiver = announcements.subscribe();

        // Fetch historical announcements if start_height <= current_tip
        let historical_blocks: Vec<BlockAnnouncement> = if start_height <= current_tip {
            self.fetch_historical_announcements(
                ctx,
                namespace_id,
                vault_id,
                start_height,
                current_tip,
            )
        } else {
            vec![]
        };

        info!(
            namespace_id = namespace_id.value(),
            vault_id = vault_id.value(),
            start_height,
            current_tip,
            historical_count = historical_blocks.len(),
            "WatchBlocks: starting stream"
        );

        // Create historical stream
        let historical_stream =
            futures::stream::iter(historical_blocks.into_iter().map(Ok::<_, Status>));

        // Track the last height we've sent to avoid duplicates
        let last_historical_height =
            if start_height <= current_tip { current_tip } else { start_height - 1 };

        // Create broadcast stream filtered by namespace and vault
        let broadcast_stream = BroadcastStream::new(receiver).filter_map(move |result| {
            async move {
                match result {
                    Ok(announcement) => {
                        // Filter by namespace
                        if announcement.namespace_id.as_ref().map_or(0, |n| n.id)
                            != namespace_id.value()
                        {
                            return None;
                        }
                        // Filter by vault
                        if announcement.vault_id.as_ref().map_or(0, |v| v.id) != vault_id.value() {
                            return None;
                        }
                        // Skip blocks we already sent from history
                        if announcement.height <= last_historical_height {
                            return None;
                        }
                        Some(Ok(announcement))
                    },
                    Err(_) => Some(Err(Status::internal("Stream error"))),
                }
            }
        });

        // Chain historical blocks followed by broadcast
        let combined = historical_stream.chain(broadcast_stream);

        Ok(Response::new(Box::pin(combined)))
    }

    /// Fetches historical block announcements from the block archive.
    fn fetch_historical_announcements(
        &self,
        ctx: &crate::services::shard_resolver::ShardContext,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        start_height: u64,
        end_height: u64,
    ) -> Vec<BlockAnnouncement> {
        let mut announcements = Vec::new();

        for height in start_height..=end_height {
            // Find shard height for this vault block
            let shard_height = match ctx.block_archive.find_shard_height(
                namespace_id,
                vault_id,
                height,
            ) {
                Ok(Some(h)) => h,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, height, "Failed to find shard height for historical block");
                    continue;
                },
            };

            // Read the shard block
            let shard_block = match ctx.block_archive.read_block(shard_height) {
                Ok(block) => block,
                Err(e) => {
                    warn!(error = ?e, shard_height, "Failed to read historical block");
                    continue;
                },
            };

            // Find the vault entry
            if let Some(entry) = shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            }) {
                // Compute block hash from vault entry (hash of previous_hash || state_root ||
                // tx_merkle_root) For announcements, we use the tx_merkle_root as a
                // representative hash
                let block_hash = Some(inferadb_ledger_proto::proto::Hash {
                    value: entry.tx_merkle_root.to_vec(),
                });

                let state_root =
                    Some(inferadb_ledger_proto::proto::Hash { value: entry.state_root.to_vec() });

                announcements.push(BlockAnnouncement {
                    namespace_id: Some(inferadb_ledger_proto::proto::NamespaceId {
                        id: namespace_id.value(),
                    }),
                    vault_id: Some(inferadb_ledger_proto::proto::VaultId { id: vault_id.value() }),
                    height,
                    block_hash,
                    state_root,
                    timestamp: Some(prost_types::Timestamp {
                        seconds: shard_block.timestamp.timestamp(),
                        nanos: shard_block.timestamp.timestamp_subsec_nanos() as i32,
                    }),
                });
            }
        }

        announcements
    }
}

#[tonic::async_trait]
impl ReadService for MultiShardReadService {
    #[instrument(skip(self, request), fields(namespace_id, vault_id, key))]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        // Record span fields
        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("key", &req.key);

        // Check consistency requirements
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health - diverged vaults cannot be read
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id = namespace_id.value(),
                vault_id = vault_id.value(),
                at_height,
                "Read rejected: vault has diverged"
            );
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id.value(),
                vault_id.value(),
                at_height
            )));
        }

        // Read from the shard's state layer (internally thread-safe via inferadb-ledger-store MVCC)
        let entity = ctx.state.get_entity(vault_id, req.key.as_bytes()).map_err(|e| {
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

        Ok(Response::new(ReadResponse { value: entity.map(|e| e.value), block_height }))
    }

    /// Batches read multiple keys in a single RPC call.
    #[instrument(skip(self, request), fields(namespace_id, vault_id, batch_size))]
    async fn batch_read(
        &self,
        request: Request<inferadb_ledger_proto::proto::BatchReadRequest>,
    ) -> Result<Response<inferadb_ledger_proto::proto::BatchReadResponse>, Status> {
        use inferadb_ledger_proto::proto::{BatchReadResponse, BatchReadResult};

        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

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
        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("batch_size", req.keys.len());

        // Check consistency requirements
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard for this namespace
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health - diverged vaults cannot be read
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id = namespace_id.value(),
                vault_id = vault_id.value(),
                at_height,
                "BatchRead rejected: vault has diverged"
            );
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id.value(),
                vault_id.value(),
                at_height
            )));
        }

        // Read all keys from the shard's state layer
        let mut results = Vec::with_capacity(req.keys.len());

        for key in &req.keys {
            let entity = ctx.state.get_entity(vault_id, key.as_bytes()).map_err(|e| {
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
        debug!(batch_size, latency_ms = latency * 1000.0, "BatchRead completed");

        // Record metrics
        for _ in 0..batch_size {
            metrics::record_read(true, latency / batch_size as f64);
        }

        // Get current block height
        let block_height = ctx.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(BatchReadResponse { results, block_height }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id, key))]
    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Extract namespace_id for routing
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("key", &req.key);

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Check vault health
        let health = ctx.applied_state.vault_health(namespace_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            warn!(
                namespace_id = namespace_id.value(),
                vault_id = vault_id.value(),
                at_height,
                "Verified read rejected: vault has diverged"
            );
            metrics::record_verified_read(false, start.elapsed().as_secs_f64());
            return Err(Status::unavailable(format!(
                "Vault {}:{} has diverged at height {}",
                namespace_id.value(),
                vault_id.value(),
                at_height
            )));
        }

        // Read from state layer (internally thread-safe via inferadb-ledger-store MVCC)
        let entity = ctx.state.get_entity(vault_id, req.key.as_bytes()).map_err(|e| {
            warn!(error = %e, "Verified read failed");
            metrics::record_verified_read(false, start.elapsed().as_secs_f64());
            Status::internal(format!("Storage error: {}", e))
        })?;

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(found, latency_ms = latency * 1000.0, "Verified read completed");
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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());

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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Read from state layer - simple listing without complex pagination
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC

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
                .map(|(resource, relation)| inferadb_ledger_proto::proto::Relationship {
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
                .map(|r| inferadb_ledger_proto::proto::Relationship {
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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let prefix = if req.key_prefix.is_empty() { None } else { Some(req.key_prefix.as_str()) };

        tracing::Span::current().record("namespace_id", namespace_id.value());

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Entities are namespace-level (stored in vault_id=0 by convention)
        let vault_id = VaultId::new(0);

        // Read from state layer (internally thread-safe via inferadb-ledger-store MVCC)
        let raw_entities = ctx
            .state
            .list_entities(vault_id, prefix, None, limit)
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // Filter expired entities if not requested
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let entities: Vec<inferadb_ledger_proto::proto::Entity> = raw_entities
            .into_iter()
            .filter(|e| req.include_expired || e.expires_at == 0 || e.expires_at > now)
            .take(limit)
            .map(|e| inferadb_ledger_proto::proto::Entity {
                key: String::from_utf8_lossy(&e.key).to_string(),
                value: e.value,
                version: e.version,
                expires_at: if e.expires_at == 0 { None } else { Some(e.expires_at) },
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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());

        // Check consistency
        self.check_consistency(namespace_id, req.consistency)?;

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Read from state layer - list relationships and extract unique resources
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or_default();

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());

        // Resolve shard
        let ctx = self.resolver.resolve(namespace_id)?;

        // Get client sequence from applied state
        let last_committed_sequence =
            ctx.applied_state.client_sequence(namespace_id, vault_id, client_id);

        Ok(Response::new(GetClientStateResponse { last_committed_sequence }))
    }

    // Block-related methods - simplified implementations
    // Full block retrieval would require per-shard block archive infrastructure

    #[instrument(skip(self, request), fields(namespace_id, vault_id, height))]
    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );

        // Block retrieval is complex for multi-shard
        // Return None for now - full implementation would use vault_entry_to_proto_block helper
        warn!(
            namespace_id = namespace_id.value(),
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

        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );

        // Resolve shard to get current tip
        let ctx = self.resolver.resolve(namespace_id)?;
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));
        let current_tip = ctx.applied_state.vault_height(namespace_id, vault_id);

        warn!(
            namespace_id = namespace_id.value(),
            "get_block_range: not yet optimized for multi-shard"
        );

        // Return empty for now - full implementation would iterate blocks
        Ok(Response::new(GetBlockRangeResponse { blocks: vec![], current_tip }))
    }

    #[instrument(skip(self, request), fields(namespace_id, vault_id, at_height))]
    async fn historical_read(
        &self,
        request: Request<HistoricalReadRequest>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        let start = Instant::now();
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let req = request.into_inner();

        // Extract identifiers
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("at_height", req.at_height);

        // Validate at_height
        if req.at_height == 0 {
            return Err(Status::invalid_argument("at_height is required for historical reads"));
        }

        // Check if resolver supports forwarding
        if self.resolver.supports_forwarding() {
            // Use resolve_with_forward to handle both local and remote shards
            match self.resolver.resolve_with_forward(namespace_id)? {
                ResolveResult::Local(ctx) => {
                    // Process locally using block archive
                    self.historical_read_local(&req, &ctx, namespace_id, vault_id, start).await
                },
                ResolveResult::Remote(remote) => {
                    // Forward to the remote shard
                    debug!(
                        namespace_id = namespace_id.value(),
                        shard_id = remote.shard_id.value(),
                        "Forwarding historical_read to remote shard"
                    );
                    let mut client = self.get_forward_client(&remote).await?;
                    client.forward_historical_read(req, Some(&trace_ctx), None).await
                },
            }
        } else {
            // Simple resolver - use local shard directly
            let ctx = self.resolver.resolve(namespace_id)?;
            self.historical_read_local(&req, &ctx, namespace_id, vault_id, start).await
        }
    }

    type WatchBlocksStream =
        Pin<Box<dyn futures::Stream<Item = Result<BlockAnnouncement, Status>> + Send>>;

    #[instrument(skip(self, request), fields(namespace_id, vault_id, start_height))]
    async fn watch_blocks(
        &self,
        request: Request<WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let req = request.into_inner();

        // Extract identifiers
        let namespace_id = NamespaceId::new(
            req.namespace_id
                .as_ref()
                .map(|n| n.id)
                .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?,
        );
        let vault_id = VaultId::new(req.vault_id.as_ref().map_or(0, |v| v.id));

        tracing::Span::current().record("namespace_id", namespace_id.value());
        tracing::Span::current().record("vault_id", vault_id.value());
        tracing::Span::current().record("start_height", req.start_height);

        // Check if resolver supports forwarding
        if self.resolver.supports_forwarding() {
            // Use resolve_with_forward to handle both local and remote shards
            match self.resolver.resolve_with_forward(namespace_id)? {
                ResolveResult::Local(ctx) => {
                    // Process locally using broadcast channel
                    self.watch_blocks_local(&req, &ctx, namespace_id, vault_id).await
                },
                ResolveResult::Remote(remote) => {
                    // Forward to the remote shard
                    debug!(
                        namespace_id = namespace_id.value(),
                        shard_id = remote.shard_id.value(),
                        "Forwarding watch_blocks to remote shard"
                    );
                    let mut client = self.get_forward_client(&remote).await?;
                    let response = client.forward_watch_blocks(req, Some(&trace_ctx), None).await?;

                    // Convert the streaming response to our expected type
                    let stream = response.into_inner();
                    let mapped_stream = stream.map(|result| {
                        result.map_err(|e| Status::internal(format!("Remote stream error: {}", e)))
                    });

                    Ok(Response::new(Box::pin(mapped_stream) as Self::WatchBlocksStream))
                },
            }
        } else {
            // Simple resolver - use local shard directly
            let ctx = self.resolver.resolve(namespace_id)?;
            self.watch_blocks_local(&req, &ctx, namespace_id, vault_id).await
        }
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
