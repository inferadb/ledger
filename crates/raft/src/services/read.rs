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

use ledger_storage::{BlockArchive, SnapshotManager, StateLayer, StorageEngine};

use crate::IdempotencyCache;
use crate::log_storage::{AppliedStateAccessor, VaultHealthStatus};

/// Read service implementation.
pub struct ReadServiceImpl {
    /// The state layer for reading data.
    state: Arc<RwLock<StateLayer>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block archive for retrieving stored blocks.
    block_archive: Option<Arc<BlockArchive>>,
    /// Snapshot manager for historical reads optimization.
    snapshot_manager: Option<Arc<SnapshotManager>>,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Idempotency cache for client state tracking.
    idempotency: Option<Arc<IdempotencyCache>>,
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
            block_archive: None,
            snapshot_manager: None,
            block_announcements,
            idempotency: None,
        }
    }

    /// Create a read service with block archive access.
    pub fn with_block_archive(
        state: Arc<RwLock<StateLayer>>,
        applied_state: AppliedStateAccessor,
        block_archive: Arc<BlockArchive>,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        Self {
            state,
            applied_state,
            block_archive: Some(block_archive),
            snapshot_manager: None,
            block_announcements,
            idempotency: None,
        }
    }

    /// Create a read service with block archive and snapshot manager.
    pub fn with_snapshots(
        state: Arc<RwLock<StateLayer>>,
        applied_state: AppliedStateAccessor,
        block_archive: Arc<BlockArchive>,
        snapshot_manager: Arc<SnapshotManager>,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        Self {
            state,
            applied_state,
            block_archive: Some(block_archive),
            snapshot_manager: Some(snapshot_manager),
            block_announcements,
            idempotency: None,
        }
    }

    /// Create a read service with full configuration.
    pub fn with_idempotency(
        state: Arc<RwLock<StateLayer>>,
        applied_state: AppliedStateAccessor,
        block_archive: Option<Arc<BlockArchive>>,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
        idempotency: Arc<IdempotencyCache>,
    ) -> Self {
        Self {
            state,
            applied_state,
            block_archive,
            snapshot_manager: None,
            block_announcements,
            idempotency: Some(idempotency),
        }
    }

    /// Create a read service with full configuration including snapshots.
    pub fn with_full_config(
        state: Arc<RwLock<StateLayer>>,
        applied_state: AppliedStateAccessor,
        block_archive: Option<Arc<BlockArchive>>,
        snapshot_manager: Option<Arc<SnapshotManager>>,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
        idempotency: Option<Arc<IdempotencyCache>>,
    ) -> Self {
        Self {
            state,
            applied_state,
            block_archive,
            snapshot_manager,
            block_announcements,
            idempotency,
        }
    }

    /// Fetch block header from archive for a given vault height.
    ///
    /// Returns None if the block is not found or archive is not available.
    fn get_block_header(
        &self,
        archive: &BlockArchive,
        namespace_id: i64,
        vault_id: i64,
        vault_height: u64,
    ) -> Option<crate::proto::BlockHeader> {
        // Height 0 means no blocks yet
        if vault_height == 0 {
            return None;
        }

        // Find the shard height containing this vault block
        let shard_height = archive
            .find_shard_height(namespace_id, vault_id, vault_height)
            .ok()
            .flatten()?;

        // Read the shard block
        let shard_block = archive.read_block(shard_height).ok()?;

        // Find the vault entry
        let entry = shard_block
            .vault_entries
            .iter()
            .find(|e| e.namespace_id == namespace_id && e.vault_id == vault_id)?;

        // Build proto block header
        Some(crate::proto::BlockHeader {
            height: entry.vault_height,
            namespace_id: Some(crate::proto::NamespaceId {
                id: entry.namespace_id,
            }),
            vault_id: Some(crate::proto::VaultId { id: entry.vault_id }),
            previous_hash: Some(crate::proto::Hash {
                value: entry.previous_vault_hash.to_vec(),
            }),
            tx_merkle_root: Some(crate::proto::Hash {
                value: entry.tx_merkle_root.to_vec(),
            }),
            state_root: Some(crate::proto::Hash {
                value: entry.state_root.to_vec(),
            }),
            timestamp: Some(prost_types::Timestamp {
                seconds: shard_block.timestamp.timestamp(),
                nanos: shard_block.timestamp.timestamp_subsec_nanos() as i32,
            }),
            leader_id: Some(crate::proto::NodeId {
                id: shard_block.leader_id.clone(),
            }),
            term: shard_block.term,
            committed_index: shard_block.committed_index,
        })
    }

    /// Get block_hash and state_root for a vault at a given height.
    ///
    /// Returns (block_hash, state_root) or (None, None) if not found.
    fn get_tip_hashes(
        &self,
        archive: &BlockArchive,
        namespace_id: i64,
        vault_id: i64,
        vault_height: u64,
    ) -> (Option<crate::proto::Hash>, Option<crate::proto::Hash>) {
        // Find the shard height containing this vault block
        let shard_height = match archive
            .find_shard_height(namespace_id, vault_id, vault_height)
            .ok()
            .flatten()
        {
            Some(h) => h,
            None => return (None, None),
        };

        // Read the shard block
        let shard_block = match archive.read_block(shard_height) {
            Ok(block) => block,
            Err(_) => return (None, None),
        };

        // Find the vault entry
        let entry = match shard_block
            .vault_entries
            .iter()
            .find(|e| e.namespace_id == namespace_id && e.vault_id == vault_id)
        {
            Some(e) => e,
            None => return (None, None),
        };

        // Compute block hash from vault entry
        let block_hash = ledger_types::vault_entry_hash(entry);

        (
            Some(crate::proto::Hash {
                value: block_hash.to_vec(),
            }),
            Some(crate::proto::Hash {
                value: entry.state_root.to_vec(),
            }),
        )
    }

    /// Find and load the nearest snapshot for historical read optimization.
    ///
    /// Returns (start_height, snapshot_loaded):
    /// - If a suitable snapshot is found and loaded, returns (snapshot_vault_height + 1, true)
    /// - If no snapshot available or loading fails, returns (1, false)
    ///
    /// The snapshot state is loaded into temp_state for the specified vault.
    fn load_nearest_snapshot_for_historical_read(
        &self,
        vault_id: i64,
        target_height: u64,
        temp_state: &StateLayer,
    ) -> (u64, bool) {
        let snapshot_manager = match &self.snapshot_manager {
            Some(sm) => sm,
            None => return (1, false),
        };

        // List available snapshots
        let snapshots = match snapshot_manager.list_snapshots() {
            Ok(s) => s,
            Err(_) => return (1, false),
        };

        // Find the largest snapshot height where the vault's height is <= target
        for &shard_height in snapshots.iter().rev() {
            let snapshot = match snapshot_manager.load(shard_height) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Find the vault's height in this snapshot
            if let Some(vault_meta) = snapshot
                .header
                .vault_states
                .iter()
                .find(|v| v.vault_id == vault_id)
            {
                if vault_meta.vault_height <= target_height {
                    // This snapshot is usable - load its entities into temp_state
                    if let Some(entities) = snapshot.state.vault_entities.get(&vault_id) {
                        // Convert entities to SetEntity operations for replay
                        let operations: Vec<ledger_types::Operation> = entities
                            .iter()
                            .map(|entity| {
                                // Entity.key is Vec<u8>, convert to String for Operation
                                let key = String::from_utf8_lossy(&entity.key).into_owned();
                                ledger_types::Operation::SetEntity {
                                    key,
                                    value: entity.value.clone(),
                                    condition: None, // No condition for snapshot restore
                                    expires_at: if entity.expires_at == 0 {
                                        None
                                    } else {
                                        Some(entity.expires_at)
                                    },
                                }
                            })
                            .collect();

                        // Apply all entities at the snapshot height
                        if !operations.is_empty() {
                            if let Err(e) = temp_state.apply_operations(
                                vault_id,
                                &operations,
                                vault_meta.vault_height,
                            ) {
                                debug!("Failed to restore entities from snapshot: {:?}", e);
                                return (1, false);
                            }
                        }
                    }

                    debug!(
                        shard_height,
                        vault_height = vault_meta.vault_height,
                        "Loaded snapshot for historical read"
                    );
                    return (vault_meta.vault_height + 1, true);
                }
            }
        }

        // No suitable snapshot found
        (1, false)
    }

    /// Build a ChainProof linking blocks from trusted_height+1 to response_height.
    ///
    /// The ChainProof allows clients to verify that the response_height block
    /// is part of the canonical chain descending from their trusted_height.
    ///
    /// Returns None if:
    /// - Block archive is not available
    /// - trusted_height >= response_height (nothing to prove)
    /// - Any block in the range is not found
    fn build_chain_proof(
        &self,
        archive: &BlockArchive,
        namespace_id: i64,
        vault_id: i64,
        trusted_height: u64,
        response_height: u64,
    ) -> Option<crate::proto::ChainProof> {
        // Nothing to prove if trusted is at or past response
        if trusted_height >= response_height {
            return Some(crate::proto::ChainProof { headers: vec![] });
        }

        // Collect headers from trusted_height+1 to response_height
        let mut headers = Vec::with_capacity((response_height - trusted_height) as usize);

        for height in (trusted_height + 1)..=response_height {
            let header = self.get_block_header(archive, namespace_id, vault_id, height)?;
            headers.push(header);
        }

        Some(crate::proto::ChainProof { headers })
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

        // Extract IDs
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Check vault health - diverged vaults cannot be read
        let health = self
            .applied_state
            .vault_health(namespace_id, vault_id as i64);
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

        // Extract IDs
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("key", &req.key);

        // Check vault health - diverged vaults cannot be read
        let health = self.applied_state.vault_health(namespace_id, vault_id);
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

        // Read from state layer
        let state = self.state.read();
        let entity = state
            .get_entity(vault_id, req.key.as_bytes())
            .map_err(|e| {
                warn!(error = %e, "Verified read failed");
                metrics::record_verified_read(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Storage error: {}", e))
            })?;
        drop(state);

        // Get current block height for this vault
        let block_height = self.applied_state.vault_height(namespace_id, vault_id);

        // Fetch block header from archive if available
        let block_header = if let Some(archive) = &self.block_archive {
            self.get_block_header(archive, namespace_id, vault_id, block_height)
        } else {
            None
        };

        // Note: State verification uses bucket-based hashing (not sparse Merkle tree),
        // so we can't generate individual key inclusion proofs. The state_root in the
        // block header commits to the entire vault state. Clients must trust the
        // state_root or reconstruct the full bucket hash to verify.
        let merkle_proof = crate::proto::MerkleProof {
            leaf_hash: None,
            siblings: vec![],
        };

        // Build chain proof if requested
        let chain_proof = if req.include_chain_proof {
            if let Some(archive) = &self.block_archive {
                let trusted_height = req.trusted_height.unwrap_or(0);
                self.build_chain_proof(
                    archive,
                    namespace_id,
                    vault_id,
                    trusted_height,
                    block_height,
                )
            } else {
                None
            }
        } else {
            None
        };

        let latency = start.elapsed().as_secs_f64();
        let found = entity.is_some();
        debug!(
            found,
            latency_ms = latency * 1000.0,
            "Verified read completed"
        );
        metrics::record_verified_read(true, latency);

        Ok(Response::new(VerifiedReadResponse {
            value: entity.map(|e| e.value),
            block_height,
            block_header,
            merkle_proof: Some(merkle_proof),
            chain_proof,
        }))
    }

    async fn historical_read(
        &self,
        request: Request<HistoricalReadRequest>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Historical read requires at_height
        if req.at_height == 0 {
            return Err(Status::invalid_argument(
                "at_height is required for historical reads",
            ));
        }

        // Extract IDs
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);

        // Get block archive - required for historical reads
        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                return Err(Status::unavailable(
                    "Block archive not configured for historical reads",
                ));
            }
        };

        // Check that requested height doesn't exceed current tip
        let tip_height = self.applied_state.vault_height(namespace_id, vault_id);
        if req.at_height > tip_height {
            return Err(Status::invalid_argument(format!(
                "Requested height {} exceeds current tip {}",
                req.at_height, tip_height
            )));
        }

        // Create in-memory state layer for replay
        let temp_engine = StorageEngine::open_in_memory()
            .map_err(|e| Status::internal(format!("Failed to create temporary state: {}", e)))?;
        let temp_state = StateLayer::new(temp_engine.db());

        // Track block timestamp for expiration check
        let mut block_timestamp = chrono::Utc::now();

        // Find the optimal starting point (snapshot or height 1)
        let (start_height, snapshot_loaded) =
            self.load_nearest_snapshot_for_historical_read(vault_id, req.at_height, &temp_state);

        debug!(
            start_height,
            snapshot_loaded,
            target_height = req.at_height,
            "Historical read replay starting"
        );

        // Replay blocks from start_height to at_height
        for height in start_height..=req.at_height {
            // Find shard height for this vault block
            let shard_height = match archive.find_shard_height(namespace_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => continue, // Block might not exist at this height (sparse)
                Err(e) => return Err(Status::internal(format!("Index lookup failed: {:?}", e))),
            };

            // Read the shard block
            let shard_block = archive
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

                // Track block timestamp at the target height for expiration check
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

        // Get block header for proof (if include_proof is set)
        let block_header = if req.include_proof {
            self.get_block_header(archive, namespace_id, vault_id, req.at_height)
        } else {
            None
        };

        // Build chain proof if requested (requires include_proof to be useful)
        let chain_proof = if req.include_chain_proof && req.include_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            self.build_chain_proof(
                archive,
                namespace_id,
                vault_id,
                trusted_height,
                req.at_height,
            )
        } else {
            None
        };

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
            block_header,
            merkle_proof: None,
            chain_proof,
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
        let req = request.into_inner();

        // Get block archive, return empty if not configured
        let archive = match &self.block_archive {
            Some(a) => a,
            None => return Ok(Response::new(GetBlockResponse { block: None })),
        };

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let height = req.height;

        // Find the shard height containing this vault block
        let shard_height = archive
            .find_shard_height(namespace_id, vault_id, height)
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        let shard_height = match shard_height {
            Some(h) => h,
            None => return Ok(Response::new(GetBlockResponse { block: None })),
        };

        // Read the shard block
        let shard_block = archive
            .read_block(shard_height)
            .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

        // Find the vault entry in the shard block
        let vault_entry = shard_block.vault_entries.iter().find(|e| {
            e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
        });

        let block = vault_entry.map(|entry| vault_entry_to_proto_block(entry, &shard_block));

        Ok(Response::new(GetBlockResponse { block }))
    }

    async fn get_block_range(
        &self,
        request: Request<GetBlockRangeRequest>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        let req = request.into_inner();

        // Get block archive, return empty if not configured
        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                return Ok(Response::new(GetBlockRangeResponse {
                    blocks: vec![],
                    current_tip: 0,
                }));
            }
        };

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let start_height = req.start_height;
        let end_height = req.end_height;

        // Limit range to 1000 blocks
        let max_range = 1000u64;
        let end_height = end_height.min(start_height.saturating_add(max_range - 1));

        let mut blocks = Vec::new();

        // Iterate through the height range
        for height in start_height..=end_height {
            // Find the shard height for this vault block
            let shard_height = match archive
                .find_shard_height(namespace_id, vault_id, height)
                .map_err(|e| Status::internal(format!("Storage error: {}", e)))?
            {
                Some(h) => h,
                None => continue, // Block not found, skip
            };

            // Read the shard block
            let shard_block = archive
                .read_block(shard_height)
                .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

            // Find the vault entry
            if let Some(entry) = shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            }) {
                blocks.push(vault_entry_to_proto_block(entry, &shard_block));
            }
        }

        // Get current tip for this vault
        let current_tip = self.applied_state.vault_height(namespace_id, vault_id);

        Ok(Response::new(GetBlockRangeResponse {
            blocks,
            current_tip,
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

        // Get block_hash and state_root from archive if available and a specific vault is requested
        let (block_hash, state_root) =
            if let (Some(archive), true) = (&self.block_archive, vault_id != 0 && height > 0) {
                self.get_tip_hashes(archive, namespace_id, vault_id, height)
            } else {
                (None, None)
            };

        Ok(Response::new(GetTipResponse {
            height,
            block_hash,
            state_root,
        }))
    }

    async fn get_client_state(
        &self,
        request: Request<GetClientStateRequest>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        let req = request.into_inner();

        // Get client ID from request
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or("");

        // Query the idempotency cache for last committed sequence
        // Note: This is in-memory with 5-minute TTL. For persistent client state,
        // we would need to store in the state layer at _system/clients/{client_id}.
        let last_committed_sequence = self
            .idempotency
            .as_ref()
            .map(|cache| cache.get_last_sequence(client_id))
            .unwrap_or(0);

        Ok(Response::new(GetClientStateResponse {
            last_committed_sequence,
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

/// Convert a VaultEntry from storage to a proto Block.
fn vault_entry_to_proto_block(
    entry: &ledger_types::VaultEntry,
    shard_block: &ledger_types::ShardBlock,
) -> crate::proto::Block {
    use prost_types::Timestamp;

    // Convert VaultEntry transactions to proto transactions
    let transactions = entry
        .transactions
        .iter()
        .map(|tx| crate::proto::Transaction {
            id: Some(crate::proto::TxId { id: tx.id.to_vec() }),
            client_id: Some(crate::proto::ClientId {
                id: tx.client_id.clone(),
            }),
            sequence: tx.sequence,
            operations: tx.operations.iter().map(operation_to_proto).collect(),
            timestamp: Some(Timestamp {
                seconds: tx.timestamp.timestamp(),
                nanos: tx.timestamp.timestamp_subsec_nanos() as i32,
            }),
            actor: tx.actor.clone(),
        })
        .collect();

    // Build block header
    let header = crate::proto::BlockHeader {
        height: entry.vault_height,
        namespace_id: Some(crate::proto::NamespaceId {
            id: entry.namespace_id,
        }),
        vault_id: Some(crate::proto::VaultId { id: entry.vault_id }),
        previous_hash: Some(crate::proto::Hash {
            value: entry.previous_vault_hash.to_vec(),
        }),
        tx_merkle_root: Some(crate::proto::Hash {
            value: entry.tx_merkle_root.to_vec(),
        }),
        state_root: Some(crate::proto::Hash {
            value: entry.state_root.to_vec(),
        }),
        timestamp: Some(Timestamp {
            seconds: shard_block.timestamp.timestamp(),
            nanos: shard_block.timestamp.timestamp_subsec_nanos() as i32,
        }),
        leader_id: Some(crate::proto::NodeId {
            id: shard_block.leader_id.clone(),
        }),
        term: shard_block.term,
        committed_index: shard_block.committed_index,
    };

    crate::proto::Block {
        header: Some(header),
        transactions,
    }
}

/// Convert a ledger_types Operation to proto Operation.
fn operation_to_proto(op: &ledger_types::Operation) -> crate::proto::Operation {
    use crate::proto::operation::Op;
    use ledger_types::Operation as LedgerOp;

    match op {
        LedgerOp::CreateRelationship {
            resource,
            relation,
            subject,
        } => crate::proto::Operation {
            op: Some(Op::CreateRelationship(crate::proto::CreateRelationship {
                resource: resource.clone(),
                relation: relation.clone(),
                subject: subject.clone(),
            })),
        },
        LedgerOp::DeleteRelationship {
            resource,
            relation,
            subject,
        } => crate::proto::Operation {
            op: Some(Op::DeleteRelationship(crate::proto::DeleteRelationship {
                resource: resource.clone(),
                relation: relation.clone(),
                subject: subject.clone(),
            })),
        },
        LedgerOp::SetEntity {
            key,
            value,
            condition,
            expires_at,
        } => {
            let condition_proto = condition.as_ref().map(condition_to_proto);

            crate::proto::Operation {
                op: Some(Op::SetEntity(crate::proto::SetEntity {
                    key: key.clone(),
                    value: value.clone(),
                    condition: condition_proto,
                    expires_at: *expires_at, // Already Option<u64>
                })),
            }
        }
        LedgerOp::DeleteEntity { key } => crate::proto::Operation {
            op: Some(Op::DeleteEntity(crate::proto::DeleteEntity {
                key: key.clone(),
            })),
        },
        LedgerOp::ExpireEntity { key, expired_at } => crate::proto::Operation {
            op: Some(Op::ExpireEntity(crate::proto::ExpireEntity {
                key: key.clone(),
                expired_at: *expired_at,
            })),
        },
    }
}

/// Convert a ledger_types SetCondition to proto SetCondition.
fn condition_to_proto(c: &ledger_types::SetCondition) -> crate::proto::SetCondition {
    use crate::proto::set_condition::Condition;

    let condition = match c {
        ledger_types::SetCondition::MustNotExist => Condition::NotExists(true),
        ledger_types::SetCondition::MustExist => {
            // Proto doesn't have must_exist, use not_exists=false as a workaround
            // or version=0 to indicate "must exist" semantics
            Condition::NotExists(false)
        }
        ledger_types::SetCondition::VersionEquals(v) => Condition::Version(*v),
        ledger_types::SetCondition::ValueEquals(bytes) => Condition::ValueEquals(bytes.clone()),
    };

    crate::proto::SetCondition {
        condition: Some(condition),
    }
}
