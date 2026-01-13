//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.
//!
//! ## Consistency Levels
//!
//! Per DESIGN.md §10, reads support two consistency levels:
//! - **EVENTUAL** (default): Read from any replica. Fastest, may be slightly stale.
//! - **LINEARIZABLE**: Read from leader only. Strong consistency, higher latency.
//!
//! Use linearizable reads when you need read-after-write consistency guarantees.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use openraft::Raft;
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
    ListResourcesResponse, ReadConsistency, ReadRequest, ReadResponse, VerifiedReadRequest,
    VerifiedReadResponse, WatchBlocksRequest,
};

use ledger_storage::{BlockArchive, SnapshotManager, StateLayer, StorageEngine};

use crate::IdempotencyCache;
use crate::log_storage::{AppliedStateAccessor, VaultHealthStatus};
use crate::pagination::{PageToken, PageTokenCodec};
use crate::types::{LedgerNodeId, LedgerTypeConfig};

/// Read service implementation.
pub struct ReadServiceImpl {
    /// The state layer for reading data.
    state: Arc<StateLayer>,
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
    /// Raft instance for consistency checks (linearizable reads).
    raft: Option<Arc<Raft<LedgerTypeConfig>>>,
    /// This node's ID for leadership checks.
    node_id: Option<LedgerNodeId>,
    /// Page token codec for secure pagination (HMAC-protected).
    page_token_codec: PageTokenCodec,
}

impl ReadServiceImpl {
    /// Create a new read service.
    pub fn new(
        state: Arc<StateLayer>,
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
            raft: None,
            node_id: None,
            page_token_codec: PageTokenCodec::with_random_key(),
        }
    }

    /// Create a read service with block archive access.
    pub fn with_block_archive(
        state: Arc<StateLayer>,
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
            raft: None,
            node_id: None,
            page_token_codec: PageTokenCodec::with_random_key(),
        }
    }

    /// Create a read service with block archive and snapshot manager.
    pub fn with_snapshots(
        state: Arc<StateLayer>,
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
            raft: None,
            node_id: None,
            page_token_codec: PageTokenCodec::with_random_key(),
        }
    }

    /// Create a read service with full configuration.
    pub fn with_idempotency(
        state: Arc<StateLayer>,
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
            raft: None,
            node_id: None,
            page_token_codec: PageTokenCodec::with_random_key(),
        }
    }

    /// Create a read service with full configuration including snapshots.
    pub fn with_full_config(
        state: Arc<StateLayer>,
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
            raft: None,
            node_id: None,
            page_token_codec: PageTokenCodec::with_random_key(),
        }
    }

    /// Add Raft instance for linearizable read support.
    ///
    /// When set, the service can enforce linearizable reads by checking
    /// that this node is the current leader before serving read requests.
    pub fn with_raft(mut self, raft: Arc<Raft<LedgerTypeConfig>>, node_id: LedgerNodeId) -> Self {
        self.raft = Some(raft);
        self.node_id = Some(node_id);
        self
    }

    /// Check if this node is the current Raft leader.
    ///
    /// Returns false if Raft is not configured.
    fn is_leader(&self) -> bool {
        match (&self.raft, &self.node_id) {
            (Some(raft), Some(node_id)) => {
                let metrics = raft.metrics().borrow().clone();
                metrics.current_leader == Some(*node_id)
            }
            _ => false,
        }
    }

    /// Check consistency requirements for a read request.
    ///
    /// Returns Ok(()) if the request can proceed, or an error if consistency
    /// requirements cannot be met.
    fn check_consistency(&self, consistency: i32) -> Result<(), Status> {
        let consistency =
            ReadConsistency::try_from(consistency).unwrap_or(ReadConsistency::Unspecified);

        match consistency {
            ReadConsistency::Linearizable => {
                // Linearizable reads require this node to be the leader
                if self.raft.is_none() {
                    // No Raft configured - cannot guarantee linearizability
                    return Err(Status::unavailable(
                        "Linearizable reads not available: Raft not configured",
                    ));
                }
                if !self.is_leader() {
                    return Err(Status::failed_precondition(
                        "Linearizable reads require leader; this node is not the leader",
                    ));
                }
                Ok(())
            }
            ReadConsistency::Eventual | ReadConsistency::Unspecified => {
                // Eventual reads can be served by any node
                Ok(())
            }
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

    /// Fetch historical block announcements from the block archive.
    ///
    /// Used by watch_blocks to replay committed blocks before streaming new ones.
    fn fetch_historical_announcements(
        &self,
        namespace_id: i64,
        vault_id: i64,
        start_height: u64,
        end_height: u64,
    ) -> Vec<BlockAnnouncement> {
        use prost_types::Timestamp;

        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                debug!("No block archive configured; cannot replay historical blocks");
                return vec![];
            }
        };

        let mut announcements = Vec::with_capacity((end_height - start_height + 1) as usize);

        for height in start_height..=end_height {
            // Find the shard height containing this vault block
            let shard_height = match archive.find_shard_height(namespace_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    debug!(height, "Vault block not found in archive");
                    continue;
                }
                Err(e) => {
                    warn!(height, error = %e, "Error finding shard height");
                    continue;
                }
            };

            // Read the shard block
            let shard_block = match archive.read_block(shard_height) {
                Ok(block) => block,
                Err(e) => {
                    warn!(shard_height, error = %e, "Error reading shard block");
                    continue;
                }
            };

            // Find the vault entry in the shard block
            if let Some(entry) = shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            }) {
                // Compute vault block hash using the same function as get_tip_hashes
                let block_hash = ledger_types::vault_entry_hash(entry);

                announcements.push(BlockAnnouncement {
                    namespace_id: Some(crate::proto::NamespaceId {
                        id: entry.namespace_id,
                    }),
                    vault_id: Some(crate::proto::VaultId { id: entry.vault_id }),
                    height: entry.vault_height,
                    block_hash: Some(crate::proto::Hash {
                        value: block_hash.to_vec(),
                    }),
                    state_root: Some(crate::proto::Hash {
                        value: entry.state_root.to_vec(),
                    }),
                    timestamp: Some(Timestamp {
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
impl ReadService for ReadServiceImpl {
    #[instrument(
        skip(self, request),
        fields(vault_id = tracing::field::Empty, key = tracing::field::Empty)
    )]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

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
        let state = &*self.state;
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

    /// Batch read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads use the same namespace/vault scope and consistency level.
    #[instrument(
        skip(self, request),
        fields(vault_id = tracing::field::Empty, batch_size = tracing::field::Empty)
    )]
    async fn batch_read(
        &self,
        request: Request<crate::proto::BatchReadRequest>,
    ) -> Result<Response<crate::proto::BatchReadResponse>, Status> {
        use crate::proto::{BatchReadResponse, BatchReadResult};

        let start = Instant::now();
        let req = request.into_inner();

        // Check consistency requirements
        self.check_consistency(req.consistency)?;

        // Limit batch size to prevent DoS
        const MAX_BATCH_SIZE: usize = 1000;
        if req.keys.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "Batch size {} exceeds maximum {}",
                req.keys.len(),
                MAX_BATCH_SIZE
            )));
        }

        // Extract IDs
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id as u64).unwrap_or(0);

        // Record span fields
        tracing::Span::current().record("vault_id", vault_id);
        tracing::Span::current().record("batch_size", req.keys.len());

        // Check vault health - diverged vaults cannot be read
        let health = self
            .applied_state
            .vault_health(namespace_id, vault_id as i64);
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

        // Read all keys from state layer
        let state = &*self.state;
        let mut results = Vec::with_capacity(req.keys.len());

        for key in &req.keys {
            let entity = state
                .get_entity(vault_id as i64, key.as_bytes())
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

        // Record metrics for each read in the batch
        for _ in 0..batch_size {
            metrics::record_read(true, latency / batch_size as f64);
        }

        // Get current block height for this vault
        let block_height = self
            .applied_state
            .vault_height(namespace_id, vault_id as i64);

        Ok(Response::new(BatchReadResponse {
            results,
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
        let state = &*self.state;
        let entity = state
            .get_entity(vault_id, req.key.as_bytes())
            .map_err(|e| {
                warn!(error = %e, "Verified read failed");
                metrics::record_verified_read(false, start.elapsed().as_secs_f64());
                Status::internal(format!("Storage error: {}", e))
            })?;

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

    /// Watch blocks with historical replay support.
    ///
    /// Per DESIGN.md §3.3:
    /// - If start_height <= current tip: replays committed blocks first, then streams new
    /// - If start_height > current tip: waits for that block, then streams
    /// - start_height must be >= 1 (0 is rejected with INVALID_ARGUMENT)
    async fn watch_blocks(
        &self,
        request: Request<WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        let req = request.into_inner();

        // Extract identifiers
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let start_height = req.start_height;

        // Validate start_height >= 1 (per DESIGN.md)
        if start_height == 0 {
            return Err(Status::invalid_argument(
                "start_height must be >= 1 (use 1 for full replay from genesis)",
            ));
        }

        // Get current tip for this vault
        let current_tip = self.applied_state.vault_height(namespace_id, vault_id);

        // Subscribe to broadcast BEFORE reading historical blocks
        // This ensures we don't miss any blocks committed between reading history and subscribing
        let receiver = self.block_announcements.subscribe();

        // Build historical blocks stream if start_height <= current_tip
        let historical_blocks: Vec<BlockAnnouncement> = if start_height <= current_tip {
            self.fetch_historical_announcements(namespace_id, vault_id, start_height, current_tip)
        } else {
            vec![]
        };

        debug!(
            namespace_id,
            vault_id,
            start_height,
            current_tip,
            historical_count = historical_blocks.len(),
            "WatchBlocks: starting stream"
        );

        // Create the combined stream:
        // 1. Historical blocks (if any)
        // 2. New blocks from broadcast (filtered and deduplicated)
        let historical_stream =
            futures::stream::iter(historical_blocks.into_iter().map(Ok::<_, Status>));

        // Track the last height we've sent to avoid duplicates
        // (broadcast might include some blocks we already sent from history)
        let last_historical_height = if start_height <= current_tip {
            current_tip
        } else {
            start_height - 1 // Will accept blocks at start_height and above
        };

        let broadcast_stream =
            tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |result| {
                async move {
                    match result {
                        Ok(announcement) => {
                            // Filter by namespace
                            if announcement
                                .namespace_id
                                .as_ref()
                                .map(|n| n.id)
                                .unwrap_or(0)
                                != namespace_id
                            {
                                return None;
                            }
                            // Filter by vault
                            if announcement.vault_id.as_ref().map(|v| v.id).unwrap_or(0) != vault_id
                            {
                                return None;
                            }
                            // Skip blocks we already sent from history
                            if announcement.height <= last_historical_height {
                                return None;
                            }
                            Some(Ok(announcement))
                        }
                        Err(_) => Some(Err(Status::internal("Stream error"))),
                    }
                }
            });

        // Chain historical blocks followed by broadcast
        let combined = historical_stream.chain(broadcast_stream);

        Ok(Response::new(Box::pin(combined)))
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

        // Extract IDs
        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or("");

        // Query the idempotency cache first (hot path)
        // Key is (namespace_id, vault_id, client_id) per DESIGN.md §5.3
        let cached_sequence = self
            .idempotency
            .as_ref()
            .map(|cache| cache.get_last_sequence(namespace_id, vault_id, client_id))
            .unwrap_or(0);

        // If cache has data, use it; otherwise fall back to persistent state
        let last_committed_sequence = if cached_sequence > 0 {
            cached_sequence
        } else {
            // Fall back to persistent AppliedState (survives restarts)
            self.applied_state
                .client_sequence(namespace_id, vault_id, client_id)
        };

        Ok(Response::new(GetClientStateResponse {
            last_committed_sequence,
        }))
    }

    async fn list_relationships(
        &self,
        request: Request<ListRelationshipsRequest>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        let req = request.into_inner();

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };

        // Compute query hash from all filter parameters for token validation
        let query_params = format!(
            "resource:{},relation:{},subject:{}",
            req.resource.as_deref().unwrap_or(""),
            req.relation.as_deref().unwrap_or(""),
            req.subject.as_deref().unwrap_or("")
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = self.applied_state.vault_height(namespace_id, vault_id);

        // Decode and validate page token if provided
        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            // Validate token context matches request
            self.page_token_codec
                .validate_context(&token, namespace_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (
                Some(String::from_utf8_lossy(&token.last_key).to_string()),
                token.at_height,
            )
        };

        let state = &*self.state;

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
                    .list_relationships(vault_id, resume_key.as_deref(), limit)
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

        // Create secure pagination token from last relationship
        let next_page_token = if relationships.len() >= limit {
            relationships
                .last()
                .map(|r| {
                    let cursor = format!("{}#{}@{}", r.resource, r.relation, r.subject);
                    let token = PageToken {
                        version: 1,
                        namespace_id,
                        vault_id,
                        last_key: cursor.into_bytes(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

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

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
        let vault_id = req.vault_id.as_ref().map(|v| v.id).unwrap_or(0);
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };

        // Compute query hash from filter parameters for token validation
        let query_params = format!("resource_type:{}", req.resource_type);
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = self.applied_state.vault_height(namespace_id, vault_id);

        // Decode and validate page token if provided
        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            // Validate token context matches request
            self.page_token_codec
                .validate_context(&token, namespace_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (
                Some(String::from_utf8_lossy(&token.last_key).to_string()),
                token.at_height,
            )
        };

        // List relationships and extract unique resources matching the type prefix
        let state = &*self.state;
        let relationships = state
            .list_relationships(vault_id, resume_key.as_deref(), limit * 10) // Over-fetch to filter
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

        // Create secure pagination token from last resource if there are more
        let next_page_token = if resources.len() >= limit {
            resources
                .last()
                .map(|res| {
                    let token = PageToken {
                        version: 1,
                        namespace_id,
                        vault_id,
                        last_key: res.as_bytes().to_vec(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
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

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id).unwrap_or(0);
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

        // Entities are namespace-level (stored in vault_id=0 by convention)
        let vault_id = 0i64;

        // Compute query hash from filter parameters for token validation
        // This prevents clients from changing filters mid-pagination
        let query_params = format!(
            "prefix:{},include_expired:{}",
            req.key_prefix, req.include_expired
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = self
            .applied_state
            .all_vault_heights()
            .iter()
            .filter(|((ns, _), _)| *ns == namespace_id)
            .map(|(_, h)| *h)
            .max()
            .unwrap_or(0);

        // Decode and validate page token if provided
        let (resume_key, at_height) = if req.page_token.is_empty() {
            (None, block_height)
        } else {
            let token = self
                .page_token_codec
                .decode(&req.page_token)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            // Validate token context matches request
            self.page_token_codec
                .validate_context(&token, namespace_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (
                Some(String::from_utf8_lossy(&token.last_key).to_string()),
                token.at_height,
            )
        };

        // Get entities from state layer
        let state = &*self.state;
        let raw_entities = state
            .list_entities(vault_id, prefix, resume_key.as_deref(), limit + 1)
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

        // Create secure pagination token from last key if there are more
        let next_page_token = if entities.len() >= limit {
            filtered
                .last()
                .map(|e| {
                    let token = PageToken {
                        version: 1,
                        namespace_id,
                        vault_id,
                        last_key: e.key.clone(),
                        at_height,
                        query_hash,
                    };
                    self.page_token_codec.encode(&token)
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

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
