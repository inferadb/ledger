//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.
//!
//! ## Consistency Levels
//!
//! Reads support two consistency levels:
//! - **EVENTUAL** (default): Read from any replica. Fastest, may be slightly stale.
//! - **LINEARIZABLE**: Read from leader only. Strong consistency, higher latency.
//!
//! Use linearizable reads when you need read-after-write consistency guarantees.

use std::{pin::Pin, sync::Arc};

use futures::StreamExt;
use inferadb_ledger_proto::{
    convert::vault_entry_to_proto_block,
    proto::{
        BlockAnnouncement, GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest,
        GetBlockResponse, GetClientStateRequest, GetClientStateResponse, GetTipRequest,
        GetTipResponse, HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest,
        ListEntitiesResponse, ListRelationshipsRequest, ListRelationshipsResponse,
        ListResourcesRequest, ListResourcesResponse, ReadConsistency, ReadRequest, ReadResponse,
        VerifiedReadRequest, VerifiedReadResponse, WatchBlocksRequest,
        read_service_server::ReadService,
    },
};
use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, VaultId, VaultSlug};
use openraft::Raft;
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::{
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    metrics,
    pagination::{PageToken, PageTokenCodec},
    services::slug_resolver::SlugResolver,
    trace_context,
    types::{LedgerNodeId, LedgerTypeConfig},
    wide_events::{OperationType, RequestContext, Sampler},
};

/// Handles read operations including verified reads, entity/relationship listing, and block
/// streaming.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct ReadServiceImpl {
    /// State layer providing entity, vault, and organization read access.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block archive for retrieving stored blocks.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Snapshot manager for historical reads optimization.
    #[builder(default)]
    snapshot_manager: Option<Arc<SnapshotManager>>,
    /// Block announcement broadcast channel.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Raft instance for consistency checks (linearizable reads).
    #[builder(default)]
    raft: Option<Arc<Raft<LedgerTypeConfig>>>,
    /// This node's ID for leadership checks.
    #[builder(default)]
    node_id: Option<LedgerNodeId>,
    /// Page token codec for secure pagination (HMAC-protected).
    #[builder(default = PageTokenCodec::with_random_key())]
    page_token_codec: PageTokenCodec,
    /// Sampler for wide events tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
}

impl ReadServiceImpl {
    /// Checks if this node is the current Raft leader.
    ///
    /// Returns false if Raft is not configured.
    fn is_leader(&self) -> bool {
        match &self.raft {
            Some(raft) => {
                let metrics = raft.metrics().borrow().clone();
                metrics.current_leader == Some(metrics.id)
            },
            None => false,
        }
    }

    /// Checks consistency requirements for a read request.
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
            },
            ReadConsistency::Eventual | ReadConsistency::Unspecified => {
                // Eventual reads can be served by any node
                Ok(())
            },
        }
    }

    /// Fetches block header from archive for a given vault height.
    ///
    /// Returns None if the block is not found or archive is not available.
    fn get_block_header(
        &self,
        archive: &BlockArchive<FileBackend>,
        organization_id: OrganizationId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> Option<inferadb_ledger_proto::proto::BlockHeader> {
        // Height 0 means no blocks yet
        if vault_height == 0 {
            return None;
        }

        // Find the shard height containing this vault block
        let shard_height =
            archive.find_shard_height(organization_id, vault_id, vault_height).ok().flatten()?;

        // Read the shard block
        let shard_block = archive.read_block(shard_height).ok()?;

        // Find the vault entry
        let entry = shard_block
            .vault_entries
            .iter()
            .find(|e| e.organization_id == organization_id && e.vault_id == vault_id)?;

        // Build proto block header
        Some(inferadb_ledger_proto::proto::BlockHeader {
            height: entry.vault_height,
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: self
                    .applied_state
                    .resolve_id_to_slug(entry.organization_id)
                    .map_or(entry.organization_id.value() as u64, |s| s.value()),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                slug: self
                    .applied_state
                    .resolve_vault_id_to_slug(entry.vault_id)
                    .map_or(0, |s| s.value()),
            }),
            previous_hash: Some(inferadb_ledger_proto::proto::Hash {
                value: entry.previous_vault_hash.to_vec(),
            }),
            tx_merkle_root: Some(inferadb_ledger_proto::proto::Hash {
                value: entry.tx_merkle_root.to_vec(),
            }),
            state_root: Some(inferadb_ledger_proto::proto::Hash {
                value: entry.state_root.to_vec(),
            }),
            timestamp: Some(prost_types::Timestamp {
                seconds: shard_block.timestamp.timestamp(),
                nanos: shard_block.timestamp.timestamp_subsec_nanos() as i32,
            }),
            leader_id: Some(inferadb_ledger_proto::proto::NodeId {
                id: shard_block.leader_id.clone(),
            }),
            term: shard_block.term,
            committed_index: shard_block.committed_index,
        })
    }

    /// Returns block_hash and state_root for a vault at a given height.
    ///
    /// Returns (block_hash, state_root) or (None, None) if not found.
    fn get_tip_hashes(
        &self,
        archive: &BlockArchive<FileBackend>,
        organization_id: OrganizationId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> (Option<inferadb_ledger_proto::proto::Hash>, Option<inferadb_ledger_proto::proto::Hash>)
    {
        // Find the shard height containing this vault block
        let shard_height =
            match archive.find_shard_height(organization_id, vault_id, vault_height).ok().flatten()
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
            .find(|e| e.organization_id == organization_id && e.vault_id == vault_id)
        {
            Some(e) => e,
            None => return (None, None),
        };

        // Compute block hash from vault entry
        let block_hash = inferadb_ledger_types::vault_entry_hash(entry);

        (
            Some(inferadb_ledger_proto::proto::Hash { value: block_hash.to_vec() }),
            Some(inferadb_ledger_proto::proto::Hash { value: entry.state_root.to_vec() }),
        )
    }

    /// Finds and loads the nearest snapshot for historical read optimization.
    ///
    /// Returns (start_height, snapshot_loaded):
    /// - If a suitable snapshot is found and loaded, returns (snapshot_vault_height + 1, true)
    /// - If no snapshot available or loading fails, returns (1, false)
    ///
    /// The snapshot state is loaded into temp_state for the specified vault.
    fn load_nearest_snapshot_for_historical_read(
        &self,
        vault_id: VaultId,
        target_height: u64,
        temp_state: &StateLayer<FileBackend>,
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
            if let Some(vault_meta) =
                snapshot.header.vault_states.iter().find(|v| v.vault_id == vault_id)
                && vault_meta.vault_height <= target_height
            {
                // This snapshot is usable - load its entities into temp_state
                if let Some(entities) = snapshot.state.vault_entities.get(&vault_id) {
                    // Convert entities to SetEntity operations for replay
                    let operations: Vec<inferadb_ledger_types::Operation> = entities
                        .iter()
                        .map(|entity| {
                            // Entity.key is Vec<u8>, convert to String for Operation
                            let key = String::from_utf8_lossy(&entity.key).into_owned();
                            inferadb_ledger_types::Operation::SetEntity {
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
                    if !operations.is_empty()
                        && let Err(e) = temp_state.apply_operations(
                            vault_id,
                            &operations,
                            vault_meta.vault_height,
                        )
                    {
                        debug!("Failed to restore entities from snapshot: {:?}", e);
                        return (1, false);
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

        // No suitable snapshot found
        (1, false)
    }

    /// Builds a ChainProof linking blocks from trusted_height+1 to response_height.
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
        archive: &BlockArchive<FileBackend>,
        organization_id: OrganizationId,
        vault_id: VaultId,
        trusted_height: u64,
        response_height: u64,
    ) -> Option<inferadb_ledger_proto::proto::ChainProof> {
        // Nothing to prove if trusted is at or past response
        if trusted_height >= response_height {
            return Some(inferadb_ledger_proto::proto::ChainProof { headers: vec![] });
        }

        // Collect headers from trusted_height+1 to response_height
        let mut headers = Vec::with_capacity((response_height - trusted_height) as usize);

        for height in (trusted_height + 1)..=response_height {
            let header = self.get_block_header(archive, organization_id, vault_id, height)?;
            headers.push(header);
        }

        Some(inferadb_ledger_proto::proto::ChainProof { headers })
    }

    /// Fetches historical block announcements from the block archive.
    ///
    /// Used by watch_blocks to replay committed blocks before streaming new ones.
    fn fetch_historical_announcements(
        &self,
        organization_id: OrganizationId,
        vault_id: VaultId,
        start_height: u64,
        end_height: u64,
    ) -> Vec<BlockAnnouncement> {
        use prost_types::Timestamp;

        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                debug!("No block archive configured; cannot replay historical blocks");
                return vec![];
            },
        };

        let mut announcements = Vec::with_capacity((end_height - start_height + 1) as usize);

        for height in start_height..=end_height {
            // Find the shard height containing this vault block
            let shard_height = match archive.find_shard_height(organization_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    debug!(height, "Vault block not found in archive");
                    continue;
                },
                Err(e) => {
                    warn!(height, error = %e, "Error finding shard height");
                    continue;
                },
            };

            // Read the shard block
            let shard_block = match archive.read_block(shard_height) {
                Ok(block) => block,
                Err(e) => {
                    warn!(shard_height, error = %e, "Error reading shard block");
                    continue;
                },
            };

            // Find the vault entry in the shard block
            if let Some(entry) = shard_block.vault_entries.iter().find(|e| {
                e.organization_id == organization_id
                    && e.vault_id == vault_id
                    && e.vault_height == height
            }) {
                // Compute vault block hash using the same function as get_tip_hashes
                let block_hash = inferadb_ledger_types::vault_entry_hash(entry);

                announcements.push(BlockAnnouncement {
                    organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                        slug: self
                            .applied_state
                            .resolve_id_to_slug(entry.organization_id)
                            .map_or(entry.organization_id.value() as u64, |s| s.value()),
                    }),
                    vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                        slug: self
                            .applied_state
                            .resolve_vault_id_to_slug(entry.vault_id)
                            .map_or(0, |s| s.value()),
                    }),
                    height: entry.vault_height,
                    block_hash: Some(inferadb_ledger_proto::proto::Hash {
                        value: block_hash.to_vec(),
                    }),
                    state_root: Some(inferadb_ledger_proto::proto::Hash {
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
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context
        let mut ctx = RequestContext::new("ReadService", "read");
        ctx.set_operation_type(OperationType::Read);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(sampler) = &self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            ctx.set_node_id(*node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Set read operation fields
        ctx.set_key(&req.key);
        let consistency = match ReadConsistency::try_from(req.consistency)
            .unwrap_or(ReadConsistency::Unspecified)
        {
            ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        ctx.set_consistency(consistency);
        ctx.set_include_proof(false);

        // Check consistency requirements first
        if let Err(e) = self.check_consistency(req.consistency) {
            ctx.set_error("consistency_error", e.message());
            return Err(e);
        }

        // Extract IDs
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let org_slug = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault_slug_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(org_slug, vault_slug_val);

        // Check vault health - diverged vaults cannot be read
        let health = self.applied_state.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            ctx.set_error("vault_diverged", &msg);
            return Err(Status::unavailable(msg));
        }

        // Start storage timer
        ctx.start_storage_timer();

        // Read from state layer
        let state = &*self.state;
        let entity = match state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                ctx.end_storage_timer();
                let msg = format!("Storage error: {}", e);
                ctx.set_error("storage_error", &msg);
                metrics::record_read(false, ctx.elapsed_secs());
                return Err(Status::internal(msg));
            },
        };

        ctx.end_storage_timer();

        // Filter out expired entities (expires_at == 0 means never expires)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let entity = entity.filter(|e| e.expires_at == 0 || e.expires_at > now);

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        ctx.set_found(found);
        ctx.set_value_size_bytes(value_size);
        ctx.set_bytes_read(value_size);

        let elapsed = ctx.elapsed_secs();
        metrics::record_read(true, elapsed);
        metrics::record_organization_operation(organization_id.value(), "read");
        metrics::record_organization_latency(organization_id.value(), "read", elapsed);
        ctx.set_success();

        // Get current block height for this vault
        let block_height = self.applied_state.vault_height(organization_id, vault_id);
        ctx.set_block_height(block_height);

        Ok(Response::new(ReadResponse { value: entity.map(|e| e.value), block_height }))
    }

    /// Batches read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads use the same organization/vault scope and consistency level.
    async fn batch_read(
        &self,
        request: Request<inferadb_ledger_proto::proto::BatchReadRequest>,
    ) -> Result<Response<inferadb_ledger_proto::proto::BatchReadResponse>, Status> {
        use inferadb_ledger_proto::proto::{BatchReadResponse, BatchReadResult};

        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context
        let mut ctx = RequestContext::new("ReadService", "batch_read");
        ctx.set_operation_type(OperationType::Read);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(sampler) = &self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            ctx.set_node_id(*node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Set read operation fields
        ctx.set_keys_count(req.keys.len());
        let consistency = match ReadConsistency::try_from(req.consistency)
            .unwrap_or(ReadConsistency::Unspecified)
        {
            ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        ctx.set_consistency(consistency);
        ctx.set_include_proof(false);

        // Check consistency requirements
        if let Err(e) = self.check_consistency(req.consistency) {
            ctx.set_error("consistency_error", e.message());
            return Err(e);
        }

        // Limit batch size to prevent DoS
        const MAX_BATCH_SIZE: usize = 1000;
        if req.keys.len() > MAX_BATCH_SIZE {
            let msg = format!("Batch size {} exceeds maximum {}", req.keys.len(), MAX_BATCH_SIZE);
            ctx.set_error("batch_too_large", &msg);
            return Err(Status::invalid_argument(msg));
        }

        // Extract IDs
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let org_slug = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault_slug_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(org_slug, vault_slug_val);

        // Check vault health - diverged vaults cannot be read
        let health = self.applied_state.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            ctx.set_error("vault_diverged", &msg);
            return Err(Status::unavailable(msg));
        }

        // Start storage timer
        ctx.start_storage_timer();

        // Get current time for TTL filtering
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Read all keys from state layer
        let state = &*self.state;
        let mut results = Vec::with_capacity(req.keys.len());
        let mut found_count = 0usize;

        for key in &req.keys {
            let entity = match state.get_entity(vault_id, key.as_bytes()) {
                Ok(e) => e,
                Err(e) => {
                    ctx.end_storage_timer();
                    let msg = format!("Storage error: {}", e);
                    ctx.set_error("storage_error", &msg);
                    return Err(Status::internal(msg));
                },
            };

            // Filter out expired entities (expires_at == 0 means never expires)
            let entity = entity.filter(|e| e.expires_at == 0 || e.expires_at > now);

            let found = entity.is_some();
            if found {
                found_count += 1;
            }
            results.push(BatchReadResult {
                key: key.clone(),
                value: entity.map(|e| e.value),
                found,
            });
        }

        ctx.end_storage_timer();
        ctx.set_found_count(found_count);

        let batch_size = results.len();
        let total_bytes: usize =
            results.iter().filter_map(|r| r.value.as_ref()).map(|v| v.len()).sum();
        ctx.set_bytes_read(total_bytes);

        // Record metrics for each read in the batch
        let latency = ctx.elapsed_secs();
        for _ in 0..batch_size {
            metrics::record_read(true, latency / batch_size as f64);
        }
        metrics::record_organization_operation(organization_id.value(), "read");
        metrics::record_organization_latency(organization_id.value(), "read", latency);

        ctx.set_success();

        // Get current block height for this vault
        let block_height = self.applied_state.vault_height(organization_id, vault_id);
        ctx.set_block_height(block_height);

        Ok(Response::new(BatchReadResponse { results, block_height }))
    }

    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context
        let mut ctx = RequestContext::new("ReadService", "verified_read");
        ctx.set_operation_type(OperationType::Read);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(sampler) = &self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            ctx.set_node_id(*node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Set read operation fields
        ctx.set_key(&req.key);
        ctx.set_include_proof(true);
        ctx.set_consistency("linearizable"); // verified reads are always linearizable

        // Extract IDs
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let org_slug = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault_slug_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(org_slug, vault_slug_val);

        // Check vault health - diverged vaults cannot be read
        let health = self.applied_state.vault_health(organization_id, vault_id);
        if let VaultHealthStatus::Diverged { at_height, .. } = &health {
            let msg = format!(
                "Vault {}:{} has diverged at height {}",
                organization_id, vault_id, at_height
            );
            ctx.set_error("vault_diverged", &msg);
            metrics::record_verified_read(false, ctx.elapsed_secs());
            return Err(Status::unavailable(msg));
        }

        // Start storage timer
        ctx.start_storage_timer();

        // Read from state layer
        let state = &*self.state;
        let entity = match state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                ctx.end_storage_timer();
                let msg = format!("Storage error: {}", e);
                ctx.set_error("storage_error", &msg);
                metrics::record_verified_read(false, ctx.elapsed_secs());
                return Err(Status::internal(msg));
            },
        };

        ctx.end_storage_timer();

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        ctx.set_found(found);
        ctx.set_value_size_bytes(value_size);
        ctx.set_bytes_read(value_size);

        // Get current block height for this vault
        let block_height = self.applied_state.vault_height(organization_id, vault_id);
        ctx.set_block_height(block_height);

        // Fetch block header from archive if available
        let block_header = if let Some(archive) = &self.block_archive {
            self.get_block_header(archive, organization_id, vault_id, block_height)
        } else {
            None
        };

        // Note: State verification uses bucket-based hashing (not sparse Merkle tree),
        // so we can't generate individual key inclusion proofs. The state_root in the
        // block header commits to the entire vault state. Clients must trust the
        // state_root or reconstruct the full bucket hash to verify.
        let merkle_proof =
            inferadb_ledger_proto::proto::MerkleProof { leaf_hash: None, siblings: vec![] };

        // Build chain proof if requested
        let chain_proof = if req.include_chain_proof {
            if let Some(archive) = &self.block_archive {
                let trusted_height = req.trusted_height.unwrap_or(0);
                self.build_chain_proof(
                    archive,
                    organization_id,
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

        // Calculate proof size (merkle proof + optional chain proof)
        let proof_size = std::mem::size_of_val(&merkle_proof)
            + chain_proof.as_ref().map_or(0, std::mem::size_of_val);
        ctx.set_proof_size_bytes(proof_size);

        metrics::record_verified_read(true, ctx.elapsed_secs());
        ctx.set_success();

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
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context
        let mut ctx = RequestContext::new("ReadService", "historical_read");
        ctx.set_operation_type(OperationType::Read);
        ctx.extract_transport_metadata(&grpc_metadata);
        if let Some(sampler) = &self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = &self.node_id {
            ctx.set_node_id(*node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Set read operation fields
        ctx.set_key(&req.key);
        ctx.set_at_height(req.at_height);
        ctx.set_include_proof(req.include_proof);
        ctx.set_consistency("historical");

        // Historical read requires at_height
        if req.at_height == 0 {
            ctx.set_error("invalid_argument", "at_height is required for historical reads");
            return Err(Status::invalid_argument("at_height is required for historical reads"));
        }

        // Extract IDs
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let org_slug = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault_slug_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(org_slug, vault_slug_val);

        // Get block archive - required for historical reads
        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                ctx.set_error("unavailable", "Block archive not configured for historical reads");
                return Err(Status::unavailable(
                    "Block archive not configured for historical reads",
                ));
            },
        };

        // Check that requested height doesn't exceed current tip
        let tip_height = self.applied_state.vault_height(organization_id, vault_id);
        if req.at_height > tip_height {
            let msg =
                format!("Requested height {} exceeds current tip {}", req.at_height, tip_height);
            ctx.set_error("invalid_argument", &msg);
            return Err(Status::invalid_argument(msg));
        }

        // Start storage timer (covers replay and read)
        ctx.start_storage_timer();

        // Create temporary state layer for replay using temp directory
        let temp_dir = match TempDir::new() {
            Ok(d) => d,
            Err(e) => {
                ctx.end_storage_timer();
                let msg = format!("Failed to create temp dir: {}", e);
                ctx.set_error("internal", &msg);
                return Err(Status::internal(msg));
            },
        };

        let temp_db = match Database::<FileBackend>::create(temp_dir.path().join("replay.db")) {
            Ok(db) => Arc::new(db),
            Err(e) => {
                ctx.end_storage_timer();
                let msg = format!("Failed to create temp db: {}", e);
                ctx.set_error("internal", &msg);
                return Err(Status::internal(msg));
            },
        };
        let temp_state = StateLayer::new(temp_db);

        // Track block timestamp for expiration check
        let mut block_timestamp = chrono::Utc::now();

        // Find the optimal starting point (snapshot or height 1)
        let (start_height, _snapshot_loaded) =
            self.load_nearest_snapshot_for_historical_read(vault_id, req.at_height, &temp_state);

        // Replay blocks from start_height to at_height
        for height in start_height..=req.at_height {
            // Find shard height for this vault block
            let shard_height = match archive.find_shard_height(organization_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => continue, // Block might not exist at this height (sparse)
                Err(e) => {
                    ctx.end_storage_timer();
                    let msg = format!("Index lookup failed: {:?}", e);
                    ctx.set_error("internal", &msg);
                    return Err(Status::internal(msg));
                },
            };

            // Read the shard block
            let shard_block = match archive.read_block(shard_height) {
                Ok(b) => b,
                Err(e) => {
                    ctx.end_storage_timer();
                    let msg = format!("Block read failed: {:?}", e);
                    ctx.set_error("internal", &msg);
                    return Err(Status::internal(msg));
                },
            };

            // Find the vault entry
            let vault_entry = shard_block.vault_entries.iter().find(|e| {
                e.organization_id == organization_id
                    && e.vault_id == vault_id
                    && e.vault_height == height
            });

            if let Some(entry) = vault_entry {
                // Apply all transactions in this block
                for tx in &entry.transactions {
                    if let Err(e) = temp_state.apply_operations(vault_id, &tx.operations, height) {
                        ctx.end_storage_timer();
                        let msg = format!("Apply failed: {:?}", e);
                        ctx.set_error("internal", &msg);
                        return Err(Status::internal(msg));
                    }
                }

                // Track block timestamp at the target height for expiration check
                if height == req.at_height {
                    block_timestamp = shard_block.timestamp;
                }
            }
        }

        // Read entity from reconstructed state
        let entity = match temp_state.get_entity(vault_id, req.key.as_bytes()) {
            Ok(e) => e,
            Err(e) => {
                ctx.end_storage_timer();
                let msg = format!("Read failed: {:?}", e);
                ctx.set_error("internal", &msg);
                return Err(Status::internal(msg));
            },
        };

        ctx.end_storage_timer();

        // Check expiration using block timestamp (not current time)
        // This is critical for deterministic historical state reconstruction
        let block_ts = block_timestamp.timestamp() as u64;
        let entity = entity.filter(|e| {
            // expires_at == 0 means never expires
            e.expires_at == 0 || e.expires_at > block_ts
        });

        let found = entity.is_some();
        let value_size = entity.as_ref().map(|e| e.value.len()).unwrap_or(0);
        ctx.set_found(found);
        ctx.set_value_size_bytes(value_size);
        ctx.set_bytes_read(value_size);
        ctx.set_block_height(req.at_height);

        // Get block header for proof (if include_proof is set)
        let block_header = if req.include_proof {
            self.get_block_header(archive, organization_id, vault_id, req.at_height)
        } else {
            None
        };

        // Build chain proof if requested (requires include_proof to be useful)
        let chain_proof = if req.include_chain_proof && req.include_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            self.build_chain_proof(
                archive,
                organization_id,
                vault_id,
                trusted_height,
                req.at_height,
            )
        } else {
            None
        };

        // Calculate proof size if proofs were included
        if req.include_proof {
            let proof_size = chain_proof.as_ref().map_or(0, std::mem::size_of_val);
            ctx.set_proof_size_bytes(proof_size);
        }

        ctx.set_success();

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
    /// Behavior:
    /// - If start_height <= current tip: replays committed blocks first, then streams new
    /// - If start_height > current tip: waits for that block, then streams
    /// - start_height must be >= 1 (0 is rejected with INVALID_ARGUMENT)
    async fn watch_blocks(
        &self,
        request: Request<WatchBlocksRequest>,
    ) -> Result<Response<Self::WatchBlocksStream>, Status> {
        let req = request.into_inner();

        // Extract identifiers
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let start_height = req.start_height;

        // Validate start_height >= 1
        if start_height == 0 {
            return Err(Status::invalid_argument(
                "start_height must be >= 1 (use 1 for full replay from genesis)",
            ));
        }

        // Get current tip for this vault
        let current_tip = self.applied_state.vault_height(organization_id, vault_id);

        // Subscribe to broadcast BEFORE reading historical blocks
        // This ensures we don't miss any blocks committed between reading history and subscribing
        let receiver = self.block_announcements.subscribe();

        // Build historical blocks stream if start_height <= current_tip
        let historical_blocks: Vec<BlockAnnouncement> = if start_height <= current_tip {
            self.fetch_historical_announcements(
                organization_id,
                vault_id,
                start_height,
                current_tip,
            )
        } else {
            vec![]
        };

        debug!(
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
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

        // Capture raw slug value for broadcast stream filtering (compare slug-to-slug,
        // not slug-to-internal-id, since announcements carry the original slug)
        let watch_slug = req.organization.as_ref().map_or(0u64, |n| n.slug);
        let vault_slug_raw = req.vault.as_ref().map_or(0u64, |v| v.slug);
        let broadcast_stream =
            tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |result| {
                async move {
                    match result {
                        Ok(announcement) => {
                            // Filter by organization
                            if announcement.organization.as_ref().map_or(0, |n| n.slug)
                                != watch_slug
                            {
                                return None;
                            }
                            // Filter by vault
                            if announcement.vault.as_ref().map_or(0, |v| v.slug) != vault_slug_raw {
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

        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let height = req.height;

        // Find the shard height containing this vault block
        let shard_height = archive
            .find_shard_height(organization_id, vault_id, height)
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
            e.organization_id == organization_id
                && e.vault_id == vault_id
                && e.vault_height == height
        });

        let vault_slug =
            self.applied_state.resolve_vault_id_to_slug(vault_id).unwrap_or(VaultSlug::new(0));
        let block =
            vault_entry.map(|entry| vault_entry_to_proto_block(entry, &shard_block, vault_slug));

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
                return Ok(Response::new(GetBlockRangeResponse { blocks: vec![], current_tip: 0 }));
            },
        };

        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
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
                .find_shard_height(organization_id, vault_id, height)
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
                e.organization_id == organization_id
                    && e.vault_id == vault_id
                    && e.vault_height == height
            }) {
                let vault_slug = self
                    .applied_state
                    .resolve_vault_id_to_slug(vault_id)
                    .unwrap_or(VaultSlug::new(0));
                blocks.push(vault_entry_to_proto_block(entry, &shard_block, vault_slug));
            }
        }

        // Get current tip for this vault
        let current_tip = self.applied_state.vault_height(organization_id, vault_id);

        Ok(Response::new(GetBlockRangeResponse { blocks, current_tip }))
    }

    async fn get_tip(
        &self,
        request: Request<GetTipRequest>,
    ) -> Result<Response<GetTipResponse>, Status> {
        let req = request.into_inner();

        // Get the vault specified in the request, or return the global max height
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;

        let height = if vault_id.value() != 0 {
            // Specific vault requested
            self.applied_state.vault_height(organization_id, vault_id)
        } else if organization_id.value() != 0 {
            // Organization requested - return max height across all vaults in organization
            self.applied_state
                .all_vault_heights()
                .iter()
                .filter(|((ns, _), _)| *ns == organization_id)
                .map(|(_, h)| *h)
                .max()
                .unwrap_or(0)
        } else {
            // No filter - return max height across all vaults
            self.applied_state.all_vault_heights().values().copied().max().unwrap_or(0)
        };

        // Get block_hash and state_root from archive if available and a specific vault is requested
        let (block_hash, state_root) = if let (Some(archive), true) =
            (&self.block_archive, vault_id.value() != 0 && height > 0)
        {
            self.get_tip_hashes(archive, organization_id, vault_id, height)
        } else {
            (None, None)
        };

        Ok(Response::new(GetTipResponse { height, block_hash, state_root }))
    }

    async fn get_client_state(
        &self,
        request: Request<GetClientStateRequest>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        let req = request.into_inner();

        // Extract IDs
        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or("");

        // Server-assigned sequences: Query the persistent AppliedState directly
        // The idempotency cache no longer tracks sequence numbers by sequence;
        // it uses idempotency keys instead.
        let last_committed_sequence =
            self.applied_state.client_sequence(organization_id, vault_id, client_id);

        Ok(Response::new(GetClientStateResponse { last_committed_sequence }))
    }

    async fn list_relationships(
        &self,
        request: Request<ListRelationshipsRequest>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        let req = request.into_inner();

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        // Compute query hash from all filter parameters for token validation
        let query_params = format!(
            "resource:{},relation:{},subject:{}",
            req.resource.as_deref().unwrap_or(""),
            req.relation.as_deref().unwrap_or(""),
            req.subject.as_deref().unwrap_or("")
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = self.applied_state.vault_height(organization_id, vault_id);

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
                .validate_context(&token, organization_id.value(), vault_id.value(), query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        let state = &*self.state;

        // Determine which method to use based on filters
        let relationships: Vec<inferadb_ledger_proto::proto::Relationship> =
            if let (Some(resource), Some(relation)) = (&req.resource, &req.relation) {
                // Optimized path: use index lookup for resource+relation
                let subjects = state
                    .list_subjects(vault_id, resource, relation)
                    .map_err(|e| Status::internal(format!("Storage error: {}", e)))?;

                subjects
                    .into_iter()
                    .take(limit)
                    .map(|subject| inferadb_ledger_proto::proto::Relationship {
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
                    .map(|(resource, relation)| inferadb_ledger_proto::proto::Relationship {
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
                    .map(|r| r.into())
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
                        organization_id: organization_id.value(),
                        vault_id: vault_id.value(),
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

        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        // Compute query hash from filter parameters for token validation
        let query_params = format!("resource_type:{}", req.resource_type);
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = self.applied_state.vault_height(organization_id, vault_id);

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
                .validate_context(&token, organization_id.value(), vault_id.value(), query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
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
                        organization_id: organization_id.value(),
                        vault_id: vault_id.value(),
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

        Ok(Response::new(ListResourcesResponse { resources, next_page_token, block_height }))
    }

    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let req = request.into_inner();

        // Check consistency requirements first
        self.check_consistency(req.consistency)?;

        let organization_id = match req.organization.as_ref() {
            Some(n) if n.slug != 0 => SlugResolver::new(self.applied_state.clone())
                .resolve(OrganizationSlug::new(n.slug))?,
            _ => OrganizationId::new(0),
        };
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let prefix = if req.key_prefix.is_empty() { None } else { Some(req.key_prefix.as_str()) };

        // Use vault_id from request, defaulting to 0 for organization-level entities
        let vault_id =
            SlugResolver::new(self.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;

        // Compute query hash from filter parameters for token validation
        // This prevents clients from changing filters mid-pagination
        let query_params = format!(
            "prefix:{},include_expired:{},vault:{}",
            req.key_prefix, req.include_expired, vault_id
        );
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = if vault_id.value() != 0 {
            // Specific vault requested - use its height
            self.applied_state.vault_height(organization_id, vault_id)
        } else {
            // Organization-level entities - use max height across all vaults in organization
            self.applied_state
                .all_vault_heights()
                .iter()
                .filter(|((ns, _), _)| *ns == organization_id)
                .map(|(_, h)| *h)
                .max()
                .unwrap_or(0)
        };

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
                .validate_context(&token, organization_id.value(), vault_id.value(), query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
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
        let entities: Vec<inferadb_ledger_proto::proto::Entity> =
            filtered.iter().map(|e| e.into()).collect();

        // Create secure pagination token from last key if there are more
        let next_page_token = if entities.len() >= limit {
            filtered
                .last()
                .map(|e| {
                    let token = PageToken {
                        version: 1,
                        organization_id: organization_id.value(),
                        vault_id: vault_id.value(),
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

        Ok(Response::new(ListEntitiesResponse { entities, next_page_token, block_height }))
    }
}
