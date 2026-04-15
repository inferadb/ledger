//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.
//!
//! ## Consistency Levels
//!
//! Reads support two consistency levels:
//! - **EVENTUAL** (default): Read from any replica. Fastest, may be slightly stale.
//! - **LINEARIZABLE**: Strong consistency via leader lease or ReadIndex protocol.
//!
//! ## Linearizable Read Path
//!
//! On the **leader**: Serve directly from local state. The leader lease provides
//! confidence the node is still the real leader (~50ns validity check), but reads
//! are served regardless since the leader's state is always up to date.
//!
//! On a **follower**: Use the ReadIndex protocol — ask the leader for its committed
//! index, then wait for the local applied index to catch up before serving. This
//! avoids forwarding the full request to the leader while still providing
//! linearizable guarantees.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

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
    },
};
use inferadb_ledger_raft::{
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    logging::{OperationType, RequestContext, Sampler},
    metrics,
    pagination::{PageToken, PageTokenCodec},
    raft_manager::RaftManager,
    trace_context,
    types::LedgerNodeId,
};
use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, VaultId, VaultSlug};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use super::{
    ForwardClient,
    forward_client::LeaderChannelCache,
    helpers::storage_err,
    region_resolver::{RegionContext, RegionResolver, RemoteRegionInfo, ResolveResult},
    slug_resolver::SlugResolver,
};

/// Validates that a read key does not target system-reserved prefixes.
///
/// System keys (prefixed with `_`) are internal infrastructure and must not be
/// readable through the public Read API. This mirrors the write-path validation
/// in [`inferadb_ledger_types::validation::validate_key`].
fn validate_read_key(key: &str) -> Result<(), Status> {
    if key.starts_with('_') {
        return Err(Status::invalid_argument(
            "key must not start with '_' (reserved for system keys)",
        ));
    }
    Ok(())
}

/// Parses a relationship cursor string (`"resource#relation@subject"`) into a
/// [`Relationship`](inferadb_ledger_types::Relationship) for cursor-based pagination.
///
/// Returns `None` if the string does not contain both `#` and `@` separators.
fn parse_relationship_cursor(cursor: &str) -> Option<inferadb_ledger_types::Relationship> {
    let hash_pos = cursor.find('#')?;
    let at_pos = cursor[hash_pos..].find('@')? + hash_pos;
    let resource = &cursor[..hash_pos];
    let relation = &cursor[hash_pos + 1..at_pos];
    let subject = &cursor[at_pos + 1..];
    Some(inferadb_ledger_types::Relationship::new(resource, relation, subject))
}

/// gRPC handler for read operations.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct ReadService {
    /// Region resolver for routing requests to the correct region's state.
    resolver: Arc<dyn RegionResolver>,
    /// Multi-raft manager for creating forward clients to remote regions.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
    /// Snapshot manager for historical reads optimization.
    #[builder(default)]
    snapshot_manager: Option<Arc<SnapshotManager>>,
    /// This node's ID for leadership checks.
    #[builder(default)]
    node_id: Option<LedgerNodeId>,
    /// Page token codec for secure pagination (HMAC-protected).
    #[builder(default = PageTokenCodec::with_random_key())]
    page_token_codec: PageTokenCodec,
    /// Sampler for log tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Cached leader channel for read forwarding.
    ///
    /// Shared across requests to avoid creating a new TCP+HTTP/2 connection
    /// per forwarded read.
    #[builder(default)]
    leader_channel_cache: LeaderChannelCache,
    /// Shared counter for active `WatchBlocks` streams across all connections.
    ///
    /// Incremented when a stream starts, decremented on drop via `StreamGuard`.
    #[builder(default = Arc::new(AtomicUsize::new(0)))]
    active_streams: Arc<AtomicUsize>,
    /// Maximum concurrent `WatchBlocks` streams allowed.
    #[builder(default = 1000)]
    max_streams: usize,
    /// Shared peer address map for resolving peer network addresses.
    ///
    /// Used by read forwarding to resolve the leader's address.
    #[builder(default)]
    peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
}

/// RAII guard that decrements the active stream counter on drop.
///
/// Wraps a `WatchBlocks` response stream so the counter is always
/// decremented when the client disconnects or the stream completes,
/// regardless of how the stream ends. The inner stream is boxed to
/// satisfy `Unpin` without requiring `pin_project`.
struct StreamGuard {
    inner: Pin<Box<dyn Stream<Item = Result<BlockAnnouncement, Status>> + Send>>,
    counter: Arc<AtomicUsize>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Stream for StreamGuard {
    type Item = Result<BlockAnnouncement, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl ReadService {
    /// Resolves organization and vault IDs from a request using the region resolver.
    ///
    /// Returns `(organization_id, vault_id, region_context)`.
    fn resolve_org_vault(
        &self,
        organization: &Option<inferadb_ledger_proto::proto::OrganizationSlug>,
        vault: &Option<inferadb_ledger_proto::proto::VaultSlug>,
    ) -> Result<(OrganizationId, VaultId, RegionContext), Status> {
        let system = self.resolver.system_region()?;
        let organization_id =
            SlugResolver::new(system.applied_state.clone()).extract_and_resolve(organization)?;
        let region = self.resolver.resolve(organization_id)?;
        // Vault slug indexes are in GLOBAL applied state, not the data region's.
        let vault_id = SlugResolver::new(system.applied_state).extract_and_resolve_vault(vault)?;
        Ok((organization_id, vault_id, region))
    }

    /// Same as `resolve_org_vault` but ensures GLOBAL state is fresh first.
    async fn resolve_org_vault_consistent(
        &self,
        organization: &Option<inferadb_ledger_proto::proto::OrganizationSlug>,
        vault: &Option<inferadb_ledger_proto::proto::VaultSlug>,
    ) -> Result<(OrganizationId, VaultId, RegionContext), Status> {
        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;
        self.resolve_org_vault(organization, vault)
    }

    /// Checks if this node is the current Raft leader for the given region context.
    fn is_leader_for(ctx: &RegionContext) -> bool {
        ctx.handle.is_leader()
    }

    /// Checks if this follower is too far behind the leader and should forward reads.
    ///
    /// Returns `Ok(None)` if reads can be served locally (leader, caught up, or no Raft).
    /// Returns `Ok(Some(client))` if reads should be forwarded to the leader.
    /// Returns `Err(Status)` if forwarding is needed but no leader is available.
    fn should_forward_to_leader(
        &self,
        ctx: &RegionContext,
    ) -> Result<Option<ForwardClient>, Status> {
        let handle = &ctx.handle;

        // Leader always serves locally
        if handle.is_leader() {
            return Ok(None);
        }

        // Without openraft metrics we can't check log lag precisely.
        // The consensus handle provides commit_index which is sufficient
        // for basic forwarding decisions.
        // For now, followers always forward reads to the leader for consistency.
        let peers = self.peer_addresses.as_ref().map(|m| m.iter_peers()).unwrap_or_default();
        self.leader_channel_cache.get_or_connect(
            handle.current_leader(),
            handle.node_id(),
            peers.into_iter(),
        )
    }

    /// Determines how to serve a read based on consistency level.
    ///
    /// - Eventual/Unspecified: serve from local state (any node).
    /// - Linearizable on leader: serve directly (lease check is informational).
    /// - Linearizable on follower: ReadIndex protocol — ask leader for committed index, wait for
    ///   local apply, then serve from local state.
    async fn resolve_read_consistency(
        &self,
        ctx: &RegionContext,
        consistency: i32,
    ) -> Result<(), Status> {
        let consistency =
            ReadConsistency::try_from(consistency).unwrap_or(ReadConsistency::Unspecified);

        match consistency {
            ReadConsistency::Eventual | ReadConsistency::Unspecified => Ok(()),
            ReadConsistency::Linearizable => {
                if Self::is_leader_for(ctx) {
                    // Leader path — lease validity is informational; serve either way.
                    // The lease renews on each apply, so a temporarily-expired lease
                    // just means we haven't applied recently (conservative, not wrong).
                    Ok(())
                } else {
                    // Follower path — ReadIndex protocol
                    self.follower_read_index(ctx).await
                }
            },
        }
    }

    /// Follower ReadIndex: ask leader for committed index, wait for local apply.
    ///
    /// 1. Obtain a gRPC channel to the current leader.
    /// 2. Call `ReadIndex` RPC to get the leader's committed index.
    /// 3. Wait for this node's applied index to reach that committed index.
    async fn follower_read_index(&self, ctx: &RegionContext) -> Result<(), Status> {
        use inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient;

        let channel = self.get_leader_channel(ctx)?;
        let mut client = RaftServiceClient::new(channel);

        let response = client
            .read_index(inferadb_ledger_proto::proto::ReadIndexRequest { region: String::new() })
            .await
            .map_err(|e| Status::unavailable(format!("ReadIndex RPC failed: {e}")))?;

        let committed_index = response.into_inner().committed_index;

        let watch = ctx
            .applied_index_rx
            .as_ref()
            .ok_or_else(|| Status::internal("Applied index watch not available for this region"))?;

        inferadb_ledger_raft::wait_for_apply(
            &mut watch.clone(),
            committed_index,
            std::time::Duration::from_secs(5),
        )
        .await
    }

    /// Returns a gRPC channel to the current leader for this region.
    ///
    /// Uses the peer address map to resolve the leader's address and creates a
    /// lazy-connected channel.
    fn get_leader_channel(&self, ctx: &RegionContext) -> Result<tonic::transport::Channel, Status> {
        let handle = &ctx.handle;
        let leader_id = handle.current_leader().ok_or_else(|| {
            super::metadata::not_leader_status_from_handle(
                handle.as_ref(),
                self.peer_addresses.as_ref(),
                "No leader available",
            )
        })?;

        if leader_id == handle.node_id() {
            return Err(Status::internal("get_leader_channel called on leader node"));
        }

        let leader_addr =
            self.peer_addresses.as_ref().and_then(|m| m.get(leader_id)).ok_or_else(|| {
                super::metadata::not_leader_status_from_handle(
                    handle.as_ref(),
                    self.peer_addresses.as_ref(),
                    "Leader address not found in peer registry",
                )
            })?;

        Ok(tonic::transport::Channel::from_shared(format!("http://{leader_addr}"))
            .map_err(|e| Status::internal(format!("invalid leader address: {e}")))?
            .connect_lazy())
    }

    /// Returns a forward client for a remote region.
    async fn get_forward_client(&self, remote: &RemoteRegionInfo) -> Result<ForwardClient, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::unavailable("Request forwarding not configured for this service")
        })?;

        let router =
            manager.router().ok_or_else(|| Status::unavailable("Region router not initialized"))?;

        let connection = router
            .get_connection(
                remote.region,
                &remote.routing.member_nodes,
                remote.routing.leader_hint.as_deref(),
            )
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to remote region: {}", e))
            })?;

        Ok(ForwardClient::new(connection))
    }

    /// Fetches block header from archive for a given vault height.
    ///
    /// Returns None if the block is not found or archive is not available.
    fn get_block_header(
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> Option<inferadb_ledger_proto::proto::BlockHeader> {
        // Height 0 means no blocks yet
        if vault_height == 0 {
            return None;
        }

        // Find the region height containing this vault block
        let region_height =
            archive.find_region_height(organization, vault, vault_height).ok().flatten()?;

        // Read the region block
        let region_block = archive.read_block(region_height).ok()?;

        // Find the vault entry
        let entry = region_block.vault_entries.iter().find(|e| {
            e.organization == organization && e.vault == vault && e.vault_height == vault_height
        })?;

        // Build proto block header
        let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
        Some(inferadb_ledger_proto::proto::BlockHeader {
            height: entry.vault_height,
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: applied_state
                    .resolve_id_to_slug(entry.organization)
                    .map_or(entry.organization.value() as u64, |s| s.value()),
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                slug: applied_state.resolve_vault_id_to_slug(entry.vault).map_or(0, |s| s.value()),
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
                seconds: region_block.timestamp.timestamp(),
                nanos: region_block.timestamp.timestamp_subsec_nanos() as i32,
            }),
            leader_id: Some(inferadb_ledger_proto::proto::NodeId {
                id: region_block.leader_id.to_string(),
            }),
            term: region_block.term,
            committed_index: region_block.committed_index,
            block_hash: Some(inferadb_ledger_proto::proto::Hash { value: block_hash.to_vec() }),
        })
    }

    /// Returns block_hash and state_root for a vault at a given height.
    ///
    /// Returns (block_hash, state_root) or (None, None) if not found.
    fn get_tip_hashes(
        archive: &BlockArchive<FileBackend>,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> (Option<inferadb_ledger_proto::proto::Hash>, Option<inferadb_ledger_proto::proto::Hash>)
    {
        // Find the region height containing this vault block
        let region_height =
            match archive.find_region_height(organization, vault, vault_height).ok().flatten() {
                Some(h) => h,
                None => return (None, None),
            };

        // Read the region block
        let region_block = match archive.read_block(region_height) {
            Ok(block) => block,
            Err(_) => return (None, None),
        };

        // Find the vault entry
        let entry = match region_block.vault_entries.iter().find(|e| {
            e.organization == organization && e.vault == vault && e.vault_height == vault_height
        }) {
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
        vault: VaultId,
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
        for &region_height in snapshots.iter().rev() {
            let snapshot = match snapshot_manager.load(region_height) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Find the vault's height in this snapshot
            if let Some(vault_meta) = snapshot.header.vault_states.iter().find(|v| v.vault == vault)
                && vault_meta.vault_height <= target_height
            {
                // This snapshot is usable - load its entities into temp_state
                if let Some(entities) = snapshot.state.vault_entities.get(&vault) {
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
                        && let Err(e) =
                            temp_state.apply_operations(vault, &operations, vault_meta.vault_height)
                    {
                        debug!("Failed to restore entities from snapshot: {:?}", e);
                        return (1, false);
                    }
                }

                debug!(
                    region_height,
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
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
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
            let header =
                Self::get_block_header(archive, applied_state, organization, vault, height)?;
            headers.push(header);
        }

        Some(inferadb_ledger_proto::proto::ChainProof { headers })
    }

    /// Fetches historical block announcements from the block archive.
    ///
    /// Used by watch_blocks to replay committed blocks before streaming new ones.
    fn fetch_historical_announcements(
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
        start_height: u64,
        end_height: u64,
    ) -> Vec<BlockAnnouncement> {
        use prost_types::Timestamp;

        let mut announcements = Vec::with_capacity((end_height - start_height + 1) as usize);

        for height in start_height..=end_height {
            // Find the region height containing this vault block
            let region_height = match archive.find_region_height(organization, vault, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    debug!(height, "Vault block not found in archive");
                    continue;
                },
                Err(e) => {
                    warn!(height, error = %e, "Error finding region height");
                    continue;
                },
            };

            // Read the region block
            let region_block = match archive.read_block(region_height) {
                Ok(block) => block,
                Err(e) => {
                    warn!(region_height, error = %e, "Error reading region block");
                    continue;
                },
            };

            // Find the vault entry in the region block
            if let Some(entry) = region_block.vault_entries.iter().find(|e| {
                e.organization == organization && e.vault == vault && e.vault_height == height
            }) {
                // Compute vault block hash using the same function as get_tip_hashes
                let block_hash = inferadb_ledger_types::vault_entry_hash(entry);

                announcements.push(BlockAnnouncement {
                    organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                        slug: applied_state
                            .resolve_id_to_slug(entry.organization)
                            .map_or(entry.organization.value() as u64, |s| s.value()),
                    }),
                    vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                        slug: applied_state
                            .resolve_vault_id_to_slug(entry.vault)
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
                        seconds: region_block.timestamp.timestamp(),
                        nanos: region_block.timestamp.timestamp_subsec_nanos() as i32,
                    }),
                });
            }
        }

        announcements
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::read_service_server::ReadService for ReadService {
    /// Reads a single entity or relationship by key.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding: if the organization lives on a remote region,
        // forward the entire RPC rather than returning UNAVAILABLE.
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_read(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "read",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("read");
            return client.forward_read(req, Some(&trace_ctx), deadline).await;
        }

        // Create logging context
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

        // Extract caller identity for canonical log line
        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Set read operation fields
        ctx.set_key(&req.key);

        // Reject system-reserved key prefixes on read path
        validate_read_key(&req.key)?;

        let consistency = match ReadConsistency::try_from(req.consistency)
            .unwrap_or(ReadConsistency::Unspecified)
        {
            ReadConsistency::Linearizable => "linearizable",
            _ => "eventual",
        };
        ctx.set_consistency(consistency);
        ctx.set_include_proof(false);

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        if let Err(e) = self.resolve_read_consistency(&region, req.consistency).await {
            ctx.set_error("consistency_error", e.message());
            return Err(e);
        }
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Check vault health - diverged vaults cannot be read
        let health = region.applied_state.vault_health(organization_id, vault_id);
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
        let state = &*region.state;
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
        metrics::record_organization_operation(organization_id, "read");
        metrics::record_organization_latency(organization_id, "read", elapsed);
        ctx.set_success();

        // Get current block height for this vault
        let block_height = region.applied_state.vault_height(organization_id, vault_id);
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

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_batch_read(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "batch_read",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("batch_read");
            return client.forward_batch_read(req, Some(&trace_ctx), deadline).await;
        }

        // Create logging context
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

        // Extract caller identity for canonical log line
        super::helpers::extract_caller(&mut ctx, &req.caller);

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

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        if let Err(e) = self.resolve_read_consistency(&region, req.consistency).await {
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
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Check vault health - diverged vaults cannot be read
        let health = region.applied_state.vault_health(organization_id, vault_id);
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

        // Reject system-reserved key prefixes on read path
        for key in &req.keys {
            validate_read_key(key)?;
        }

        // Read all keys from state layer
        let state = &*region.state;
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
        metrics::record_organization_operation(organization_id, "read");
        metrics::record_organization_latency(organization_id, "read", latency);

        ctx.set_success();

        // Get current block height for this vault
        let block_height = region.applied_state.vault_height(organization_id, vault_id);
        ctx.set_block_height(block_height);

        Ok(Response::new(BatchReadResponse { results, block_height }))
    }

    /// Reads a single entity or relationship with a cryptographic merkle proof and chain proof.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn verified_read(
        &self,
        request: Request<VerifiedReadRequest>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_verified_read(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "verified_read",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("verified_read");
            return client.forward_verified_read(req, Some(&trace_ctx), deadline).await;
        }

        // Create logging context
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

        // Extract caller identity for canonical log line
        super::helpers::extract_caller(&mut ctx, &req.caller);

        // Set read operation fields
        ctx.set_key(&req.key);

        // Reject system-reserved key prefixes on read path
        validate_read_key(&req.key)?;

        ctx.set_include_proof(true);
        ctx.set_consistency("linearizable"); // verified reads are always linearizable
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Verified reads require linearizable consistency.
        // On the leader, serve directly. On followers, use ReadIndex protocol
        // to wait for local state to catch up to the leader's committed index.
        if let Err(e) =
            self.resolve_read_consistency(&region, ReadConsistency::Linearizable as i32).await
        {
            ctx.set_error("consistency_error", e.message());
            return Err(e);
        }

        // Check vault health - diverged vaults cannot be read
        let health = region.applied_state.vault_health(organization_id, vault_id);
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
        let state = &*region.state;
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
        let block_height = region.applied_state.vault_height(organization_id, vault_id);
        ctx.set_block_height(block_height);

        // Fetch block header from archive
        let block_header = Self::get_block_header(
            &region.block_archive,
            &region.applied_state,
            organization_id,
            vault_id,
            block_height,
        );

        // Note: State verification uses bucket-based hashing (not sparse Merkle tree),
        // so we can't generate individual key inclusion proofs. The state_root in the
        // block header commits to the entire vault state. Clients must trust the
        // state_root or reconstruct the full bucket hash to verify.
        let merkle_proof =
            inferadb_ledger_proto::proto::MerkleProof { leaf_hash: None, siblings: vec![] };

        // Build chain proof if requested
        let chain_proof = if req.include_chain_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            Self::build_chain_proof(
                &region.block_archive,
                &region.applied_state,
                organization_id,
                vault_id,
                trusted_height,
                block_height,
            )
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

    /// Reads entity state at a specific block height from the block archive.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn historical_read(
        &self,
        request: Request<HistoricalReadRequest>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_historical_read(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "historical_read",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("historical_read");
            return client.forward_historical_read(req, Some(&trace_ctx), deadline).await;
        }

        // Create logging context
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

        // Reject system-reserved key prefixes on read path
        validate_read_key(&req.key)?;

        ctx.set_at_height(req.at_height);
        ctx.set_include_proof(req.include_proof);
        ctx.set_consistency("historical");

        // Historical read requires at_height
        if req.at_height == 0 {
            ctx.set_error("invalid_argument", "at_height is required for historical reads");
            return Err(Status::invalid_argument("at_height is required for historical reads"));
        }
        let organization = req.organization.as_ref().map_or(0, |n| n.slug);
        let vault = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization, vault);

        // Get block archive for historical reads
        let archive = &region.block_archive;

        // Check that requested height doesn't exceed current tip
        let tip_height = region.applied_state.vault_height(organization_id, vault_id);
        if req.at_height > tip_height {
            let msg =
                format!("Requested height {} exceeds current tip {}", req.at_height, tip_height);
            ctx.set_error("invalid_argument", &msg);
            return Err(Status::invalid_argument(msg));
        }

        // Start storage timer (covers replay and read)
        ctx.start_storage_timer();

        // Create temporary state layer for replay using temp directory
        let (_temp_dir, temp_state) = match super::helpers::create_replay_context() {
            Ok(ctx_pair) => ctx_pair,
            Err(status) => {
                ctx.end_storage_timer();
                ctx.set_error("internal", status.message());
                return Err(status);
            },
        };

        // Track block timestamp for expiration check
        let mut block_timestamp = chrono::Utc::now();

        // Find the optimal starting point (snapshot or height 1)
        let (start_height, _snapshot_loaded) =
            self.load_nearest_snapshot_for_historical_read(vault_id, req.at_height, &temp_state);

        // Replay blocks from start_height to at_height
        for height in start_height..=req.at_height {
            // Find region height for this vault block
            let region_height = match archive.find_region_height(organization_id, vault_id, height)
            {
                Ok(Some(h)) => h,
                Ok(None) => continue, // Block might not exist at this height (sparse)
                Err(e) => {
                    ctx.end_storage_timer();
                    let msg = format!("Index lookup failed: {:?}", e);
                    ctx.set_error("internal", &msg);
                    return Err(Status::internal(msg));
                },
            };

            // Read the region block
            let region_block = match archive.read_block(region_height) {
                Ok(b) => b,
                Err(e) => {
                    ctx.end_storage_timer();
                    let msg = format!("Block read failed: {:?}", e);
                    ctx.set_error("internal", &msg);
                    return Err(Status::internal(msg));
                },
            };

            // Find the vault entry
            let vault_entry = region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
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
                    block_timestamp = region_block.timestamp;
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
            Self::get_block_header(
                archive,
                &region.applied_state,
                organization_id,
                vault_id,
                req.at_height,
            )
        } else {
            None
        };

        // Build chain proof if requested (requires include_proof to be useful)
        let chain_proof = if req.include_chain_proof && req.include_proof {
            let trusted_height = req.trusted_height.unwrap_or(0);
            Self::build_chain_proof(
                archive,
                &region.applied_state,
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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let resp = client.forward_watch_blocks(req, Some(&trace_ctx), deadline).await?;
                metrics::record_cross_region_forward(
                    "watch_blocks",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return Ok(Response::new(Box::pin(resp.into_inner()) as Self::WatchBlocksStream));
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("watch_blocks");
            let resp = client.forward_watch_blocks(req, Some(&trace_ctx), deadline).await?;
            return Ok(Response::new(Box::pin(resp.into_inner()) as Self::WatchBlocksStream));
        }
        let start_height = req.start_height;

        // Validate start_height >= 1
        if start_height == 0 {
            return Err(Status::invalid_argument(
                "start_height must be >= 1 (use 1 for full replay from genesis)",
            ));
        }

        // Enforce global concurrent stream limit
        let prev = self.active_streams.fetch_add(1, Ordering::Relaxed);
        if prev >= self.max_streams {
            self.active_streams.fetch_sub(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("Maximum concurrent watch streams exceeded"));
        }

        // Get current tip for this vault
        let current_tip = region.applied_state.vault_height(organization_id, vault_id);

        // Subscribe to broadcast BEFORE reading historical blocks
        // This ensures we don't miss any blocks committed between reading history and subscribing
        let announcements = region.block_announcements.as_ref().ok_or_else(|| {
            Status::unavailable("Block announcements not available for this region")
        })?;
        let receiver = announcements.subscribe();

        // Build historical blocks stream if start_height <= current_tip.
        // Cap historical replay to 10,000 blocks to prevent memory exhaustion.
        // If the gap exceeds 10,000, start from (current_tip - 10,000 + 1) and
        // log the skipped range.
        const MAX_HISTORICAL_BLOCKS: u64 = 10_000;
        let historical_blocks: Vec<BlockAnnouncement> = if start_height <= current_tip {
            let effective_start = if current_tip - start_height + 1 > MAX_HISTORICAL_BLOCKS {
                let effective = current_tip.saturating_sub(MAX_HISTORICAL_BLOCKS - 1);
                warn!(
                    requested_start = start_height,
                    effective_start = effective,
                    current_tip,
                    skipped = effective - start_height,
                    "WatchBlocks historical replay exceeds limit, skipping oldest blocks"
                );
                effective
            } else {
                start_height
            };
            Self::fetch_historical_announcements(
                &region.block_archive,
                &region.applied_state,
                organization_id,
                vault_id,
                effective_start,
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

        // Filter by internal IDs and normalize announcement slugs.
        // REGIONAL Raft apply handlers populate announcement org/vault fields from
        // internal-ID-to-slug maps that may be empty in the REGIONAL applied state.
        // We filter by internal ID and then replace the announcement's org/vault
        // slugs with the correct external Snowflake slugs.
        let watch_org_id = organization_id;
        let watch_vault_id = vault_id;
        let org_slug_proto = req.organization;
        let vault_slug_proto = req.vault;
        let broadcast_stream =
            tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |result| {
                let org_slug_proto = org_slug_proto;
                let vault_slug_proto = vault_slug_proto;
                async move {
                    match result {
                        Ok(mut announcement) => {
                            let ann_org = announcement.organization.as_ref().map_or(0, |n| n.slug);
                            let ann_vault = announcement.vault.as_ref().map_or(0, |v| v.slug);

                            // Match on internal ID (REGIONAL) or external slug (GLOBAL).
                            // In REGIONAL mode, announcements carry internal IDs as fallback.
                            if ann_org != watch_org_id.value() as u64 {
                                return None;
                            }
                            if ann_vault != watch_vault_id.value() as u64 {
                                return None;
                            }
                            // Skip blocks we already sent from history
                            if announcement.height <= last_historical_height {
                                return None;
                            }
                            // Normalize: ensure announcement carries external slugs,
                            // not internal IDs (REGIONAL apply handler may set internal IDs)
                            announcement.organization = org_slug_proto;
                            announcement.vault = vault_slug_proto;
                            Some(Ok(announcement))
                        },
                        Err(_) => Some(Err(Status::internal("Stream error"))),
                    }
                }
            });

        // Chain historical blocks followed by broadcast, wrapped in a guard
        // that decrements the active stream counter when the stream ends or
        // the client disconnects.
        let combined = historical_stream.chain(broadcast_stream);
        let guarded =
            StreamGuard { inner: Box::pin(combined), counter: Arc::clone(&self.active_streams) };

        Ok(Response::new(Box::pin(guarded)))
    }

    /// Retrieves a single block by height from the block archive.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_get_block(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "get_block",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("get_block");
            return client.forward_get_block(req, Some(&trace_ctx), deadline).await;
        }

        let archive = &region.block_archive;
        let height = req.height;

        // Find the region height containing this vault block
        let region_height =
            archive.find_region_height(organization_id, vault_id, height).map_err(storage_err)?;

        let region_height = match region_height {
            Some(h) => h,
            None => return Ok(Response::new(GetBlockResponse { block: None })),
        };

        // Read the region block
        let region_block = archive.read_block(region_height).map_err(storage_err)?;

        // Find the vault entry in the region block
        let vault_entry = region_block.vault_entries.iter().find(|e| {
            e.organization == organization_id && e.vault == vault_id && e.vault_height == height
        });

        let vault =
            region.applied_state.resolve_vault_id_to_slug(vault_id).unwrap_or(VaultSlug::new(0));
        let block =
            vault_entry.map(|entry| vault_entry_to_proto_block(entry, &region_block, vault));

        Ok(Response::new(GetBlockResponse { block }))
    }

    /// Retrieves a contiguous range of blocks by start and end height.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_block_range(
        &self,
        request: Request<GetBlockRangeRequest>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_get_block_range(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "get_block_range",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("get_block_range");
            return client.forward_get_block_range(req, Some(&trace_ctx), deadline).await;
        }

        let archive = &region.block_archive;
        let start_height = req.start_height;
        let end_height = req.end_height;

        // Limit range to 1000 blocks
        let max_range = 1000u64;
        let end_height = end_height.min(start_height.saturating_add(max_range - 1));

        let mut blocks = Vec::new();

        // Iterate through the height range
        for height in start_height..=end_height {
            // Find the region height for this vault block
            let region_height = match archive
                .find_region_height(organization_id, vault_id, height)
                .map_err(storage_err)?
            {
                Some(h) => h,
                None => continue, // Block not found, skip
            };

            // Read the region block
            let region_block = archive.read_block(region_height).map_err(storage_err)?;

            // Find the vault entry
            if let Some(entry) = region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
            }) {
                let vault = region
                    .applied_state
                    .resolve_vault_id_to_slug(vault_id)
                    .unwrap_or(VaultSlug::new(0));
                blocks.push(vault_entry_to_proto_block(entry, &region_block, vault));
            }
        }

        // Get current tip for this vault
        let current_tip = region.applied_state.vault_height(organization_id, vault_id);

        Ok(Response::new(GetBlockRangeResponse { blocks, current_tip }))
    }

    /// Returns the latest block height, hash, and state root for a vault.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_tip(
        &self,
        request: Request<GetTipRequest>,
    ) -> Result<Response<GetTipResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_get_tip(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "get_tip",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("get_tip");
            return client.forward_get_tip(req, Some(&trace_ctx), deadline).await;
        }

        let height = if vault_id.value() != 0 {
            // Specific vault requested
            region.applied_state.vault_height(organization_id, vault_id)
        } else if organization_id.value() != 0 {
            // Organization requested - return max height across all vaults in organization
            region.applied_state.org_max_vault_height(organization_id)
        } else {
            // No filter - return max height across all vaults
            region.applied_state.max_vault_height()
        };

        // Get block_hash and state_root from archive
        let (block_hash, state_root) = if vault_id.value() != 0 && height > 0 {
            Self::get_tip_hashes(&region.block_archive, organization_id, vault_id, height)
        } else {
            (None, None)
        };

        Ok(Response::new(GetTipResponse { height, block_hash, state_root }))
    }

    /// Returns the last committed sequence number for a client within a vault.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_client_state(
        &self,
        request: Request<GetClientStateRequest>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_get_client_state(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "get_client_state",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("get_client_state");
            return client.forward_get_client_state(req, Some(&trace_ctx), deadline).await;
        }
        let client_id = req.client_id.as_ref().map(|c| c.id.as_str()).unwrap_or("");

        // Server-assigned sequences: Query the persistent AppliedState directly
        // The idempotency cache no longer tracks sequence numbers by sequence;
        // it uses idempotency keys instead.
        let last_committed_sequence =
            region.applied_state.client_sequence(organization_id, vault_id, client_id);

        Ok(Response::new(GetClientStateResponse { last_committed_sequence }))
    }

    /// Lists relationships in a vault with optional resource/relation/subject filters and
    /// pagination.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_relationships(
        &self,
        request: Request<ListRelationshipsRequest>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result =
                    client.forward_list_relationships(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "list_relationships",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("list_relationships");
            return client.forward_list_relationships(req, Some(&trace_ctx), deadline).await;
        }

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        self.resolve_read_consistency(&region, req.consistency).await?;
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
        let block_height = region.applied_state.vault_height(organization_id, vault_id);

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
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        let state = &*region.state;

        // Determine which method to use based on filters
        let relationships: Vec<inferadb_ledger_proto::proto::Relationship> =
            if let (Some(resource), Some(relation)) = (&req.resource, &req.relation) {
                // Optimized path: use index lookup for resource+relation
                let subjects =
                    state.list_subjects(vault_id, resource, relation).map_err(storage_err)?;

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
                let resources =
                    state.list_resources_for_subject(vault_id, subject).map_err(storage_err)?;

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
                let cursor_rel = resume_key.as_deref().and_then(parse_relationship_cursor);
                let raw_rels = state
                    .list_relationships(vault_id, cursor_rel.as_ref(), limit)
                    .map_err(storage_err)?;

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
                        organization: organization_id,
                        vault: vault_id,
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

    /// Lists distinct resource identifiers in a vault, optionally filtered by resource type.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_resources(
        &self,
        request: Request<ListResourcesRequest>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_list_resources(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "list_resources",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("list_resources");
            return client.forward_list_resources(req, Some(&trace_ctx), deadline).await;
        }

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        self.resolve_read_consistency(&region, req.consistency).await?;
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };

        // Compute query hash from filter parameters for token validation
        let query_params = format!("resource_type:{}", req.resource_type);
        let query_hash = PageTokenCodec::compute_query_hash(query_params.as_bytes());

        // Get current block height for consistent pagination
        let block_height = region.applied_state.vault_height(organization_id, vault_id);

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
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        // List relationships and extract unique resources matching the type prefix
        let state = &*region.state;
        let cursor_rel = resume_key.as_deref().and_then(parse_relationship_cursor);
        let relationships = state
            .list_relationships(vault_id, cursor_rel.as_ref(), limit * 10) // Over-fetch to filter
            .map_err(storage_err)?;

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
                        organization: organization_id,
                        vault: vault_id,
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

    /// Lists entities in a vault with optional key-prefix filter and pagination.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Cross-region forwarding
        if self.resolver.supports_forwarding() {
            let system = self.resolver.system_region()?;
            let organization_id =
                SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;
            if let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
            {
                let source_region =
                    self.manager.as_ref().map(|m| m.local_region().as_str()).unwrap_or("unknown");
                let forward_start = std::time::Instant::now();
                let deadline =
                    inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
                let mut client = self.get_forward_client(&remote).await?;
                let result = client.forward_list_entities(req, Some(&trace_ctx), deadline).await;
                metrics::record_cross_region_forward(
                    "list_entities",
                    source_region,
                    remote.region.as_str(),
                    forward_start.elapsed().as_secs_f64(),
                );
                return result;
            }
        }

        // Resolve region first for region-aware forwarding and consistency checks
        let (organization_id, vault_id, region) =
            self.resolve_org_vault_consistent(&req.organization, &req.vault).await?;

        // Forward to leader if this follower is lagging behind
        if let Some(mut client) = self.should_forward_to_leader(&region)? {
            let deadline =
                inferadb_ledger_raft::deadline::extract_deadline_from_metadata(&grpc_metadata);
            metrics::record_read_forward("list_entities");
            return client.forward_list_entities(req, Some(&trace_ctx), deadline).await;
        }

        // Check consistency requirements (may execute ReadIndex protocol on followers)
        self.resolve_read_consistency(&region, req.consistency).await?;

        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let prefix = if req.key_prefix.is_empty() { None } else { Some(req.key_prefix.as_str()) };

        // Reject system-reserved key prefixes on list path
        if let Some(p) = prefix
            && p.starts_with('_')
        {
            return Err(Status::invalid_argument(
                "key_prefix must not start with '_' (reserved for system keys)",
            ));
        }

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
            region.applied_state.vault_height(organization_id, vault_id)
        } else {
            // Organization-level entities - use max height across all vaults in organization
            region.applied_state.org_max_vault_height(organization_id)
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
                .validate_context(&token, organization_id, vault_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            (Some(String::from_utf8_lossy(&token.last_key).to_string()), token.at_height)
        };

        // Get entities from state layer
        let state = &*region.state;
        let raw_entities = state
            .list_entities(vault_id, prefix, resume_key.as_deref(), limit + 1)
            .map_err(storage_err)?;

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
                        organization: organization_id,
                        vault: vault_id,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn validate_read_key_accepts_normal_key() {
        assert!(validate_read_key("user:1").is_ok());
    }

    #[test]
    fn validate_read_key_accepts_empty_key() {
        // Empty keys are allowed by the read validator (other validation catches this)
        assert!(validate_read_key("").is_ok());
    }

    #[test]
    fn validate_read_key_rejects_system_prefix() {
        let result = validate_read_key("_meta:seq");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("reserved for system keys"));
    }

    #[test]
    fn validate_read_key_rejects_underscore_only() {
        let result = validate_read_key("_");
        assert!(result.is_err());
    }

    #[test]
    fn validate_read_key_rejects_various_system_prefixes() {
        for prefix in ["_dir:", "_idx:", "_meta:", "_shred:", "_tmp:", "_audit:"] {
            let result = validate_read_key(prefix);
            assert!(result.is_err(), "Should reject key with prefix {prefix}");
        }
    }

    #[test]
    fn validate_read_key_accepts_key_with_underscore_not_at_start() {
        assert!(validate_read_key("user_name:1").is_ok());
        assert!(validate_read_key("a_b").is_ok());
    }
}
