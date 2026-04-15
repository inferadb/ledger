//! gRPC clients for forwarding requests across Raft leadership and region
//! boundaries.
//!
//! Two distinct scenarios share the same underlying gRPC mechanics but differ
//! structurally:
//!
//! 1. **Same-node leader forwarding** — a follower receives a write (or a linearizable read) and
//!    must forward it to the current leader for the region it already serves. Use
//!    [`LeaderForwardClient`], constructed via `current_leader_client`, which consults the local
//!    Raft state + the node-level [`NodeConnectionRegistry`].
//!
//! 2. **Cross-region routing** — the organization resolves to a region served by a different node.
//!    Use [`RegionForwardClient`], constructed from a [`RegionConnection`] handed out by the region
//!    router.
//!
//! Both types expose the same set of `forward_*` methods and delegate to
//! shared free-function implementations so request building, trace-context
//! injection, and deadline propagation stay identical.
//!
//! ## Usage
//!
//! ```no_run
//! # use inferadb_ledger_services::services::RegionForwardClient;
//! # use inferadb_ledger_raft::region_router::RegionConnection;
//! # use inferadb_ledger_types::Region;
//! # use inferadb_ledger_proto::proto::ReadRequest;
//! # use tonic::transport::Channel;
//! # use std::net::SocketAddr;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let channel = Channel::from_static("http://[::1]:50051").connect_lazy();
//! # let addr: SocketAddr = "[::1]:50051".parse()?;
//! # let connection = RegionConnection {
//! #     region: Region::US_EAST_VA, channel, address: addr, is_leader: true,
//! # };
//! let mut client = RegionForwardClient::new(connection);
//! let request = ReadRequest::default();
//! let response = client.forward_read(request, None, None).await?;
//! # Ok(())
//! # }
//! ```

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use inferadb_ledger_proto::proto::{
    BatchReadRequest, BatchReadResponse, BatchWriteRequest, BatchWriteResponse, BlockAnnouncement,
    GetBlockRangeRequest, GetBlockRangeResponse, GetBlockRequest, GetBlockResponse,
    GetClientStateRequest, GetClientStateResponse, GetTipRequest, GetTipResponse,
    HistoricalReadRequest, HistoricalReadResponse, ListEntitiesRequest, ListEntitiesResponse,
    ListRelationshipsRequest, ListRelationshipsResponse, ListResourcesRequest,
    ListResourcesResponse, ReadRequest, ReadResponse, VerifiedReadRequest, VerifiedReadResponse,
    WatchBlocksRequest, WriteRequest, WriteResponse, read_service_client::ReadServiceClient,
    write_service_client::WriteServiceClient,
};
use inferadb_ledger_raft::{
    node_registry::NodeConnectionRegistry,
    region_router::RegionConnection,
    trace_context::{self, TraceContext},
};
use inferadb_ledger_types::Region;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{debug, warn};

/// Default timeout for forwarded requests.
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

// ============================================================================
// Shared request-building and RPC dispatch helpers
// ============================================================================

/// Builds a tonic [`Request`] with trace-context injection and deadline
/// propagation. Shared by both forward-client types.
fn build_request<T>(
    message: T,
    trace_ctx: Option<&TraceContext>,
    grpc_deadline: Option<Duration>,
) -> Request<T> {
    let mut req = Request::new(message);
    let timeout =
        inferadb_ledger_raft::deadline::forwarding_timeout(grpc_deadline, DEFAULT_FORWARD_TIMEOUT);
    req.set_timeout(timeout);
    if let Some(ctx) = trace_ctx {
        let child = ctx.child();
        trace_context::inject_into_metadata(req.metadata_mut(), &child);
    }
    req
}

macro_rules! forward_impl {
    ($name:ident, $client_ty:ident, $method:ident, $req_ty:ty, $resp_ty:ty) => {
        async fn $name(
            client: &mut $client_ty<Channel>,
            region: Region,
            request: $req_ty,
            trace_ctx: Option<&TraceContext>,
            grpc_deadline: Option<Duration>,
        ) -> Result<Response<$resp_ty>, Status> {
            debug!(region = region.as_str(), concat!("Forwarding ", stringify!($method), " request"));
            let req = build_request(request, trace_ctx, grpc_deadline);
            client.$method(req).await.map_err(|e| {
                warn!(
                    region = region.as_str(),
                    error = %e,
                    concat!("Forward ", stringify!($method), " failed")
                );
                e
            })
        }
    };
}

forward_impl!(forward_read_impl, ReadServiceClient, read, ReadRequest, ReadResponse);
forward_impl!(
    forward_verified_read_impl,
    ReadServiceClient,
    verified_read,
    VerifiedReadRequest,
    VerifiedReadResponse
);
forward_impl!(
    forward_historical_read_impl,
    ReadServiceClient,
    historical_read,
    HistoricalReadRequest,
    HistoricalReadResponse
);
forward_impl!(
    forward_watch_blocks_impl,
    ReadServiceClient,
    watch_blocks,
    WatchBlocksRequest,
    tonic::Streaming<BlockAnnouncement>
);
forward_impl!(
    forward_get_block_impl,
    ReadServiceClient,
    get_block,
    GetBlockRequest,
    GetBlockResponse
);
forward_impl!(
    forward_get_block_range_impl,
    ReadServiceClient,
    get_block_range,
    GetBlockRangeRequest,
    GetBlockRangeResponse
);
forward_impl!(forward_get_tip_impl, ReadServiceClient, get_tip, GetTipRequest, GetTipResponse);
forward_impl!(
    forward_get_client_state_impl,
    ReadServiceClient,
    get_client_state,
    GetClientStateRequest,
    GetClientStateResponse
);
forward_impl!(
    forward_list_relationships_impl,
    ReadServiceClient,
    list_relationships,
    ListRelationshipsRequest,
    ListRelationshipsResponse
);
forward_impl!(
    forward_list_resources_impl,
    ReadServiceClient,
    list_resources,
    ListResourcesRequest,
    ListResourcesResponse
);
forward_impl!(
    forward_list_entities_impl,
    ReadServiceClient,
    list_entities,
    ListEntitiesRequest,
    ListEntitiesResponse
);
forward_impl!(
    forward_batch_read_impl,
    ReadServiceClient,
    batch_read,
    BatchReadRequest,
    BatchReadResponse
);
forward_impl!(forward_write_impl, WriteServiceClient, write, WriteRequest, WriteResponse);
forward_impl!(
    forward_batch_write_impl,
    WriteServiceClient,
    batch_write,
    BatchWriteRequest,
    BatchWriteResponse
);

// ============================================================================
// LeaderForwardClient — same-node leader forwarding
// ============================================================================

/// Forwards requests to the current Raft leader for a region.
///
/// Constructed via `current_leader_client` once per forwarding decision —
/// the caller has already observed leader identity via the local Raft
/// handle and resolved the peer through the [`NodeConnectionRegistry`].
/// This type holds the resolved [`Arc<PeerConnection>`][peer] so successive
/// `forward_*` calls reuse the shared HTTP/2 channel owned by the registry.
///
/// [peer]: inferadb_ledger_raft::node_registry::PeerConnection
pub struct LeaderForwardClient {
    read_client: ReadServiceClient<Channel>,
    write_client: WriteServiceClient<Channel>,
    region: Region,
}

impl LeaderForwardClient {
    fn from_channel(channel: Channel, region: Region) -> Self {
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            region,
        }
    }

    /// Returns the region ID this client forwards to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Forwards a Read request to the leader.
    pub async fn forward_read(
        &mut self,
        request: ReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ReadResponse>, Status> {
        forward_read_impl(&mut self.read_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a VerifiedRead request to the leader.
    pub async fn forward_verified_read(
        &mut self,
        request: VerifiedReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        forward_verified_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a HistoricalRead request to the leader.
    pub async fn forward_historical_read(
        &mut self,
        request: HistoricalReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        forward_historical_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a WatchBlocks request to the leader.
    pub async fn forward_watch_blocks(
        &mut self,
        request: WatchBlocksRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<tonic::Streaming<BlockAnnouncement>>, Status> {
        forward_watch_blocks_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetBlock request to the leader.
    pub async fn forward_get_block(
        &mut self,
        request: GetBlockRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        forward_get_block_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetBlockRange request to the leader.
    pub async fn forward_get_block_range(
        &mut self,
        request: GetBlockRangeRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        forward_get_block_range_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetTip request to the leader.
    pub async fn forward_get_tip(
        &mut self,
        request: GetTipRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetTipResponse>, Status> {
        forward_get_tip_impl(&mut self.read_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a GetClientState request to the leader.
    pub async fn forward_get_client_state(
        &mut self,
        request: GetClientStateRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        forward_get_client_state_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListRelationships request to the leader.
    pub async fn forward_list_relationships(
        &mut self,
        request: ListRelationshipsRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        forward_list_relationships_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListResources request to the leader.
    pub async fn forward_list_resources(
        &mut self,
        request: ListResourcesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        forward_list_resources_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListEntities request to the leader.
    pub async fn forward_list_entities(
        &mut self,
        request: ListEntitiesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        forward_list_entities_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a BatchRead request to the leader.
    pub async fn forward_batch_read(
        &mut self,
        request: BatchReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchReadResponse>, Status> {
        forward_batch_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a Write request to the leader.
    pub async fn forward_write(
        &mut self,
        request: WriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<WriteResponse>, Status> {
        forward_write_impl(&mut self.write_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a BatchWrite request to the leader.
    pub async fn forward_batch_write(
        &mut self,
        request: BatchWriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        forward_batch_write_impl(
            &mut self.write_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }
}

// ============================================================================
// RegionForwardClient — cross-region routing
// ============================================================================

/// Forwards requests to a remote region's gateway.
///
/// Constructed from a [`RegionConnection`] supplied by the region router.
/// The underlying [`Channel`] ultimately originates in the node-level
/// registry (the router resolves member nodes through the same registry
/// path), so channel reuse is preserved across subsystems.
pub struct RegionForwardClient {
    read_client: ReadServiceClient<Channel>,
    write_client: WriteServiceClient<Channel>,
    region: Region,
}

impl RegionForwardClient {
    /// Creates a new cross-region forward client from a region connection.
    pub fn new(connection: RegionConnection) -> Self {
        let channel = connection.channel;
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            region: connection.region,
        }
    }

    /// Returns the region ID this client forwards to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Forwards a Read request to the remote region.
    pub async fn forward_read(
        &mut self,
        request: ReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ReadResponse>, Status> {
        forward_read_impl(&mut self.read_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a VerifiedRead request to the remote region.
    pub async fn forward_verified_read(
        &mut self,
        request: VerifiedReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        forward_verified_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a HistoricalRead request to the remote region.
    pub async fn forward_historical_read(
        &mut self,
        request: HistoricalReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        forward_historical_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a WatchBlocks request to the remote region.
    pub async fn forward_watch_blocks(
        &mut self,
        request: WatchBlocksRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<tonic::Streaming<BlockAnnouncement>>, Status> {
        forward_watch_blocks_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetBlock request to the remote region.
    pub async fn forward_get_block(
        &mut self,
        request: GetBlockRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        forward_get_block_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetBlockRange request to the remote region.
    pub async fn forward_get_block_range(
        &mut self,
        request: GetBlockRangeRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        forward_get_block_range_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a GetTip request to the remote region.
    pub async fn forward_get_tip(
        &mut self,
        request: GetTipRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetTipResponse>, Status> {
        forward_get_tip_impl(&mut self.read_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a GetClientState request to the remote region.
    pub async fn forward_get_client_state(
        &mut self,
        request: GetClientStateRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        forward_get_client_state_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListRelationships request to the remote region.
    pub async fn forward_list_relationships(
        &mut self,
        request: ListRelationshipsRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        forward_list_relationships_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListResources request to the remote region.
    pub async fn forward_list_resources(
        &mut self,
        request: ListResourcesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        forward_list_resources_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a ListEntities request to the remote region.
    pub async fn forward_list_entities(
        &mut self,
        request: ListEntitiesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        forward_list_entities_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a BatchRead request to the remote region.
    pub async fn forward_batch_read(
        &mut self,
        request: BatchReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchReadResponse>, Status> {
        forward_batch_read_impl(
            &mut self.read_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }

    /// Forwards a Write request to the remote region.
    pub async fn forward_write(
        &mut self,
        request: WriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<WriteResponse>, Status> {
        forward_write_impl(&mut self.write_client, self.region, request, trace_ctx, grpc_deadline)
            .await
    }

    /// Forwards a BatchWrite request to the remote region.
    pub async fn forward_batch_write(
        &mut self,
        request: BatchWriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        forward_batch_write_impl(
            &mut self.write_client,
            self.region,
            request,
            trace_ctx,
            grpc_deadline,
        )
        .await
    }
}

// ============================================================================
// Leader identity cache + resolver
// ============================================================================

/// Tracks the current known leader node ID for a region.
///
/// Replaces the old `LeaderChannelCache` — the channel itself is owned by
/// the [`NodeConnectionRegistry`] on the node level. This cache tracks only
/// leader identity (an [`AtomicU64`]), which means leader flaps are cheap
/// (no channel teardown) and observable via the `update` metric hook.
///
/// Sentinel: `0` means "leader unknown". Valid node IDs are `>= 1`.
#[derive(Debug, Default)]
pub struct LeaderIdCache {
    leader_id: AtomicU64,
}

impl LeaderIdCache {
    /// Creates a new empty leader id cache.
    pub fn new() -> Self {
        Self { leader_id: AtomicU64::new(0) }
    }

    /// Returns the cached leader node ID, or `None` if unknown.
    pub fn current(&self) -> Option<u64> {
        let id = self.leader_id.load(Ordering::Acquire);
        if id == 0 { None } else { Some(id) }
    }

    /// Updates the cached leader. Pass `0` to clear.
    pub fn update(&self, id: u64) {
        self.leader_id.store(id, Ordering::Release);
    }
}

/// Resolves a [`LeaderForwardClient`] pointing at the current Raft leader.
///
/// Returns `Ok(None)` when this node IS the leader (serve locally). Returns
/// `Ok(Some(client))` when forwarding is required. Returns `Err(Status)` when
/// no leader is known or the leader's address cannot be resolved.
///
/// The channel is fetched from the node-level [`NodeConnectionRegistry`], so
/// concurrent callers and multiple service instances share a single HTTP/2
/// connection per peer. The `cache` parameter records the observed leader
/// identity for observability; it does not gate channel creation.
pub async fn current_leader_client<NI>(
    cache: &LeaderIdCache,
    registry: &NodeConnectionRegistry,
    current_leader: Option<u64>,
    this_node: u64,
    nodes: NI,
    region: Region,
) -> Result<Option<LeaderForwardClient>, Status>
where
    NI: IntoIterator<Item = (u64, String)>,
{
    let leader_id = match current_leader {
        Some(id) if id == this_node => return Ok(None),
        Some(id) => id,
        None => {
            return Err(super::metadata::status_with_not_leader_hint(
                "No leader available for forwarding",
                None,
                None,
                None,
            ));
        },
    };

    cache.update(leader_id);

    let leader_addr =
        nodes.into_iter().find(|(id, _)| *id == leader_id).map(|(_, addr)| addr).ok_or_else(
            || {
                super::metadata::status_with_not_leader_hint(
                    "Leader address not found in membership",
                    Some(leader_id),
                    None,
                    None,
                )
            },
        )?;

    let peer = registry.get_or_register(leader_id, &leader_addr).await.map_err(|e| {
        super::metadata::status_with_not_leader_hint(
            format!("Failed to register leader peer: {e}"),
            Some(leader_id),
            Some(&leader_addr),
            None,
        )
    })?;

    debug!(leader_id, %leader_addr, "Resolved leader channel via registry");

    Ok(Some(LeaderForwardClient::from_channel(peer.channel(), region)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn leader_id_cache_starts_empty() {
        let cache = LeaderIdCache::default();
        assert!(cache.current().is_none());
    }

    #[test]
    fn leader_id_cache_update_and_clear() {
        let cache = LeaderIdCache::new();
        cache.update(42);
        assert_eq!(cache.current(), Some(42));
        cache.update(0);
        assert!(cache.current().is_none());
    }

    #[tokio::test]
    async fn current_leader_client_returns_none_when_self_is_leader() {
        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let result = current_leader_client(
            &cache,
            &registry,
            Some(1),
            1,
            Vec::<(u64, String)>::new(),
            Region::GLOBAL,
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn current_leader_client_errors_when_no_leader() {
        use inferadb_ledger_proto::proto;
        use prost::Message;

        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let status = match current_leader_client(
            &cache,
            &registry,
            None,
            1,
            Vec::<(u64, String)>::new(),
            Region::GLOBAL,
        )
        .await
        {
            Err(s) => s,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert!(details.is_retryable);
        assert!(!details.context.contains_key("leader_id"));
        assert!(!details.context.contains_key("leader_endpoint"));
    }

    #[tokio::test]
    async fn current_leader_client_errors_when_address_missing() {
        use inferadb_ledger_proto::proto;
        use prost::Message;

        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let status = match current_leader_client(
            &cache,
            &registry,
            Some(2),
            1,
            Vec::<(u64, String)>::new(),
            Region::GLOBAL,
        )
        .await
        {
            Err(s) => s,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert_eq!(details.context.get("leader_id").unwrap(), "2");
        assert!(!details.context.contains_key("leader_endpoint"));
    }

    #[tokio::test]
    async fn current_leader_client_errors_on_invalid_address() {
        use inferadb_ledger_proto::proto;
        use prost::Message;

        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let nodes = vec![(2u64, "bad\u{0}host:5000".to_string())];
        let status =
            match current_leader_client(&cache, &registry, Some(2), 1, nodes, Region::GLOBAL).await
            {
                Err(s) => s,
                Ok(_) => panic!("expected error"),
            };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let details = proto::ErrorDetails::decode(status.details()).unwrap();
        assert_eq!(details.context.get("leader_id").unwrap(), "2");
        assert_eq!(details.context.get("leader_endpoint").unwrap(), "bad\u{0}host:5000");
    }

    #[tokio::test]
    async fn current_leader_client_builds_client_and_caches_id() {
        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let nodes = vec![(2u64, "127.0.0.1:50051".to_string())];
        let result =
            current_leader_client(&cache, &registry, Some(2), 1, nodes, Region::GLOBAL).await;
        assert!(result.is_ok());
        let client = result.unwrap();
        assert!(client.is_some());
        assert_eq!(client.unwrap().region(), Region::GLOBAL);
        assert_eq!(cache.current(), Some(2));
    }

    #[tokio::test]
    async fn current_leader_client_reuses_registry_peer_across_calls() {
        let cache = LeaderIdCache::new();
        let registry = Arc::new(NodeConnectionRegistry::new());
        let nodes = vec![(2u64, "127.0.0.1:50051".to_string())];

        let _ = current_leader_client(&cache, &registry, Some(2), 1, nodes.clone(), Region::GLOBAL)
            .await
            .unwrap();
        let peer_a = registry.get(2).unwrap();

        let _ = current_leader_client(&cache, &registry, Some(2), 1, nodes, Region::GLOBAL)
            .await
            .unwrap();
        let peer_b = registry.get(2).unwrap();

        // Same Arc<PeerConnection> — registry coalesced both lookups.
        assert!(Arc::ptr_eq(&peer_a, &peer_b));
    }

    #[tokio::test]
    async fn current_leader_client_updates_cache_on_leader_change() {
        let cache = LeaderIdCache::new();
        let registry = NodeConnectionRegistry::new();
        let nodes_a = vec![(2u64, "127.0.0.1:50051".to_string())];
        let _ = current_leader_client(&cache, &registry, Some(2), 1, nodes_a, Region::GLOBAL)
            .await
            .unwrap();
        assert_eq!(cache.current(), Some(2));

        let nodes_b = vec![(3u64, "127.0.0.1:50052".to_string())];
        let _ = current_leader_client(&cache, &registry, Some(3), 1, nodes_b, Region::GLOBAL)
            .await
            .unwrap();
        assert_eq!(cache.current(), Some(3));
    }
}
