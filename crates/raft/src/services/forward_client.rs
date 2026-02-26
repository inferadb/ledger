//! gRPC client for forwarding requests to remote shards.
//!
//! When a organization is assigned to a shard on a different node, requests
//! must be forwarded via gRPC. This module provides the client infrastructure
//! for transparent request forwarding.
//!
//! ## Usage
//!
//! ```no_run
//! # use std::net::SocketAddr;
//! # use inferadb_ledger_raft::services::ForwardClient;
//! # use inferadb_ledger_raft::shard_router::ShardConnection;
//! # use inferadb_ledger_types::ShardId;
//! # use inferadb_ledger_proto::proto::ReadRequest;
//! # use tonic::transport::Channel;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let channel = Channel::from_static("http://[::1]:50051").connect_lazy();
//! # let addr: SocketAddr = "[::1]:50051".parse()?;
//! # let connection = ShardConnection {
//! #     shard_id: ShardId::new(1), channel, address: addr, is_leader: true,
//! # };
//! let mut client = ForwardClient::new(connection);
//!
//! // Forward a read request
//! let request = ReadRequest::default();
//! let response = client.forward_read(request, None, None).await?;
//! # Ok(())
//! # }
//! ```

use std::{sync::Arc, time::Duration};

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
use inferadb_ledger_types::ShardId;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{debug, warn};

use crate::{
    shard_router::ShardConnection,
    trace_context::{self, TraceContext},
};

/// Default timeout for forwarded requests.
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

/// Client for forwarding requests to remote shards.
///
/// Wraps gRPC clients for ReadService and WriteService to forward
/// requests transparently to the correct shard.
pub struct ForwardClient {
    read_client: ReadServiceClient<Channel>,
    write_client: WriteServiceClient<Channel>,
    shard_id: ShardId,
}

impl ForwardClient {
    /// Creates a new forward client from a shard connection.
    pub fn new(connection: ShardConnection) -> Self {
        let channel = connection.channel;
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            shard_id: connection.shard_id,
        }
    }

    /// Creates a new forward client from a channel directly.
    pub fn from_channel(channel: Channel, shard_id: ShardId) -> Self {
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            shard_id,
        }
    }

    /// Returns the shard ID this client forwards to.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Creates a gRPC request with trace context and deadline propagation.
    ///
    /// Injects W3C Trace Context headers (`traceparent`, `tracestate`) into
    /// the outgoing request metadata, enabling distributed trace continuity
    /// across shard boundaries.
    ///
    /// If a gRPC deadline is provided (extracted from the original incoming
    /// request), it is propagated as the forwarded request's timeout. Otherwise
    /// the default forward timeout is used.
    fn make_request<T>(
        &self,
        message: T,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Request<T> {
        let mut req = Request::new(message);
        let timeout = crate::deadline::forwarding_timeout(grpc_deadline, DEFAULT_FORWARD_TIMEOUT);
        req.set_timeout(timeout);
        if let Some(ctx) = trace_ctx {
            let child = ctx.child();
            trace_context::inject_into_metadata(req.metadata_mut(), &child);
        }
        req
    }

    // ========================================================================
    // Read Service Forwarding
    // ========================================================================

    /// Forwards a Read request to the remote shard.
    pub async fn forward_read(
        &mut self,
        request: ReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ReadResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding read request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward read failed");
            e
        })
    }

    /// Forwards a VerifiedRead request to the remote shard.
    pub async fn forward_verified_read(
        &mut self,
        request: VerifiedReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding verified_read request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.verified_read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward verified_read failed");
            e
        })
    }

    /// Forwards a HistoricalRead request to the remote shard.
    pub async fn forward_historical_read(
        &mut self,
        request: HistoricalReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding historical_read request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.historical_read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward historical_read failed");
            e
        })
    }

    /// Forwards a WatchBlocks request to the remote shard.
    ///
    /// Note: This returns a streaming response that must be handled appropriately.
    pub async fn forward_watch_blocks(
        &mut self,
        request: WatchBlocksRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<tonic::Streaming<BlockAnnouncement>>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding watch_blocks request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.watch_blocks(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward watch_blocks failed");
            e
        })
    }

    /// Forwards a GetBlock request to the remote shard.
    pub async fn forward_get_block(
        &mut self,
        request: GetBlockRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding get_block request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.get_block(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward get_block failed");
            e
        })
    }

    /// Forwards a GetBlockRange request to the remote shard.
    pub async fn forward_get_block_range(
        &mut self,
        request: GetBlockRangeRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding get_block_range request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.get_block_range(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward get_block_range failed");
            e
        })
    }

    /// Forwards a GetTip request to the remote shard.
    pub async fn forward_get_tip(
        &mut self,
        request: GetTipRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetTipResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding get_tip request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.get_tip(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward get_tip failed");
            e
        })
    }

    /// Forwards a GetClientState request to the remote shard.
    pub async fn forward_get_client_state(
        &mut self,
        request: GetClientStateRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding get_client_state request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.get_client_state(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward get_client_state failed");
            e
        })
    }

    /// Forwards a ListRelationships request to the remote shard.
    pub async fn forward_list_relationships(
        &mut self,
        request: ListRelationshipsRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding list_relationships request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.list_relationships(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward list_relationships failed");
            e
        })
    }

    /// Forwards a ListResources request to the remote shard.
    pub async fn forward_list_resources(
        &mut self,
        request: ListResourcesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding list_resources request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.list_resources(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward list_resources failed");
            e
        })
    }

    /// Forwards a ListEntities request to the remote shard.
    pub async fn forward_list_entities(
        &mut self,
        request: ListEntitiesRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding list_entities request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.list_entities(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward list_entities failed");
            e
        })
    }

    /// Forwards a BatchRead request to the remote shard.
    pub async fn forward_batch_read(
        &mut self,
        request: BatchReadRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchReadResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding batch_read request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.read_client.batch_read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward batch_read failed");
            e
        })
    }

    // ========================================================================
    // Write Service Forwarding
    // ========================================================================

    /// Forwards a Write request to the remote shard.
    pub async fn forward_write(
        &mut self,
        request: WriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<WriteResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding write request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.write_client.write(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward write failed");
            e
        })
    }

    /// Forwards a BatchWrite request to the remote shard.
    pub async fn forward_batch_write(
        &mut self,
        request: BatchWriteRequest,
        trace_ctx: Option<&TraceContext>,
        grpc_deadline: Option<Duration>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        debug!(shard_id = self.shard_id.value(), "Forwarding batch_write request");
        let req = self.make_request(request, trace_ctx, grpc_deadline);
        self.write_client.batch_write(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id.value(), error = %e, "Forward batch_write failed");
            e
        })
    }
}

/// Cached gRPC channel to the current Raft leader.
///
/// Avoids creating a new TCP+HTTP/2 connection per forwarded request.
/// The cache is invalidated when the leader changes (detected via node ID comparison).
/// Uses `connect_lazy()` so channel creation is synchronous — the actual TCP handshake
/// is deferred until the first RPC call.
#[derive(Clone)]
pub struct LeaderChannelCache {
    inner: Arc<parking_lot::Mutex<Option<(u64, Channel)>>>,
}

impl Default for LeaderChannelCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaderChannelCache {
    /// Creates a new empty leader channel cache.
    pub fn new() -> Self {
        Self { inner: Arc::new(parking_lot::Mutex::new(None)) }
    }

    /// Returns a `ForwardClient` to the current leader, or `Ok(None)` if this node is the leader.
    ///
    /// Uses a cached channel if the leader hasn't changed; creates a new `connect_lazy()`
    /// channel otherwise.
    pub fn get_or_connect<NI>(
        &self,
        current_leader: Option<u64>,
        this_node: u64,
        nodes: NI,
    ) -> Result<Option<ForwardClient>, Status>
    where
        NI: Iterator<Item = (u64, String)>,
    {
        // If this node is the leader, serve locally
        let leader_id = match current_leader {
            Some(id) if id == this_node => return Ok(None),
            Some(id) => id,
            None => {
                return Err(Status::unavailable("No leader available for forwarding"));
            },
        };

        // Check cache
        {
            let cache = self.inner.lock();
            if let Some((cached_id, ref channel)) = *cache
                && cached_id == leader_id
            {
                return Ok(Some(ForwardClient::from_channel(channel.clone(), ShardId::new(0))));
            }
        }

        // Cache miss — find leader address and create a lazy channel
        let leader_addr = nodes
            .into_iter()
            .find(|(id, _)| *id == leader_id)
            .map(|(_, addr)| addr)
            .ok_or_else(|| Status::unavailable("Leader address not found in membership"))?;

        let endpoint = format!("http://{}", leader_addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| Status::unavailable(format!("Invalid leader endpoint: {e}")))?
            .connect_lazy();

        debug!(leader_id, %leader_addr, "Cached new leader channel");

        let client = ForwardClient::from_channel(channel.clone(), ShardId::new(0));
        {
            let mut cache = self.inner.lock();
            *cache = Some((leader_id, channel));
        }

        Ok(Some(client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_forward_client_creation() {
        // Basic struct test - full testing requires gRPC setup
    }

    #[test]
    fn test_leader_channel_cache_default() {
        let cache = LeaderChannelCache::default();
        // This node is the leader — should return None
        let result = cache.get_or_connect(Some(1), 1, std::iter::empty());
        assert!(result.is_ok());
        assert!(result.as_ref().is_ok_and(|o| o.is_none()));
    }

    #[test]
    fn test_leader_channel_cache_no_leader() {
        let cache = LeaderChannelCache::new();
        let result = cache.get_or_connect(None, 1, std::iter::empty());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_leader_channel_cache_creates_channel() {
        let cache = LeaderChannelCache::new();
        let nodes = vec![(2, "127.0.0.1:50051".to_string())];
        let result = cache.get_or_connect(Some(2), 1, nodes.into_iter());
        assert!(result.is_ok());
        assert!(result.as_ref().is_ok_and(|o| o.is_some()));
    }

    #[tokio::test]
    async fn test_leader_channel_cache_reuses_channel() {
        let cache = LeaderChannelCache::new();
        let nodes = vec![(2, "127.0.0.1:50051".to_string())];

        // First call creates
        let _ = cache.get_or_connect(Some(2), 1, nodes.clone().into_iter());
        // Second call should reuse (no nodes needed since cached)
        let result = cache.get_or_connect(Some(2), 1, std::iter::empty());
        assert!(result.is_ok());
        assert!(result.as_ref().is_ok_and(|o| o.is_some()));
    }

    #[tokio::test]
    async fn test_leader_channel_cache_invalidates_on_leader_change() {
        let cache = LeaderChannelCache::new();
        let nodes = vec![(2, "127.0.0.1:50051".to_string())];
        let _ = cache.get_or_connect(Some(2), 1, nodes.into_iter());

        // Leader changed to 3 — cache miss, but node 3 not in membership
        let result = cache.get_or_connect(Some(3), 1, std::iter::empty());
        assert!(result.is_err()); // No address for node 3
    }
}
