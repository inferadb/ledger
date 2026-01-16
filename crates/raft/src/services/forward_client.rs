//! gRPC client for forwarding requests to remote shards.
//!
//! When a namespace is assigned to a shard on a different node, requests
//! must be forwarded via gRPC. This module provides the client infrastructure
//! for transparent request forwarding.
//!
//! ## Usage
//!
//! ```ignore
//! let client = ForwardClient::new(connection).await?;
//!
//! // Forward a read request
//! let response = client.forward_read(request).await?;
//!
//! // Forward a write request
//! let response = client.forward_write(request).await?;
//! ```

use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::proto::read_service_client::ReadServiceClient;
use crate::proto::write_service_client::WriteServiceClient;
use crate::proto::{
    BatchWriteRequest, BatchWriteResponse, BlockAnnouncement, GetBlockRangeRequest,
    GetBlockRangeResponse, GetBlockRequest, GetBlockResponse, GetClientStateRequest,
    GetClientStateResponse, GetTipRequest, GetTipResponse, HistoricalReadRequest,
    HistoricalReadResponse, ListEntitiesRequest, ListEntitiesResponse, ListRelationshipsRequest,
    ListRelationshipsResponse, ListResourcesRequest, ListResourcesResponse, ReadRequest,
    ReadResponse, VerifiedReadRequest, VerifiedReadResponse, WatchBlocksRequest, WriteRequest,
    WriteResponse,
};
use crate::shard_router::ShardConnection;

use inferadb_ledger_types::ShardId;

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
    /// Create a new forward client from a shard connection.
    pub fn new(connection: ShardConnection) -> Self {
        let channel = connection.channel;
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            shard_id: connection.shard_id,
        }
    }

    /// Create a new forward client from a channel directly.
    pub fn from_channel(channel: Channel, shard_id: ShardId) -> Self {
        Self {
            read_client: ReadServiceClient::new(channel.clone()),
            write_client: WriteServiceClient::new(channel),
            shard_id,
        }
    }

    /// Get the shard ID this client forwards to.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    // ========================================================================
    // Read Service Forwarding
    // ========================================================================

    /// Forward a Read request to the remote shard.
    pub async fn forward_read(
        &mut self,
        request: ReadRequest,
    ) -> Result<Response<ReadResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding read request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward read failed");
            e
        })
    }

    /// Forward a VerifiedRead request to the remote shard.
    pub async fn forward_verified_read(
        &mut self,
        request: VerifiedReadRequest,
    ) -> Result<Response<VerifiedReadResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding verified_read request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.verified_read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward verified_read failed");
            e
        })
    }

    /// Forward a HistoricalRead request to the remote shard.
    pub async fn forward_historical_read(
        &mut self,
        request: HistoricalReadRequest,
    ) -> Result<Response<HistoricalReadResponse>, Status> {
        debug!(
            shard_id = self.shard_id,
            "Forwarding historical_read request"
        );
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.historical_read(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward historical_read failed");
            e
        })
    }

    /// Forward a WatchBlocks request to the remote shard.
    ///
    /// Note: This returns a streaming response that must be handled appropriately.
    pub async fn forward_watch_blocks(
        &mut self,
        request: WatchBlocksRequest,
    ) -> Result<Response<tonic::Streaming<BlockAnnouncement>>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding watch_blocks request");
        let req = Request::new(request);
        self.read_client.watch_blocks(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward watch_blocks failed");
            e
        })
    }

    /// Forward a GetBlock request to the remote shard.
    pub async fn forward_get_block(
        &mut self,
        request: GetBlockRequest,
    ) -> Result<Response<GetBlockResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding get_block request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.get_block(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward get_block failed");
            e
        })
    }

    /// Forward a GetBlockRange request to the remote shard.
    pub async fn forward_get_block_range(
        &mut self,
        request: GetBlockRangeRequest,
    ) -> Result<Response<GetBlockRangeResponse>, Status> {
        debug!(
            shard_id = self.shard_id,
            "Forwarding get_block_range request"
        );
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.get_block_range(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward get_block_range failed");
            e
        })
    }

    /// Forward a GetTip request to the remote shard.
    pub async fn forward_get_tip(
        &mut self,
        request: GetTipRequest,
    ) -> Result<Response<GetTipResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding get_tip request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.get_tip(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward get_tip failed");
            e
        })
    }

    /// Forward a GetClientState request to the remote shard.
    pub async fn forward_get_client_state(
        &mut self,
        request: GetClientStateRequest,
    ) -> Result<Response<GetClientStateResponse>, Status> {
        debug!(
            shard_id = self.shard_id,
            "Forwarding get_client_state request"
        );
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.get_client_state(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward get_client_state failed");
            e
        })
    }

    /// Forward a ListRelationships request to the remote shard.
    pub async fn forward_list_relationships(
        &mut self,
        request: ListRelationshipsRequest,
    ) -> Result<Response<ListRelationshipsResponse>, Status> {
        debug!(
            shard_id = self.shard_id,
            "Forwarding list_relationships request"
        );
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.list_relationships(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward list_relationships failed");
            e
        })
    }

    /// Forward a ListResources request to the remote shard.
    pub async fn forward_list_resources(
        &mut self,
        request: ListResourcesRequest,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        debug!(
            shard_id = self.shard_id,
            "Forwarding list_resources request"
        );
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.list_resources(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward list_resources failed");
            e
        })
    }

    /// Forward a ListEntities request to the remote shard.
    pub async fn forward_list_entities(
        &mut self,
        request: ListEntitiesRequest,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding list_entities request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.read_client.list_entities(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward list_entities failed");
            e
        })
    }

    // ========================================================================
    // Write Service Forwarding
    // ========================================================================

    /// Forward a Write request to the remote shard.
    pub async fn forward_write(
        &mut self,
        request: WriteRequest,
    ) -> Result<Response<WriteResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding write request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.write_client.write(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward write failed");
            e
        })
    }

    /// Forward a BatchWrite request to the remote shard.
    pub async fn forward_batch_write(
        &mut self,
        request: BatchWriteRequest,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        debug!(shard_id = self.shard_id, "Forwarding batch_write request");
        let mut req = Request::new(request);
        req.set_timeout(DEFAULT_FORWARD_TIMEOUT);
        self.write_client.batch_write(req).await.map_err(|e| {
            warn!(shard_id = self.shard_id, error = %e, "Forward batch_write failed");
            e
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_forward_client_creation() {
        // Basic struct test - full testing requires gRPC setup
    }
}
