//! Mock implementation of the discovery gRPC service.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockDiscoveryService {
    pub(super) state: Arc<MockState>,
}

impl MockDiscoveryService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::system_discovery_service_server::SystemDiscoveryService
    for MockDiscoveryService
{
    async fn get_peers(
        &self,
        _request: Request<proto::GetPeersRequest>,
    ) -> Result<Response<proto::GetPeersResponse>, Status> {
        self.state.check_injection().await?;

        let peers = self.state.peers.read();
        Ok(Response::new(proto::GetPeersResponse { peers: peers.clone(), system_version: 1 }))
    }

    async fn announce_peer(
        &self,
        request: Request<proto::AnnouncePeerRequest>,
    ) -> Result<Response<proto::AnnouncePeerResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if let Some(peer) = req.peer {
            let mut peers = self.state.peers.write();
            peers.push(peer);
        }

        Ok(Response::new(proto::AnnouncePeerResponse { accepted: true }))
    }

    async fn get_system_state(
        &self,
        _request: Request<proto::GetSystemStateRequest>,
    ) -> Result<Response<proto::GetSystemStateResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::GetSystemStateResponse {
            version: 1,
            nodes: vec![],
            organizations: vec![],
        }))
    }

    async fn resolve_region_leader(
        &self,
        _request: Request<proto::ResolveRegionLeaderRequest>,
    ) -> Result<Response<proto::ResolveRegionLeaderResponse>, Status> {
        Err(Status::unimplemented("ResolveRegionLeader not available in mock"))
    }
}
