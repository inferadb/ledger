//! Raft service implementation for inter-node communication.
//!
//! This service handles incoming consensus protocol messages from peer nodes.
//! The legacy openraft RPC methods (vote, append_entries, install_snapshot)
//! are retained for proto compatibility but return `UNIMPLEMENTED`.
//! Active consensus messaging uses the bidirectional streaming
//! `consensus_stream` RPC — one long-lived stream per peer with
//! HTTP/2 flow-controlled backpressure.

use std::{net::SocketAddr, str::FromStr, sync::Arc};

use inferadb_ledger_proto::proto::{
    BatchRaftRequest, BatchRaftResponse, ConsensusAck, ConsensusEnvelope,
    ForwardRegionalProposalRequest, ForwardRegionalProposalResponse, RaftAppendEntriesRequest,
    RaftAppendEntriesResponse, RaftInstallSnapshotRequest, RaftInstallSnapshotResponse,
    RaftVoteRequest, RaftVoteResponse, ReadIndexRequest, ReadIndexResponse, TriggerElectionRequest,
    TriggerElectionResponse,
};
use inferadb_ledger_raft::{
    raft_manager::RaftManager,
    types::{LedgerRequest, RaftPayload},
};
use inferadb_ledger_types::{decode, encode};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

/// Handles incoming consensus RPCs from peer nodes.
///
/// Routes `forward_consensus` requests to the correct region's consensus engine.
/// Legacy openraft RPCs (vote, append_entries, etc.) are no longer functional.
pub struct RaftService {
    manager: Arc<RaftManager>,
    /// Per-node liveness timestamps. Updated on every successful Raft message.
    /// Shared with the admin service and bootstrap liveness checker.
    peer_liveness:
        Option<Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>>,
}

impl RaftService {
    /// Creates a new Raft service backed by a region-aware manager.
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager, peer_liveness: None }
    }

    /// Attaches a shared peer liveness map for tracking when peers were last heard from.
    #[must_use]
    pub fn with_peer_liveness(
        mut self,
        liveness: Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
    ) -> Self {
        self.peer_liveness = Some(liveness);
        self
    }

    /// Returns true if `addr` matches the IP of any known cluster peer.
    ///
    /// Used to authenticate `ForwardRegionalProposal` callers. The cluster
    /// runs on a trusted (private / WireGuard) network, so IP-match against
    /// the peer address map is the pragmatic guard — ports differ between
    /// inbound (ephemeral client port) and outbound (configured server port),
    /// so only the IP portion is compared.
    fn is_known_peer(&self, addr: &SocketAddr) -> bool {
        let peers = self.manager.peer_addresses();
        if peers.is_empty() {
            // In dev / test scenarios with no peer map wired, allow to avoid
            // breaking single-node bootstraps where the node is its own peer
            // and has not yet announced itself. Log a warning so operational
            // misconfiguration in production is visible.
            tracing::warn!(
                remote = %addr,
                "is_known_peer: empty peer_addresses map; allowing ForwardRegionalProposal unauthenticated"
            );
            return true;
        }
        let caller_ip = addr.ip().to_string();
        peers.iter_peers().into_iter().any(|(_, entry)| {
            // entry is `host:port` (or `[ipv6]:port`); compare the host
            // portion only. `rsplit_once(':')` handles both IPv4 and
            // bracketed IPv6.
            let host = entry.rsplit_once(':').map_or(entry.as_str(), |(h, _)| h).to_owned();
            // Strip IPv6 brackets if present.
            let host = host
                .strip_prefix('[')
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or(&host)
                .to_owned();
            host == caller_ip
        })
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::raft_service_server::RaftService for RaftService {
    async fn vote(
        &self,
        _request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        Err(Status::unimplemented(
            "Legacy openraft vote RPC is no longer supported. Use forward_consensus instead.",
        ))
    }

    async fn append_entries(
        &self,
        _request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        Err(Status::unimplemented(
            "Legacy openraft append_entries RPC is no longer supported. Use forward_consensus instead.",
        ))
    }

    async fn install_snapshot(
        &self,
        _request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "Legacy openraft install_snapshot RPC is no longer supported. Use forward_consensus instead.",
        ))
    }

    async fn trigger_election(
        &self,
        _request: Request<TriggerElectionRequest>,
    ) -> Result<Response<TriggerElectionResponse>, Status> {
        Err(Status::unimplemented("Legacy openraft trigger_election RPC is no longer supported."))
    }

    async fn batch_send(
        &self,
        _request: Request<BatchRaftRequest>,
    ) -> Result<Response<BatchRaftResponse>, Status> {
        Err(Status::unimplemented("Legacy openraft batch_send RPC is no longer supported."))
    }

    async fn read_index(
        &self,
        request: Request<ReadIndexRequest>,
    ) -> Result<Response<ReadIndexResponse>, Status> {
        let req = request.into_inner();

        // Resolve region from string field — empty means GLOBAL, non-empty must parse.
        let region = if req.region.is_empty() {
            inferadb_ledger_types::Region::GLOBAL
        } else {
            inferadb_ledger_types::Region::from_str(&req.region)
                .map_err(|_| Status::invalid_argument(format!("invalid region: {}", req.region)))?
        };

        let group = self
            .manager
            .get_region_group(region)
            .map_err(|_| Status::not_found("region group not found"))?;

        let handle = group.handle();

        // Must be leader to serve ReadIndex.
        if !handle.is_leader() {
            return Err(super::metadata::not_leader_status_from_handle(
                handle.as_ref(),
                Some(self.manager.peer_addresses()),
                "Not the leader",
            ));
        }

        // Fast path: valid lease means we're still the leader without a quorum check.
        if group.leader_lease().is_valid() {
            return Ok(Response::new(ReadIndexResponse {
                committed_index: handle.commit_index(),
                leader_term: handle.current_term(),
            }));
        }

        // Slow path: lease expired (idle cluster, just elected, etc.).
        // Confirm leadership via the consensus engine's read_index, which
        // performs a quorum heartbeat round.
        let committed_index = handle
            .engine_read_index()
            .await
            .map_err(|e| Status::unavailable(format!("ReadIndex quorum check failed: {e}")))?;

        Ok(Response::new(ReadIndexResponse { committed_index, leader_term: handle.current_term() }))
    }

    type ConsensusStreamStream = ReceiverStream<Result<ConsensusAck, Status>>;

    async fn consensus_stream(
        &self,
        request: Request<tonic::Streaming<ConsensusEnvelope>>,
    ) -> Result<Response<Self::ConsensusStreamStream>, Status> {
        let mut inbound = request.into_inner();
        // Bounded ack channel — bidi backpressure prevents unbounded fan-out.
        // Capacity mirrors the peer-sender queue capacity (1024) so an active
        // peer stream can have up to that many acks in flight before the
        // server's write task parks on HTTP/2 flow control.
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(1024);

        let manager = Arc::clone(&self.manager);
        let peer_liveness = self.peer_liveness.clone();

        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                let ack = match msg {
                    Ok(req) => handle_consensus_message(&manager, peer_liveness.as_ref(), req)
                        .await
                        .map(|_| ConsensusAck {}),
                    Err(e) => {
                        tracing::debug!(error = %e, "Consensus stream inbound error; closing");
                        break;
                    },
                };
                if ack_tx.send(ack).await.is_err() {
                    // Client dropped the ack stream.
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }

    async fn forward_regional_proposal(
        &self,
        request: Request<ForwardRegionalProposalRequest>,
    ) -> Result<Response<ForwardRegionalProposalResponse>, Status> {
        // Verify the caller is a known cluster peer. `ForwardRegionalProposal`
        // is an internal server-to-server RPC used by saga orchestration;
        // allowing unauthenticated external callers would let them forge
        // arbitrary LedgerRequest payloads and bypass JWT / org scoping /
        // rate limiting.
        let Some(remote_addr) = request.remote_addr() else {
            return Err(Status::unauthenticated(
                "ForwardRegionalProposal requires a known peer source address",
            ));
        };
        if !self.is_known_peer(&remote_addr) {
            return Err(Status::unauthenticated(format!(
                "ForwardRegionalProposal from unknown peer {remote_addr}"
            )));
        }

        let req = request.into_inner();

        // Resolve the target region — None means GLOBAL (same logic as forward_consensus).
        let region = match req.region {
            None => inferadb_ledger_types::Region::GLOBAL,
            Some(v) => {
                let proto_region = inferadb_ledger_proto::proto::Region::try_from(v)
                    .map_err(|_| Status::invalid_argument(format!("invalid region enum: {v}")))?;
                inferadb_ledger_types::Region::try_from(proto_region)
                    .map_err(|_| Status::invalid_argument(format!("unsupported region: {v}")))?
            },
        };

        let group = self
            .manager
            .get_region_group(region)
            .map_err(|_| Status::not_found("region group not found"))?;

        // Deserialize the proposal payload.
        let ledger_request: LedgerRequest = decode(&req.request_payload)
            .map_err(|e| Status::invalid_argument(format!("deserialize request: {e}")))?;

        let payload = RaftPayload::new(ledger_request, req.caller);
        let timeout = std::time::Duration::from_millis(u64::from(req.timeout_ms));

        match group.handle().propose_and_wait(payload, timeout).await {
            Ok(response) => {
                let response_bytes = encode(&response)
                    .map_err(|e| Status::internal(format!("serialize response: {e}")))?;
                let committed_index = group.handle().commit_index();
                Ok(Response::new(ForwardRegionalProposalResponse {
                    response_payload: response_bytes,
                    status_code: 0,
                    error_message: String::new(),
                    committed_index,
                }))
            },
            Err(e) => Ok(Response::new(ForwardRegionalProposalResponse {
                response_payload: Vec::new(),
                status_code: tonic::Code::Internal as i32,
                error_message: e.to_string(),
                committed_index: 0,
            })),
        }
    }
}

/// Core per-message consensus handler, shared by the streaming RPC and tests.
///
/// Performs region resolution, membership validation, optional auto-registration
/// of the sender's transport channel, postcard deserialization, delivery to the
/// consensus engine, and peer liveness tracking. Returns `Ok(())` on successful
/// delivery and a `tonic::Status` on any validation or engine failure.
async fn handle_consensus_message(
    manager: &Arc<RaftManager>,
    peer_liveness: Option<
        &Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
    >,
    req: ConsensusEnvelope,
) -> Result<(), Status> {
    // Resolve the target region from the request — None means GLOBAL,
    // Some(invalid) is an error.
    let region = match req.region {
        None => inferadb_ledger_types::Region::GLOBAL,
        Some(v) => {
            let proto_region = inferadb_ledger_proto::proto::Region::try_from(v)
                .map_err(|_| Status::invalid_argument(format!("invalid region enum: {v}")))?;
            inferadb_ledger_types::Region::try_from(proto_region)
                .map_err(|_| Status::invalid_argument(format!("unsupported region: {v}")))?
        },
    };

    let group = manager
        .get_region_group(region)
        .map_err(|_| Status::not_found("region group not found"))?;

    // Validate that the sender is a known cluster member (voter or learner).
    // Skip validation when:
    // - This node is the sole voter (freshly bootstrapped)
    // - This node is NOT in the voter set (newly joined node with stale initial membership — must
    //   accept messages to receive updated membership)
    let from_node = inferadb_ledger_consensus::types::NodeId(req.from_node);
    let local_node = inferadb_ledger_consensus::types::NodeId(group.handle().node_id());
    let state = group.handle().shard_state();
    let is_sole_voter = state.voters.len() == 1 && state.voters.contains(&local_node);
    let is_non_member =
        !state.voters.contains(&local_node) && !state.learners.contains(&local_node);
    if !is_sole_voter
        && !is_non_member
        && !state.voters.contains(&from_node)
        && !state.learners.contains(&from_node)
    {
        return Err(Status::permission_denied(format!(
            "node {} is not a member of the cluster",
            req.from_node
        )));
    }

    // Auto-register the sender's transport channel if not yet known.
    // This enables the return path: when the leader sends AppendEntries to a
    // joining node, the joining node needs a channel back to the leader for
    // AppendEntriesResponse.
    if !req.from_address.is_empty() {
        if let Some(transport) = group.consensus_transport()
            && !transport.peers().contains(&req.from_node)
            && let Err(e) = transport.set_peer_via_registry(req.from_node, &req.from_address).await
        {
            tracing::warn!(
                from_node = req.from_node,
                from_address = %req.from_address,
                error = %e,
                "Failed to auto-register sender via registry",
            );
        }
        // Also update the peer address map so future lookups work.
        manager.peer_addresses().insert(req.from_node, req.from_address.clone());
    }

    // Deserialize the consensus message from postcard bytes.
    let message: inferadb_ledger_consensus::Message =
        decode(&req.payload).map_err(|e| Status::invalid_argument(format!("deserialize: {e}")))?;

    // Route to the consensus engine via the handle.
    group
        .handle()
        .peer_message(req.from_node, message)
        .await
        .map_err(|e| Status::internal(format!("consensus: {e}")))?;

    // Update peer liveness for the sender.
    if let Some(liveness) = peer_liveness {
        liveness.write().insert(req.from_node, std::time::Instant::now());
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_consensus::Message;
    use inferadb_ledger_proto::proto::{
        ConsensusEnvelope, RaftAppendEntriesRequest, RaftInstallSnapshotRequest, RaftVoteRequest,
        Region as ProtoRegion, raft_service_server::RaftService as RaftServiceProto,
    };
    use inferadb_ledger_raft::{RaftManager, RaftManagerConfig, RegionConfig};
    use inferadb_ledger_test_utils::TestDir;
    use inferadb_ledger_types::{Region, encode};
    use tonic::Request;

    use super::{RaftService, handle_consensus_message};

    fn create_basic_service() -> (RaftService, Arc<RaftManager>, TestDir) {
        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        (RaftService::new(Arc::clone(&manager)), manager, temp)
    }

    async fn create_service_with_region() -> (RaftService, Arc<RaftManager>, TestDir) {
        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        manager.start_system_region(region_config).await.expect("start system region");
        let service = RaftService::new(Arc::clone(&manager));
        (service, manager, temp)
    }

    #[tokio::test]
    async fn vote_returns_unimplemented() {
        let (service, _manager, _temp) = create_basic_service();
        let result = service
            .vote(Request::new(RaftVoteRequest { vote: None, last_log_id: None, region: None }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn append_entries_returns_unimplemented() {
        let (service, _manager, _temp) = create_basic_service();
        let result = service
            .append_entries(Request::new(RaftAppendEntriesRequest {
                vote: None,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
                region: None,
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn install_snapshot_returns_unimplemented() {
        let (service, _manager, _temp) = create_basic_service();
        let result = service
            .install_snapshot(Request::new(RaftInstallSnapshotRequest {
                vote: None,
                meta: None,
                offset: 0,
                data: vec![],
                done: true,
                region: None,
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }

    // Byzantine fault tests for the streaming consensus handler.
    // These target `handle_consensus_message` directly — the per-message
    // core the streaming RPC loops over.

    /// A request targeting a region that has not been started returns NotFound.
    /// This prevents a rogue node from probing which regions exist before they
    /// are announced.
    #[tokio::test]
    async fn forward_consensus_unknown_region_returns_not_found() {
        let (_service, manager, _temp) = create_basic_service();
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(ProtoRegion::Global as i32),
                payload: vec![0xde, 0xad],
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    /// A sole-voter node (fresh/uninitialized) accepts messages from any sender
    /// to enable bootstrap replication. The message passes membership validation
    /// but fails on payload deserialization (InvalidArgument for corrupt data).
    #[tokio::test]
    async fn forward_consensus_sole_voter_accepts_unknown_sender() {
        let (_service, manager, _temp) = create_service_with_region().await;
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 99,
                region: Some(ProtoRegion::Global as i32),
                payload: vec![0xde, 0xad],
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// from_node=0 on a sole-voter node passes membership check (sole-voter bypass)
    /// but fails on deserialization since the payload is corrupt.
    #[tokio::test]
    async fn forward_consensus_zero_node_id_sole_voter_accepts() {
        let (_service, manager, _temp) = create_service_with_region().await;
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 0,
                region: Some(ProtoRegion::Global as i32),
                payload: vec![0x01],
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// A member node sending a corrupt (non-postcard) payload is rejected.
    /// Corruption in transit or an intentionally malformed message must not
    /// reach the consensus engine.
    #[tokio::test]
    async fn forward_consensus_corrupt_payload_returns_invalid_argument() {
        let (_service, manager, _temp) = create_service_with_region().await;
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(ProtoRegion::Global as i32),
                payload: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// An empty payload from a known member is rejected during deserialization.
    /// Empty messages carry no consensus content and signal a buggy or
    /// malicious peer.
    #[tokio::test]
    async fn forward_consensus_empty_payload_returns_invalid_argument() {
        let (_service, manager, _temp) = create_service_with_region().await;
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(ProtoRegion::Global as i32),
                payload: vec![],
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn is_known_peer_rejects_unknown_ip() {
        let (service, manager, _temp) = create_basic_service();
        manager.peer_addresses().insert(1, "10.0.0.1:50051".to_string());
        manager.peer_addresses().insert(2, "10.0.0.2:50051".to_string());
        let unknown: std::net::SocketAddr = "192.168.1.1:12345".parse().unwrap();
        assert!(!service.is_known_peer(&unknown));
    }

    #[tokio::test]
    async fn is_known_peer_accepts_matching_ip_any_port() {
        let (service, manager, _temp) = create_basic_service();
        manager.peer_addresses().insert(1, "10.0.0.1:50051".to_string());
        // Inbound port differs from outbound port, IP must still match.
        let caller: std::net::SocketAddr = "10.0.0.1:40123".parse().unwrap();
        assert!(service.is_known_peer(&caller));
    }

    #[tokio::test]
    async fn is_known_peer_allows_when_map_empty() {
        let (service, _manager, _temp) = create_basic_service();
        let caller: std::net::SocketAddr = "10.0.0.1:12345".parse().unwrap();
        // Empty map — single-node dev / test scenario. Allow.
        assert!(service.is_known_peer(&caller));
    }

    /// A well-formed message from a known member reaches the consensus engine.
    /// The engine will reject it (single-node cluster in test mode has no peers
    /// to route to), but the service layer must not block it before delivery.
    /// The response is either Ok or an internal engine error — never a
    /// membership or deserialization rejection.
    #[tokio::test]
    async fn forward_consensus_valid_message_from_member_passes_validation() {
        let (_service, manager, _temp) = create_service_with_region().await;
        let msg = Message::TimeoutNow;
        let payload = encode(&msg).expect("encode");
        let result = handle_consensus_message(
            &manager,
            None,
            ConsensusEnvelope {
                shard_id: 0,
                from_node: 1,
                region: Some(ProtoRegion::Global as i32),
                payload,
                from_address: String::new(),
                cluster_id: 0,
            },
        )
        .await;
        // The single-node cluster may return Ok or Internal depending on engine
        // state, but must not return NotFound, PermissionDenied, or
        // InvalidArgument — those indicate a validation failure, not an engine
        // decision.
        if let Err(status) = result {
            let code = status.code();
            assert_ne!(code, tonic::Code::NotFound);
            assert_ne!(code, tonic::Code::PermissionDenied);
            assert_ne!(code, tonic::Code::InvalidArgument);
        }
    }
}
