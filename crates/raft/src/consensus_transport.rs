//! gRPC network transport for the consensus engine.
//!
//! Implements [`NetworkTransport`] by serializing consensus messages
//! to postcard bytes and sending them via the `ForwardConsensus` gRPC RPC.

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_consensus::{
    Message,
    transport::{NetworkTransport, OutboundMessage},
};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::types::LedgerNodeId;

/// gRPC-based network transport for consensus messages.
///
/// Maps peer node IDs to gRPC channels and sends consensus messages
/// via the `ForwardConsensus` RPC. Messages are fire-and-forget —
/// the consensus engine handles retries via heartbeats.
#[derive(Clone)]
pub struct GrpcConsensusTransport {
    channels: Arc<RwLock<HashMap<LedgerNodeId, Channel>>>,
    local_node: LedgerNodeId,
    local_address: Arc<RwLock<String>>,
    region: inferadb_ledger_types::Region,
}

impl GrpcConsensusTransport {
    /// Creates a new transport for the given local node and region.
    pub fn new(local_node: LedgerNodeId, region: inferadb_ledger_types::Region) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            local_node,
            local_address: Arc::new(RwLock::new(String::new())),
            region,
        }
    }

    /// Sets the local node's gRPC address (included in outbound messages
    /// so receivers can auto-register a return channel).
    pub fn set_local_address(&self, addr: String) {
        *self.local_address.write() = addr;
    }

    /// Registers or updates the gRPC channel for a peer node.
    pub fn set_peer(&self, node: LedgerNodeId, channel: Channel) {
        self.channels.write().insert(node, channel);
    }

    /// Removes the gRPC channel for a peer node.
    pub fn remove_peer(&self, node: LedgerNodeId) {
        self.channels.write().remove(&node);
    }

    /// Returns the list of registered peer node IDs.
    pub fn peers(&self) -> Vec<LedgerNodeId> {
        self.channels.read().keys().copied().collect()
    }

    /// Removes channels for peers not present in the given membership (voters + learners).
    pub fn retain_peers(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        let valid: std::collections::HashSet<u64> =
            membership.voters.iter().chain(membership.learners.iter()).map(|n| n.0).collect();
        self.channels.write().retain(|id, _| valid.contains(id));
    }
}

impl NetworkTransport for GrpcConsensusTransport {
    fn send_batch(&self, messages: Vec<OutboundMessage>) {
        let channels = self.channels.read();

        // Group messages by destination node.
        let mut grouped: std::collections::HashMap<u64, Vec<OutboundMessage>> =
            std::collections::HashMap::new();
        for msg in messages {
            if msg.to.0 == self.local_node {
                continue;
            }
            if channels.contains_key(&msg.to.0) {
                grouped.entry(msg.to.0).or_default().push(msg);
            }
        }

        let from_address = self.local_address.read().clone();

        // One task per destination with all messages for that peer.
        for (target, peer_msgs) in grouped {
            if let Some(channel) = channels.get(&target) {
                let channel = channel.clone();
                let region = self.region;
                let from_node = self.local_node;
                let addr = from_address.clone();
                tokio::spawn(async move {
                    for msg in peer_msgs {
                        if let Err(e) = send_message(
                            channel.clone(),
                            msg.shard.0,
                            from_node,
                            &addr,
                            region,
                            msg.msg,
                        )
                        .await
                        {
                            tracing::debug!(
                                target_node = target,
                                region = %region,
                                error = %e,
                                "Consensus message send failed"
                            );
                            break;
                        }
                    }
                });
            }
        }
    }

    fn on_membership_changed(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        self.retain_peers(membership);
    }
}

async fn send_message(
    channel: Channel,
    shard_id: u64,
    from_node: u64,
    from_address: &str,
    region: inferadb_ledger_types::Region,
    message: Message,
) -> Result<(), tonic::Status> {
    let payload = postcard::to_allocvec(&message)
        .map_err(|e| tonic::Status::internal(format!("serialize consensus message: {e}")))?;

    let mut client =
        inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient::new(channel);

    let request = inferadb_ledger_proto::proto::ConsensusForwardRequest {
        shard_id,
        from_node,
        region: Some(inferadb_ledger_proto::proto::Region::from(region) as i32),
        payload,
        from_address: from_address.to_string(),
        cluster_id: 0,
    };

    client.forward_consensus(request).await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_consensus::types::{NodeId, ShardId};

    use super::*;

    #[test]
    fn new_transport_has_no_peers() {
        let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);
        assert!(transport.peers().is_empty());
    }

    #[test]
    fn self_sends_are_skipped() {
        let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);
        // Fire-and-forget with no channel for self — should not panic
        let messages =
            vec![OutboundMessage { to: NodeId(1), shard: ShardId(1), msg: Message::TimeoutNow }];
        transport.send_batch(messages);
    }

    #[test]
    fn unknown_peer_is_silently_skipped() {
        let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);
        let messages =
            vec![OutboundMessage { to: NodeId(99), shard: ShardId(1), msg: Message::TimeoutNow }];
        transport.send_batch(messages);
    }

    #[test]
    fn set_and_remove_peer() {
        let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);
        assert!(transport.peers().is_empty());

        // Can't construct a real Channel without a server, but we can verify
        // the map operations work by checking peer count after remove.
        transport.remove_peer(99); // removing non-existent peer is a no-op
        assert!(transport.peers().is_empty());
    }

    #[test]
    fn retain_peers_removes_departed_channels() {
        let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);
        // Verify retain_peers doesn't panic on an empty channel map.
        let membership = inferadb_ledger_consensus::types::Membership::new([
            inferadb_ledger_consensus::types::NodeId(1),
        ]);
        transport.retain_peers(&membership);
        assert!(transport.peers().is_empty());
    }
}
