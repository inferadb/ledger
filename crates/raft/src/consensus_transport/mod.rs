//! gRPC network transport for the consensus engine.
//!
//! Implements [`NetworkTransport`] by serializing consensus messages to
//! postcard bytes and shipping them over a long-lived bidirectional stream
//! (`ConsensusStream`) per peer. The stream amortizes connection
//! setup across every message and leverages HTTP/2 flow control for
//! transport-level backpressure.

mod peer_sender;

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_consensus::transport::{NetworkTransport, OutboundMessage};
use parking_lot::RwLock;
use tonic::transport::Channel;

use self::peer_sender::PeerSender;
use crate::{node_registry::NodeConnectionRegistry, types::LedgerNodeId};

/// gRPC-based network transport for consensus messages.
///
/// Each peer gets a dedicated `PeerSender` (see the `peer_sender` submodule)
/// that owns a bounded outbound queue and a single drain task. `send_batch`
/// enqueues messages to the per-peer queue (drop-oldest on overflow); the
/// drain task serializes and ships them via a long-lived bidirectional
/// `ConsensusStream` RPC. Acks are discarded — messages are fire-and-forget
/// and the consensus engine handles retries via heartbeats.
#[derive(Clone)]
pub struct GrpcConsensusTransport {
    senders: Arc<RwLock<HashMap<LedgerNodeId, Arc<PeerSender>>>>,
    local_node: LedgerNodeId,
    local_address: Arc<RwLock<String>>,
    region: inferadb_ledger_types::Region,
    registry: Arc<NodeConnectionRegistry>,
}

impl GrpcConsensusTransport {
    /// Creates a new transport for the given local node and region.
    ///
    /// The `registry` is shared across all subsystems so HTTP/2 channels are
    /// deduplicated per peer; see [`NodeConnectionRegistry`] for details.
    pub fn new(
        local_node: LedgerNodeId,
        region: inferadb_ledger_types::Region,
        registry: Arc<NodeConnectionRegistry>,
    ) -> Self {
        Self {
            senders: Arc::new(RwLock::new(HashMap::new())),
            local_node,
            local_address: Arc::new(RwLock::new(String::new())),
            region,
            registry,
        }
    }

    /// Sets the local node's gRPC address (included in outbound messages
    /// so receivers can auto-register a return channel).
    pub fn set_local_address(&self, addr: String) {
        *self.local_address.write() = addr;
    }

    /// Registers or replaces the per-peer sender for a node.
    ///
    /// Spawns a fresh drain task bound to the provided channel. Any
    /// previous sender for the same node is dropped when the map is
    /// updated, which signals its drain task to shut down.
    pub(crate) fn set_peer(&self, node: LedgerNodeId, channel: Channel) {
        let from_address = self.local_address.read().clone();
        let sender =
            Arc::new(PeerSender::spawn(channel, node, self.region, self.local_node, from_address));
        self.senders.write().insert(node, sender);
    }

    /// Registers a peer by address via the node connection registry, then
    /// starts a `PeerSender` for it. Production code should prefer this
    /// over `set_peer(node, channel)` to ensure channel deduplication
    /// across regions and subsystems.
    ///
    /// # Errors
    ///
    /// Returns a `RegistryError` when the address cannot be parsed.
    pub async fn set_peer_via_registry(
        &self,
        node: LedgerNodeId,
        addr: &str,
    ) -> Result<(), crate::node_registry::RegistryError> {
        let conn = self.registry.get_or_register(node, addr).await?;
        self.set_peer(node, conn.channel());
        Ok(())
    }

    /// Removes the sender for a peer node. The drain task observes the
    /// shutdown signal on drop and exits.
    pub fn remove_peer(&self, node: LedgerNodeId) {
        self.senders.write().remove(&node);
    }

    /// Returns the list of registered peer node IDs.
    pub fn peers(&self) -> Vec<LedgerNodeId> {
        self.senders.read().keys().copied().collect()
    }

    /// Removes senders for peers not present in the given membership (voters + learners).
    pub fn retain_peers(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        let valid: std::collections::HashSet<u64> =
            membership.voters.iter().chain(membership.learners.iter()).map(|n| n.0).collect();
        self.senders.write().retain(|id, _| valid.contains(id));
    }

    #[cfg(test)]
    pub(crate) fn peer_queue_depth(&self, node: LedgerNodeId) -> Option<usize> {
        self.senders.read().get(&node).map(|s| s.depth())
    }

    #[cfg(test)]
    pub(crate) fn peer_drop_count(&self, node: LedgerNodeId) -> Option<usize> {
        self.senders.read().get(&node).map(|s| s.dropped_count())
    }
}

impl NetworkTransport for GrpcConsensusTransport {
    fn send_batch(&self, messages: Vec<OutboundMessage>) {
        let senders = self.senders.read();
        for msg in messages {
            if msg.to.0 == self.local_node {
                continue;
            }
            if let Some(sender) = senders.get(&msg.to.0) {
                // Push returns a PushOutcome that metrics already record.
                let _ = sender.push(msg);
            }
            // Missing peer: silently dropped (same as pre-refactor behavior).
        }
    }

    fn on_membership_changed(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        self.retain_peers(membership);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_consensus::{
        Message,
        types::{NodeId, ShardId},
    };

    use super::*;
    use crate::node_registry::NodeConnectionRegistry;

    fn make_transport() -> GrpcConsensusTransport {
        let registry = Arc::new(NodeConnectionRegistry::new());
        GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL, registry)
    }

    #[test]
    fn new_transport_has_no_peers() {
        let transport = make_transport();
        assert!(transport.peers().is_empty());
    }

    #[test]
    fn self_sends_are_skipped() {
        let transport = make_transport();
        // Fire-and-forget with no channel for self — should not panic
        let messages =
            vec![OutboundMessage { to: NodeId(1), shard: ShardId(1), msg: Message::TimeoutNow }];
        transport.send_batch(messages);
    }

    #[test]
    fn unknown_peer_is_silently_skipped() {
        let transport = make_transport();
        let messages =
            vec![OutboundMessage { to: NodeId(99), shard: ShardId(1), msg: Message::TimeoutNow }];
        transport.send_batch(messages);
    }

    #[tokio::test]
    async fn set_and_remove_peer() {
        let transport = make_transport();
        assert!(transport.peers().is_empty());

        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel);
        assert_eq!(transport.peers(), vec![2]);

        transport.remove_peer(2);
        assert!(transport.peers().is_empty());

        // Removing a non-existent peer is a no-op.
        transport.remove_peer(99);
        assert!(transport.peers().is_empty());
    }

    #[tokio::test]
    async fn retain_peers_removes_departed_channels() {
        let transport = make_transport();
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel.clone());
        transport.set_peer(3, channel);
        assert_eq!(transport.peers().len(), 2);

        let membership = inferadb_ledger_consensus::types::Membership::new([
            inferadb_ledger_consensus::types::NodeId(1),
            inferadb_ledger_consensus::types::NodeId(2),
        ]);
        transport.retain_peers(&membership);
        assert_eq!(transport.peers(), vec![2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_batch_enqueues_to_peer_sender() {
        let transport = make_transport();

        // Lazy channel — never connects.
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel);

        // Push 10 messages.
        let messages: Vec<_> = (0..10)
            .map(|i| OutboundMessage { to: NodeId(2), shard: ShardId(i), msg: Message::TimeoutNow })
            .collect();
        transport.send_batch(messages);

        // Give the drain task a beat to process.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(
            transport.peer_queue_depth(2),
            Some(0),
            "queue should drain; messages fail to send but are consumed from the queue"
        );
    }

    #[tokio::test]
    async fn remove_peer_drops_sender() {
        let transport = make_transport();
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel);
        assert!(transport.peer_queue_depth(2).is_some());

        transport.remove_peer(2);
        assert_eq!(transport.peer_queue_depth(2), None);
    }

    /// Stress test: sustained overflow bounds queue depth and tracks drops correctly.
    ///
    /// Validates that:
    /// - Queue depth stays at or below capacity regardless of push volume.
    /// - Drop count reflects the number of oldest messages evicted.
    /// - The per-peer drain task does not leak or panic under backpressure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sustained_overflow_bounds_queue_and_tracks_drops() {
        use super::peer_sender::PEER_QUEUE_CAPACITY;

        const TOTAL: usize = 10_000;
        let transport = make_transport();

        // Lazy channel to an unreachable endpoint. Every send fails instantly.
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel);

        // Fire TOTAL messages in one batch (worst case for the push path).
        let messages: Vec<_> = (0..TOTAL)
            .map(|i| OutboundMessage {
                to: NodeId(2),
                shard: ShardId(i as u64),
                msg: Message::TimeoutNow,
            })
            .collect();
        transport.send_batch(messages);

        // Give the drain task a window to make progress. It cannot clear the
        // queue entirely (since sends fail and are effectively no-ops from a
        // throughput standpoint), but it will drain batches.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let depth = transport.peer_queue_depth(2).expect("peer registered");
        let drops = transport.peer_drop_count(2).expect("peer registered");

        // Invariant 1: queue depth never exceeds capacity.
        assert!(
            depth <= PEER_QUEUE_CAPACITY,
            "queue depth {depth} exceeded capacity {PEER_QUEUE_CAPACITY}"
        );

        // Invariant 2: conservation — drops + depth never exceeds TOTAL.
        assert!(
            drops + depth <= TOTAL,
            "conservation violated: drops {drops} + depth {depth} > TOTAL {TOTAL}"
        );

        // Invariant 3: we pushed far over capacity, so drops must be substantial.
        // The exact count is nondeterministic (depends on how many batches the
        // drain task pulled before the next push hit), but it's definitely
        // bounded below by TOTAL - capacity - one full batch window.
        assert!(
            drops > 0,
            "expected overflow drops after pushing {TOTAL} messages at capacity {PEER_QUEUE_CAPACITY}"
        );
        assert!(drops <= TOTAL, "cannot drop more than pushed: {drops} > {TOTAL}");
    }

    /// Stress test: peer removal cleanly tears down the sender task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_removal_cleans_up_sender() {
        let transport = make_transport();
        let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
        transport.set_peer(2, channel);

        assert!(transport.peer_queue_depth(2).is_some());

        transport.remove_peer(2);
        assert!(transport.peer_queue_depth(2).is_none());

        // Allow the drain task a beat to observe shutdown. This is a
        // smoke test — a real leak would manifest under longer soak testing.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn set_peer_via_registry_registers_and_starts_sender() {
        let registry = Arc::new(NodeConnectionRegistry::new());
        let transport = GrpcConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::clone(&registry),
        );

        transport.set_peer_via_registry(2, "127.0.0.1:5000").await.unwrap();

        // Registry has the peer.
        assert_eq!(registry.len(), 1);
        assert!(registry.get(2).is_some());

        // Transport has a PeerSender for it.
        assert!(transport.peer_queue_depth(2).is_some());
    }

    #[tokio::test]
    async fn set_peer_via_registry_invalid_address_errors() {
        let registry = Arc::new(NodeConnectionRegistry::new());
        let transport = GrpcConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::clone(&registry),
        );

        let err = transport.set_peer_via_registry(2, "not a uri").await;
        assert!(err.is_err());
        // Registry leaves an uninitialized OnceCell entry behind, but no
        // PeerConnection is observable via `get`, and the transport has no
        // sender.
        assert!(registry.get(2).is_none());
        assert!(transport.peer_queue_depth(2).is_none());
    }
}
