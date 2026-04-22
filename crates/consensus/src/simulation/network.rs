//! Simulated network with partition support for deterministic testing.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::{
    message::Message,
    types::{ConsensusStateId, NodeId},
};

/// A message in flight through the simulated network.
#[derive(Clone, Debug)]
pub struct InFlightMessage {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// The shard group this message belongs to.
    pub shard_id: ConsensusStateId,
    /// The Raft message payload.
    pub message: Message,
}

/// Simulated network that queues messages per-destination with partition support.
///
/// Messages sent between partitioned nodes are silently dropped.
pub struct SimulatedNetwork {
    queues: HashMap<NodeId, VecDeque<InFlightMessage>>,
    partitions: HashSet<(NodeId, NodeId)>,
    delivered_count: u64,
    dropped_count: u64,
}

impl Default for SimulatedNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulatedNetwork {
    /// Creates a new empty simulated network.
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            partitions: HashSet::new(),
            delivered_count: 0,
            dropped_count: 0,
        }
    }

    /// Enqueues a message from `from` to `to` for the given shard group,
    /// or drops it if the pair is partitioned.
    pub fn send(&mut self, from: NodeId, to: NodeId, shard_id: ConsensusStateId, message: Message) {
        if self.is_partitioned(from, to) {
            self.dropped_count += 1;
            return;
        }
        self.queues.entry(to).or_default().push_back(InFlightMessage {
            from,
            to,
            shard_id,
            message,
        });
    }

    /// Drains and returns all pending messages for `node`.
    pub fn receive(&mut self, node: NodeId) -> Vec<InFlightMessage> {
        let msgs: Vec<InFlightMessage> = self.queues.entry(node).or_default().drain(..).collect();
        self.delivered_count += msgs.len() as u64;
        msgs
    }

    /// Returns `true` if any node has pending messages.
    pub fn has_pending(&self) -> bool {
        self.queues.values().any(|q| !q.is_empty())
    }

    /// Partitions two groups of nodes bidirectionally.
    ///
    /// Messages between any node in `group_a` and any node in `group_b` will be
    /// dropped in both directions.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partitions.insert((a, b));
                self.partitions.insert((b, a));
            }
        }
    }

    /// Removes all partitions, restoring full connectivity.
    pub fn heal(&mut self) {
        self.partitions.clear();
    }

    /// Returns `true` if delivery from `from` to `to` is blocked.
    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.contains(&(from, to))
    }

    /// Total number of messages successfully delivered (drained by `receive`).
    pub fn delivered_count(&self) -> u64 {
        self.delivered_count
    }

    /// Total number of messages dropped due to partitions.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    const TEST_SHARD: ConsensusStateId = ConsensusStateId(1);

    fn msg() -> Message {
        Message::PreVoteResponse { term: 1, vote_granted: true }
    }

    // ── Send / Receive ────────────────────────────────────────────

    #[test]
    fn send_enqueues_message_for_destination() {
        let mut net = SimulatedNetwork::new();
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());

        let received = net.receive(NodeId(2));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].from, NodeId(1));
        assert_eq!(received[0].to, NodeId(2));
        assert_eq!(received[0].shard_id, TEST_SHARD);
    }

    #[test]
    fn receive_increments_delivered_count() {
        let mut net = SimulatedNetwork::new();
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        net.receive(NodeId(2));

        assert_eq!(net.delivered_count(), 1);
        assert_eq!(net.dropped_count(), 0);
    }

    #[test]
    fn receive_drains_queue() {
        let mut net = SimulatedNetwork::new();
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        net.receive(NodeId(2));

        let second = net.receive(NodeId(2));
        assert!(second.is_empty());
    }

    #[test]
    fn receive_returns_empty_for_unknown_node() {
        let mut net = SimulatedNetwork::new();
        let received = net.receive(NodeId(99));
        assert!(received.is_empty());
    }

    // ── has_pending ───────────────────────────────────────────────

    #[test]
    fn has_pending_returns_true_with_queued_message() {
        let mut net = SimulatedNetwork::new();
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        assert!(net.has_pending());
    }

    #[test]
    fn has_pending_returns_false_on_empty_network() {
        let net = SimulatedNetwork::new();
        assert!(!net.has_pending());
    }

    #[test]
    fn has_pending_returns_false_after_drain() {
        let mut net = SimulatedNetwork::new();
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        net.receive(NodeId(2));
        assert!(!net.has_pending());
    }

    // ── Partitions ────────────────────────────────────────────────

    #[test]
    fn partition_drops_sent_messages() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);
        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());

        let received = net.receive(NodeId(2));
        assert!(received.is_empty());
        assert_eq!(net.dropped_count(), 1);
    }

    #[test]
    fn partition_is_bidirectional() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);

        assert!(net.is_partitioned(NodeId(1), NodeId(2)));
        assert!(net.is_partitioned(NodeId(2), NodeId(1)));
    }

    #[test]
    fn partition_drops_both_directions() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);

        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        net.send(NodeId(2), NodeId(1), TEST_SHARD, msg());
        assert_eq!(net.dropped_count(), 2);
    }

    #[test]
    fn partition_does_not_affect_uninvolved_nodes() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);

        net.send(NodeId(1), NodeId(3), TEST_SHARD, msg());
        let received = net.receive(NodeId(3));
        assert_eq!(received.len(), 1);
    }

    #[test]
    fn partition_between_groups_blocks_all_cross_group_pairs() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1), NodeId(2)], &[NodeId(3), NodeId(4)]);

        assert!(net.is_partitioned(NodeId(1), NodeId(3)));
        assert!(net.is_partitioned(NodeId(1), NodeId(4)));
        assert!(net.is_partitioned(NodeId(2), NodeId(3)));
        assert!(net.is_partitioned(NodeId(2), NodeId(4)));
        // Within-group should remain connected.
        assert!(!net.is_partitioned(NodeId(1), NodeId(2)));
        assert!(!net.is_partitioned(NodeId(3), NodeId(4)));
    }

    // ── Heal ──────────────────────────────────────────────────────

    #[test]
    fn heal_clears_all_partitions() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);
        net.partition(&[NodeId(3)], &[NodeId(4)]);
        net.heal();

        assert!(!net.is_partitioned(NodeId(1), NodeId(2)));
        assert!(!net.is_partitioned(NodeId(3), NodeId(4)));
    }

    #[test]
    fn heal_restores_message_delivery() {
        let mut net = SimulatedNetwork::new();
        net.partition(&[NodeId(1)], &[NodeId(2)]);
        net.heal();

        net.send(NodeId(1), NodeId(2), TEST_SHARD, msg());
        let received = net.receive(NodeId(2));
        assert_eq!(received.len(), 1);
    }

    // ── Default ───────────────────────────────────────────────────

    #[test]
    fn default_creates_empty_network() {
        let net = SimulatedNetwork::default();
        assert!(!net.has_pending());
        assert_eq!(net.delivered_count(), 0);
        assert_eq!(net.dropped_count(), 0);
    }

    // ── Message ordering ──────────────────────────────────────────

    #[test]
    fn messages_delivered_in_send_order() {
        let mut net = SimulatedNetwork::new();
        let m1 = Message::PreVoteResponse { term: 1, vote_granted: true };
        let m2 = Message::PreVoteResponse { term: 2, vote_granted: false };
        let m3 = Message::PreVoteResponse { term: 3, vote_granted: true };

        net.send(NodeId(1), NodeId(2), TEST_SHARD, m1);
        net.send(NodeId(1), NodeId(2), TEST_SHARD, m2);
        net.send(NodeId(1), NodeId(2), TEST_SHARD, m3);

        let received = net.receive(NodeId(2));
        assert_eq!(received.len(), 3);
        // FIFO order preserved by VecDeque.
        match &received[0].message {
            Message::PreVoteResponse { term, .. } => assert_eq!(*term, 1),
            other => panic!("unexpected message: {other:?}"),
        }
        match &received[2].message {
            Message::PreVoteResponse { term, .. } => assert_eq!(*term, 3),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    // ── ConsensusState-aware routing ───────────────────────────────────────

    #[test]
    fn messages_preserve_shard_id() {
        let mut net = SimulatedNetwork::new();
        let shard_a = ConsensusStateId(1);
        let shard_b = ConsensusStateId(2);

        net.send(NodeId(1), NodeId(2), shard_a, msg());
        net.send(NodeId(1), NodeId(2), shard_b, msg());

        let received = net.receive(NodeId(2));
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].shard_id, shard_a);
        assert_eq!(received[1].shard_id, shard_b);
    }
}
