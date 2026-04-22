//! Per-destination message batching for network efficiency.

use std::collections::HashMap;

use crate::{
    message::Message,
    transport::{NetworkTransport, OutboundMessage},
    types::{ConsensusStateId, NodeId},
};

/// Groups outbound messages by destination node for batch sending.
///
/// Non-election messages accumulate in per-node buffers and are flushed
/// together, reducing the number of network round-trips when a single reactor
/// tick produces many sends to the same peer. Vote messages bypass the outbox
/// entirely so that election latency is unaffected.
pub struct NetworkOutbox {
    buffers: HashMap<NodeId, Vec<(ConsensusStateId, Message)>>,
}

impl NetworkOutbox {
    /// Creates an empty outbox.
    pub fn new() -> Self {
        Self { buffers: HashMap::new() }
    }

    /// Enqueues a message for delivery to `to` on the given `shard`.
    pub fn enqueue(&mut self, to: NodeId, shard: ConsensusStateId, msg: Message) {
        self.buffers.entry(to).or_default().push((shard, msg));
    }

    /// Drains all buffered messages and delivers them via `transport`.
    ///
    /// Each destination node's messages are sent as a single batch. After
    /// flushing the outbox is empty.
    pub fn flush(&mut self, transport: &impl NetworkTransport) {
        if self.buffers.is_empty() {
            return;
        }
        let outbound: Vec<OutboundMessage> = self
            .buffers
            .drain()
            .flat_map(|(to, msgs)| {
                msgs.into_iter().map(move |(shard, msg)| OutboundMessage { to, shard, msg })
            })
            .collect();
        transport.send_batch(outbound);
    }

    /// Returns the total number of buffered messages across all destinations.
    pub fn len(&self) -> usize {
        self.buffers.values().map(|v| v.len()).sum()
    }

    /// Returns `true` if no messages are buffered.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }
}

impl Default for NetworkOutbox {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::transport::InMemoryTransport;

    #[test]
    fn enqueue_three_destinations_reports_correct_len() {
        let mut outbox = NetworkOutbox::new();
        assert!(outbox.is_empty());
        assert_eq!(outbox.len(), 0);

        outbox.enqueue(NodeId(1), ConsensusStateId(0), Message::TimeoutNow);
        outbox.enqueue(NodeId(2), ConsensusStateId(0), Message::TimeoutNow);
        outbox.enqueue(NodeId(3), ConsensusStateId(0), Message::TimeoutNow);

        assert_eq!(outbox.len(), 3);
        assert!(!outbox.is_empty());
    }

    #[test]
    fn flush_empties_outbox_and_delivers_messages() {
        let mut outbox = NetworkOutbox::new();
        let transport = InMemoryTransport::new();

        outbox.enqueue(NodeId(1), ConsensusStateId(0), Message::TimeoutNow);
        outbox.enqueue(NodeId(2), ConsensusStateId(1), Message::TimeoutNow);
        outbox.enqueue(NodeId(3), ConsensusStateId(2), Message::TimeoutNow);

        outbox.flush(&transport);

        assert!(outbox.is_empty());
        assert_eq!(outbox.len(), 0);
        assert_eq!(transport.sent_count(), 3);
    }

    #[test]
    fn flush_empty_outbox_is_noop() {
        let mut outbox = NetworkOutbox::new();
        let transport = InMemoryTransport::new();

        outbox.flush(&transport);

        assert_eq!(transport.sent_count(), 0);
    }

    #[test]
    fn multiple_messages_to_same_destination_all_delivered() {
        let mut outbox = NetworkOutbox::new();
        let transport = InMemoryTransport::new();

        outbox.enqueue(NodeId(2), ConsensusStateId(0), Message::TimeoutNow);
        outbox.enqueue(NodeId(2), ConsensusStateId(1), Message::TimeoutNow);

        assert_eq!(outbox.len(), 2);

        outbox.flush(&transport);

        assert_eq!(transport.sent_count(), 2);
        assert!(outbox.is_empty());
    }

    #[test]
    fn consecutive_flushes_are_independent() {
        let mut outbox = NetworkOutbox::new();
        let transport = InMemoryTransport::new();

        outbox.enqueue(NodeId(1), ConsensusStateId(0), Message::TimeoutNow);
        outbox.flush(&transport);
        assert_eq!(transport.sent_count(), 1);

        outbox.enqueue(NodeId(2), ConsensusStateId(0), Message::TimeoutNow);
        outbox.enqueue(NodeId(3), ConsensusStateId(0), Message::TimeoutNow);
        outbox.flush(&transport);

        // Transport accumulates across flushes.
        assert_eq!(transport.sent_count(), 3);
    }
}
