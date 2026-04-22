//! Network transport abstraction for sending Raft messages between nodes.
//!
//! The [`NetworkTransport`] trait decouples the reactor's message-sending logic
//! from the actual network layer, enabling deterministic testing with
//! [`InMemoryTransport`].

use crate::{
    message::Message,
    types::{ConsensusStateId, NodeId},
};

/// A message destined for a remote node.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// Target node.
    pub to: NodeId,
    /// ConsensusState the message belongs to.
    pub shard: ConsensusStateId,
    /// The Raft protocol message.
    pub msg: Message,
}

/// Trait for sending batches of outbound Raft messages.
pub trait NetworkTransport: Send + 'static {
    /// Sends a batch of messages to their respective target nodes.
    fn send_batch(&self, messages: Vec<OutboundMessage>);

    /// Called when a shard's membership changes. Implementations can use this
    /// to clean up channels for departed peers.
    fn on_membership_changed(&self, _membership: &crate::types::Membership) {}
}

/// In-memory transport that collects messages for test inspection.
#[derive(Debug)]
pub struct InMemoryTransport {
    messages: parking_lot::Mutex<Vec<OutboundMessage>>,
}

impl InMemoryTransport {
    /// Creates a new empty in-memory transport.
    pub fn new() -> Self {
        Self { messages: parking_lot::Mutex::new(Vec::new()) }
    }

    /// Drains all collected messages, returning them and leaving the buffer empty.
    pub fn drain(&self) -> Vec<OutboundMessage> {
        let mut guard = self.messages.lock();
        std::mem::take(&mut *guard)
    }

    /// Returns the number of messages currently held.
    pub fn sent_count(&self) -> usize {
        self.messages.lock().len()
    }
}

impl Default for InMemoryTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkTransport for InMemoryTransport {
    fn send_batch(&self, messages: Vec<OutboundMessage>) {
        self.messages.lock().extend(messages);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_transport_collects_and_drains() {
        let transport = InMemoryTransport::new();

        assert_eq!(transport.sent_count(), 0);

        transport.send_batch(vec![
            OutboundMessage { to: NodeId(2), shard: ConsensusStateId(1), msg: Message::TimeoutNow },
            OutboundMessage { to: NodeId(3), shard: ConsensusStateId(1), msg: Message::TimeoutNow },
        ]);

        assert_eq!(transport.sent_count(), 2);

        let drained = transport.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].to, NodeId(2));
        assert_eq!(drained[1].to, NodeId(3));

        // Buffer is empty after drain.
        assert_eq!(transport.sent_count(), 0);
        assert!(transport.drain().is_empty());
    }

    #[test]
    fn in_memory_transport_accumulates_across_batches() {
        let transport = InMemoryTransport::new();

        transport.send_batch(vec![OutboundMessage {
            to: NodeId(1),
            shard: ConsensusStateId(0),
            msg: Message::TimeoutNow,
        }]);
        transport.send_batch(vec![OutboundMessage {
            to: NodeId(2),
            shard: ConsensusStateId(0),
            msg: Message::TimeoutNow,
        }]);

        assert_eq!(transport.sent_count(), 2);

        let drained = transport.drain();
        assert_eq!(drained.len(), 2);
    }

    #[test]
    fn empty_batch_does_not_change_count() {
        let transport = InMemoryTransport::new();
        transport.send_batch(vec![]);
        assert_eq!(transport.sent_count(), 0);
        assert!(transport.drain().is_empty());
    }
}
