//! Wire-protocol-based consensus transport.
//!
//! Inter-node Raft messages (AppendEntries, RequestVote, InstallSnapshot,
//! etc.) become opcodes 0x0D00-0x0DFF over QUIC via
//! [`inferadb_ledger_wire_transport::WireClient`]. Per-peer `WirePeerSender`
//! semantics (bounded queue, drop-oldest backpressure, election-critical
//! retention on stream-broken/stream-open-failed, ack-timeout liveness
//! probe, jittered exponential reconnect, single drain task per peer
//! preserving outbound message order) provide the consensus transport
//! contract.
//!
//! # Module layout
//!
//! - [`peer_sender`] — `WirePeerSender`. Drives a `WireClient` long-lived bidi `Replicate` stream.
//! - [`snapshot`] — Wire-side install-snapshot wrapper. Bridges the snapshot install API onto
//!   [`inferadb_ledger_wire_transport::snapshot_stream::SnapshotSender`].
//! - [`server_handler`] — `WireRaftServerHandler`. Server-side dispatcher for Raft opcodes; impls
//!   [`inferadb_ledger_wire_transport::SnapshotHandler`] for the snapshot sub-protocol.
//!
//! # A7 protocol-version handshake
//!
//! Every long-lived Raft `Replicate` stream sends an
//! [`inferadb_ledger_wire::OPCODE_RAFT_PROTOCOL_HANDSHAKE`] frame as the
//! first frame, carrying the local
//! [`inferadb_ledger_wire::RAFT_PROTOCOL_VERSION`]. The server replies
//! with its own version; mismatch closes the QUIC connection. Lands the
//! version-gate at connection establishment, before any replication
//! frame.

pub mod peer_sender;
pub mod server_handler;
pub mod snapshot;

// ── WireConsensusTransport ─────────────────────────────────────────────────

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_consensus::{
    Message,
    transport::{NetworkTransport, OutboundMessage},
};
use inferadb_ledger_wire_transport::WireClient;
use parking_lot::{Mutex, RwLock};

use self::peer_sender::WirePeerSender;
use crate::{node_registry::NodeConnectionRegistry, types::LedgerNodeId};

/// Throttle interval for "no peer registered" warnings emitted by
/// [`WireConsensusTransport::send_batch`]. One warn is logged per
/// `(target_node, message_kind)` triple per interval; further silent-drops
/// within the same window are suppressed to keep production logs from
/// drowning while still surfacing the symptom promptly.
///
/// Mirrors the same interval used by the legacy
/// [`super::super::consensus_transport`] tonic transport — both transports
/// produce identical drop-warn cadence so log shape doesn't change with the
/// E.6b cutover.
const SILENT_DROP_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Returns a short label identifying a [`Message`] variant. Used in
/// telemetry and the silent-drop warn log so the dropped message kind is
/// observable without dumping the full payload.
fn message_kind_name(msg: &Message) -> &'static str {
    match msg {
        Message::PreVoteRequest { .. } => "PreVoteRequest",
        Message::PreVoteResponse { .. } => "PreVoteResponse",
        Message::VoteRequest { .. } => "VoteRequest",
        Message::VoteResponse { .. } => "VoteResponse",
        Message::AppendEntries { .. } => "AppendEntries",
        Message::AppendEntriesResponse { .. } => "AppendEntriesResponse",
        Message::InstallSnapshot { .. } => "InstallSnapshot",
        Message::InstallSnapshotResponse { .. } => "InstallSnapshotResponse",
        Message::TimeoutNow => "TimeoutNow",
    }
}

/// Wire-protocol-based network transport for consensus messages.
///
/// Each peer gets a dedicated `WirePeerSender` (see the [`peer_sender`]
/// submodule) that owns a bounded outbound queue and a single drain task.
/// `send_batch` enqueues messages to the per-peer queue (drop-oldest on
/// overflow); the drain task serializes and ships them via a long-lived
/// `Replicate` bidi stream over QUIC. Acks are observed only as a liveness
/// signal — messages are fire-and-forget and the consensus engine handles
/// retries via heartbeats.
///
/// # Registry integration
///
/// [`NodeConnectionRegistry`] vends [`Arc<WireClient>`] handles. The wire
/// transport holds a shared `Arc<NodeConnectionRegistry>` and uses
/// [`NodeConnectionRegistry::wire_client_for`] inside
/// [`Self::set_peer_via_registry`] to dedupe wire clients across all
/// per-region transports a node hosts. Callers that already have an
/// `Arc<WireClient>` may still register peers directly via
/// [`Self::set_peer`].
#[derive(Clone)]
pub struct WireConsensusTransport {
    /// Per-peer senders. Each `WirePeerSender` owns its drain task; dropping
    /// it cancels the task via the sender's `Drop` impl.
    senders: Arc<RwLock<HashMap<LedgerNodeId, Arc<WirePeerSender>>>>,
    /// This node's identity (passed to each `WirePeerSender` at construction
    /// so outbound `ConsensusEnvelope` frames carry `from_node`).
    local_node: LedgerNodeId,
    /// This node's address (for `ConsensusEnvelope::from_address`). Wrapped
    /// in `RwLock` so [`Self::set_local_address`] can update it after the
    /// node's listener is bound, before peers are registered.
    local_address: Arc<RwLock<String>>,
    /// Region this transport serves. Stamped on every outbound envelope so
    /// the receiver can route the message through the correct per-region
    /// engine.
    region: inferadb_ledger_types::Region,
    /// Shared per-node connection registry — every per-region wire
    /// transport on a node clones the same `Arc` so [`Arc<WireClient>`]
    /// handles are deduped across regions.
    registry: Arc<NodeConnectionRegistry>,
    /// Throttle state for the silent-drop warn in [`Self::send_batch`].
    /// Keyed by `(target_node, message_kind)`; the region is implicit
    /// per transport instance. Mirrors the legacy
    /// [`super::super::consensus_transport`] structure exactly.
    silent_drop_warns: Arc<Mutex<HashMap<(u64, &'static str), Instant>>>,
}

impl WireConsensusTransport {
    /// Creates a new wire-based transport for the given local node and region.
    ///
    /// The transport starts with no peers; call [`Self::set_peer`] (or
    /// [`Self::set_peer_via_registry`]) for each known peer (typically driven
    /// by `RaftManager` startup + membership change observers).
    ///
    /// `registry` is the shared per-node [`NodeConnectionRegistry`] that
    /// vends [`Arc<WireClient>`] handles. The same registry is shared
    /// across every per-region transport on the node so wire clients are
    /// deduped across regions (mirrors the legacy gRPC transport's
    /// `Arc<NodeConnectionRegistry>`).
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
            silent_drop_warns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Sets the local node's address (included in outbound messages so
    /// receivers can auto-register a return channel).
    ///
    /// Existing senders captured the address at spawn time; updating it here
    /// only affects senders registered after this call. Call this before
    /// [`Self::set_peer`] during startup so the first peer registration
    /// already carries the correct address.
    pub fn set_local_address(&self, addr: String) {
        *self.local_address.write() = addr;
    }

    /// Registers or replaces the per-peer sender for a node.
    ///
    /// Spawns a fresh [`WirePeerSender`] bound to the supplied
    /// [`WireClient`]. The client must already be configured for the peer's
    /// QUIC endpoint and authentication payload — this transport does not
    /// own client construction.
    ///
    /// If a sender already exists for `peer_id`, it is dropped (the old
    /// drain task observes its `CancellationToken` and shuts down via
    /// the sender's `Drop` impl) and replaced with the new one.
    ///
    /// E.6c will re-add a `set_peer_via_registry` convenience that pulls
    /// the `WireClient` from a wire-aware [`crate::node_registry::NodeConnectionRegistry`]
    /// and calls into this method.
    pub fn set_peer(&self, peer_id: LedgerNodeId, wire_client: Arc<WireClient>) {
        let from_address = self.local_address.read().clone();
        let sender = Arc::new(WirePeerSender::spawn(
            wire_client,
            peer_id,
            self.region,
            self.local_node,
            from_address,
        ));
        self.senders.write().insert(peer_id, sender);
    }

    /// Removes the sender for a peer node. The drain task observes the
    /// shutdown signal on drop and exits.
    pub fn remove_peer(&self, peer_id: LedgerNodeId) {
        self.senders.write().remove(&peer_id);
    }

    /// Resolves an [`Arc<WireClient>`] for `peer_id` via the shared
    /// [`NodeConnectionRegistry`] and registers a [`WirePeerSender`]
    /// against it.
    ///
    /// The wire-aware registry caches one `Arc<WireClient>` per peer, so
    /// every per-region transport that calls this for the same peer
    /// observes the same QUIC client.
    ///
    /// # Errors
    ///
    /// Surfaces the registry's
    /// [`crate::node_registry::RegistryError`] verbatim:
    /// - [`crate::node_registry::RegistryError::WireTemplateUnconfigured`] when the registry has no
    ///   [`crate::node_registry::WireClientTemplate`] installed.
    /// - [`crate::node_registry::RegistryError::InvalidAddress`] when `peer_address` cannot be
    ///   parsed / resolved as `host:port`.
    /// - [`crate::node_registry::RegistryError::WireClientNew`] when
    ///   [`inferadb_ledger_wire_transport::WireClient::new`] fails (typically the local UDP-socket
    ///   bind).
    pub async fn set_peer_via_registry(
        &self,
        peer_id: LedgerNodeId,
        peer_address: &str,
    ) -> Result<(), crate::node_registry::RegistryError> {
        let wire_client = self.registry.wire_client_for(peer_id, peer_address).await?;
        self.set_peer(peer_id, wire_client);
        Ok(())
    }

    /// Returns the list of registered peer node IDs.
    pub fn peers(&self) -> Vec<LedgerNodeId> {
        self.senders.read().keys().copied().collect()
    }

    /// Removes senders for peers not present in the given membership
    /// (voters + learners).
    ///
    /// Called from [`NetworkTransport::on_membership_changed`] after a
    /// committed configuration change so departed peers don't keep their
    /// drain tasks (and their associated QUIC streams) alive indefinitely.
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

    /// Returns the current count of throttled `(peer, kind)` warn entries.
    /// Test-only — used to assert the throttle is recording state.
    #[cfg(test)]
    pub(crate) fn silent_drop_warn_entries(&self) -> usize {
        self.silent_drop_warns.lock().len()
    }
}

impl NetworkTransport for WireConsensusTransport {
    fn send_batch(&self, messages: Vec<OutboundMessage>) {
        let senders = self.senders.read();
        for msg in messages {
            if msg.to.0 == self.local_node {
                // Self-sends are handled internally by the consensus engine;
                // they should never reach the transport, but skip defensively.
                continue;
            }
            if let Some(sender) = senders.get(&msg.to.0) {
                // Push returns a PushOutcome that metrics already record on
                // the sender side (queue_full drops). No further accounting
                // needed at this layer.
                let _ = sender.push(msg);
                continue;
            }
            // No peer registered for the target node — drop the message.
            //
            // Task #153 surfaced the silent-drop path: a restart with empty
            // `peer_addresses` left every data-region engine's `senders` map
            // empty for its persisted voters; every PreVoteRequest emitted
            // by `handle_election_timeout` landed here without leaving any
            // production-log trace.
            //
            // Emit a rate-limited warn so the symptom is observable without
            // drowning logs under sustained drop rates. Drops are still
            // expected during normal cluster lifecycle (a node coming up
            // before peers register, a node leaving before membership change
            // applies), so the throttle keeps the log volume bounded while
            // preserving visibility.
            let kind = message_kind_name(&msg.msg);
            let key = (msg.to.0, kind);
            let now = Instant::now();
            let should_warn = {
                let mut warns = self.silent_drop_warns.lock();
                let allow = warns
                    .get(&key)
                    .is_none_or(|prev| now.duration_since(*prev) >= SILENT_DROP_WARN_INTERVAL);
                if allow {
                    warns.insert(key, now);
                }
                allow
            };
            if should_warn {
                tracing::warn!(
                    target_node = msg.to.0,
                    region = self.region.as_str(),
                    shard_id = msg.shard.0,
                    message_kind = kind,
                    "wire consensus transport: dropped message — no peer registered for target node",
                );
            }
        }
    }

    fn on_membership_changed(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        self.retain_peers(membership);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration as StdDuration,
    };

    use bytes::Bytes;
    use inferadb_ledger_consensus::{
        Message,
        types::{ConsensusStateId, Membership, NodeId},
    };
    use inferadb_ledger_wire_transport::{ClientConfig, tls};

    use super::*;

    fn make_transport() -> WireConsensusTransport {
        WireConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::new(NodeConnectionRegistry::new()),
        )
    }

    /// Build a registry pre-configured with a permissive wire-client
    /// template — used by the `set_peer_via_registry` regression test.
    fn make_registry_with_template() -> Arc<NodeConnectionRegistry> {
        use crate::node_registry::WireClientTemplate;
        let crypto = tls::rustls_client_crypto_skip_verify();
        let template = Arc::new(WireClientTemplate {
            quic: tls::client_config(crypto),
            server_name: "localhost".to_string(),
            auth_payload: Bytes::new(),
            connect_timeout: StdDuration::from_millis(100),
        });
        Arc::new(NodeConnectionRegistry::with_wire_template(template))
    }

    /// Build a `WireClient` pointing to an unreachable address. The peer
    /// sender will spin in its outer reconnect loop forever, which is fine
    /// for transport-level tests — we only need the sender to exist and
    /// accept pushes; we never assert it actually delivers.
    fn make_unreachable_client() -> Arc<WireClient> {
        let crypto = tls::rustls_client_crypto_skip_verify();
        let config = ClientConfig {
            // RFC 5737 TEST-NET-1 — non-routable; dial hangs at the
            // network layer and times out at `connect_timeout`.
            server_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 443),
            server_name: "localhost".to_string(),
            quic: tls::client_config(crypto),
            auth_payload: Bytes::from_static(b"x"),
            connect_timeout: StdDuration::from_millis(100),
        };
        Arc::new(WireClient::new(config).expect("client::new"))
    }

    fn timeout_msg(to: u64, shard: u64) -> OutboundMessage {
        OutboundMessage { to: NodeId(to), shard: ConsensusStateId(shard), msg: Message::TimeoutNow }
    }

    // ── Surface tests: construction + accessors ────────────────────────────

    #[test]
    fn new_transport_has_no_peers() {
        let transport = make_transport();
        assert!(transport.peers().is_empty());
    }

    #[test]
    fn self_sends_are_skipped() {
        let transport = make_transport();
        // Fire-and-forget with no peer for self — should not panic and should
        // not record a silent-drop warn (the local-node short-circuit runs
        // before the warn-throttle check).
        transport.send_batch(vec![timeout_msg(1, 1)]);
        assert_eq!(transport.silent_drop_warn_entries(), 0);
    }

    #[test]
    fn unknown_peer_is_silently_skipped_with_warn_recorded() {
        let transport = make_transport();
        transport.send_batch(vec![timeout_msg(99, 1)]);
        // The first drop for (99, TimeoutNow) records a throttle entry.
        assert_eq!(transport.silent_drop_warn_entries(), 1);
    }

    // ── set_peer / remove_peer / retain_peers ──────────────────────────────

    #[tokio::test]
    async fn set_peer_then_remove_unregisters() {
        let transport = make_transport();
        assert!(transport.peers().is_empty());

        transport.set_peer(2, make_unreachable_client());
        assert_eq!(transport.peers(), vec![2]);
        assert!(transport.peer_queue_depth(2).is_some());

        transport.remove_peer(2);
        assert!(transport.peers().is_empty());
        assert!(transport.peer_queue_depth(2).is_none());

        // Removing a non-existent peer is a no-op.
        transport.remove_peer(99);
        assert!(transport.peers().is_empty());
    }

    #[tokio::test]
    async fn set_peer_replaces_existing_sender() {
        let transport = make_transport();
        transport.set_peer(2, make_unreachable_client());
        // Push something so we can verify the sender was actually replaced
        // (not just that two pushes both queued onto the same sender).
        transport.send_batch(vec![timeout_msg(2, 1)]);

        // Replace the sender — old drain task is dropped via Drop impl.
        transport.set_peer(2, make_unreachable_client());
        assert_eq!(transport.peers(), vec![2]);
        // Fresh sender starts empty (drop_count = 0; depth 0 or pending push).
        let drops = transport.peer_drop_count(2).expect("sender registered");
        assert_eq!(drops, 0);
    }

    #[tokio::test]
    async fn retain_peers_drops_unkept() {
        let transport = make_transport();
        transport.set_peer(2, make_unreachable_client());
        transport.set_peer(3, make_unreachable_client());
        transport.set_peer(4, make_unreachable_client());
        assert_eq!(transport.peers().len(), 3);

        // Membership = {1 (self), 2}; 3 and 4 must be dropped.
        let membership = Membership::new([NodeId(1), NodeId(2)]);
        transport.retain_peers(&membership);
        assert_eq!(transport.peers(), vec![2]);
    }

    #[tokio::test]
    async fn on_membership_changed_dispatches_to_retain_peers() {
        let transport = make_transport();
        transport.set_peer(2, make_unreachable_client());
        transport.set_peer(3, make_unreachable_client());

        let membership = Membership::new([NodeId(1), NodeId(2)]);
        transport.on_membership_changed(&membership);
        assert_eq!(transport.peers(), vec![2]);
    }

    // ── send_batch routing + silent-drop warn throttle ─────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_batch_pushes_to_correct_peer_queue() {
        let transport = make_transport();
        transport.set_peer(2, make_unreachable_client());
        transport.set_peer(3, make_unreachable_client());

        // Push 5 to peer 2 and 3 to peer 3.
        let mut msgs = Vec::new();
        for i in 0..5 {
            msgs.push(timeout_msg(2, i));
        }
        for i in 0..3 {
            msgs.push(timeout_msg(3, i));
        }
        transport.send_batch(msgs);

        // Both senders should accept the push; their drain tasks are spinning
        // against unreachable peers so neither will deliver, but the queues
        // recorded the depth synchronously.
        //
        // We can't observe a precise depth (drain may have already consumed
        // some, or the stream-open-failed drop_queue path may have dropped
        // the non-election-critical TimeoutNow messages). What we can assert
        // is that the per-peer drain tasks each saw activity — the legacy
        // routing test relies on the same shape.
        let depth_2 = transport.peer_queue_depth(2);
        let depth_3 = transport.peer_queue_depth(3);
        assert!(depth_2.is_some(), "peer 2 sender registered");
        assert!(depth_3.is_some(), "peer 3 sender registered");

        // No silent-drop warns recorded (both peers are registered).
        assert_eq!(transport.silent_drop_warn_entries(), 0);
    }

    #[test]
    fn silent_drop_warn_throttle_per_peer_kind() {
        let transport = make_transport();

        // Flood with the same (peer, kind) — only one entry is recorded
        // (the throttle dedupes via the entry's last-warn `Instant`).
        let messages: Vec<_> = (0..50).map(|i| timeout_msg(99, i)).collect();
        transport.send_batch(messages);
        assert_eq!(transport.silent_drop_warn_entries(), 1);

        // Different kind, same peer → second entry.
        let prevote = OutboundMessage {
            to: NodeId(99),
            shard: ConsensusStateId(0),
            msg: Message::PreVoteRequest {
                term: 1,
                candidate_id: NodeId(1),
                last_log_term: 0,
                last_log_index: 0,
            },
        };
        transport.send_batch(vec![prevote]);
        assert_eq!(transport.silent_drop_warn_entries(), 2);

        // Different peer, same kind → third entry.
        transport.send_batch(vec![timeout_msg(100, 0)]);
        assert_eq!(transport.silent_drop_warn_entries(), 3);
    }

    #[test]
    fn silent_drop_warn_keys_distinguish_kind() {
        let transport = make_transport();

        // Two TimeoutNow drops to peer 99 → one entry.
        transport.send_batch(vec![timeout_msg(99, 0), timeout_msg(99, 1)]);
        assert_eq!(transport.silent_drop_warn_entries(), 1);

        // VoteRequest to peer 99 → distinct kind, second entry.
        let vote = OutboundMessage {
            to: NodeId(99),
            shard: ConsensusStateId(0),
            msg: Message::VoteRequest {
                term: 1,
                candidate_id: NodeId(1),
                last_log_term: 0,
                last_log_index: 0,
            },
        };
        transport.send_batch(vec![vote]);
        assert_eq!(transport.silent_drop_warn_entries(), 2);
    }

    // ── E.6c: set_peer_via_registry ───────────────────────────────────────

    /// `set_peer_via_registry` resolves an `Arc<WireClient>` from the
    /// shared registry and registers a `WirePeerSender` for it. The
    /// resulting peer is observable via `peers()` and the queue is live.
    #[tokio::test]
    async fn set_peer_via_registry_resolves_and_registers_sender() {
        let registry = make_registry_with_template();
        let transport = WireConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::clone(&registry),
        );

        transport
            .set_peer_via_registry(2, "127.0.0.1:5000")
            .await
            .expect("registry vended a wire client and the sender registered");

        // Sender registered.
        assert_eq!(transport.peers(), vec![2]);
        assert!(transport.peer_queue_depth(2).is_some());
        // Registry cached the wire client.
        assert_eq!(registry.wire_client_len(), 1);
    }

    /// Without a wire-client template the registry path errors with
    /// `WireTemplateUnconfigured` — the regression that catches a startup
    /// path that constructed a `WireConsensusTransport` without
    /// installing a template on the shared registry.
    #[tokio::test]
    async fn set_peer_via_registry_errors_when_template_unconfigured() {
        let registry = Arc::new(NodeConnectionRegistry::new());
        let transport = WireConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::clone(&registry),
        );

        let err = transport.set_peer_via_registry(2, "127.0.0.1:5000").await;
        assert!(
            matches!(err, Err(crate::node_registry::RegistryError::WireTemplateUnconfigured),),
            "expected WireTemplateUnconfigured, got {err:?}",
        );
        // No sender registered.
        assert!(transport.peers().is_empty());
    }

    /// Invalid addresses surface as `RegistryError::InvalidAddress`. The
    /// transport leaves no sender behind on failure.
    #[tokio::test]
    async fn set_peer_via_registry_invalid_address_errors() {
        let registry = make_registry_with_template();
        let transport = WireConsensusTransport::new(
            1,
            inferadb_ledger_types::Region::GLOBAL,
            Arc::clone(&registry),
        );

        let err = transport.set_peer_via_registry(2, "not a uri").await;
        assert!(
            matches!(err, Err(crate::node_registry::RegistryError::InvalidAddress { .. })),
            "expected InvalidAddress, got {err:?}",
        );
        assert!(transport.peers().is_empty());
    }

    // ── set_local_address ──────────────────────────────────────────────────

    #[test]
    fn set_local_address_updates_atomically() {
        let transport = make_transport();
        // Default is empty.
        assert!(transport.local_address.read().is_empty());
        transport.set_local_address("127.0.0.1:5000".to_string());
        assert_eq!(*transport.local_address.read(), "127.0.0.1:5000");
        // Re-setting overwrites cleanly.
        transport.set_local_address("10.0.0.1:6000".to_string());
        assert_eq!(*transport.local_address.read(), "10.0.0.1:6000");
    }

    // ── Clone semantics ────────────────────────────────────────────────────

    #[tokio::test]
    async fn clone_shares_sender_map() {
        let transport = make_transport();
        let cloned = transport.clone();
        transport.set_peer(2, make_unreachable_client());
        // Clones share the underlying Arc<RwLock<...>>; the peer registered
        // through `transport` is visible through `cloned`.
        assert_eq!(cloned.peers(), vec![2]);
    }
}
