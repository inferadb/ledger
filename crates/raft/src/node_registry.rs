//! Node-level connection registry.
//!
//! Owns one [`PeerConnection`] per peer `NodeId`, shared across all
//! server-to-server subsystems: consensus transport (Phase 2 bidi stream),
//! saga orchestration (`SubmitRegionalProposal`), follower `ReadIndex`
//! consistency queries, and discovery announcements. HTTP/2 multiplexing
//! means a single TCP connection per peer serves every subsystem,
//! replacing the previous per-region, per-subsystem channel duplication.
//!
//! Client-request forwarding was removed in Phase 5; clients route
//! directly to regional leaders via `NotLeader` hints (see
//! `docs/operations/runbooks/architecture-redirect-routing.md`).

use std::{collections::HashMap, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto;
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

use crate::types::LedgerNodeId;

/// Keepalive interval for inter-node HTTP/2 connections.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Keepalive timeout — close connection if no pong within this window.
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP-level keepalive — probes idle sockets even without HTTP/2 traffic.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Per-peer connection bundle: one `Channel` plus pre-constructed tonic
/// service clients so callers obtain typed clients without per-call
/// construction.
///
/// `Channel::clone` is cheap (internally `Arc`), so the client accessor
/// methods return fresh client instances that share the underlying
/// HTTP/2 connection.
pub struct PeerConnection {
    node_id: LedgerNodeId,
    addr: String,
    channel: Channel,
}

impl PeerConnection {
    /// Returns the peer node id.
    pub fn node_id(&self) -> LedgerNodeId {
        self.node_id
    }

    /// Returns the peer address (host:port) the channel was constructed for.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Returns a clone of the underlying tonic [`Channel`]. Cloning is
    /// cheap — internally it's an `Arc` over the shared HTTP/2 connection.
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// Constructs a new Raft service client over the shared channel.
    pub fn raft_client(&self) -> proto::raft_service_client::RaftServiceClient<Channel> {
        proto::raft_service_client::RaftServiceClient::new(self.channel.clone())
    }
}

/// Node-level connection registry.
///
/// One instance per `LedgerServer` process. Keyed by peer `NodeId`. Single
/// `Channel` per peer — HTTP/2 multiplexes all subsystems over it.
///
/// Concurrent `get_or_register` calls for the same `NodeId` coalesce onto
/// a single `OnceCell`, so channel construction never races.
#[derive(Default)]
pub struct NodeConnectionRegistry {
    entries: RwLock<HashMap<LedgerNodeId, Arc<OnceCell<Arc<PeerConnection>>>>>,
}

impl NodeConnectionRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the registered peer if present. Does not create an entry.
    #[must_use]
    pub fn get(&self, node: LedgerNodeId) -> Option<Arc<PeerConnection>> {
        self.entries.read().get(&node).and_then(|cell| cell.get().cloned())
    }

    /// Returns or creates the peer entry. Concurrent callers for the same
    /// `node_id` observe the same `Arc<PeerConnection>`.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::InvalidAddress`] when `addr` does not parse
    /// as a valid URI.
    pub async fn get_or_register(
        &self,
        node: LedgerNodeId,
        addr: &str,
    ) -> Result<Arc<PeerConnection>, RegistryError> {
        // If a prior entry exists for `node` with a different address
        // (peer restart, container rescheduling, topology change), evict
        // it so the next init builds a fresh channel to the new address.
        // Without this, `get_or_try_init` pins the first address forever.
        {
            let entries = self.entries.read();
            if let Some(cell) = entries.get(&node)
                && let Some(existing) = cell.get()
                && existing.addr() != addr
            {
                drop(entries);
                // Race-ok: if another caller already evicted, remove is a no-op.
                self.entries.write().remove(&node);
                crate::metrics::record_node_connection_event(node, "replaced");
            }
        }

        let cell = {
            let mut entries = self.entries.write();
            let was_new = !entries.contains_key(&node);
            let cell = Arc::clone(entries.entry(node).or_insert_with(|| Arc::new(OnceCell::new())));
            if was_new {
                // Emit the gauge here so we only fire on real inserts; concurrent
                // lookups that hit an existing `OnceCell` skip this path.
                crate::metrics::record_node_connections_active(entries.len());
            }
            cell
        };
        let conn = cell
            .get_or_try_init(|| async {
                let conn = Self::make_connection(node, addr)?;
                crate::metrics::record_node_connection_event(node, "registered");
                Ok::<_, RegistryError>(conn)
            })
            .await?;
        Ok(Arc::clone(conn))
    }

    fn make_connection(
        node: LedgerNodeId,
        addr: &str,
    ) -> Result<Arc<PeerConnection>, RegistryError> {
        let channel = if addr.starts_with('/') {
            // Unix Domain Socket path — use a UDS connector instead of TCP.
            let endpoint = Endpoint::try_from("http://[::]:50051").map_err(|e| {
                RegistryError::InvalidAddress { addr: addr.to_owned(), reason: e.to_string() }
            })?;
            let path = std::path::PathBuf::from(addr);
            endpoint.connect_with_connector_lazy(tower::service_fn(
                move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                            tokio::net::UnixStream::connect(&path).await?,
                        ))
                    }
                },
            ))
        } else {
            let uri = format!("http://{addr}");
            let endpoint = Endpoint::from_shared(uri).map_err(|e| {
                RegistryError::InvalidAddress { addr: addr.to_owned(), reason: e.to_string() }
            })?;
            let endpoint = endpoint
                .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
                .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
                .keep_alive_while_idle(true)
                .tcp_keepalive(Some(TCP_KEEPALIVE));
            endpoint.connect_lazy()
        };
        Ok(Arc::new(PeerConnection { node_id: node, addr: addr.to_owned(), channel }))
    }

    /// Removes a peer entry. Returns the dropped connection if one was
    /// present (in case the caller wants to drain in-flight state before
    /// release).
    pub fn unregister(&self, node: LedgerNodeId) -> Option<Arc<PeerConnection>> {
        let removed = self.entries.write().remove(&node).and_then(|cell| cell.get().cloned());
        if removed.is_some() {
            crate::metrics::record_node_connection_event(node, "unregistered");
            crate::metrics::record_node_connections_active(self.len());
        }
        removed
    }

    /// Prunes peer entries not present in the given membership.
    pub fn on_membership_changed(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        let valid: std::collections::HashSet<u64> =
            membership.voters.iter().chain(membership.learners.iter()).map(|n| n.0).collect();
        let pruned: Vec<u64> = {
            let mut entries = self.entries.write();
            let pruned: Vec<u64> =
                entries.keys().copied().filter(|id| !valid.contains(id)).collect();
            entries.retain(|id, _| valid.contains(id));
            pruned
        };
        if !pruned.is_empty() {
            for id in pruned {
                crate::metrics::record_node_connection_event(id, "pruned");
            }
            crate::metrics::record_node_connections_active(self.len());
        }
    }

    /// Returns the number of peer entries currently registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true when no peers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the list of currently registered peer node ids.
    #[must_use]
    pub fn peers(&self) -> Vec<LedgerNodeId> {
        self.entries.read().keys().copied().collect()
    }
}

/// Errors returned by [`NodeConnectionRegistry`].
#[derive(Debug, Snafu)]
pub enum RegistryError {
    /// The provided peer address could not be parsed as a URI.
    #[snafu(display("Invalid peer address {addr}: {reason}"))]
    InvalidAddress {
        /// The address that failed to parse.
        addr: String,
        /// The underlying parse error message.
        reason: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_creates_peer() {
        let reg = NodeConnectionRegistry::new();
        let conn = reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        assert_eq!(conn.node_id(), 2);
        assert_eq!(conn.addr(), "127.0.0.1:5000");
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn concurrent_register_coalesces() {
        let reg = Arc::new(NodeConnectionRegistry::new());
        let mut handles = Vec::new();
        for _ in 0..50 {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                reg.get_or_register(2, "127.0.0.1:5000").await.unwrap()
            }));
        }
        let mut conns = Vec::new();
        for h in handles {
            conns.push(h.await.unwrap());
        }
        let first = Arc::as_ptr(&conns[0]);
        for c in &conns[1..] {
            assert_eq!(Arc::as_ptr(c), first, "registry must coalesce concurrent get_or_register");
        }
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn unregister_removes_entry() {
        let reg = NodeConnectionRegistry::new();
        reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        assert!(reg.get(2).is_some());
        reg.unregister(2);
        assert!(reg.get(2).is_none());
        assert_eq!(reg.len(), 0);
    }

    #[tokio::test]
    async fn membership_change_prunes_absent_peers() {
        use inferadb_ledger_consensus::types::{Membership, NodeId};
        let reg = NodeConnectionRegistry::new();
        reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        reg.get_or_register(3, "127.0.0.1:5001").await.unwrap();
        reg.get_or_register(4, "127.0.0.1:5002").await.unwrap();
        assert_eq!(reg.len(), 3);

        let membership = Membership::new([NodeId(1), NodeId(2), NodeId(4)]);
        reg.on_membership_changed(&membership);

        assert_eq!(reg.len(), 2);
        assert!(reg.get(2).is_some());
        assert!(reg.get(3).is_none());
        assert!(reg.get(4).is_some());
    }

    #[tokio::test]
    async fn registry_deduplicates_across_regions() {
        // Spec Criterion 1: a node participating in R regions with P overlapping
        // peers holds P PeerConnection entries, not R × P. Simulate 5 region
        // startups each registering the same 3 peer node ids.
        let reg = NodeConnectionRegistry::new();
        for _region in 0..5 {
            reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
            reg.get_or_register(3, "127.0.0.1:5001").await.unwrap();
            reg.get_or_register(4, "127.0.0.1:5002").await.unwrap();
        }
        assert_eq!(
            reg.len(),
            3,
            "registry must dedupe across regions — expected 3 peers, got {}",
            reg.len()
        );
    }

    #[tokio::test]
    async fn registry_entries_are_identical_across_region_registrations() {
        // Spec Criterion 2 (testable invariant): each region-startup path that
        // calls get_or_register for the same peer must observe the same
        // Arc<PeerConnection>, confirming a single shared HTTP/2 channel.
        let reg = NodeConnectionRegistry::new();
        let first = reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        let second = reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        let third = reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &third));
    }

    #[tokio::test]
    async fn register_with_new_address_replaces_entry() {
        let reg = NodeConnectionRegistry::new();
        let first = reg.get_or_register(2, "127.0.0.1:5000").await.unwrap();
        assert_eq!(first.addr(), "127.0.0.1:5000");

        let second = reg.get_or_register(2, "127.0.0.1:5001").await.unwrap();
        assert_eq!(second.addr(), "127.0.0.1:5001");
        assert!(!Arc::ptr_eq(&first, &second), "address change must produce a new PeerConnection");

        // A subsequent lookup at the new address returns the same (new) entry.
        let third = reg.get_or_register(2, "127.0.0.1:5001").await.unwrap();
        assert!(Arc::ptr_eq(&second, &third));
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn invalid_address_returns_error() {
        let reg = NodeConnectionRegistry::new();
        let err = reg.get_or_register(2, "not a valid uri").await;
        assert!(err.is_err());
    }
}
