//! Node-level connection registry.
//!
//! Owns one [`Arc<WireClient>`] per peer `NodeId`, shared across all
//! server-to-server subsystems: consensus transport (Phase 2 bidi stream),
//! saga orchestration (`RegionalProposal`), follower `CommittedIndex`
//! consistency queries, snapshot streaming, and discovery announcements.
//! The underlying QUIC connection is shared across every subsystem.
//!
//! Client-request forwarding was removed in Phase 5; clients route
//! directly to regional leaders via `NotLeader` hints (see
//! `docs/architecture/request-routing.md`).
//!
//! Wire-client construction requires a [`WireClientTemplate`] to supply the
//! shared TLS / QUIC configuration. The template is supplied via
//! [`NodeConnectionRegistry::with_wire_template`] (or
//! [`NodeConnectionRegistry::set_wire_template`] post-construction). When no
//! template is configured, [`NodeConnectionRegistry::wire_client_for`]
//! returns [`RegistryError::WireTemplateUnconfigured`].

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use inferadb_ledger_wire_transport::{ClientConfig, WireClient};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::sync::OnceCell;

use crate::types::LedgerNodeId;

/// Shared TLS / QUIC configuration template used when the registry
/// constructs an [`Arc<WireClient>`] for a peer.
///
/// One template is shared across every peer in the cluster — only the
/// `server_addr` and `server_name` differ per peer (resolved from the
/// peer's address string at construction time). The template owns the
/// pre-built [`quinn::ClientConfig`] (TLS + transport caps) plus the
/// auth payload sent on the wire-transport's auth handshake frame.
///
/// # `auth_payload` is currently empty
///
/// Inter-node Raft traffic relies on **mTLS at the QUIC layer** for peer
/// authentication. The wire-transport auth handshake frame
/// (`OPCODE_AUTH_ESTABLISH`) carries an application-level payload —
/// typically a JWT for client → server SDK calls. For server-to-server
/// Raft, mTLS is sufficient and the auth payload is intentionally empty;
/// a future task can layer a node-identity token on top if defence-in-depth
/// is required, but it's not blocking E.6c.
///
/// The receiving server's
/// [`inferadb_ledger_wire_transport::AuthVerifier`] impl is expected to
/// be permissive for inter-node traffic (peer mTLS already authenticated
/// the connection).
#[derive(Clone)]
pub struct WireClientTemplate {
    /// QUIC client config — TLS roots, ALPN, transport caps. Built via
    /// [`inferadb_ledger_wire_transport::tls::client_config`] with a
    /// rustls config that trusts the cluster's CA (or, in test fixtures
    /// gated by `insecure-skip-verify`, an unverified rustls config).
    pub quic: quinn::ClientConfig,
    /// SNI / certificate verification name. Defaults to `"inferadb-ledger"`
    /// (a stable cluster-wide name; production deployments may choose a
    /// different convention so peer certs can be issued for it). The
    /// per-peer override takes effect when a hostname is provided in the
    /// peer's address; for raw IP addresses, this template-level name is
    /// used and must match the peer cert's SAN list.
    pub server_name: String,
    /// Bytes shipped in the `OPCODE_AUTH_ESTABLISH` frame on every fresh
    /// connection. Empty for inter-node Raft (mTLS authenticates the
    /// peer); reserved for future node-identity tokens.
    pub auth_payload: Bytes,
    /// Per-attempt connect+auth timeout. Reconnect retries restart this
    /// clock each iteration (mirrors `ClientConfig::connect_timeout`).
    pub connect_timeout: Duration,
}

/// Node-level connection registry.
///
/// One instance per `LedgerServer` process. Keyed by peer `NodeId`. Single
/// `Arc<WireClient>` per peer; the underlying QUIC connection is shared
/// across every subsystem (consensus transport, saga forwarding, follower
/// `CommittedIndex` probes, snapshot streaming, discovery).
///
/// Concurrent `wire_client_for` calls for the same `NodeId` coalesce onto
/// a single `OnceCell`, so client construction never races.
#[derive(Default)]
pub struct NodeConnectionRegistry {
    /// Wire-client cache.
    wire_clients: RwLock<HashMap<LedgerNodeId, WireEntry>>,
    /// Shared TLS / QUIC template used by [`Self::wire_client_for`]. When
    /// `None`, the wire-client path returns
    /// [`RegistryError::WireTemplateUnconfigured`].
    wire_template: RwLock<Option<Arc<WireClientTemplate>>>,
}

/// Cached wire-client entry. `addr` records the address the client was
/// constructed for so we can detect peer-address changes.
struct WireEntry {
    cell: Arc<OnceCell<Arc<WireClient>>>,
    addr: String,
}

impl NodeConnectionRegistry {
    /// Creates a new empty registry with no wire-client template.
    /// Callers that want the wire-client path must subsequently configure
    /// a template via [`Self::set_wire_template`] (or use
    /// [`Self::with_wire_template`] at construction).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new empty registry pre-configured with a wire-client
    /// template. Convenience for production callers that build the
    /// template at startup.
    #[must_use]
    pub fn with_wire_template(template: Arc<WireClientTemplate>) -> Self {
        Self {
            wire_clients: RwLock::new(HashMap::new()),
            wire_template: RwLock::new(Some(template)),
        }
    }

    /// Installs (or replaces) the shared wire-client template. Existing
    /// cached `Arc<WireClient>` entries keep using their original
    /// template — the new template only affects clients constructed after
    /// this call. Call this before the first
    /// [`Self::wire_client_for`] invocation during startup.
    pub fn set_wire_template(&self, template: Arc<WireClientTemplate>) {
        *self.wire_template.write() = Some(template);
    }

    /// Returns `true` when a [`WireClientTemplate`] is installed.
    ///
    /// Used by integration tests + diagnostic surfaces (admin RPC) to
    /// confirm the wire transport's prerequisite is in place before
    /// the first peer dial. Production callers go straight to
    /// [`Self::wire_client_for`] and let the
    /// [`RegistryError::WireTemplateUnconfigured`] error propagate.
    #[must_use]
    pub fn has_wire_template(&self) -> bool {
        self.wire_template.read().is_some()
    }

    /// Returns or constructs the [`Arc<WireClient>`] for the given peer.
    /// Concurrent callers for the same `node_id` (with the same `addr`)
    /// observe the same `Arc<WireClient>` — construction never races.
    ///
    /// When a prior entry exists for the same peer at a different
    /// address, the existing entry is evicted before the new one is
    /// constructed.
    ///
    /// # Errors
    ///
    /// - [`RegistryError::WireTemplateUnconfigured`] — no wire template has been set on the
    ///   registry.
    /// - [`RegistryError::InvalidAddress`] — `addr` could not be parsed as `host:port` or resolved
    ///   to a [`std::net::SocketAddr`].
    /// - [`RegistryError::WireClientNew`] — [`WireClient::new`] failed (typically the local UDP
    ///   bind).
    pub async fn wire_client_for(
        &self,
        node: LedgerNodeId,
        addr: &str,
    ) -> Result<Arc<WireClient>, RegistryError> {
        // Address-change eviction: a peer restarting at a new address
        // must not reuse the cached client pinned to the old one.
        {
            let entries = self.wire_clients.read();
            if let Some(entry) = entries.get(&node)
                && entry.addr != addr
            {
                drop(entries);
                self.wire_clients.write().remove(&node);
                crate::metrics::record_node_connection_event(node, "wire_replaced");
            }
        }

        let cell = {
            let mut entries = self.wire_clients.write();
            let was_new = !entries.contains_key(&node);
            let entry = entries.entry(node).or_insert_with(|| WireEntry {
                cell: Arc::new(OnceCell::new()),
                addr: addr.to_owned(),
            });
            if was_new {
                crate::metrics::record_node_connection_event(node, "wire_registered");
            }
            Arc::clone(&entry.cell)
        };

        // Snapshot the template under the lock — held only long enough to
        // clone the Arc, no I/O.
        let template = self
            .wire_template
            .read()
            .as_ref()
            .map(Arc::clone)
            .ok_or(RegistryError::WireTemplateUnconfigured)?;

        let client = cell
            .get_or_try_init(|| async {
                let config = build_wire_client_config(addr, &template)?;
                WireClient::new(config).map(Arc::new).map_err(|e| RegistryError::WireClientNew {
                    addr: addr.to_owned(),
                    reason: e.to_string(),
                })
            })
            .await?;
        Ok(Arc::clone(client))
    }

    /// Drops the cached [`Arc<WireClient>`] for `node`, if one exists.
    /// The next [`Self::wire_client_for`] call constructs a fresh
    /// client. Used on disconnect / membership removal so a torn-down
    /// peer doesn't leave a stale client behind.
    pub fn drop_wire_client(&self, node: LedgerNodeId) -> bool {
        let removed = self.wire_clients.write().remove(&node).is_some();
        if removed {
            crate::metrics::record_node_connection_event(node, "wire_unregistered");
        }
        removed
    }

    /// Prunes wire-client entries not present in the given membership.
    /// A peer departing the cluster releases its `Arc<WireClient>`.
    pub fn on_membership_changed(&self, membership: &inferadb_ledger_consensus::types::Membership) {
        let valid: std::collections::HashSet<u64> =
            membership.voters.iter().chain(membership.learners.iter()).map(|n| n.0).collect();
        let pruned_wire: Vec<u64> = {
            let mut entries = self.wire_clients.write();
            let pruned: Vec<u64> =
                entries.keys().copied().filter(|id| !valid.contains(id)).collect();
            entries.retain(|id, _| valid.contains(id));
            pruned
        };
        if !pruned_wire.is_empty() {
            for id in pruned_wire {
                crate::metrics::record_node_connection_event(id, "wire_pruned");
            }
        }
    }

    /// Returns the number of cached wire-client entries.
    #[must_use]
    pub fn wire_client_len(&self) -> usize {
        self.wire_clients.read().len()
    }

    /// Constructs a one-off [`Arc<WireClient>`] for `addr` using the
    /// installed [`WireClientTemplate`], without inserting it into the
    /// per-peer cache.
    ///
    /// Used by callers that dial a peer whose `LedgerNodeId` is not yet
    /// known — most notably [`crate::wire_consensus_transport`] consumers
    /// in the bootstrap path's discovery loop. The cache is keyed by node
    /// id, so a discovery dial cannot reuse a slot, and reusing a sentinel
    /// id (e.g. `LedgerNodeId(0)`) would collide with subsequent dials to
    /// different unknown peers.
    ///
    /// The returned client is not retained by the registry; the caller
    /// holds the only `Arc`. Drop it after the discovery handshake to
    /// release the QUIC connection.
    ///
    /// # Errors
    ///
    /// - [`RegistryError::WireTemplateUnconfigured`] when the registry has no
    ///   [`WireClientTemplate`] installed.
    /// - [`RegistryError::InvalidAddress`] when `addr` cannot be parsed / resolved as `host:port`.
    /// - [`RegistryError::WireClientNew`] when [`WireClient::new`] fails (typically the local
    ///   UDP-socket bind).
    pub fn build_anonymous_wire_client(
        &self,
        addr: &str,
    ) -> Result<Arc<WireClient>, RegistryError> {
        let template = self
            .wire_template
            .read()
            .as_ref()
            .map(Arc::clone)
            .ok_or(RegistryError::WireTemplateUnconfigured)?;
        let config = build_wire_client_config(addr, &template)?;
        let client = WireClient::new(config).map_err(|e| RegistryError::WireClientNew {
            addr: addr.to_owned(),
            reason: e.to_string(),
        })?;
        Ok(Arc::new(client))
    }
}

/// Resolves `addr` (`host:port`) to a [`std::net::SocketAddr`] and
/// builds a [`ClientConfig`] sourced from the shared template.
///
/// Hostnames are resolved synchronously via
/// [`std::net::ToSocketAddrs`] (Quinn requires a `SocketAddr` upfront —
/// a hostname-only `ClientConfig` is not supported). When the address
/// is a hostname, the resolved hostname is also used as the SNI /
/// `server_name` so the receiving cert is verified against it; raw IP
/// addresses fall back to the template's `server_name` (typically the
/// cluster CA's expected name).
fn build_wire_client_config(
    addr: &str,
    template: &WireClientTemplate,
) -> Result<ClientConfig, RegistryError> {
    use std::net::ToSocketAddrs;

    // Reject UDS — wire transport is QUIC-over-UDP, not socket-file.
    if addr.starts_with('/') {
        return Err(RegistryError::InvalidAddress {
            addr: addr.to_owned(),
            reason: "wire transport does not support unix domain socket addresses".to_owned(),
        });
    }

    // Split the host portion off so we can use it as the SNI name when
    // the address is a hostname (vs a raw IP). `rsplit_once` handles
    // bracketed IPv6 (`[::1]:5000`) by splitting on the final `:`.
    let host = match addr.rsplit_once(':') {
        Some((h, _)) => h.trim_start_matches('[').trim_end_matches(']'),
        None => {
            return Err(RegistryError::InvalidAddress {
                addr: addr.to_owned(),
                reason: "address missing :port".to_owned(),
            });
        },
    };

    let mut iter = addr.to_socket_addrs().map_err(|e| RegistryError::InvalidAddress {
        addr: addr.to_owned(),
        reason: e.to_string(),
    })?;
    let server_addr = iter.next().ok_or_else(|| RegistryError::InvalidAddress {
        addr: addr.to_owned(),
        reason: "no socket addresses resolved".to_owned(),
    })?;

    // If the host parses as an IP literal, use the template's server_name
    // (peer certs in IP-only deployments carry an IP SAN matching the IP
    // and a CN for the cluster name; defaulting to the template's name
    // works for both). Otherwise use the hostname directly so SNI matches.
    let server_name = if host.parse::<std::net::IpAddr>().is_ok() {
        template.server_name.clone()
    } else {
        host.to_owned()
    };

    Ok(ClientConfig {
        server_addr,
        server_name,
        quic: template.quic.clone(),
        auth_payload: template.auth_payload.clone(),
        connect_timeout: template.connect_timeout,
    })
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
    /// [`NodeConnectionRegistry::wire_client_for`] was called before a
    /// [`WireClientTemplate`] was installed via
    /// [`NodeConnectionRegistry::set_wire_template`] /
    /// [`NodeConnectionRegistry::with_wire_template`].
    #[snafu(display(
        "wire-client template not configured on registry; install via set_wire_template before \
         registering wire peers"
    ))]
    WireTemplateUnconfigured,
    /// [`inferadb_ledger_wire_transport::WireClient::new`] failed (typically
    /// the local UDP socket bind).
    #[snafu(display("WireClient::new failed for {addr}: {reason}"))]
    WireClientNew {
        /// The address the client was being constructed for.
        addr: String,
        /// Human-readable cause.
        reason: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration as StdDuration;

    use inferadb_ledger_wire_transport::tls;

    use super::*;

    fn test_template() -> Arc<WireClientTemplate> {
        let crypto = tls::rustls_client_crypto_skip_verify();
        Arc::new(WireClientTemplate {
            quic: tls::client_config(crypto),
            server_name: "localhost".to_owned(),
            auth_payload: Bytes::new(),
            connect_timeout: StdDuration::from_millis(100),
        })
    }

    #[tokio::test]
    async fn wire_client_for_errors_when_template_unconfigured() {
        let reg = NodeConnectionRegistry::new();
        let err = reg.wire_client_for(2, "127.0.0.1:5000").await;
        assert!(matches!(err, Err(RegistryError::WireTemplateUnconfigured)));
    }

    #[tokio::test]
    async fn wire_client_for_caches_per_peer() {
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        let first = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        let second = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        assert!(Arc::ptr_eq(&first, &second), "second call returns cached client");
        assert_eq!(reg.wire_client_len(), 1);
    }

    #[tokio::test]
    async fn wire_client_for_distinguishes_peers() {
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        let a = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        let b = reg.wire_client_for(3, "127.0.0.1:5001").await.unwrap();
        assert!(!Arc::ptr_eq(&a, &b), "different peer ids must produce different clients");
        assert_eq!(reg.wire_client_len(), 2);
    }

    #[tokio::test]
    async fn drop_wire_client_evicts_cache() {
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        let first = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        assert!(reg.drop_wire_client(2));
        let second = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "post-drop wire_client_for must construct a fresh client",
        );
        // Dropping a non-cached peer is a no-op.
        assert!(!reg.drop_wire_client(99));
    }

    #[tokio::test]
    async fn wire_client_for_invalid_endpoint_errors() {
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        // No port → InvalidAddress.
        let err = reg.wire_client_for(2, "127.0.0.1").await;
        assert!(matches!(err, Err(RegistryError::InvalidAddress { .. })));
        // UDS path → InvalidAddress (wire transport is QUIC).
        let err = reg.wire_client_for(2, "/tmp/sock").await;
        assert!(matches!(err, Err(RegistryError::InvalidAddress { .. })));
    }

    #[tokio::test]
    async fn wire_client_for_address_change_replaces_entry() {
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        let first = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        let second = reg.wire_client_for(2, "127.0.0.1:5001").await.unwrap();
        assert!(!Arc::ptr_eq(&first, &second), "address change must produce a fresh wire client",);
        assert_eq!(reg.wire_client_len(), 1);
    }

    #[tokio::test]
    async fn wire_client_for_concurrent_construction_serializes() {
        let reg = Arc::new(NodeConnectionRegistry::with_wire_template(test_template()));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let r = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                r.wire_client_for(2, "127.0.0.1:5000").await.unwrap()
            }));
        }
        let mut clients = Vec::new();
        for h in handles {
            clients.push(h.await.unwrap());
        }
        let first = Arc::as_ptr(&clients[0]);
        for c in &clients[1..] {
            assert_eq!(
                Arc::as_ptr(c),
                first,
                "concurrent wire_client_for must coalesce on a single client",
            );
        }
        assert_eq!(reg.wire_client_len(), 1);
    }

    #[tokio::test]
    async fn membership_change_prunes_wire_clients() {
        use inferadb_ledger_consensus::types::{Membership, NodeId};
        let reg = NodeConnectionRegistry::with_wire_template(test_template());
        let _ = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        let _ = reg.wire_client_for(3, "127.0.0.1:5001").await.unwrap();
        let _ = reg.wire_client_for(4, "127.0.0.1:5002").await.unwrap();
        assert_eq!(reg.wire_client_len(), 3);

        let membership = Membership::new([NodeId(1), NodeId(2), NodeId(4)]);
        reg.on_membership_changed(&membership);

        assert_eq!(reg.wire_client_len(), 2);
    }

    #[tokio::test]
    async fn set_wire_template_after_construction() {
        let reg = NodeConnectionRegistry::new();
        // Without a template — fails.
        assert!(matches!(
            reg.wire_client_for(2, "127.0.0.1:5000").await,
            Err(RegistryError::WireTemplateUnconfigured),
        ));
        reg.set_wire_template(test_template());
        // With a template — succeeds.
        let client = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        assert_eq!(reg.wire_client_len(), 1);
        // Idempotent re-set; existing cached entry stays.
        reg.set_wire_template(test_template());
        let again = reg.wire_client_for(2, "127.0.0.1:5000").await.unwrap();
        assert!(Arc::ptr_eq(&client, &again));
    }
}
