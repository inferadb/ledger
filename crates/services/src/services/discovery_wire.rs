//! Wire-protocol implementation for `SystemDiscoveryService` (Phase F.1.f.1.12).
//!
//! Provides the [`inferadb_ledger_wire_services::SystemDiscoveryService`]
//! impl on [`super::discovery::DiscoveryService`]. The wire impl operates
//! on wire types directly — no proto conversion, no UFCS-delegation
//! through the tonic impl. The two paths share only the underlying
//! `ConsensusHandle` / `AppliedStateAccessor` / `PeerTracker` /
//! `RaftManager` calls, plus the `make_leader_update` free fn and the
//! `is_voter` / `is_cache_stale` / `get_leader_hint` /
//! `resolve_region_leader_impl` accessors made `pub(super)` for shared
//! reuse.
//!
//! Streaming `watch_leader`:
//!
//! - The wire impl subscribes to the picked region's `state_watch()` directly, builds a wire
//!   [`w::LeaderUpdate`] from each
//!   [`ShardState`](inferadb_ledger_consensus::leadership::ShardState) snapshot, postcard-encodes
//!   it, and pushes the bytes into the [`StreamSink`]. No `tokio::sync::mpsc` / `ReceiverStream`
//!   round-trip and no proto detour — the watcher loop runs in the same task that holds the sink so
//!   backpressure flows directly.
//! - Sink-closed by the peer aborts cleanly (returns `Ok(())`). A `state_watch().changed()` error
//!   means the underlying watch sender was dropped — also a clean shutdown.

use std::{collections::BTreeMap, str::FromStr};

use bytes::Bytes;
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{discovery as w, shared as ws},
};
use inferadb_ledger_wire_transport::StreamSink;

use super::{discovery::DiscoveryService, wire_helpers::tonic_status_to_wire_error};

// ---------------------------------------------------------------------------
// Enum tag mapping.
// ---------------------------------------------------------------------------

/// Numeric-tag mapping from proto `OrganizationStatus` (raw i32) to the wire enum.
///
/// Both enums declare the same `Unspecified = 0`, `Active = 1`,
/// `Migrating = 2`, `Suspended = 3`, `Deleted = 4`, `Provisioning = 5`
/// values, so the mapping is identity by integer tag. Unknown tags
/// collapse to `Unspecified` to match proto's open-enum semantics.
#[allow(dead_code)]
fn proto_organization_status_to_wire(value: i32) -> ws::OrganizationStatus {
    match value {
        x if x == ws::OrganizationStatus::Active as i32 => ws::OrganizationStatus::Active,
        x if x == ws::OrganizationStatus::Migrating as i32 => ws::OrganizationStatus::Migrating,
        x if x == ws::OrganizationStatus::Suspended as i32 => ws::OrganizationStatus::Suspended,
        x if x == ws::OrganizationStatus::Deleted as i32 => ws::OrganizationStatus::Deleted,
        x if x == ws::OrganizationStatus::Provisioning as i32 => {
            ws::OrganizationStatus::Provisioning
        },
        _ => ws::OrganizationStatus::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// Domain → wire helpers (used by the inlined wire-trait impl).
// ---------------------------------------------------------------------------

/// Map a domain `OrganizationStatus` (from `inferadb_ledger_state::system`)
/// directly to the wire enum.
///
/// The proto path goes domain → proto → i32; the wire path skips the proto
/// detour. Both target enums share matching numeric tags, so this is a
/// 1:1 mapping with no lossy collapses.
fn domain_organization_status_to_wire(
    status: inferadb_ledger_state::system::OrganizationStatus,
) -> ws::OrganizationStatus {
    use inferadb_ledger_state::system::OrganizationStatus as Domain;
    match status {
        Domain::Active => ws::OrganizationStatus::Active,
        Domain::Provisioning => ws::OrganizationStatus::Provisioning,
        Domain::Migrating => ws::OrganizationStatus::Migrating,
        Domain::Suspended => ws::OrganizationStatus::Suspended,
        Domain::Deleted => ws::OrganizationStatus::Deleted,
    }
}

/// Builds a wire `LeaderUpdate` directly from a shard-state snapshot and
/// the shared peer address map.
///
/// Wire-native sibling of [`super::discovery::make_leader_update`] — both
/// share the same "look up leader endpoint, fall back to empty, suppress
/// `leader_node_id` when no endpoint resolved" rules. Inlined into the
/// `watch_leader` shim so the streamed updates skip the proto detour.
fn make_wire_leader_update(
    state: &inferadb_ledger_consensus::leadership::ShardState,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
) -> w::LeaderUpdate {
    use super::metadata::ensure_endpoint_url;
    match state.leader {
        Some(leader) => {
            let endpoint = peer_addresses
                .and_then(|m| m.get(leader.0))
                .map(ensure_endpoint_url)
                .unwrap_or_default();
            let leader_node_id = if endpoint.is_empty() { 0 } else { leader.0 };
            w::LeaderUpdate { endpoint, raft_term: state.term, leader_node_id }
        },
        None => {
            w::LeaderUpdate { endpoint: String::new(), raft_term: state.term, leader_node_id: 0 }
        },
    }
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `DiscoveryService`.
//
// Inlined directly on the wire types — no proto round-trip, no UFCS
// dispatch through the tonic impl. The bodies mirror the tonic
// `SystemDiscoveryService` impls in [`super::discovery`] one-for-one;
// both impls access the same `DiscoveryService` private fields (made
// `pub(super)` for this purpose) and call into the same `ConsensusHandle`
// / `AppliedStateAccessor` / `PeerTracker` / `RaftManager` primitives.
// The two paths share NO code beyond the helper free functions
// (`make_leader_update`, `validate_peer_addresses`) that already lived in
// `super::discovery`.
//
// `region_from_str` and `validate_peer_addresses` still return
// `tonic::Status` (their tonic callers consume the result directly). The
// wire impl converts via [`tonic_status_to_wire_error`] — that helper
// preserves embedded `ErrorDetails` (e.g. `NotLeader` hints) into the
// wire `WireError`'s context map, so SDK clients see the same routing
// guidance regardless of transport.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::SystemDiscoveryService for DiscoveryService {
    async fn get_peers(
        &self,
        _request: w::GetPeersRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetPeersResponse, WireError> {
        let current_term = self.handle.current_term();
        let state = self.handle.shard_state();

        let peers: Vec<ws::PeerInfo> = state
            .voters
            .iter()
            .chain(state.learners.iter())
            .filter_map(|node_id| {
                let addr_with_port = self.peer_addresses.as_ref()?.get(node_id.0)?;
                let (host, port_str) = addr_with_port.rsplit_once(':')?;
                let grpc_port: u32 = port_str.parse().ok()?;
                Some(ws::PeerInfo {
                    node_id: Some(ws::NodeId::new(node_id.0.to_string())),
                    addresses: vec![host.to_string()],
                    grpc_port,
                    // Wire `last_seen: 0` mirrors proto `last_seen: None` —
                    // both encode "unknown / not tracked at this layer".
                    last_seen: 0,
                })
            })
            .collect();

        Ok(w::GetPeersResponse { peers, system_version: current_term })
    }

    async fn announce_peer(
        &self,
        request: w::AnnouncePeerRequest,
        _ctx: RequestContext,
    ) -> Result<w::AnnouncePeerResponse, WireError> {
        // Validate the peer info — same shape as the tonic handler.
        let peer = request.peer.ok_or_else(|| WireError {
            code: ErrorCode::InvalidArgument,
            message: "Missing peer info".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        })?;

        let node_id = peer.node_id.as_ref().ok_or_else(|| WireError {
            code: ErrorCode::InvalidArgument,
            message: "Missing node_id".to_string(),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        })?;

        // Validate gRPC port range.
        if peer.grpc_port == 0 || peer.grpc_port > 65535 {
            return Err(WireError {
                code: ErrorCode::InvalidArgument,
                message: format!("Invalid gRPC port: {}. Must be 1-65535", peer.grpc_port),
                retryable: false,
                retry_after_ms: 0,
                context: BTreeMap::new(),
                suggested_action: String::new(),
            });
        }

        // Validate addresses are private/WireGuard IPs. The shared helper
        // returns `tonic::Status`; convert through the standard mapping so
        // any embedded ErrorDetails carry over to the wire context.
        super::discovery::validate_peer_addresses(&peer.addresses)
            .map_err(tonic_status_to_wire_error)?;

        // Record that we've seen this peer.
        {
            let mut tracker = self.peer_tracker.write();
            tracker.record_seen(node_id.value());
        }

        // Update the shared peer address map so other services can resolve
        // this peer's address for forwarding and health checks.
        if let Some(ref peer_addresses) = self.peer_addresses
            && let (Ok(parsed_id), Some(first_addr)) =
                (node_id.value().parse::<u64>(), peer.addresses.first())
        {
            let address = format!("{first_addr}:{}", peer.grpc_port);
            peer_addresses.insert(parsed_id, address);
        }

        tracing::info!(
            node_id = %node_id.value(),
            addresses = ?peer.addresses,
            grpc_port = peer.grpc_port,
            "Peer announced"
        );

        Ok(w::AnnouncePeerResponse { accepted: true })
    }

    async fn get_system_state(
        &self,
        request: w::GetSystemStateRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetSystemStateResponse, WireError> {
        // Learner staleness check — return UNAVAILABLE with leader hint
        // so the SDK reroutes to the current leader.
        if self.is_cache_stale() {
            let leader_hint = self.get_leader_hint();
            return Err(WireError {
                code: ErrorCode::StaleRouting,
                message: format!(
                    "Learner cache is stale. Retry with leader: {}",
                    leader_hint.unwrap_or_else(|| "unknown".to_string())
                ),
                retryable: true,
                retry_after_ms: 0,
                context: BTreeMap::new(),
                suggested_action: String::new(),
            });
        }

        let current_version = self.handle.current_term();

        // Caller already has current version — return empty delta.
        if request.if_version_greater_than >= current_version {
            return Ok(w::GetSystemStateResponse {
                version: current_version,
                nodes: vec![],
                organizations: vec![],
            });
        }

        let state = self.handle.shard_state();

        // Build node list from voters + learners that have announced
        // their addresses.
        let nodes: Vec<ws::NodeInfo> = state
            .voters
            .iter()
            .chain(state.learners.iter())
            .filter_map(|node_id| {
                let addr_with_port = self.peer_addresses.as_ref()?.get(node_id.0)?;
                let (host, port_str) = addr_with_port.rsplit_once(':')?;
                let grpc_port: u32 = port_str.parse().ok()?;
                Some(ws::NodeInfo {
                    node_id: Some(ws::NodeId::new(node_id.0.to_string())),
                    addresses: vec![host.to_string()],
                    grpc_port,
                    last_heartbeat: 0,
                    joined_at: 0,
                    region: inferadb_ledger_types::Region::GLOBAL.as_str().to_string(),
                })
            })
            .collect();

        // Member node IDs for the organization registry entries.
        let member_nodes: Vec<ws::NodeId> =
            state.voters.iter().map(|n| ws::NodeId::new(n.0.to_string())).collect();

        let organizations: Vec<ws::OrganizationRegistry> = self
            .applied_state
            .list_organizations()
            .into_iter()
            .map(|ns| {
                let status = domain_organization_status_to_wire(ns.status);
                ws::OrganizationRegistry {
                    slug: Some(ws::OrganizationSlug::new(
                        self.applied_state
                            .resolve_id_to_slug(ns.organization)
                            .map_or(ns.organization.value() as u64, |s| s.value()),
                    )),
                    name: String::new(),
                    region: inferadb_ledger_types::Region::GLOBAL.as_str().to_string(),
                    members: member_nodes.clone(),
                    status,
                    config_version: current_version,
                    created_at: 0,
                }
            })
            .collect();

        Ok(w::GetSystemStateResponse { version: current_version, nodes, organizations })
    }

    async fn resolve_region_leader(
        &self,
        request: w::ResolveRegionLeaderRequest,
        _ctx: RequestContext,
    ) -> Result<w::ResolveRegionLeaderResponse, WireError> {
        let region = inferadb_ledger_types::Region::from_str(&request.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        let (endpoint, raft_term, ttl_seconds) =
            self.resolve_region_leader_impl(region).map_err(tonic_status_to_wire_error)?;

        Ok(w::ResolveRegionLeaderResponse { endpoint, raft_term, ttl_seconds })
    }

    async fn watch_leader(
        &self,
        request: w::WatchLeaderRequest,
        _ctx: RequestContext,
        sink: StreamSink,
    ) -> Result<(), WireError> {
        // Validate region — reject unrecognized values.
        let region = inferadb_ledger_types::Region::from_str(&request.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        // Subscribe to the requested region's leader watch — not the
        // GLOBAL group's. Under multi-tier consensus the GLOBAL leader
        // and the data-region leader can live on different nodes;
        // subscribing to GLOBAL would feed the SDK's region-leader
        // cache the GLOBAL endpoint and override hints applied via
        // `apply_region_leader_hint`, making "Not the leader for this
        // region" retries unrecoverable until the retry budget
        // exhausted.
        //
        // Falls back to the embedded GLOBAL handle when no `RaftManager`
        // is wired (single-region setups) or when the requested region
        // is not yet registered locally.
        let mut state_rx = if let Some(manager) = &self.raft_manager
            && region != inferadb_ledger_types::Region::GLOBAL
            && let Ok(region_group) = manager.get_region_group(region)
        {
            region_group.handle().state_watch()
        } else {
            self.handle.state_watch()
        };
        let peer_addresses = self.peer_addresses.clone();

        // Emit initial state immediately, then stream subsequent
        // leader-change updates until the sink closes or the watch
        // sender is dropped.
        let initial = make_wire_leader_update(&state_rx.borrow(), peer_addresses.as_ref());
        let bytes = postcard::to_allocvec(&initial).map_err(|err| WireError {
            code: ErrorCode::Internal,
            message: format!("encode LeaderUpdate: {err}"),
            retryable: false,
            retry_after_ms: 0,
            context: BTreeMap::new(),
            suggested_action: String::new(),
        })?;
        if sink.send(Bytes::from(bytes)).await.is_err() {
            // Peer dropped the stream before we could push the initial
            // snapshot. Clean exit.
            return Ok(());
        }

        while state_rx.changed().await.is_ok() {
            let update = make_wire_leader_update(&state_rx.borrow(), peer_addresses.as_ref());
            let bytes = postcard::to_allocvec(&update).map_err(|err| WireError {
                code: ErrorCode::Internal,
                message: format!("encode LeaderUpdate: {err}"),
                retryable: false,
                retry_after_ms: 0,
                context: BTreeMap::new(),
                suggested_action: String::new(),
            })?;
            if sink.send(Bytes::from(bytes)).await.is_err() {
                // Peer dropped the stream — clean exit, no error frame.
                return Ok(());
            }
        }

        // `state_rx.changed()` returned `Err` — the watch sender was
        // dropped (shard shutdown). Treat as a clean stream end.
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Enum tag mappings.
    // -----------------------------------------------------------------------

    #[test]
    fn proto_organization_status_each_known_tag_maps_correctly() {
        assert_eq!(proto_organization_status_to_wire(0), ws::OrganizationStatus::Unspecified);
        assert_eq!(proto_organization_status_to_wire(1), ws::OrganizationStatus::Active);
        assert_eq!(proto_organization_status_to_wire(2), ws::OrganizationStatus::Migrating);
        assert_eq!(proto_organization_status_to_wire(3), ws::OrganizationStatus::Suspended);
        assert_eq!(proto_organization_status_to_wire(4), ws::OrganizationStatus::Deleted);
        assert_eq!(proto_organization_status_to_wire(5), ws::OrganizationStatus::Provisioning);
    }

    #[test]
    fn proto_organization_status_unknown_tag_collapses_to_unspecified() {
        assert_eq!(proto_organization_status_to_wire(99), ws::OrganizationStatus::Unspecified);
    }

    // End-to-end shim test (DiscoveryService instantiation against a live
    // ConsensusHandle / StateLayer / RaftManager) is deferred to E.7
    // (TestCluster migration). The conversion helpers above are
    // unit-tested in isolation; tonic-status → WireError mapping is covered
    // by `wire_helpers::tests`.
}
