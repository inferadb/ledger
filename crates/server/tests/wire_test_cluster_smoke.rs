//! Smoke tests for the F.1.f.2.Stage1a wire-mode `TestCluster` builder.
//!
//! Verifies that `TestCluster::with_wire_transport(0)` brings a single-node
//! cluster up, populates the wire-side scaffolding (per-node `wire_addr`,
//! shared `quinn::ClientConfig` template), and that the wire-shaped
//! sibling helpers (`wire_health_client`, etc.) construct typed
//! wire-services clients without panicking.
//!
//! ## Why no end-to-end RPC round-trip
//!
//! The full RPC path (`WireClient` ↔ `LedgerServer::serve_wire` ↔ wire
//! dispatcher) requires the dispatcher to be built with a configured
//! `token_service`, which in turn requires the client to send a valid
//! JWT in `auth_payload`. F.1.f.2.Stage1a is purely additive scaffolding;
//! threading a test-issued JWT through `wire_health_client` is in scope
//! for the SDK-side migration stages (F.1.f.2.B / .C). The standalone
//! `WireTestCluster` covers the round-trip path with a HealthService-only
//! dispatcher + `PermissiveVerifier`; this builder covers the *production*
//! `LedgerServer::serve_wire` path end-to-end up to the auth boundary.
//!
//! The additivity proof here: the legacy `TestCluster` paths still work
//! exactly as before, AND a new `with_wire_transport` builder lights up
//! the production wire bind on the same harness without breaking anything.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use crate::common::{
    TestCluster, wire_admin_client, wire_app_client, wire_discovery_client, wire_events_client,
    wire_health_client, wire_invitation_client, wire_organization_client, wire_read_client,
    wire_token_client, wire_user_client, wire_vault_client, wire_write_client,
};

/// Minimal wire-builder happy path: 1-node cluster, no data regions.
///
/// Asserts the wire-mode `TestCluster` builder:
///   1. Brings a single-node bootstrap up against `LedgerServer::serve_wire`.
///   2. Populates `TestNode::wire_addr` with the QUIC bind address.
///   3. Vends a `WireClient` from `TestCluster::wire_client_for` without panicking on the inner
///      QUIC endpoint construction.
///   4. Wraps the `WireClient` in each of the 12 typed wire-services clients via the new
///      `wire_*_client` helpers without panicking.
///
/// Uses the multi-thread runtime: `LedgerServer::serve_wire` is
/// `tokio::spawn`ed by `bootstrap_node` and the QUIC bind runs inside
/// that spawned task — under the current-thread runtime the spawned task
/// can starve the test future during cluster startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_test_cluster_scaffolding_lights_up() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let nodes = cluster.nodes();
    assert_eq!(nodes.len(), 1, "with_wire_transport asks for one node");

    let node_id = nodes[0].id;
    let wire_addr = nodes[0].wire_addr.expect(
        "wire-mode TestCluster nodes must expose a wire_addr; \
         build_wire populated wire_listen on the bootstrap node",
    );
    assert_eq!(
        wire_addr.ip().to_string(),
        "127.0.0.1",
        "wire bind addr must stay on loopback for tests"
    );

    // Smoke each typed wire-services client constructor. Each panics if
    // either `wire_client_for` panics (e.g. missing wire_client_quic) or
    // the inner `WireClient::new` fails to bind a local UDP endpoint.
    // No RPC issued — see module-level docs for why.
    let _ = wire_health_client(&cluster, node_id);
    let _ = wire_write_client(&cluster, node_id);
    let _ = wire_read_client(&cluster, node_id);
    let _ = wire_admin_client(&cluster, node_id);
    let _ = wire_organization_client(&cluster, node_id);
    let _ = wire_vault_client(&cluster, node_id);
    let _ = wire_app_client(&cluster, node_id);
    let _ = wire_token_client(&cluster, node_id);
    let _ = wire_user_client(&cluster, node_id);
    let _ = wire_invitation_client(&cluster, node_id);
    let _ = wire_events_client(&cluster, node_id);
    let _ = wire_discovery_client(&cluster, node_id);
}
