//! Smoke test for the F.1.f.2.Stage1b multi-node wire `TestCluster`
//! builder.
//!
//! Verifies that `TestCluster::with_wire_transport_and_size(0, 3)`:
//!
//! 1. Brings three wire-mode nodes up, each binding `LedgerServer::serve_wire`.
//! 2. Joins the two non-bootstrap nodes via the wire-side `AdminServiceClient::join_cluster` RPC
//!    against node 0's QUIC listener (no tonic listeners anywhere).
//! 3. Lets every node's `WireConsensusTransport` reach the others — the cluster converges on a
//!    single GLOBAL leader visible from all three nodes.
//! 4. A wire-side `HealthService::check` against any node returns `Healthy` end-to-end through the
//!    production wire dispatcher (the legacy `WireTestCluster` only routes `HealthService`; this
//!    builder uses `LedgerServer::build_wire_dispatcher`, the full 14-service surface).
//!
//! Stage F.1.f.2.Final deletes this builder and the legacy Grpc-mode
//! `TestCluster`; until then the smoke test pins the multi-node wire
//! infrastructure against regression while subsequent stages migrate
//! existing tests onto the wire path.
//!
//! ## Stage 1c + Stage 1d fixes landed
//!
//! Stage 1c wired the QUIC server's `OPCODE_RAFT_PROTOCOL_HANDSHAKE` dispatch
//! (so `WireRaftServerHandler::handle_replicate_stream` actually serves the
//! Replicate bidi stream) and the inbound `set_peer_via_registry` auto-register
//! (so the joining node's first contact via inbound envelope establishes the
//! return channel). Stage 1d replaced the dependency-health TCP-reachability
//! probe (which dialed the wire transport's QUIC/UDP port over TCP and failed
//! 100% of the time) with a transport-agnostic in-process check that reads the
//! same `peer_liveness` map both transports already populate on every
//! successful inbound consensus envelope. With both fixes in place the cluster
//! elects a leader, the saga orchestrator commits the bootstrap signing-key,
//! and the readiness probe returns `Healthy` end-to-end.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_wire::services::{health as w, shared as ws};

use crate::common::{TestCluster, wire_health_client};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_node_wire_bootstrap_lights_up() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let cluster = TestCluster::with_wire_transport_and_size(0, 3).await;
    let nodes = cluster.nodes();
    assert_eq!(nodes.len(), 3, "with_wire_transport_and_size(0, 3) must yield 3 nodes");

    // Every node must expose a wire address; the legacy gRPC fields
    // are unused on this path.
    for n in nodes {
        assert!(n.wire_addr.is_some(), "node {} missing wire_addr on wire builder", n.id);
    }

    // Cluster convergence: every node must agree on a single GLOBAL
    // leader within the timeout. Uses the existing `TestCluster`
    // helper, which already polls every node's consensus handle.
    let leader = cluster
        .wait_for_leader_agreement(Duration::from_secs(15))
        .await
        .expect("3-node wire cluster must converge on a leader");
    assert!(
        nodes.iter().any(|n| n.id == leader),
        "elected leader {leader} must be a member of the cluster"
    );

    // Round-trip a HealthService::check through every node's wire
    // listener. Asserts the wire dispatcher is wired end-to-end on
    // each node — not just the bootstrap leader. Empty request →
    // node-level health, not vault health.
    for node in nodes {
        let health = wire_health_client(&cluster, node.id);
        let request = w::HealthCheckRequest { organization: None, vault: None };
        // Per-node stable request_id; arbitrary but distinct.
        let response = health
            .check(request, /* request_id = */ node.id as u128)
            .await
            .unwrap_or_else(|e| panic!("HealthService::check on node {} failed: {e}", node.id));
        assert_eq!(
            response.status,
            ws::HealthStatus::Healthy,
            "node {} should report Healthy on the wire path; got {:?} (message {:?})",
            node.id,
            response.status,
            response.message,
        );
    }
}
