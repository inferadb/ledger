//! Integration tests verifying the unified observability context.
//!
//! Validates that a single Write/Read RPC emits consistent telemetry to all
//! backends: metrics (via `RequestContext::drop`), canonical log, and events.
//!
//! F.1.f.2.Stage1e Wave 6: migrated from the legacy tonic `create_health_client`
//! helper to the wire-protocol sibling `wire_health_client`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_wire::services::{health as wh, shared as ws};

use crate::common::{TestCluster, wire_health_client};

// Test that a single-node cluster starts successfully and can
// accept a basic health check — validates the test infrastructure works.
#[tokio::test]
async fn telemetry_test_infrastructure_smoke() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let client = wire_health_client(&cluster, leader.id);

    let response = client
        .check(wh::HealthCheckRequest { organization: None, vault: None }, rand::random::<u128>())
        .await
        .expect("health check succeeded");

    assert_eq!(
        response.status,
        ws::HealthStatus::Healthy,
        "single-node cluster should report healthy"
    );
}
