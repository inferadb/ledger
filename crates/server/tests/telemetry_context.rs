//! Integration tests verifying the unified observability context.
//!
//! These tests validate that after handler migration (Phase 1.2+),
//! a single Write/Read RPC emits consistent telemetry to all backends:
//! metrics (via RequestContext::drop), canonical log, and events.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_proto::proto;

use crate::common::{TestCluster, create_health_client};

// Test that a single-node cluster starts successfully and can
// accept a basic health check — validates the test infrastructure works.
#[tokio::test]
async fn telemetry_test_infrastructure_smoke() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    let mut client = create_health_client(&leader.addr).await.expect("connect to health service");

    let response = client
        .check(proto::HealthCheckRequest { organization: None, vault: None })
        .await
        .expect("health check succeeded");

    assert_eq!(
        response.into_inner().status(),
        proto::HealthStatus::Healthy,
        "single-node cluster should report healthy"
    );
}
