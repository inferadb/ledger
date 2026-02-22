//! Integration tests for background jobs and cluster health.
//!
//! These tests run against an external Ledger cluster started by
//! `scripts/run-server-integration-tests.sh`. The cluster endpoints are
//! provided via the `LEDGER_ENDPOINTS` environment variable.
//!
//! When `LEDGER_ENDPOINTS` is not set, all tests skip gracefully — this
//! allows `cargo test --workspace` to pass without a running cluster.
//!
//! Tests verify via gRPC APIs:
//! - `HealthService::Check` for node/cluster health
//! - `AdminService::GetClusterInfo` for membership and leadership
//! - `AdminService` mutations for organization/vault creation

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{ExternalCluster, create_admin_client_from_url, create_health_client_from_url};
use inferadb_ledger_proto::proto;

/// Skip macro: returns early if no external cluster is available.
macro_rules! require_cluster {
    () => {
        match ExternalCluster::from_env() {
            Some(c) => c,
            None => {
                eprintln!(
                    "LEDGER_ENDPOINTS not set — skipping integration test. \
                     Run via scripts/run-server-integration-tests.sh"
                );
                return;
            },
        }
    };
}

// ============================================================================
// Auto-Recovery Tests
// ============================================================================

/// Tests that the cluster is healthy (auto-recovery job hasn't crashed anything).
#[tokio::test]
async fn test_auto_recovery_job_starts() {
    let cluster = require_cluster!();

    // Health check on any node — verifies the node is running and healthy,
    // which implies background jobs (including auto-recovery) haven't crashed it.
    let mut health_client =
        create_health_client_from_url(cluster.any_endpoint()).await.expect("connect to health");

    let response = health_client
        .check(proto::HealthCheckRequest { organization: None, vault_id: None })
        .await
        .expect("health check");

    let health = response.into_inner();
    assert_eq!(
        health.status,
        proto::HealthStatus::Healthy as i32,
        "node should be healthy: {}",
        health.message
    );

    // Small delay then re-check — auto-recovery job runs periodically
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = health_client
        .check(proto::HealthCheckRequest { organization: None, vault_id: None })
        .await
        .expect("second health check");

    let health = response.into_inner();
    assert_eq!(
        health.status,
        proto::HealthStatus::Healthy as i32,
        "node should remain healthy after recovery cycle"
    );
}

/// Tests vault creation and health tracking.
#[tokio::test]
async fn test_vault_health_tracking() {
    let cluster = require_cluster!();

    let leader_ep = cluster.wait_for_leader(Duration::from_secs(10)).await;
    let mut admin_client =
        create_admin_client_from_url(&leader_ep).await.expect("connect to admin");

    let ns_name = format!("test-health-{}", uuid::Uuid::new_v4());

    // Create organization
    let ns_response = admin_client
        .create_organization(proto::CreateOrganizationRequest {
            name: ns_name,
            shard_id: None,
            quota: None,
        })
        .await
        .expect("create organization");

    let organization_id = ns_response.into_inner().slug.map(|n| n.slug as i64).unwrap();

    // Create vault
    let vault_response = admin_client
        .create_vault(proto::CreateVaultRequest {
            organization: Some(proto::OrganizationSlug { slug: organization_id as u64 }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await
        .expect("create vault");

    let vault_id = vault_response.into_inner().vault_id.map(|v| v.id).unwrap();
    assert!(vault_id > 0, "vault should have valid ID");

    // Wait for auto-recovery job to potentially scan the new vault
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cluster should still be healthy (no recovery needed for fresh vault)
    let mut health_client =
        create_health_client_from_url(&leader_ep).await.expect("connect to health");

    let response = health_client
        .check(proto::HealthCheckRequest { organization: None, vault_id: None })
        .await
        .expect("health check after vault creation");

    let health = response.into_inner();
    assert_eq!(
        health.status,
        proto::HealthStatus::Healthy as i32,
        "cluster should remain healthy after vault creation"
    );
}

// ============================================================================
// Learner Refresh Tests
// ============================================================================

/// Tests that nodes are healthy (learner refresh job hasn't crashed anything).
#[tokio::test]
async fn test_learner_refresh_job_starts() {
    let cluster = require_cluster!();

    // Check health on each endpoint — learner refresh should be running
    // on all nodes without causing issues
    for endpoint in cluster.endpoints() {
        let mut health_client =
            create_health_client_from_url(endpoint).await.expect("connect to health");

        let response = health_client
            .check(proto::HealthCheckRequest { organization: None, vault_id: None })
            .await
            .expect("health check");

        let health = response.into_inner();
        assert_eq!(
            health.status,
            proto::HealthStatus::Healthy as i32,
            "node at {} should be healthy",
            endpoint
        );
    }
}

/// Tests that all nodes in a 3-node cluster are voters.
#[tokio::test]
async fn test_voter_detection() {
    let cluster = require_cluster!();

    // GetClusterInfo from any node — all 3 should be voters
    let info =
        ExternalCluster::get_cluster_info(cluster.any_endpoint()).await.expect("get cluster info");

    assert!(!info.members.is_empty(), "cluster should have members");

    for member in &info.members {
        assert_eq!(
            member.role,
            proto::ClusterMemberRole::Voter as i32,
            "node {} should be a voter, got role {}",
            member.node_id,
            member.role
        );
    }
}

/// Tests that organization data is replicated to all nodes (learner cache initialization).
#[tokio::test]
async fn test_learner_cache_initialization() {
    let cluster = require_cluster!();

    let leader_ep = cluster.wait_for_leader(Duration::from_secs(10)).await;

    // Create a organization via the leader
    let mut admin_client =
        create_admin_client_from_url(&leader_ep).await.expect("connect to admin");

    let ns_name = format!("test-cache-{}", uuid::Uuid::new_v4());

    let ns_response = admin_client
        .create_organization(proto::CreateOrganizationRequest {
            name: ns_name.clone(),
            shard_id: None,
            quota: None,
        })
        .await
        .expect("create organization");

    let organization_id = ns_response.into_inner().slug.map(|n| n.slug as i64).unwrap();

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify organization is readable from every endpoint
    for endpoint in cluster.endpoints() {
        let mut client = create_admin_client_from_url(endpoint).await.expect("connect to admin");

        let response = client
            .get_organization(proto::GetOrganizationRequest {
                slug: Some(proto::OrganizationSlug { slug: organization_id as u64 }),
            })
            .await
            .unwrap_or_else(|e| panic!("get_organization from {} failed: {}", endpoint, e));

        let ns = response.into_inner();
        assert_eq!(ns.name, ns_name, "organization name should match on {}", endpoint);
    }
}

// ============================================================================
// Combined Background Job Tests
// ============================================================================

/// Tests that all background jobs run concurrently without interfering with
/// cluster stability. All nodes should agree on the same leader.
#[tokio::test]
async fn test_concurrent_background_jobs() {
    let cluster = require_cluster!();

    let leader_ep = cluster.wait_for_leader(Duration::from_secs(10)).await;

    // Create some activity to exercise background jobs
    let mut admin_client =
        create_admin_client_from_url(&leader_ep).await.expect("connect to admin");

    let ns_name = format!("test-concurrent-{}", uuid::Uuid::new_v4());

    let ns_response = admin_client
        .create_organization(proto::CreateOrganizationRequest {
            name: ns_name,
            shard_id: None,
            quota: None,
        })
        .await
        .expect("create organization");

    let organization_id = ns_response.into_inner().slug.map(|n| n.slug as i64).unwrap();

    let _vault_response = admin_client
        .create_vault(proto::CreateVaultRequest {
            organization: Some(proto::OrganizationSlug { slug: organization_id as u64 }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await
        .expect("create vault");

    // Let background jobs run
    tokio::time::sleep(Duration::from_secs(1)).await;

    // All nodes should report the same leader via GetClusterInfo
    let mut leader_ids = Vec::new();
    for endpoint in cluster.endpoints() {
        let info = ExternalCluster::get_cluster_info(endpoint)
            .await
            .unwrap_or_else(|e| panic!("get_cluster_info from {} failed: {}", endpoint, e));

        assert!(info.leader_id > 0, "node at {} should know the leader", endpoint);
        leader_ids.push(info.leader_id);
    }

    // All nodes should agree on who the leader is
    let first = leader_ids[0];
    assert!(
        leader_ids.iter().all(|&id| id == first),
        "all nodes should agree on leader, got {:?}",
        leader_ids
    );
}

/// Tests that leadership is stable (auto-recovery doesn't trigger spurious elections).
#[tokio::test]
async fn test_recovery_only_on_leader() {
    let cluster = require_cluster!();

    // Get initial leader
    let info =
        ExternalCluster::get_cluster_info(cluster.any_endpoint()).await.expect("get cluster info");
    let initial_leader = info.leader_id;
    assert!(initial_leader > 0, "should have initial leader");

    // Wait for background jobs to run a few cycles
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Leader should not have changed
    let info = ExternalCluster::get_cluster_info(cluster.any_endpoint())
        .await
        .expect("get cluster info after sleep");

    assert_eq!(
        info.leader_id, initial_leader,
        "leader should remain stable (was {}, now {})",
        initial_leader, info.leader_id
    );
}
