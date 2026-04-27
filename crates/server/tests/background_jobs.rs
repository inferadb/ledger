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

use std::time::Duration;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::OrganizationSlug;

use crate::common::{
    ExternalCluster, create_health_client_from_url, create_organization_client_from_url,
    create_user_client_from_url,
};

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

/// Creates a user and organization via the onboarding flow.
///
/// Uses the three-phase onboarding pipeline (InitiateEmailVerification →
/// VerifyEmailCode → CompleteRegistration) to create a valid admin user and
/// organization. Retries across all cluster endpoints to handle data region
/// creation and leader election timing. Returns `(org_slug, user_slug)`.
async fn setup_user_and_org(cluster: &ExternalCluster) -> (OrganizationSlug, u64) {
    let email = format!("test-{}@example.com", uuid::Uuid::new_v4());
    let region = 10; // REGION_US_EAST_VA

    // Phase 1: Initiate email verification — retry across endpoints because
    // the first call may create the data region (UNAVAILABLE until leader elected).
    let mut code = String::new();
    let mut working_endpoint = String::new();
    for _attempt in 0..30 {
        for ep in cluster.endpoints() {
            let mut client = match create_user_client_from_url(ep).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            match client
                .initiate_email_verification(proto::InitiateEmailVerificationRequest {
                    email: email.clone(),
                    region,
                })
                .await
            {
                Ok(resp) => {
                    code = resp.into_inner().code;
                    working_endpoint = ep.to_string();
                    break;
                },
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        || e.code() == tonic::Code::Internal =>
                {
                    continue;
                },
                Err(e) => panic!("initiate email verification: {e}"),
            }
        }
        if !code.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        !code.is_empty(),
        "initiate email verification: all endpoints failed after 30 attempts"
    );

    // Phase 2: Verify email code → get onboarding token. Iterate
    // endpoints — under multi-tier consensus the GLOBAL leader (which
    // serves the email-code flow) can differ from the data-region
    // leader that handled phase 1.
    let mut onboarding_token = String::new();
    'outer_verify: for _attempt in 0..30 {
        for ep in cluster.endpoints() {
            let mut client = match create_user_client_from_url(ep).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            match client
                .verify_email_code(proto::VerifyEmailCodeRequest {
                    email: email.clone(),
                    code: code.clone(),
                    region,
                })
                .await
            {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    match inner.result.expect("verify should return result") {
                        proto::verify_email_code_response::Result::NewUser(session) => {
                            onboarding_token = session.onboarding_token;
                            break 'outer_verify;
                        },
                        other => panic!("expected NewUser result, got {other:?}"),
                    }
                },
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        || e.code() == tonic::Code::Internal =>
                {
                    continue;
                },
                Err(e) => panic!("verify email code: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(!onboarding_token.is_empty(), "verify email code: all endpoints failed");

    // Phase 3: Complete registration → creates user + organization. Saga
    // runs on the GLOBAL leader, which can differ from the per-region
    // leader, so iterate endpoints again.
    let org_name = format!("test-org-{}", uuid::Uuid::new_v4());
    let mut reg_resp = None;
    for _attempt in 0..30 {
        for ep in cluster.endpoints() {
            let mut client = match create_user_client_from_url(ep).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            match client
                .complete_registration(proto::CompleteRegistrationRequest {
                    onboarding_token: onboarding_token.clone(),
                    email: email.clone(),
                    region,
                    name: "Test Admin".to_string(),
                    organization_name: org_name.clone(),
                })
                .await
            {
                Ok(resp) => {
                    reg_resp = Some(resp.into_inner());
                    break;
                },
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        || e.code() == tonic::Code::Internal =>
                {
                    continue;
                },
                Err(e) => panic!("complete registration: {e}"),
            }
        }
        if reg_resp.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let reg_resp = reg_resp.expect("complete registration: all endpoints failed");
    let _ = working_endpoint; // unused after the iteration refactor

    let org_slug = reg_resp.organization.expect("org slug in response").slug;
    let user_slug = reg_resp.user.expect("user in response").slug.expect("user slug").slug;

    (OrganizationSlug::new(org_slug), user_slug)
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
        .check(proto::HealthCheckRequest { organization: None, vault: None })
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
        .check(proto::HealthCheckRequest { organization: None, vault: None })
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

    // Create user + organization via onboarding flow
    let (organization, user_slug) = setup_user_and_org(&cluster).await;

    // Create vault by iterating cluster endpoints — under multi-tier
    // consensus, the data-region leader for us-east-va can differ from
    // the GLOBAL leader (`leader_ep`), so a vault_client pinned to a
    // single node would `panic!("create vault: ...")` on
    // `Not the leader for region us-east-va`.
    let endpoints: Vec<String> =
        cluster.endpoints().iter().map(|s| s.to_string()).collect();
    let vault =
        crate::common::create_vault_with_retry_endpoints(&endpoints, organization, user_slug).await;
    assert!(vault.value() > 0, "vault should have valid ID");

    // Wait for auto-recovery job to potentially scan the new vault
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cluster should still be healthy (no recovery needed for fresh vault)
    let mut health_client =
        create_health_client_from_url(&leader_ep).await.expect("connect to health");

    let response = health_client
        .check(proto::HealthCheckRequest { organization: None, vault: None })
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
            .check(proto::HealthCheckRequest { organization: None, vault: None })
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

    let _leader_ep = cluster.wait_for_leader(Duration::from_secs(10)).await;

    // Create user + organization via onboarding flow
    let (organization, user_slug) = setup_user_and_org(&cluster).await;

    // Wait for provisioning saga and replication
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify organization is readable from every endpoint (retry for replication lag)
    for endpoint in cluster.endpoints() {
        let mut client =
            create_organization_client_from_url(endpoint).await.expect("connect to organization");

        let mut response = None;
        for attempt in 0..15 {
            match client
                .get_organization(proto::GetOrganizationRequest {
                    slug: Some(proto::OrganizationSlug { slug: organization.value() }),
                    caller: Some(proto::UserSlug { slug: user_slug }),
                })
                .await
            {
                Ok(r) => {
                    response = Some(r);
                    break;
                },
                Err(e) if e.message().contains("not found") && attempt < 14 => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
                Err(e) => panic!("get_organization from {} failed: {}", endpoint, e),
            }
        }
        let response = response.unwrap_or_else(|| {
            panic!("get_organization from {} timed out after retries", endpoint)
        });

        let ns = response.into_inner();
        // Organization name is PII (Pattern 2), only available from the regional node.
        // On non-leader replicas it may be empty — just verify the org exists.
        assert!(ns.slug.is_some(), "organization slug should be present on {}", endpoint);
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

    let _leader_ep = cluster.wait_for_leader(Duration::from_secs(10)).await;

    // Create user + organization via onboarding flow
    let (organization, user_slug) = setup_user_and_org(&cluster).await;

    // Iterate cluster endpoints for vault creation — under multi-tier
    // consensus, the data-region leader can differ from the GLOBAL leader.
    let endpoints: Vec<String> =
        cluster.endpoints().iter().map(|s| s.to_string()).collect();
    let _vault =
        crate::common::create_vault_with_retry_endpoints(&endpoints, organization, user_slug).await;

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
