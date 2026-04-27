//! End-to-end tests for the Ledger SDK against a real server cluster.
//!
//! These tests run against an external Ledger cluster started by
//! `scripts/run-sdk-integration-tests.sh`. The cluster endpoints are
//! provided via the `LEDGER_ENDPOINTS` environment variable.
//!
//! When `LEDGER_ENDPOINTS` is not set, all tests skip gracefully — this
//! allows `cargo test --workspace` to pass without a running cluster.
//!
//! ## Test Categories
//!
//! - **Write/Read Cycle**: Basic CRUD operations through consensus
//! - **Replication**: Write to leader, read from followers
//! - **Streaming**: WatchBlocks stream setup
//! - **Admin**: Organization/vault lifecycle
//! - **Health**: Health check endpoints

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto::{
    GetClusterInfoRequest, admin_service_client::AdminServiceClient,
    user_service_client::UserServiceClient,
};
use inferadb_ledger_sdk::{
    ClientConfig, LedgerClient, Operation, OrganizationSlug, RetryPolicy, ServerSource, UserSlug,
    VaultSlug,
};

// ============================================================================
// External Cluster Helpers
// ============================================================================

/// Reads `LEDGER_ENDPOINTS` env var. Returns `None` if not set.
fn require_external_cluster() -> Option<Vec<String>> {
    let raw = std::env::var("LEDGER_ENDPOINTS").ok()?;
    let endpoints: Vec<String> =
        raw.split(',').map(|e| e.trim().to_string()).filter(|e| !e.is_empty()).collect();

    if endpoints.is_empty() {
        return None;
    }

    Some(endpoints)
}

/// Skip macro: returns early if no external cluster is available.
macro_rules! require_cluster {
    () => {
        match require_external_cluster() {
            Some(eps) => eps,
            None => {
                eprintln!(
                    "LEDGER_ENDPOINTS not set — skipping SDK e2e test. \
                     Run via scripts/run-sdk-integration-tests.sh"
                );
                return;
            },
        }
    };
}

/// Creates a `LedgerClient` connected to all cluster endpoints.
async fn create_sdk_client(endpoints: &[String], client_id: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static(endpoints.iter().cloned()))
        .client_id(client_id)
        // Setting `preferred_region` activates the SDK's region-leader cache
        // so `NotLeader` + `LeaderHint` responses route subsequent retries
        // to the actual data-region leader instead of round-robining
        // across endpoints. Without this, multi-tier consensus tests where
        // GLOBAL leader and region leader live on different nodes loop on
        // not-leader rejections until the retry budget is exhausted.
        .preferred_region(inferadb_ledger_types::Region::US_EAST_VA)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(2))
                .build(),
        )
        .build()
        .expect("valid config");

    LedgerClient::new(config).await.expect("client creation")
}

/// Creates a `LedgerClient` connected to a single specific endpoint.
async fn create_single_endpoint_client(endpoint: &str, client_id: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([endpoint.to_string()]))
        .client_id(client_id)
        // See comment on `create_sdk_client` — preferred_region is required
        // for the SDK to honor `NotLeader` + `LeaderHint` redirects under
        // multi-tier consensus.
        .preferred_region(inferadb_ledger_types::Region::US_EAST_VA)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(2))
                .build(),
        )
        .build()
        .expect("valid config");

    LedgerClient::new(config).await.expect("client creation")
}

/// Creates a user and organization via the onboarding flow, then creates a vault.
///
/// Returns `(user_slug, org_slug, vault_slug)`. Retries vault creation because
/// the organization provisioning saga runs asynchronously and may take up to 30s.
async fn setup_user_and_org(endpoints: &[String]) -> (UserSlug, OrganizationSlug, VaultSlug) {
    // Try each endpoint — the data region leader may differ from the GLOBAL leader.
    // Retry with different endpoints on UNAVAILABLE ("Not the leader for region").
    let email = format!("test-{}@e2e.local", uuid::Uuid::new_v4());
    let mut code = String::new();
    let mut working_endpoint = String::new();

    for _attempt in 0..30 {
        for ep in endpoints {
            let mut user_client = match UserServiceClient::connect(ep.clone()).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            match user_client
                .initiate_email_verification(
                    inferadb_ledger_proto::proto::InitiateEmailVerificationRequest {
                        email: email.clone(),
                        region: 10, // REGION_US_EAST_VA
                    },
                )
                .await
            {
                Ok(resp) => {
                    code = resp.into_inner().code;
                    working_endpoint = ep.clone();
                    break;
                },
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        || e.code() == tonic::Code::Internal =>
                {
                    // Data region leader might be on a different node — try next endpoint
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
    if code.is_empty() {
        panic!("initiate email verification: all endpoints failed after 30 attempts");
    }

    // Use the working endpoint for the rest of the flow
    let endpoint = working_endpoint;
    let mut user_client =
        UserServiceClient::connect(endpoint.clone()).await.expect("connect user service");

    // 3. Verify email code — expect OnboardingSession for new users
    let verify_resp = user_client
        .verify_email_code(inferadb_ledger_proto::proto::VerifyEmailCodeRequest {
            email: email.clone(),
            code,
            region: 10,
        })
        .await
        .expect("verify email code");
    let inner = verify_resp.into_inner();
    let onboarding_token = match inner.result {
        Some(inferadb_ledger_proto::proto::verify_email_code_response::Result::NewUser(
            session,
        )) => session.onboarding_token,
        other => panic!("expected OnboardingSession (NewUser), got {:?}", other),
    };

    // 4. Complete registration — creates user + organization atomically.
    //
    // `complete_registration` submits a saga that runs on the GLOBAL leader.
    // Under multi-tier consensus the GLOBAL leader can differ from the
    // regional (data-region) leader served the verification flow above —
    // iterate endpoints until we hit the GLOBAL leader so the saga
    // submission lands on the right node. The handler returns
    // `Unavailable` ("Not the GLOBAL leader") on non-GLOBAL-leader nodes.
    let org_name = format!("test-org-{}", uuid::Uuid::new_v4());
    let mut reg = None;
    for _attempt in 0..30 {
        for ep in endpoints {
            let mut client = match UserServiceClient::connect(ep.clone()).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            match client
                .complete_registration(
                    inferadb_ledger_proto::proto::CompleteRegistrationRequest {
                        onboarding_token: onboarding_token.clone(),
                        email: email.clone(),
                        region: 10,
                        name: "E2E Test User".to_string(),
                        organization_name: org_name.clone(),
                    },
                )
                .await
            {
                Ok(resp) => {
                    reg = Some(resp.into_inner());
                    break;
                },
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        || e.code() == tonic::Code::Internal =>
                {
                    // Not the GLOBAL leader — try next endpoint.
                    continue;
                },
                Err(e) => panic!("complete registration: {e}"),
            }
        }
        if reg.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let reg = reg.expect("complete registration: all endpoints failed after 30 attempts");
    let user_slug = UserSlug::new(
        reg.user.expect("user in registration response").slug.expect("user slug").slug,
    );
    let org_slug =
        OrganizationSlug::new(reg.organization.expect("org slug in registration response").slug);

    // 5. Create vault — retry for async provisioning saga AND for the
    // brief window after `complete_registration` where the per-org Raft
    // group is still bootstrapping / adopting leadership from the parent
    // region. Under multi-tier consensus the per-org group's adoption
    // races with the immediate `create_vault` call, so the early
    // not-leader rejection is expected for the first few attempts.
    let client = create_sdk_client(endpoints, "setup-client").await;
    for attempt in 0..60 {
        match client.create_vault(user_slug, org_slug).await {
            Ok(info) => return (user_slug, org_slug, info.vault),
            Err(e)
                if (format!("{e}").contains("not found")
                    || format!("{e}").contains("provisioned")
                    || format!("{e}").contains("Not the leader")
                    || format!("{e}").contains("not active on this node"))
                    && attempt < 59 =>
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
            },
            Err(e) => panic!("create vault: {e}"),
        }
    }
    panic!("create vault timed out after 60 attempts");
}

/// Finds the leader endpoint via `GetClusterInfo`.
async fn find_leader_endpoint(endpoints: &[String]) -> String {
    for endpoint in endpoints {
        let mut client =
            AdminServiceClient::connect(endpoint.clone()).await.expect("connect to admin");

        let response =
            client.get_cluster_info(GetClusterInfoRequest {}).await.expect("get cluster info");

        let info = response.into_inner();
        if info.leader_id > 0
            && let Some(leader) = info.members.iter().find(|m| m.is_leader)
            && let Some(ep) = endpoints
                .iter()
                .find(|ep| ep.ends_with(&leader.address) || ep.contains(&leader.address))
        {
            return ep.clone();
        }
    }
    panic!("no leader found in cluster");
}

/// Finds non-leader endpoints via `GetClusterInfo`.
async fn find_non_leader_endpoints(endpoints: &[String]) -> Vec<String> {
    for endpoint in endpoints {
        if let Ok(mut client) = AdminServiceClient::connect(endpoint.clone()).await
            && let Ok(response) = client.get_cluster_info(GetClusterInfoRequest {}).await
        {
            let info = response.into_inner();
            let leader_addr = info
                .members
                .iter()
                .find(|m| m.is_leader)
                .map(|m| m.address.clone())
                .unwrap_or_default();

            return endpoints
                .iter()
                .filter(|ep| !ep.ends_with(&leader_addr) && !ep.contains(&leader_addr))
                .cloned()
                .collect();
        }
    }
    vec![]
}

// ============================================================================
// E2E Tests: Write/Read Cycle
// ============================================================================

/// Tests write → read cycle.
#[tokio::test]
async fn test_write_read_cycle() {
    let endpoints = require_cluster!();

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;
    let client = create_sdk_client(&endpoints, "e2e-write-read").await;

    // Write an entity
    let ops = vec![Operation::set_entity("user:alice", b"Alice Data".to_vec(), None, None)];
    let write_result =
        client.write(user, ns_id, Some(vault), ops, None).await.expect("write should succeed");

    assert!(!write_result.tx_id.is_empty(), "should have tx_id");
    assert!(write_result.block_height > 0, "should have block height");

    // Read the entity back
    let read_result = client
        .read(user, ns_id, Some(vault), "user:alice", None, None)
        .await
        .expect("read should succeed");

    assert_eq!(read_result, Some(b"Alice Data".to_vec()), "should read back written value");
}

/// Tests multiple writes and reads.
#[tokio::test]
async fn test_multiple_writes_reads() {
    let endpoints = require_cluster!();

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;
    let client = create_sdk_client(&endpoints, "e2e-multi-rw").await;

    // Write multiple entities
    for i in 0..5 {
        let key = format!("item:{}", i);
        let value = format!("value-{}", i).into_bytes();
        let ops = vec![Operation::set_entity(&key, value, None, None)];
        client.write(user, ns_id, Some(vault), ops, None).await.expect("write should succeed");
    }

    // Read all entities back
    for i in 0..5 {
        let key = format!("item:{}", i);
        let expected = format!("value-{}", i).into_bytes();
        let result = client
            .read(user, ns_id, Some(vault), &key, None, None)
            .await
            .expect("read should succeed");
        assert_eq!(result, Some(expected), "should read back value for {}", key);
    }
}

/// Tests batch read functionality.
#[tokio::test]
async fn test_batch_read() {
    let endpoints = require_cluster!();

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;
    let client = create_sdk_client(&endpoints, "e2e-batch-read").await;

    // Write several entities
    for i in 0..3 {
        let key = format!("batch:{}", i);
        let value = format!("batch-value-{}", i).into_bytes();
        let ops = vec![Operation::set_entity(&key, value, None, None)];
        client.write(user, ns_id, Some(vault), ops, None).await.expect("write should succeed");
    }

    // Batch read including a missing key
    let keys = vec![
        "batch:0".to_string(),
        "batch:1".to_string(),
        "batch:2".to_string(),
        "batch:missing".to_string(),
    ];
    let results = client
        .batch_read(user, ns_id, Some(vault), keys, None, None)
        .await
        .expect("batch read should succeed");

    assert_eq!(results.len(), 4);
    assert_eq!(results[0], ("batch:0".to_string(), Some(b"batch-value-0".to_vec())));
    assert_eq!(results[1], ("batch:1".to_string(), Some(b"batch-value-1".to_vec())));
    assert_eq!(results[2], ("batch:2".to_string(), Some(b"batch-value-2".to_vec())));
    assert_eq!(results[3], ("batch:missing".to_string(), None));
}

// ============================================================================
// E2E Tests: Replication
// ============================================================================

/// Tests that writes to the leader replicate to followers.
///
/// Writes via a leader-connected client, then reads from each non-leader
/// endpoint to verify Raft replication is working.
#[tokio::test]
async fn test_write_replication_to_followers() {
    let endpoints = require_cluster!();

    if endpoints.len() < 3 {
        eprintln!("Skipping replication test: need >= 3 nodes, got {}", endpoints.len());
        return;
    }

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;

    let leader_ep = find_leader_endpoint(&endpoints).await;
    let leader_client = create_single_endpoint_client(&leader_ep, "repl-leader").await;

    // Write through the leader
    let ops = vec![Operation::set_entity("repl:key", b"replicated-data".to_vec(), None, None)];
    let result = leader_client
        .write(user, ns_id, Some(vault), ops, None)
        .await
        .expect("write should succeed");
    assert!(result.block_height > 0);

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read from each non-leader endpoint
    let non_leaders = find_non_leader_endpoints(&endpoints).await;
    assert!(!non_leaders.is_empty(), "should have non-leader endpoints");

    for (i, ep) in non_leaders.iter().enumerate() {
        let follower_client =
            create_single_endpoint_client(ep, &format!("repl-follower-{}", i)).await;

        let read_result = follower_client
            .read(user, ns_id, Some(vault), "repl:key", None, None)
            .await
            .expect("read from follower should succeed");

        assert_eq!(
            read_result,
            Some(b"replicated-data".to_vec()),
            "data should be replicated to follower at {}",
            ep
        );
    }
}

/// Tests write replicates to all nodes.
#[tokio::test]
async fn test_three_node_write_replication() {
    let endpoints = require_cluster!();

    if endpoints.len() < 3 {
        eprintln!("Skipping 3-node test: need >= 3 nodes, got {}", endpoints.len());
        return;
    }

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;

    let leader_ep = find_leader_endpoint(&endpoints).await;
    let client = create_single_endpoint_client(&leader_ep, "repl-3node").await;

    // Write through the leader
    let ops =
        vec![Operation::set_entity("replicated:key", b"replicated-data".to_vec(), None, None)];
    client.write(user, ns_id, Some(vault), ops, None).await.expect("write should succeed");

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read from every endpoint (including leader)
    for (i, ep) in endpoints.iter().enumerate() {
        let reader = create_single_endpoint_client(ep, &format!("repl-reader-{}", i)).await;

        let result = reader
            .read(user, ns_id, Some(vault), "replicated:key", None, None)
            .await
            .expect("read from node");

        assert_eq!(
            result,
            Some(b"replicated-data".to_vec()),
            "data should be replicated to node at {}",
            ep
        );
    }
}

// ============================================================================
// E2E Tests: Multiple Sessions
// ============================================================================

/// Tests multiple client sessions with independent IDs.
#[tokio::test]
async fn test_multiple_client_sessions() {
    let endpoints = require_cluster!();

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;

    let client1 = create_sdk_client(&endpoints, "session-1").await;

    // First session writes
    let ops1 = vec![Operation::set_entity("session:key1", b"from-session-1".to_vec(), None, None)];
    let first_result =
        client1.write(user, ns_id, Some(vault), ops1, None).await.expect("first write");

    // Second independent session
    let client2 = create_sdk_client(&endpoints, "session-2").await;

    let ops2 = vec![Operation::set_entity("session:key2", b"from-session-2".to_vec(), None, None)];
    let second_result =
        client2.write(user, ns_id, Some(vault), ops2, None).await.expect("second write");

    // Both writes should succeed with unique tx_ids
    assert!(!first_result.tx_id.is_empty());
    assert!(!second_result.tx_id.is_empty());
    assert_ne!(first_result.tx_id, second_result.tx_id, "should have different tx_ids");

    // Read back both keys
    let value1 = client2
        .read(user, ns_id, Some(vault), "session:key1", None, None)
        .await
        .expect("read first");
    let value2 = client2
        .read(user, ns_id, Some(vault), "session:key2", None, None)
        .await
        .expect("read second");

    assert_eq!(value1, Some(b"from-session-1".to_vec()));
    assert_eq!(value2, Some(b"from-session-2".to_vec()));
}

// ============================================================================
// E2E Tests: Streaming
// ============================================================================

/// Tests watch_blocks stream setup.
#[tokio::test]
async fn test_watch_blocks_stream_setup() {
    let endpoints = require_cluster!();

    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;
    let client = create_sdk_client(&endpoints, "stream-client").await;

    // Start watching blocks from height 1 (genesis). Retry while the
    // SDK's region-leader cache settles after `setup_user_and_org` —
    // streaming RPCs don't go through the same retry-with-leader-cache
    // path as unary RPCs, so the first connect can land on a non-leader
    // node and immediately surface `Not the leader for this region`.
    let mut last_err = None;
    let mut stream_result = Err(inferadb_ledger_sdk::SdkError::Shutdown);
    for _ in 0..30 {
        stream_result = client.watch_blocks(user, ns_id, vault, 1).await;
        if stream_result.is_ok() {
            break;
        }
        last_err = stream_result.as_ref().err().map(|e| format!("{e}"));
        if last_err.as_deref().is_some_and(|s| s.contains("Not the leader")) {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        break;
    }

    assert!(stream_result.is_ok(), "watch_blocks should succeed: {:?}", last_err);
}

// ============================================================================
// E2E Tests: Persistence
// ============================================================================

/// Tests that data persists across client sessions.
#[tokio::test]
async fn test_data_persistence_across_sessions() {
    let endpoints = require_cluster!();

    // Setup: create user/organization/vault
    let (user, ns_id, vault) = setup_user_and_org(&endpoints).await;

    // First client session — write data
    {
        let client = create_sdk_client(&endpoints, "writer-session-1").await;

        for i in 0..5 {
            let key = format!("persist:{}", i);
            let value = format!("data-{}", i).into_bytes();
            let ops = vec![Operation::set_entity(&key, value, None, None)];
            client.write(user, ns_id, Some(vault), ops, None).await.expect("write should succeed");
        }
    } // Client dropped

    // Second client session — read previously written data. The SDK's
    // default consistency for `read` is EVENTUAL, which can serve from a
    // follower whose per-vault apply pipeline hasn't yet caught up to
    // the writes from the first session — especially because the first
    // client was dropped between sessions, so the second client picks a
    // potentially-cold connection. Poll on each key for a few cycles
    // until convergence.
    {
        let client = create_sdk_client(&endpoints, "reader-session-2").await;

        for i in 0..5 {
            let key = format!("persist:{}", i);
            let expected = format!("data-{}", i).into_bytes();
            let mut read_result = None;
            for _ in 0..30 {
                read_result =
                    client.read(user, ns_id, Some(vault), &key, None, None).await.expect("read");
                if read_result.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert_eq!(read_result, Some(expected), "should read {}", key);
        }

        // Write additional data from new session
        let key = "persist:5";
        let value = b"data-5".to_vec();
        let ops = vec![Operation::set_entity(key, value.clone(), None, None)];
        let result = client
            .write(user, ns_id, Some(vault), ops, None)
            .await
            .expect("write from new session");
        assert!(!result.tx_id.is_empty(), "should have tx_id");

        let read_result =
            client.read(user, ns_id, Some(vault), key, None, None).await.expect("read new write");
        assert_eq!(read_result, Some(value));
    }
}

// ============================================================================
// E2E Tests: Admin Operations
// ============================================================================

/// Tests admin operations (organization/vault lifecycle).
#[tokio::test]
async fn test_admin_operations() {
    let endpoints = require_cluster!();

    let (user, organization, vault) = setup_user_and_org(&endpoints).await;
    let client = create_sdk_client(&endpoints, "admin-client").await;

    assert!(organization.value() > 0, "should get valid organization slug");

    // Get the organization
    let org_info = client.get_organization(organization, user).await.expect("get organization");
    assert!(!org_info.name.is_empty(), "organization should have a name");

    // Vault was already created by setup
    assert!(vault.value() > 0, "should get valid vault slug");

    // List organizations
    let (organizations, _next) =
        client.list_organizations(user, 0, None).await.expect("list organizations");
    assert!(
        organizations.iter().any(|org| org.slug == organization),
        "created organization should be in list"
    );
}

// ============================================================================
// E2E Tests: Health Check
// ============================================================================

/// Tests health check endpoints.
#[tokio::test]
async fn test_health_check() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "health-client").await;

    // Simple health check
    let is_healthy = client.health_check().await.expect("health check");
    assert!(is_healthy, "cluster should be healthy");

    // Detailed health check
    let health_result = client.health_check_detailed().await.expect("detailed health");
    assert!(health_result.is_healthy(), "detailed health should report healthy");
}
