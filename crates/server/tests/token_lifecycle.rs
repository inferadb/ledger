//! Integration tests for JWT token lifecycle operations.
//!
//! Tests cross-cutting token flows spanning creation, validation, refresh, and revocation:
//! - Full user session lifecycle: create → validate → refresh → revoke
//! - Full vault token lifecycle: create → validate → refresh → revoke
//! - Revoke all user sessions (atomic: tokens revoked + TokenVersion incremented)
//! - Organization deletion cascades: refresh tokens revoked
//! - Refresh token theft detection: reuse poisons the entire family
//! - Race condition: concurrent RefreshToken + RevokeAllUserSessions
//! - Concurrent refresh of the same token: one succeeds, other triggers reuse detection
//! - Scope & authorization tests: vault token scope validation, app disable/enable, connection
//!   removal, scope changes on refresh
//! - Signing key lifecycle: rotation with grace period, immediate revocation, grace_period_secs=0,
//!   auto-bootstrap (global + org), idempotency, missing key error
//! - State machine determinism: proposed_at timestamps, poisoned family persistence across Raft
//!   replication
//! - Operational: TokenMaintenanceJob signing key transition after grace period expiry, maintenance
//!   job idempotency across multiple cycles
//!
//! F.1.f.2.Stage1e Wave 7: migrated from the legacy tonic helpers
//! (`create_token_client` / `create_app_client` / `create_organization_client` /
//! `create_test_organization` / `create_test_vault`) to their wire-protocol
//! siblings (`wire_token_client` / `wire_app_client` /
//! `wire_organization_client` / `wire_create_test_organization` /
//! `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::{AppSlug, OrganizationSlug, UserSlug, VaultSlug};
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{
        app as wa, organization as wo, shared as ws, shared::UserSlug as WUserSlug, token as wt,
    },
};
use inferadb_ledger_wire_services::TokenServiceClient;
use inferadb_ledger_wire_transport::RpcError;

use crate::common::{
    TestCluster, setup_user, wire_app_client, wire_organization_client, wire_token_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug and admin user slug.
async fn create_organization(
    cluster: &TestCluster,
    node_id: u64,
    name: &str,
) -> Result<(OrganizationSlug, u64), Box<dyn std::error::Error>> {
    crate::common::wire_create_test_organization(cluster, node_id, name).await
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::wire_create_test_vault(cluster, node_id, organization).await
}

/// Bootstraps a global signing key via the TokenService RPC.
///
/// The saga orchestrator auto-creates signing keys on its poll cycle (default 30s),
/// but integration tests call this explicitly to avoid waiting.
async fn ensure_signing_key(
    cluster: &TestCluster,
    node_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_token_client(cluster, node_id);
    let result = client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        // Key already exists — idempotent success
        Err(RpcError::WireError(wire_err)) if wire_err.code == ErrorCode::FailedPrecondition => {
            Ok(())
        },
        Err(e) => Err(e.into()),
    }
}

/// Waits for the signing key to become available for token issuance.
///
/// After `CreateSigningKey` the key must propagate through Raft and be loaded
/// into the JwtEngine's in-memory cache before tokens can be signed. This polls
/// `CreateUserSession` until it succeeds or times out.
async fn wait_for_signing_key_ready(
    cluster: &TestCluster,
    node_id: u64,
    user: UserSlug,
) -> ws::TokenPair {
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        let client = wire_token_client(cluster, node_id);
        let result = client
            .create_user_session(
                wt::CreateUserSessionRequest {
                    user: Some(WUserSlug::new(user.value())),
                    credential_used: None,
                    caller: Some(WUserSlug::new(user.value())),
                },
                rand::random::<u128>(),
            )
            .await;

        match result {
            Ok(response) => {
                return response.tokens.expect("CreateUserSession should return tokens");
            },
            Err(e) => {
                if start.elapsed() > timeout {
                    panic!("Signing key not ready after {timeout:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
        }
    }
}

/// Returns the kid of the active global signing key.
async fn get_active_global_kid(client: &TokenServiceClient) -> String {
    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get public keys")
        .keys;
    keys.iter().find(|k| k.status == "active").expect("should have active key").kid.clone()
}

/// Validates an access token with the given audience, returning the response.
async fn validate_token(
    client: &TokenServiceClient,
    token: &str,
    audience: &str,
) -> Result<wt::ValidateTokenResponse, RpcError> {
    client
        .validate_token(
            wt::ValidateTokenRequest {
                token: token.to_string(),
                expected_audience: audience.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
}

/// Polls `GetPublicKeys` until at least one key appears, returning the keys.
///
/// Used by auto-bootstrap tests where the saga orchestrator creates signing keys
/// asynchronously. Panics if no keys appear within the timeout.
async fn poll_for_public_keys(
    cluster: &TestCluster,
    node_id: u64,
    organization: Option<OrganizationSlug>,
    timeout: Duration,
    context: &str,
) -> Vec<wt::PublicKeyInfo> {
    let start = tokio::time::Instant::now();

    while start.elapsed() < timeout {
        let client = wire_token_client(cluster, node_id);
        let result = client
            .get_public_keys(
                wt::GetPublicKeysRequest { organization, caller: None },
                rand::random::<u128>(),
            )
            .await;

        if let Ok(resp) = result {
            let keys = resp.keys;
            if !keys.is_empty() {
                return keys;
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    panic!("{context}: no public keys appeared within {timeout:?}");
}

/// Helper to map common WireError → ErrorCode classification for tests
/// that need to assert on the error code.
fn wire_err_code(err: &RpcError) -> Option<ErrorCode> {
    match err {
        RpcError::WireError(wire_err) => Some(wire_err.code),
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Full user session lifecycle: create → validate → refresh → revoke.
///
/// Exercises the complete happy-path flow for user sessions, verifying that
/// each operation correctly transitions the token state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_user_session_lifecycle() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user42@test.com", node).await;
    let user = UserSlug::new(user_slug);

    // Bootstrap a global signing key so tokens can be issued
    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Step 1: Create user session (polls until signing key is loaded)
    let tokens = wait_for_signing_key_ready(&cluster, node.id, user).await;
    assert!(!tokens.access_token.is_empty(), "access token should be non-empty");
    assert!(!tokens.refresh_token.is_empty(), "refresh token should be non-empty");

    // Step 2: Validate the access token
    let client = wire_token_client(&cluster, node.id);
    let validated = validate_token(&client, &tokens.access_token, "inferadb-control")
        .await
        .expect("validate should succeed");
    assert_eq!(validated.subject, format!("user:{user}"));
    assert_eq!(validated.token_type, "user_session");

    // Step 3: Refresh the token pair
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("refresh should succeed");
    let new_tokens = refresh_resp.tokens.expect("refresh should return new tokens");
    assert!(!new_tokens.access_token.is_empty());
    assert!(!new_tokens.refresh_token.is_empty());
    // Rotated: old refresh token should differ from new one
    assert_ne!(
        tokens.refresh_token, new_tokens.refresh_token,
        "refresh should rotate the refresh token"
    );

    // Step 4: Old refresh token should be invalidated (rotation theft detection)
    let old_refresh_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(old_refresh_result.is_err(), "old refresh token should be rejected after rotation");

    // Step 5: Revoke via the new refresh token
    client
        .revoke_token(
            wt::RevokeTokenRequest { refresh_token: new_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("revoke should succeed");

    // Step 6: After revocation, refresh with the new token should fail
    let post_revoke = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: new_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(post_revoke.is_err(), "refresh should fail after revocation");
}

/// Full vault token lifecycle: create → validate → refresh → revoke.
///
/// Tests vault-scoped tokens which carry organization, app, vault, and scope claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_token_lifecycle() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Set up org + vault + app (enabled)
    let (org, admin_slug) =
        create_organization(&cluster, node.id, "vault-token-test").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app = create_app(&cluster, node.id, org, "vault-token-app", admin_slug)
        .await
        .expect("create app");
    add_app_vault(&cluster, node.id, org, app, vault, &["read", "write"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    // Bootstrap a global signing key
    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Wait for signing key propagation by polling vault token creation
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read", "write"]).await;
    assert!(!tokens.access_token.is_empty());
    assert!(!tokens.refresh_token.is_empty());

    // Validate the vault access token
    let client = wire_token_client(&cluster, node.id);
    let validated = validate_token(&client, &tokens.access_token, "inferadb-engine")
        .await
        .expect("validate vault token");
    assert_eq!(validated.token_type, "vault_access");

    // Refresh the vault token pair
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("refresh vault token");
    let new_tokens = refresh_resp.tokens.expect("refresh returns tokens");
    assert_ne!(tokens.refresh_token, new_tokens.refresh_token);

    // Revoke the vault token
    client
        .revoke_token(
            wt::RevokeTokenRequest { refresh_token: new_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("revoke vault token");

    // After revocation, refresh should fail
    let post_revoke = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: new_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(post_revoke.is_err(), "vault token refresh should fail after revocation");
}

/// Revoke all user sessions atomically.
///
/// Creates multiple sessions for the same user, then revokes them all at once.
/// Verifies that all refresh tokens are invalidated and the revoked count matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_revoke_all_user_sessions() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user99@test.com", node).await;
    let user = UserSlug::new(user_slug);

    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Create 3 separate sessions for the same user
    let session1 = wait_for_signing_key_ready(&cluster, node.id, user).await;
    let client = wire_token_client(&cluster, node.id);
    let session2 = client
        .create_user_session(
            wt::CreateUserSessionRequest {
                user: Some(WUserSlug::new(user.value())),
                credential_used: None,
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("session 2")
        .tokens
        .expect("tokens");
    let session3 = client
        .create_user_session(
            wt::CreateUserSessionRequest {
                user: Some(WUserSlug::new(user.value())),
                credential_used: None,
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("session 3")
        .tokens
        .expect("tokens");

    // All three should be independently refreshable
    for (i, token) in [&session1.refresh_token, &session2.refresh_token, &session3.refresh_token]
        .iter()
        .enumerate()
    {
        let result = client
            .refresh_token(
                wt::RefreshTokenRequest { refresh_token: (*token).clone() },
                rand::random::<u128>(),
            )
            .await;
        assert!(result.is_ok(), "session {} should be refreshable before revoke-all", i + 1);
    }

    // Revoke all sessions
    let revoke_resp = client
        .revoke_all_user_sessions(
            wt::RevokeAllUserSessionsRequest {
                user: Some(WUserSlug::new(user.value())),
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("revoke_all should succeed");
    let revoked_count = revoke_resp.revoked_count;
    // We created 3 sessions, but each refresh above created a new token family entry,
    // so the exact count depends on implementation. At minimum, the original 3 should
    // be revoked.
    assert!(revoked_count >= 3, "should have revoked at least 3 sessions, got {revoked_count}");

    // After revoke-all, none of the original refresh tokens should work.
    // (The refreshed tokens from the loop above are also revoked because
    // revoke-all increments the user's TokenVersion.)
    for (i, token) in [&session1.refresh_token, &session2.refresh_token, &session3.refresh_token]
        .iter()
        .enumerate()
    {
        let result = client
            .refresh_token(
                wt::RefreshTokenRequest { refresh_token: (*token).clone() },
                rand::random::<u128>(),
            )
            .await;
        assert!(result.is_err(), "session {} refresh should fail after revoke-all", i + 1);
    }

    // New sessions for the same user should still work (user isn't blocked)
    let new_session = client
        .create_user_session(
            wt::CreateUserSessionRequest {
                user: Some(WUserSlug::new(user.value())),
                credential_used: None,
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("new session after revoke-all should work");
    assert!(new_session.tokens.is_some(), "new session should return tokens");
}

/// Organization deletion cascades: refresh tokens revoked.
///
/// When an organization is soft-deleted, all refresh tokens for that organization's
/// vault tokens should be revoked. Signing keys are retained during the retention
/// window for in-flight access token validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_org_deletion_cascades_token_revocation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Set up org + vault + app (enabled) + vault token
    let (org, admin_slug) =
        create_organization(&cluster, node.id, "cascade-test").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "cascade-app", admin_slug).await.expect("create app");
    add_app_vault(&cluster, node.id, org, app, vault, &["read"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Create a vault token (poll until signing key is ready)
    let vault_tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Verify the vault token is valid before deletion
    let token_client = wire_token_client(&cluster, node.id);
    validate_token(&token_client, &vault_tokens.access_token, "inferadb-engine")
        .await
        .expect("vault token should be valid before org deletion");

    // Delete the organization
    let org_client = wire_organization_client(&cluster, node.id);
    org_client
        .delete_organization(
            wo::DeleteOrganizationRequest {
                slug: Some(org),
                caller: Some(WUserSlug::new(admin_slug)),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("delete organization");

    // Wait briefly for cascade to propagate through Raft
    tokio::time::sleep(Duration::from_millis(500)).await;

    // After org deletion, refresh should fail (tokens revoked in cascade)
    let refresh_result = token_client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: vault_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(refresh_result.is_err(), "vault token refresh should fail after org deletion");
}

// ============================================================================
// Race Condition & Concurrency Tests
// ============================================================================

/// Refresh token theft detection: reusing a consumed token poisons the family.
///
/// The "poisoned family" pattern works as follows:
/// 1. User gets tokens (access + refresh_A) in family F
/// 2. Legitimate refresh: refresh_A → refresh_B (refresh_A marked used)
/// 3. Attacker replays refresh_A → state machine sees used=true, poisons family F
/// 4. Legitimate user tries refresh_B → rejected because family F is poisoned
///
/// This ensures that token theft is detected and the entire session lineage is
/// invalidated, forcing both parties to re-authenticate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_refresh_token_theft_detection() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user300@test.com", node).await;
    let user = UserSlug::new(user_slug);

    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Step 1: Create a session (family F, refresh_A)
    let tokens = wait_for_signing_key_ready(&cluster, node.id, user).await;
    let refresh_a = tokens.refresh_token.clone();

    // Step 2: Legitimate refresh: refresh_A → refresh_B
    let client = wire_token_client(&cluster, node.id);
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_a.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("legitimate refresh should succeed");
    let new_tokens = refresh_resp.tokens.expect("should return new tokens");
    let refresh_b = new_tokens.refresh_token.clone();

    // Step 3: Attacker replays refresh_A → family gets poisoned
    let replay_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_a.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(replay_result.is_err(), "replaying used refresh_A should fail");
    let replay_err = replay_result.unwrap_err();
    assert_eq!(
        wire_err_code(&replay_err),
        Some(ErrorCode::Unauthenticated),
        "replay should return Unauthenticated, got: {replay_err:?}"
    );

    // Step 4: Legitimate user tries refresh_B → rejected (family poisoned)
    let poisoned_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_b.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(
        poisoned_result.is_err(),
        "refresh_B should fail because the family is poisoned by the replay in step 3"
    );
    let poisoned_err = poisoned_result.unwrap_err();
    assert_eq!(
        wire_err_code(&poisoned_err),
        Some(ErrorCode::Unauthenticated),
        "poisoned family should return Unauthenticated"
    );
}

/// Race condition: concurrent RefreshToken + RevokeAllUserSessions.
///
/// When a user revokes all sessions (e.g. password change) while a concurrent
/// refresh is in flight, the system must ensure the refresh fails. The service
/// layer reads the current `TokenVersion` and passes it as `expected_version`
/// to the Raft proposal. If `RevokeAllUserSessions` commits first (incrementing
/// the version), the refresh's `expected_version` won't match and the state
/// machine rejects it with `TokenVersionMismatch`.
///
/// Regardless of ordering, the invariant is: after `RevokeAllUserSessions`
/// completes, no prior refresh tokens should be usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_refresh_and_revoke_all() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user400@test.com", node).await;
    let user = UserSlug::new(user_slug);

    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Create a session
    let tokens = wait_for_signing_key_ready(&cluster, node.id, user).await;
    let refresh_token = tokens.refresh_token.clone();

    // Fire both operations concurrently.
    // Raft serializes them — one commits first. We test the invariant
    // that after revoke-all, the refresh token cannot be used.
    let client_for_refresh = wire_token_client(&cluster, node.id);
    let refresh_token_clone = refresh_token.clone();

    let refresh_handle = tokio::spawn(async move {
        client_for_refresh
            .refresh_token(
                wt::RefreshTokenRequest { refresh_token: refresh_token_clone },
                rand::random::<u128>(),
            )
            .await
    });

    let client_for_revoke = wire_token_client(&cluster, node.id);
    let revoke_handle = tokio::spawn(async move {
        client_for_revoke
            .revoke_all_user_sessions(
                wt::RevokeAllUserSessionsRequest {
                    user: Some(WUserSlug::new(user.value())),
                    caller: Some(WUserSlug::new(user.value())),
                },
                rand::random::<u128>(),
            )
            .await
    });

    let (refresh_result, revoke_result) = tokio::join!(refresh_handle, revoke_handle);
    let refresh_result = refresh_result.expect("refresh task should not panic");
    let revoke_result = revoke_result.expect("revoke task should not panic");

    // RevokeAllUserSessions should always succeed
    revoke_result.expect("revoke_all should succeed");

    // The key invariant: after revoke-all completes, the original refresh token
    // must not be usable. Two possible orderings:
    //
    // Case A: Revoke commits first → refresh fails with TokenVersionMismatch
    //   (expected_version doesn't match the incremented version)
    //
    // Case B: Refresh commits first → refresh succeeds, but the token it returned
    //   is now part of a revoked family (revoke-all revokes all subject tokens).
    //   Attempting to use the NEW refresh token should fail.
    match refresh_result {
        Err(err) => {
            // Case A: Refresh rejected because version was incremented
            assert_eq!(
                wire_err_code(&err),
                Some(ErrorCode::Unauthenticated),
                "rejected refresh should be Unauthenticated, got: {err:?}"
            );
        },
        Ok(response) => {
            // Case B: Refresh succeeded before revoke. The new tokens should be
            // invalid because revoke-all also revoked all families for this user.
            let new_tokens = response.tokens.expect("should have tokens");

            // The new refresh token should be unusable (family was revoked by revoke-all)
            let client = wire_token_client(&cluster, node.id);
            let post_revoke_refresh = client
                .refresh_token(
                    wt::RefreshTokenRequest { refresh_token: new_tokens.refresh_token.clone() },
                    rand::random::<u128>(),
                )
                .await;
            assert!(
                post_revoke_refresh.is_err(),
                "new refresh token should be invalid after revoke-all"
            );

            // The new access token should also be invalid (TokenVersion was bumped)
            let validate_result =
                validate_token(&client, &new_tokens.access_token, "inferadb-control").await;
            assert!(
                validate_result.is_err(),
                "access token signed with old version should fail validation after revoke-all"
            );
        },
    }
}

/// Concurrent refresh of the same token: one succeeds, the other gets reuse detection.
///
/// When two concurrent `RefreshToken` calls arrive with the same refresh token,
/// Raft serializes the `UseRefreshToken` proposals. The first one succeeds and
/// marks the token as used. The second one sees `used=true` and poisons the
/// family (same mechanism as theft detection).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_refresh_same_token() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user500@test.com", node).await;
    let user = UserSlug::new(user_slug);

    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Create a session
    let tokens = wait_for_signing_key_ready(&cluster, node.id, user).await;
    let refresh_token = tokens.refresh_token.clone();

    // Fire two refresh calls concurrently with the same refresh token
    let refresh_1 = refresh_token.clone();
    let refresh_2 = refresh_token.clone();

    let client_1 = wire_token_client(&cluster, node.id);
    let handle_1 = tokio::spawn(async move {
        client_1
            .refresh_token(
                wt::RefreshTokenRequest { refresh_token: refresh_1 },
                rand::random::<u128>(),
            )
            .await
    });

    let client_2 = wire_token_client(&cluster, node.id);
    let handle_2 = tokio::spawn(async move {
        client_2
            .refresh_token(
                wt::RefreshTokenRequest { refresh_token: refresh_2 },
                rand::random::<u128>(),
            )
            .await
    });

    let (result_1, result_2) = tokio::join!(handle_1, handle_2);
    let result_1 = result_1.expect("task 1 should not panic");
    let result_2 = result_2.expect("task 2 should not panic");

    // Exactly one should succeed, one should fail.
    // Partition results into winner (Ok) and loser (Err).
    let (winning_response, failed_err) = match (result_1, result_2) {
        (Ok(r1), Err(e2)) => (r1, e2),
        (Err(e1), Ok(r2)) => (r2, e1),
        (Ok(_), Ok(_)) => panic!("both concurrent refreshes succeeded — expected exactly one"),
        (Err(e1), Err(e2)) => panic!(
            "both concurrent refreshes failed — expected exactly one success.\n  error 1: {e1:?}\n  error 2: {e2:?}",
        ),
    };

    assert_eq!(
        wire_err_code(&failed_err),
        Some(ErrorCode::Unauthenticated),
        "failed concurrent refresh should be Unauthenticated, got: {failed_err:?}"
    );

    // Furthermore: the family should now be poisoned. The winning refresh
    // returned a new token — that new token should also be unusable because
    // the losing refresh poisoned the family.
    let winning_tokens = winning_response.tokens.expect("should have tokens");

    let client = wire_token_client(&cluster, node.id);
    let poisoned_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: winning_tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(
        poisoned_result.is_err(),
        "new refresh token from winning concurrent refresh should be invalid (family poisoned)"
    );
}

// ============================================================================
// Scope & Authorization Tests
// ============================================================================

/// Creates an app within an organization and returns its slug.
async fn create_app(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    name: &str,
    caller: u64,
) -> Result<AppSlug, Box<dyn std::error::Error>> {
    let client = wire_app_client(cluster, node_id);
    let app_slug = inferadb_ledger_types::snowflake::generate_app_slug()?;
    let response = client
        .create_app(
            wa::CreateAppRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(caller)),
                name: name.to_string(),
                description: None,
                slug: Some(app_slug),
            },
            rand::random::<u128>(),
        )
        .await?;

    let slug = response.app.and_then(|a| a.slug).ok_or("No app slug in response")?;

    Ok(slug)
}

/// Connects an app to a vault with given scopes.
async fn add_app_vault(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    app: AppSlug,
    vault: VaultSlug,
    scopes: &[&str],
    caller: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_app_client(cluster, node_id);
    client
        .add_app_vault(
            wa::AddAppVaultRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(caller)),
                app: Some(app),
                vault: Some(vault),
                allowed_scopes: scopes.iter().map(|s| s.to_string()).collect(),
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(())
}

/// Updates allowed scopes on an app-vault connection.
async fn update_app_vault_scopes(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    app: AppSlug,
    vault: VaultSlug,
    scopes: &[&str],
    caller: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_app_client(cluster, node_id);
    client
        .update_app_vault(
            wa::UpdateAppVaultRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(caller)),
                app: Some(app),
                vault: Some(vault),
                allowed_scopes: scopes.iter().map(|s| s.to_string()).collect(),
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(())
}

/// Removes an app-vault connection.
async fn remove_app_vault(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    app: AppSlug,
    vault: VaultSlug,
    caller: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_app_client(cluster, node_id);
    client
        .remove_app_vault(
            wa::RemoveAppVaultRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(caller)),
                app: Some(app),
                vault: Some(vault),
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(())
}

/// Sets an app's enabled/disabled status.
async fn set_app_enabled(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    app: AppSlug,
    enabled: bool,
    caller: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_app_client(cluster, node_id);
    client
        .set_app_enabled(
            wa::SetAppEnabledRequest {
                organization: Some(org),
                caller: Some(WUserSlug::new(caller)),
                app: Some(app),
                enabled,
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(())
}

/// Bootstraps an org-scoped signing key via the TokenService RPC.
async fn ensure_org_signing_key(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = wire_token_client(cluster, node_id);
    client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Organization,
                organization: Some(org),
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(())
}

/// Creates a vault token, polling until signing key is loaded.
///
/// Returns the token pair on success.
async fn wait_for_vault_token(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    app: AppSlug,
    vault: VaultSlug,
    scopes: &[&str],
) -> ws::TokenPair {
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        let client = wire_token_client(cluster, node_id);
        let result = client
            .create_vault_token(
                wt::CreateVaultTokenRequest {
                    organization: Some(org),
                    app: Some(app),
                    vault: Some(vault),
                    scopes: scopes.iter().map(|s| s.to_string()).collect(),
                },
                rand::random::<u128>(),
            )
            .await;

        match result {
            Ok(response) => {
                return response.tokens.expect("should return tokens");
            },
            Err(e) => {
                if start.elapsed() > timeout {
                    panic!("Vault token creation not ready after {timeout:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
        }
    }
}

/// Vault token scope validation: requested scopes must be a subset of allowed.
///
/// When creating a vault token, any scope not in the app-vault connection's
/// `allowed_scopes` is rejected with PERMISSION_DENIED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_vault_token_scope_validation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "scope-validation").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "scope-test-app", admin_slug).await.expect("create app");

    // Connect app to vault with limited scopes
    add_app_vault(&cluster, node.id, org, app, vault, &["read", "write"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    // Bootstrap signing keys
    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Creating a token with allowed scopes should succeed
    let _tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Creating a token with a disallowed scope should fail
    let client = wire_token_client(&cluster, node.id);
    let result = client
        .create_vault_token(
            wt::CreateVaultTokenRequest {
                organization: Some(org),
                app: Some(app),
                vault: Some(vault),
                scopes: vec!["read".to_string(), "admin".to_string()],
            },
            rand::random::<u128>(),
        )
        .await;

    assert!(result.is_err(), "scope 'admin' should be rejected");
    let err = result.unwrap_err();
    assert_eq!(
        wire_err_code(&err),
        Some(ErrorCode::PermissionDenied),
        "disallowed scope should return PermissionDenied, got: {err:?}"
    );
    if let RpcError::WireError(wire_err) = &err {
        assert!(
            wire_err.message.contains("admin"),
            "error message should mention the rejected scope: {}",
            wire_err.message
        );
    }
}

/// Scope reduction on vault token refresh: new token reflects current policy.
///
/// When `allowed_scopes` on an app-vault connection is reduced between token
/// creation and refresh, the new access token uses the reduced scopes (current
/// policy is authoritative).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_scope_reduction_on_vault_refresh() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "scope-reduce").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "reduce-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read", "write", "delete"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create vault token with all three scopes
    let tokens =
        wait_for_vault_token(&cluster, node.id, org, app, vault, &["read", "write", "delete"])
            .await;

    // Reduce allowed scopes on the connection
    update_app_vault_scopes(&cluster, node.id, org, app, vault, &["read"], admin_slug)
        .await
        .expect("update scopes");

    // Refresh the token — new access token should have reduced scopes
    let client = wire_token_client(&cluster, node.id);
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("refresh should succeed with reduced scopes");
    let new_tokens = refresh_resp.tokens.expect("should return new tokens");

    // Validate the new access token and check its claims
    let validated = validate_token(&client, &new_tokens.access_token, "inferadb-engine")
        .await
        .expect("validate new token");
    assert_eq!(validated.token_type, "vault_access");

    // The claims should contain only the reduced scopes
    match validated.claims {
        Some(wt::ValidateTokenClaims::VaultAccess(vault_claims)) => {
            assert_eq!(
                vault_claims.scopes,
                vec!["read".to_string()],
                "refreshed token scopes should match reduced connection scopes"
            );
        },
        other => panic!("Expected VaultAccess claims, got: {other:?}"),
    }
}

/// Scope expansion on vault token refresh: new token gains expanded scopes.
///
/// When `allowed_scopes` on an app-vault connection is expanded between token
/// creation and refresh, the new access token gains the expanded scopes on
/// refresh (current policy is authoritative).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_scope_expansion_on_vault_refresh() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "scope-expand").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "expand-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create vault token with just "read"
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Expand allowed scopes on the connection
    update_app_vault_scopes(
        &cluster,
        node.id,
        org,
        app,
        vault,
        &["read", "write", "admin"],
        admin_slug,
    )
    .await
    .expect("expand scopes");

    // Refresh the token — new access token should have expanded scopes
    let client = wire_token_client(&cluster, node.id);
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("refresh should succeed with expanded scopes");
    let new_tokens = refresh_resp.tokens.expect("should return new tokens");

    // Validate the new access token and check its claims
    let validated = validate_token(&client, &new_tokens.access_token, "inferadb-engine")
        .await
        .expect("validate new token");

    match validated.claims {
        Some(wt::ValidateTokenClaims::VaultAccess(vault_claims)) => {
            let mut scopes = vault_claims.scopes.clone();
            scopes.sort();
            assert_eq!(
                scopes,
                vec!["admin".to_string(), "read".to_string(), "write".to_string()],
                "refreshed token scopes should match expanded connection scopes"
            );
        },
        other => panic!("Expected VaultAccess claims, got: {other:?}"),
    }
}

/// Connection removal on vault token refresh: refresh fails.
///
/// When an app-vault connection is removed between token creation and refresh,
/// the state machine rejects the refresh because `AppVaultConnection` no longer
/// exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_connection_removal_on_vault_refresh() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "conn-remove").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "remove-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read", "write"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create vault token
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Remove the vault connection
    remove_app_vault(&cluster, node.id, org, app, vault, admin_slug)
        .await
        .expect("remove app vault");

    // Wait for Raft to propagate the removal
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Refresh should fail — connection no longer exists
    let client = wire_token_client(&cluster, node.id);
    let result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;

    assert!(result.is_err(), "refresh should fail after connection removal");
}

/// App-enabled check on vault token refresh: disabled app rejects refresh.
///
/// When an app is disabled between token creation and refresh, the state
/// machine rejects the refresh because the app is no longer enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_app_disabled_rejects_vault_refresh() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "app-disable-refresh").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "disable-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create vault token while app is enabled
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Disable the app
    set_app_enabled(&cluster, node.id, org, app, false, admin_slug).await.expect("disable app");

    // Wait for Raft cascade (app disable revokes tokens via inline cascade)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Refresh should fail — app is disabled and tokens are revoked
    let client = wire_token_client(&cluster, node.id);
    let result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;

    assert!(result.is_err(), "refresh should fail after app is disabled");
}

/// App disabled → vault token creation fails AND existing tokens revoked.
///
/// Disabling an app:
/// 1. Immediately rejects new `CreateVaultToken` requests (FAILED_PRECONDITION)
/// 2. Revokes all existing tokens via inline cascade in the SetAppEnabled apply handler
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_app_disabled_blocks_creation_and_revokes_tokens() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "app-disable-create").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app =
        create_app(&cluster, node.id, org, "block-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create a vault token while app is enabled
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Disable the app
    set_app_enabled(&cluster, node.id, org, app, false, admin_slug).await.expect("disable app");

    // Wait for cascade revocation through Raft
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New vault token creation should fail with FailedPrecondition
    let client = wire_token_client(&cluster, node.id);
    let create_result = client
        .create_vault_token(
            wt::CreateVaultTokenRequest {
                organization: Some(org),
                app: Some(app),
                vault: Some(vault),
                scopes: vec!["read".to_string()],
            },
            rand::random::<u128>(),
        )
        .await;

    assert!(create_result.is_err(), "vault token creation should fail for disabled app");
    let err = create_result.unwrap_err();
    assert_eq!(
        wire_err_code(&err),
        Some(ErrorCode::FailedPrecondition),
        "disabled app should return FailedPrecondition, got: {err:?}"
    );

    // Existing refresh token should be revoked by cascade
    let refresh_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(refresh_result.is_err(), "existing refresh token should be revoked after app disable");
}

/// App vault connection removed → vault token creation fails.
///
/// After removing an app-vault connection, `CreateVaultToken` should fail because
/// no connection exists. The RemoveAppVault apply handler inline-cascades
/// revocation of existing tokens for that app+vault combination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_connection_removed_blocks_creation_and_revokes_tokens() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    let (org, admin_slug) =
        create_organization(&cluster, node.id, "conn-remove-create").await.expect("create org");
    let vault = create_vault(&cluster, node.id, org).await.expect("create vault");
    let app = create_app(&cluster, node.id, org, "conn-app", admin_slug).await.expect("create app");

    add_app_vault(&cluster, node.id, org, app, vault, &["read", "write"], admin_slug)
        .await
        .expect("add app vault");
    set_app_enabled(&cluster, node.id, org, app, true, admin_slug).await.expect("enable app");

    ensure_signing_key(&cluster, node.id).await.expect("create global key");
    ensure_org_signing_key(&cluster, node.id, org).await.expect("create org key");

    // Create a vault token while connection exists
    let tokens = wait_for_vault_token(&cluster, node.id, org, app, vault, &["read"]).await;

    // Remove the app-vault connection
    remove_app_vault(&cluster, node.id, org, app, vault, admin_slug)
        .await
        .expect("remove app vault");

    // Wait for cascade revocation through Raft
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New vault token creation should fail (no connection)
    let client = wire_token_client(&cluster, node.id);
    let create_result = client
        .create_vault_token(
            wt::CreateVaultTokenRequest {
                organization: Some(org),
                app: Some(app),
                vault: Some(vault),
                scopes: vec!["read".to_string()],
            },
            rand::random::<u128>(),
        )
        .await;

    assert!(create_result.is_err(), "vault token creation should fail without connection");
    let err = create_result.unwrap_err();
    assert_eq!(
        wire_err_code(&err),
        Some(ErrorCode::NotFound),
        "missing connection should return NotFound, got: {err:?}"
    );

    // Existing refresh token should be revoked by cascade
    let refresh_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: tokens.refresh_token.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(
        refresh_result.is_err(),
        "existing refresh token should be revoked after connection removal"
    );
}

// ============================================================================
// Signing Key Lifecycle Tests
// ============================================================================

/// Signing key rotation during active sessions: old tokens remain valid during grace period.
///
/// When a signing key is rotated with a non-zero grace period, the old key transitions
/// to `Rotated` status. Tokens signed with the old key remain verifiable until
/// `valid_until` (proposed_at + grace_period) expires. New tokens are signed with the new key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_signing_key_rotation_during_active_sessions() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user600@test.com", node).await;
    let user = UserSlug::new(user_slug);

    // Create the initial global signing key
    ensure_signing_key(&cluster, node.id).await.expect("create signing key");

    // Create a user session signed with the original key
    let tokens_old = wait_for_signing_key_ready(&cluster, node.id, user).await;

    // Get the original key's kid
    let client = wire_token_client(&cluster, node.id);
    let old_kid = get_active_global_kid(&client).await;

    // Rotate the key with a generous grace period (3600 seconds = 1 hour)
    let rotate_resp = client
        .rotate_signing_key(
            wt::RotateSigningKeyRequest {
                kid: old_kid.clone(),
                grace_period_secs: 3600,
                force_revoke: false,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("rotate signing key");
    let new_key = rotate_resp.new_key.expect("rotation should return new key");
    let new_kid = new_key.kid.clone();
    assert_ne!(old_kid, new_kid, "new key should have a different kid");
    assert_eq!(new_key.status, "active");

    // Wait briefly for cache update
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Old token should still validate (grace period active)
    let validate_old = validate_token(&client, &tokens_old.access_token, "inferadb-control").await;
    assert!(
        validate_old.is_ok(),
        "old token should still be valid during grace period: {:?}",
        validate_old.err()
    );

    // New session should be created with the new key
    let tokens_new = client
        .create_user_session(
            wt::CreateUserSessionRequest {
                user: Some(WUserSlug::new(user.value())),
                credential_used: None,
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await
        .expect("new session after rotation")
        .tokens
        .expect("should return tokens");

    // New token should also validate
    validate_token(&client, &tokens_new.access_token, "inferadb-control")
        .await
        .expect("new token should be valid after rotation");

    // Public keys should show both: new key as active, old key as rotated
    let keys_after = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get public keys after rotation")
        .keys;
    assert_eq!(keys_after.len(), 2, "should have both old (rotated) and new (active) keys");

    let active_keys: Vec<_> = keys_after.iter().filter(|k| k.status == "active").collect();
    let rotated_keys: Vec<_> = keys_after.iter().filter(|k| k.status == "rotated").collect();
    assert_eq!(active_keys.len(), 1, "exactly one key should be active");
    assert_eq!(rotated_keys.len(), 1, "exactly one key should be rotated");
    assert_eq!(active_keys[0].kid, new_kid);
    assert_eq!(rotated_keys[0].kid, old_kid);
    assert!(rotated_keys[0].valid_until > 0, "rotated key should have valid_until set");
}

/// Auto-bootstrap: global signing key created on cluster init via saga.
///
/// The saga orchestrator automatically checks for a missing global signing key
/// on each poll cycle and creates one if absent. This test verifies the auto-
/// bootstrap fires without any explicit `CreateSigningKey` RPC call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_auto_bootstrap_global_signing_key() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Do NOT call ensure_signing_key — let the saga orchestrator auto-create it.
    // TestCluster uses saga poll_interval_secs=2, so the key should appear within ~5s.
    let keys = poll_for_public_keys(
        &cluster,
        node.id,
        None,
        Duration::from_secs(10),
        "global signing key auto-bootstrap",
    )
    .await;
    assert_eq!(keys.len(), 1, "auto-bootstrap should create exactly one global key");
    assert_eq!(keys[0].status, "active");
}

/// Auto-bootstrap: org signing key created on org creation via saga.
///
/// When an organization is created, the apply handler writes a
/// `CreateSigningKeySaga` record. The saga orchestrator picks it up on its next
/// poll cycle and generates an org-scoped signing key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_auto_bootstrap_org_signing_key() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Create an organization — this triggers a CreateSigningKeySaga for the org scope
    let (org, _admin_slug) =
        create_organization(&cluster, node.id, "auto-key-org").await.expect("create org");

    // Wait for the saga orchestrator to auto-create the org signing key.
    // TestCluster uses saga poll_interval_secs=2.
    let keys = poll_for_public_keys(
        &cluster,
        node.id,
        Some(org),
        Duration::from_secs(10),
        "org signing key auto-bootstrap",
    )
    .await;
    assert_eq!(keys.len(), 1, "auto-bootstrap should create exactly one org key");
    assert_eq!(keys[0].status, "active");
}

/// CreateSigningKey saga idempotency: re-running after success is a no-op.
///
/// The `CreateSigningKey` Raft handler checks for an existing active key with
/// the same scope before creating. If one already exists, it returns success
/// without creating a duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_signing_key_idempotency() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Create a global signing key
    let client = wire_token_client(&cluster, node.id);
    let first = client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("first CreateSigningKey should succeed");
    let first_kid = first.key.expect("should return key info").kid;

    // Call CreateSigningKey again for the same scope — should not create a duplicate
    let second = client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await;

    // The second call may succeed (idempotent) or fail (scope already has active key).
    // Either way, GetPublicKeys should show exactly one active global key.
    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get public keys")
        .keys;

    let active_keys: Vec<_> = keys.iter().filter(|k| k.status == "active").collect();
    assert_eq!(
        active_keys.len(),
        1,
        "should have exactly one active global key after duplicate create, got: {:?}",
        keys
    );

    // If the second call succeeded, it should have returned the same key (idempotent)
    if let Ok(resp) = second
        && let Some(key) = resp.key
    {
        assert_eq!(
            key.kid, first_kid,
            "idempotent CreateSigningKey should return the existing key's kid"
        );
    }
}

/// Global key not found: CreateUserSession fails with NoActiveSigningKey.
///
/// Without a global signing key, user session creation should fail because
/// there's no key available to sign the JWT. Since the saga orchestrator
/// auto-creates the global key on a 2s poll cycle, we call immediately
/// after cluster startup. If the saga hasn't fired yet, we get
/// FAILED_PRECONDITION. If it raced ahead, we verify the success path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_global_key_not_found_returns_error() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];
    let user_slug = setup_user(&node.addr, "Test User", "user700@test.com", node).await;
    let user = UserSlug::new(user_slug);

    // Immediately try to create a user session WITHOUT explicitly creating a signing key.
    // The saga orchestrator may or may not have auto-created the key by now.
    let client = wire_token_client(&cluster, node.id);
    let result = client
        .create_user_session(
            wt::CreateUserSessionRequest {
                user: Some(WUserSlug::new(user.value())),
                credential_used: None,
                caller: Some(WUserSlug::new(user.value())),
            },
            rand::random::<u128>(),
        )
        .await;

    match result {
        Err(err) => {
            // Expected path: saga hasn't fired yet, no signing key available
            assert_eq!(
                wire_err_code(&err),
                Some(ErrorCode::FailedPrecondition),
                "missing signing key should return FailedPrecondition, got: {err:?}"
            );
        },
        Ok(_) => {
            // Saga auto-bootstrap raced ahead — verify it created the key.
            // This is still a valid test outcome: the saga fired, key was created,
            // and CreateUserSession succeeded with it.
            let keys = client
                .get_public_keys(
                    wt::GetPublicKeysRequest { organization: None, caller: None },
                    rand::random::<u128>(),
                )
                .await
                .expect("get public keys")
                .keys;
            assert!(
                !keys.is_empty(),
                "if CreateUserSession succeeded, a global signing key must exist"
            );
        },
    }
}

// ============================================================================
// State Machine Determinism Tests
// ============================================================================

/// Verifies that the Raft state machine uses `proposed_at` for all timestamps.
///
/// The state machine must never call `Utc::now()` — all timestamps must derive
/// from the Raft entry's `proposed_at` field to ensure deterministic replay.
/// If two timestamps are set in the same apply handler from `proposed_at`, they
/// are *exactly* equal. With `Utc::now()`, nanosecond drift between calls would
/// make them differ.
///
/// Checks:
/// 1. After `CreateSigningKey`: `created_at == valid_from` (same apply handler)
/// 2. After `RotateSigningKey`: new key's `created_at == valid_from` (same handler)
/// 3. After `RotateSigningKey`: old key's `valid_until - new_key.created_at == grace_period` (both
///    set from the same rotation `proposed_at`)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_machine_timestamps_use_proposed_at() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Create a global signing key
    let client = wire_token_client(&cluster, node.id);
    client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create signing key");

    // Read the created key's timestamps
    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get public keys")
        .keys;
    assert_eq!(keys.len(), 1, "should have exactly one key");
    let key = &keys[0];

    // Check 1: created_at == valid_from (both from proposed_at in CreateSigningKey handler)
    assert_eq!(
        key.created_at, key.valid_from,
        "created_at and valid_from must be identical (both derived from proposed_at)"
    );

    // Rotate the key with a known grace period
    let grace_period_secs: u64 = 7200; // 2 hours
    let old_kid = key.kid.clone();
    client
        .rotate_signing_key(
            wt::RotateSigningKeyRequest {
                kid: old_kid.clone(),
                grace_period_secs,
                force_revoke: false,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("rotate signing key");

    // Read both keys (old=rotated, new=active)
    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get public keys after rotation")
        .keys;
    assert_eq!(keys.len(), 2, "should have old (rotated) + new (active) key");

    let new_key = keys.iter().find(|k| k.status == "active").expect("should have active key");
    let old_key = keys.iter().find(|k| k.status == "rotated").expect("should have rotated key");

    // Check 2: new key's created_at == valid_from (both from RotateSigningKey's proposed_at)
    assert_eq!(
        new_key.created_at, new_key.valid_from,
        "new key: created_at and valid_from must be identical (same proposed_at)"
    );

    // Check 3: old key's valid_until == new key's created_at + grace_period
    // Both the rotation timestamp (new_key.created_at) and the grace end
    // (old_key.valid_until) are computed from the same proposed_at:
    //   new_key.created_at = proposed_at (UNIX nanoseconds)
    //   old_key.valid_until = proposed_at + grace_period_secs * 1e9 (UNIX nanoseconds)
    let new_created_ns = new_key.created_at;
    let old_valid_until_ns = old_key.valid_until;
    assert!(old_valid_until_ns > 0, "rotated key should have valid_until set");

    let expected_valid_until_ns = new_created_ns + grace_period_secs * 1_000_000_000;
    assert_eq!(
        old_valid_until_ns, expected_valid_until_ns,
        "valid_until must be exactly proposed_at + grace_period_secs * 1e9 ns (deterministic, no wall-clock jitter)"
    );
}

/// Verifies that the poisoned family flag persists through Raft replication.
///
/// The poisoned family marker is written as a Raft-committed state entry. This
/// test uses a 3-node cluster to verify that after poisoning a token family on
/// the leader, the poisoned state is replicated to all nodes and remains
/// effective. Since all Raft state mutations are replicated, this also proves
/// the flag would survive a leader election (new leader has the same state).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_poisoned_family_persists_across_replication() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;

    // Wait for leader election and cluster stabilization
    let leader_id = cluster.wait_for_leader().await;
    let leader = cluster.node(leader_id).expect("leader node");

    ensure_signing_key(&cluster, leader.id).await.expect("create signing key");
    let user_slug = setup_user(&leader.addr, "Test User", "user950@test.com", leader).await;
    let user = UserSlug::new(user_slug);

    // Create a session → family F, refresh_A
    let tokens = wait_for_signing_key_ready(&cluster, leader.id, user).await;
    let refresh_a = tokens.refresh_token.clone();

    // Legitimate refresh: refresh_A → refresh_B (refresh_A marked used)
    let client = wire_token_client(&cluster, leader.id);
    let refresh_resp = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_a.clone() },
            rand::random::<u128>(),
        )
        .await
        .expect("legitimate refresh");
    let refresh_b = refresh_resp.tokens.expect("should return tokens").refresh_token;

    // Replay refresh_A → poisons family F
    let replay = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_a.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(replay.is_err(), "replaying used refresh_A should fail");

    // Wait for all nodes to sync the poisoned state
    assert!(
        cluster.wait_for_sync(Duration::from_secs(10)).await,
        "cluster should sync after family poisoning"
    );

    // Verify the poison persists: refresh_B should fail (family is poisoned)
    // This request goes through Raft to the leader, which has the poisoned
    // state committed and replicated to all nodes.
    let poisoned_result = client
        .refresh_token(
            wt::RefreshTokenRequest { refresh_token: refresh_b.clone() },
            rand::random::<u128>(),
        )
        .await;
    assert!(
        poisoned_result.is_err(),
        "refresh_B must fail: poisoned family flag committed through Raft persists"
    );
    let poisoned_err = poisoned_result.unwrap_err();
    assert_eq!(
        wire_err_code(&poisoned_err),
        Some(ErrorCode::Unauthenticated),
        "poisoned family should return Unauthenticated, got: {poisoned_err:?}"
    );

    // Additional verification: connect to a follower and verify it has
    // replicated state by performing a read-only operation.
    // ValidateToken reads local applied state (not forwarded to leader).
    let followers = cluster.followers();
    assert!(!followers.is_empty(), "should have follower nodes");
    let follower = followers[0];

    let follower_client = wire_token_client(&cluster, follower.id);

    // The access token should still validate on the follower (it's a JWT
    // verified locally — token validity is independent of refresh token state).
    // This proves the follower has the signing key state replicated.
    let validate_result =
        validate_token(&follower_client, &tokens.access_token, "inferadb-control").await;
    assert!(
        validate_result.is_ok(),
        "access token should still validate on follower (JWT verified locally)"
    );
}

// ============================================================================
// Operational Tests
// ============================================================================

/// TokenMaintenanceJob: rotated signing keys past grace period are transitioned to revoked.
///
/// Creates a signing key, rotates it with a short grace period (1 second), then waits
/// for the `TokenMaintenanceJob` (3s interval in tests) to pick up the rotated key and
/// propose `TransitionSigningKeyRevoked` through Raft. After the transition, the rotated
/// key should no longer appear in `GetPublicKeys` (only revoked or active keys are shown,
/// and revoked keys are filtered out).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_token_maintenance_transitions_rotated_keys() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Create and rotate a signing key with a short grace period
    let client = wire_token_client(&cluster, node.id);
    client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create signing key");

    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys")
        .keys;
    assert_eq!(keys.len(), 1);
    let old_kid = keys[0].kid.clone();

    // Rotate with 1-second grace period — the old key will expire quickly
    client
        .rotate_signing_key(
            wt::RotateSigningKeyRequest {
                kid: old_kid.clone(),
                grace_period_secs: 1,
                force_revoke: false,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("rotate signing key");

    // Immediately after rotation: should have 2 keys (active + rotated)
    let keys_after_rotate = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys after rotate")
        .keys;
    assert_eq!(
        keys_after_rotate.len(),
        2,
        "should have old (rotated) + new (active) key immediately after rotation"
    );

    // Wait for grace period to expire (1s) + maintenance cycle (3s interval) + margin
    tokio::time::sleep(Duration::from_secs(8)).await;

    // After maintenance: the rotated key should be transitioned to revoked and
    // no longer visible in GetPublicKeys (revoked keys are filtered out)
    let keys_after_maintenance = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys after maintenance")
        .keys;
    assert_eq!(
        keys_after_maintenance.len(),
        1,
        "only the new active key should remain after maintenance transitions the rotated key"
    );
    assert_eq!(keys_after_maintenance[0].status, "active");
    assert_ne!(
        keys_after_maintenance[0].kid, old_kid,
        "the remaining key should be the new one, not the old rotated key"
    );
}

/// TokenMaintenanceJob idempotency: running multiple cycles produces no double-counting.
///
/// After maintenance transitions a rotated key, subsequent cycles should find no more
/// keys to transition. This verifies idempotency — the job correctly skips already-
/// transitioned keys, and the public key count remains stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_token_maintenance_idempotency() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let node = &cluster.nodes()[0];

    // Create and rotate a signing key with a short grace period
    let client = wire_token_client(&cluster, node.id);
    client
        .create_signing_key(
            wt::CreateSigningKeyRequest {
                scope: wt::SigningKeyScope::Global,
                organization: None,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("create signing key");

    let keys = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys")
        .keys;
    let old_kid = keys[0].kid.clone();

    // Rotate with 1-second grace
    client
        .rotate_signing_key(
            wt::RotateSigningKeyRequest {
                kid: old_kid,
                grace_period_secs: 1,
                force_revoke: false,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("rotate signing key");

    // Wait for grace period (1s) + two full maintenance cycles (3s each) + margin.
    // This ensures the job runs at least twice after the grace period expires.
    tokio::time::sleep(Duration::from_secs(12)).await;

    // After two+ maintenance cycles: should still have exactly 1 key (the new active one).
    let keys_final = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys after multiple maintenance cycles")
        .keys;
    assert_eq!(
        keys_final.len(),
        1,
        "exactly one active key should remain after multiple maintenance cycles"
    );
    assert_eq!(keys_final[0].status, "active");

    // Rotate again to verify the maintenance job is still functional (not stuck/broken)
    let current_kid = keys_final[0].kid.clone();
    client
        .rotate_signing_key(
            wt::RotateSigningKeyRequest {
                kid: current_kid.clone(),
                grace_period_secs: 1,
                force_revoke: false,
                caller: None,
            },
            rand::random::<u128>(),
        )
        .await
        .expect("second rotation should succeed");

    // Wait for grace + maintenance cycle
    tokio::time::sleep(Duration::from_secs(8)).await;

    let keys_after_second = client
        .get_public_keys(
            wt::GetPublicKeysRequest { organization: None, caller: None },
            rand::random::<u128>(),
        )
        .await
        .expect("get keys after second rotation maintenance")
        .keys;
    assert_eq!(
        keys_after_second.len(),
        1,
        "maintenance job should still be functional and transition the second rotated key"
    );
    assert_ne!(
        keys_after_second[0].kid, current_kid,
        "the old key should have been transitioned by the second maintenance cycle"
    );
}
