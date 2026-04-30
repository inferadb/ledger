//! Integration tests for the email-based onboarding flow.
//!
//! Tests the three-phase onboarding pipeline:
//! 1. `InitiateEmailVerification` → verification code
//! 2. `VerifyEmailCode` → onboarding token (new user) or session (existing user)
//! 3. `CompleteRegistration` → user + session + organization (requires saga — tested separately)
//!
//! Tests that require the `CreateOnboardingUserSaga` to complete (happy-path complete_registration,
//! idempotent re-registration, cross-region existing user) are deferred until the saga steps
//! are fully implemented in the orchestrator.
//!
//! F.1.f.2.Stage1e Wave 4: migrated from the legacy tonic helper
//! `create_user_client` to the wire-protocol sibling `wire_user_client`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_wire::{error::ErrorCode, services::user as wu};
use inferadb_ledger_wire_services::UserServiceClient;
use inferadb_ledger_wire_transport::RpcError;

use crate::common::{TestCluster, wire_user_client};

// ============================================================================
// Test Helpers
// ============================================================================

/// Region slug for GLOBAL. TestCluster only registers the GLOBAL Raft group,
/// so all onboarding requests use this region.
const REGION_GLOBAL: &str = "global";

/// Initiates email verification and returns the code from the response.
async fn initiate(
    client: &UserServiceClient,
    email: &str,
    region: &str,
) -> Result<String, RpcError> {
    let resp = client
        .initiate_email_verification(
            wu::InitiateEmailVerificationRequest {
                email: email.to_string(),
                region: region.to_string(),
            },
            rand::random::<u128>(),
        )
        .await?;
    Ok(resp.code)
}

/// Verifies an email code and returns the raw response.
async fn verify(
    client: &UserServiceClient,
    email: &str,
    code: &str,
    region: &str,
) -> Result<wu::VerifyEmailCodeResponse, RpcError> {
    client
        .verify_email_code(
            wu::VerifyEmailCodeRequest {
                email: email.to_string(),
                code: code.to_string(),
                region: region.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
}

/// Extracts the onboarding token from a VerifyEmailCodeResponse (new-user path).
fn extract_onboarding_token(resp: &wu::VerifyEmailCodeResponse) -> &str {
    match resp.result.as_ref().expect("response should have result") {
        wu::VerifyEmailCodeResult::NewUser(new_user) => &new_user.onboarding_token,
        wu::VerifyEmailCodeResult::ExistingUser(_) => {
            panic!("expected NewUser result, got ExistingUser")
        },
        wu::VerifyEmailCodeResult::TotpRequired(_) => {
            panic!("expected NewUser result, got TotpRequired")
        },
    }
}

/// Asserts the error is a `WireError` with the expected code.
fn assert_wire_code(err: &RpcError, expected: ErrorCode, label: &str) {
    match err {
        RpcError::WireError(wire_err) => {
            assert_eq!(
                wire_err.code, expected,
                "{label}: expected {expected:?}, got {:?}: {}",
                wire_err.code, wire_err.message
            );
        },
        other => panic!("{label}: expected WireError({expected:?}), got: {other:?}"),
    }
}

// ============================================================================
// Tests: Initiate Email Verification
// ============================================================================

/// Initiate email verification returns a non-empty verification code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_email_verification_returns_code() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let code = initiate(&client, "alice@example.com", REGION_GLOBAL)
        .await
        .expect("initiate should succeed");

    assert!(!code.is_empty(), "verification code should be non-empty");
    // Codes are 6-character alphanumeric (A-Z, 0-9)
    assert_eq!(code.len(), 6, "code should be 6 characters");
    assert!(
        code.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        "code should be uppercase alphanumeric"
    );
}

/// Initiating verification twice for the same email returns a code each time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_twice_returns_codes() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let code1 = initiate(&client, "bob@example.com", REGION_GLOBAL).await.expect("first initiate");
    let code2 = initiate(&client, "bob@example.com", REGION_GLOBAL).await.expect("second initiate");

    assert!(!code1.is_empty());
    assert!(!code2.is_empty());
}

// ============================================================================
// Tests: Verify Email Code
// ============================================================================

/// Verify with a correct code returns a new-user result with onboarding token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_email_code_new_user() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let code = initiate(&client, "charlie@example.com", REGION_GLOBAL).await.expect("initiate");

    let resp = verify(&client, "charlie@example.com", &code, REGION_GLOBAL)
        .await
        .expect("verify should succeed");

    let token = extract_onboarding_token(&resp);
    assert!(!token.is_empty(), "onboarding token should be non-empty");
}

/// Verify with a wrong code returns an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_wrong_code_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    initiate(&client, "dave@example.com", REGION_GLOBAL).await.expect("initiate");

    let err = verify(&client, "dave@example.com", "ZZZZZZ", REGION_GLOBAL)
        .await
        .expect_err("wrong code should fail");

    // Wrong code results in an error — we don't constrain the specific
    // ErrorCode variant because the server may return NotFound,
    // Unauthenticated, or PermissionDenied depending on the rejection path.
    let _ = err;
}

/// Verify without prior initiate returns an error (no verification record).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_without_initiate_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = verify(&client, "nobody@example.com", "ABCDEF", REGION_GLOBAL)
        .await
        .expect_err("verify without initiate should fail");

    // Any error is sufficient — the server may return NotFound,
    // Unauthenticated, or PermissionDenied depending on the rejection path.
    let _ = err;
}

/// Verify with empty code returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_empty_code_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = verify(&client, "empty@example.com", "", REGION_GLOBAL)
        .await
        .expect_err("empty code should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "empty code");
}

// ============================================================================
// Tests: Region Validation
// ============================================================================

/// Malformed region slug returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_invalid_region_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = initiate(&client, "region@example.com", "BAD_REGION")
        .await
        .expect_err("malformed region slug should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "malformed region");
    if let RpcError::WireError(wire_err) = &err {
        assert!(
            wire_err.message.contains("region"),
            "error should mention region: {}",
            wire_err.message
        );
    }
}

/// Empty region returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_unspecified_region_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = initiate(&client, "unspec@example.com", "")
        .await
        .expect_err("empty region slug should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "empty region");
}

// ============================================================================
// Tests: Input Validation
// ============================================================================

/// Empty email returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_empty_email_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = initiate(&client, "", REGION_GLOBAL).await.expect_err("empty email should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "empty email");
}

/// Invalid email format returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_malformed_email_rejected() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = initiate(&client, "not-an-email", REGION_GLOBAL)
        .await
        .expect_err("malformed email should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "malformed email");
}

// ============================================================================
// Tests: Complete Registration (Error Paths)
// ============================================================================

/// Complete registration with a malformed onboarding token returns InvalidArgument
/// (or NotFound / PermissionDenied depending on the validator).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_malformed_token() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = client
        .complete_registration(
            wu::CompleteRegistrationRequest {
                onboarding_token: "not-a-valid-token".to_string(),
                email: "alice@example.com".to_string(),
                name: "Alice".to_string(),
                organization_name: "Alice Corp".to_string(),
                region: REGION_GLOBAL.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
        .expect_err("malformed token should fail");

    // Should be InvalidArgument (token decode failure) or similar
    match &err {
        RpcError::WireError(wire_err) => {
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::InvalidArgument
                        | ErrorCode::NotFound
                        | ErrorCode::PermissionDenied
                        | ErrorCode::Unauthenticated
                ),
                "expected validation error, got {:?}: {}",
                wire_err.code,
                wire_err.message,
            );
        },
        other => panic!("expected WireError, got: {other:?}"),
    }
}

/// Complete registration with empty onboarding token returns InvalidArgument.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_empty_token() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    let err = client
        .complete_registration(
            wu::CompleteRegistrationRequest {
                onboarding_token: String::new(),
                email: "alice@example.com".to_string(),
                name: "Alice".to_string(),
                organization_name: "Alice Corp".to_string(),
                region: REGION_GLOBAL.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
        .expect_err("empty token should fail");

    assert_wire_code(&err, ErrorCode::InvalidArgument, "empty token");
}

/// Complete registration without prior verification returns Unauthenticated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_without_verify() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    // Generate a valid-format onboarding token that doesn't match any record
    let (token, _hash) = inferadb_ledger_types::onboarding::generate_onboarding_token();

    let err = client
        .complete_registration(
            wu::CompleteRegistrationRequest {
                onboarding_token: token,
                email: "unverified@example.com".to_string(),
                name: "Nobody".to_string(),
                organization_name: "No Corp".to_string(),
                region: REGION_GLOBAL.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
        .expect_err("complete without verify should fail");

    // Task 4.1 unified every onboarding-auth failure mode to
    // `Unauthenticated: Authentication failed` — the specific shape (missing
    // account vs. wrong token) is deliberately not leaked over the wire.
    assert_wire_code(&err, ErrorCode::Unauthenticated, "complete without verify");
}

/// Complete registration with wrong token hash returns Unauthenticated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_wrong_token_hash() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    // Initiate and verify to create an onboarding account
    let code = initiate(&client, "wrong-hash@example.com", REGION_GLOBAL).await.expect("initiate");
    let resp =
        verify(&client, "wrong-hash@example.com", &code, REGION_GLOBAL).await.expect("verify");
    let _token = extract_onboarding_token(&resp);

    // Use a DIFFERENT valid token (not the one from verification)
    let (wrong_token, _) = inferadb_ledger_types::onboarding::generate_onboarding_token();

    let err = client
        .complete_registration(
            wu::CompleteRegistrationRequest {
                onboarding_token: wrong_token,
                email: "wrong-hash@example.com".to_string(),
                name: "WrongHash".to_string(),
                organization_name: "Wrong Corp".to_string(),
                region: REGION_GLOBAL.to_string(),
            },
            rand::random::<u128>(),
        )
        .await
        .expect_err("wrong token hash should fail");

    // Task 4.1 unified every onboarding-auth failure mode to
    // `Unauthenticated: Authentication failed`, so a wrong token hash is
    // indistinguishable from any other auth failure from the client's side.
    assert_wire_code(&err, ErrorCode::Unauthenticated, "wrong token hash");
}

// ============================================================================
// Tests: Re-verification
// ============================================================================

/// Re-verifying the same email after a successful verify produces a new onboarding token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reverify_produces_new_token() {
    let cluster = TestCluster::with_wire_transport(0).await;
    let client = wire_user_client(&cluster, cluster.nodes()[0].id);

    // First flow
    let code1 = initiate(&client, "reverify@example.com", REGION_GLOBAL).await.expect("initiate 1");
    let resp1 =
        verify(&client, "reverify@example.com", &code1, REGION_GLOBAL).await.expect("verify 1");
    let token1 = extract_onboarding_token(&resp1).to_string();

    // Second flow (re-initiate and re-verify)
    let code2 = initiate(&client, "reverify@example.com", REGION_GLOBAL).await.expect("initiate 2");
    let resp2 =
        verify(&client, "reverify@example.com", &code2, REGION_GLOBAL).await.expect("verify 2");
    let token2 = extract_onboarding_token(&resp2).to_string();

    // Both tokens should be valid but different
    assert!(!token1.is_empty());
    assert!(!token2.is_empty());
    assert_ne!(token1, token2, "re-verification should produce a different token");
}

// ============================================================================
// Tests: Domain Separation
// ============================================================================

/// Verification code hashes and email HMACs use different derivation paths,
/// so the same input string produces different outputs.
#[test]
fn test_domain_separation_code_hash_vs_email_hmac() {
    let key_bytes: [u8; 32] = [0xDE; 32];
    let key = inferadb_ledger_types::EmailBlindingKey::new(key_bytes, 1);

    let input = "test@example.com";
    let hmac1 = inferadb_ledger_types::compute_email_hmac(&key, input);
    let code_hash = inferadb_ledger_types::email_hash::compute_code_hash(&key, input);

    // Compute HMAC of the same input twice to verify determinism
    let hmac2 = inferadb_ledger_types::compute_email_hmac(&key, input);
    assert_eq!(hmac1, hmac2, "HMAC should be deterministic");

    // code_hash is [u8; 32], hmac is hex string — format them consistently
    let code_hash_hex = inferadb_ledger_types::bytes_to_hex(&code_hash);
    assert_ne!(
        hmac1, code_hash_hex,
        "email HMAC and code hash should use different derivation domains"
    );
}

// ============================================================================
// Tests: Missing Blinding Key
// ============================================================================

/// When email_blinding_key is not configured, onboarding RPCs return FailedPrecondition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_missing_blinding_key_returns_failed_precondition() {
    use inferadb_ledger_wire::{error::ErrorCode, services::user as wu};

    let cluster = crate::common::TestCluster::without_blinding_key(1, 1).await;
    let leader = cluster.nodes().iter().find(|n| n.is_leader()).expect("leader");
    let client = crate::common::wire_user_client(&cluster, leader.id);

    // Wait for cluster to stabilize
    cluster.wait_for_leaders(Duration::from_secs(10)).await;

    let request = wu::InitiateEmailVerificationRequest {
        email: "nokey@example.com".to_string(),
        region: "us-east-va".to_string(),
    };
    let err = client
        .initiate_email_verification(request, 1u128)
        .await
        .expect_err("should fail without blinding key");

    let wire_err = match err {
        inferadb_ledger_wire_transport::RpcError::WireError(w) => w,
        other => panic!("expected WireError, got {other:?}"),
    };
    assert_eq!(
        wire_err.code,
        ErrorCode::FailedPrecondition,
        "expected FailedPrecondition, got {:?}: {}",
        wire_err.code,
        wire_err.message
    );
    assert!(
        wire_err.message.contains("blinding key"),
        "error should mention blinding key: {}",
        wire_err.message
    );
}
