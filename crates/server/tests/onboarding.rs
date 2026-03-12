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

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto;

use crate::common::{TestCluster, create_user_client};

// ============================================================================
// Test Helpers
// ============================================================================

/// Proto region value for GLOBAL (1). TestCluster only registers the GLOBAL
/// Raft group, so all onboarding requests use this region.
const REGION_GLOBAL: i32 = 1;

/// Initiates email verification and returns the code from the response.
async fn initiate(
    client: &mut proto::user_service_client::UserServiceClient<tonic::transport::Channel>,
    email: &str,
    region: i32,
) -> Result<String, tonic::Status> {
    let resp = client
        .initiate_email_verification(proto::InitiateEmailVerificationRequest {
            email: email.to_string(),
            region,
        })
        .await?;
    Ok(resp.into_inner().code)
}

/// Verifies an email code and returns the raw response.
async fn verify(
    client: &mut proto::user_service_client::UserServiceClient<tonic::transport::Channel>,
    email: &str,
    code: &str,
    region: i32,
) -> Result<proto::VerifyEmailCodeResponse, tonic::Status> {
    client
        .verify_email_code(proto::VerifyEmailCodeRequest {
            email: email.to_string(),
            code: code.to_string(),
            region,
        })
        .await
        .map(|r| r.into_inner())
}

/// Extracts the onboarding token from a VerifyEmailCodeResponse (new-user path).
fn extract_onboarding_token(resp: &proto::VerifyEmailCodeResponse) -> &str {
    match resp.result.as_ref().expect("response should have result") {
        proto::verify_email_code_response::Result::NewUser(new_user) => &new_user.onboarding_token,
        proto::verify_email_code_response::Result::ExistingUser(_) => {
            panic!("expected NewUser result, got ExistingUser")
        },
    }
}

// ============================================================================
// Tests: Initiate Email Verification
// ============================================================================

/// Initiate email verification returns a non-empty verification code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_email_verification_returns_code() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let code = initiate(&mut client, "alice@example.com", REGION_GLOBAL)
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
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let code1 =
        initiate(&mut client, "bob@example.com", REGION_GLOBAL).await.expect("first initiate");
    let code2 =
        initiate(&mut client, "bob@example.com", REGION_GLOBAL).await.expect("second initiate");

    assert!(!code1.is_empty());
    assert!(!code2.is_empty());
}

// ============================================================================
// Tests: Verify Email Code
// ============================================================================

/// Verify with a correct code returns a new-user result with onboarding token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_email_code_new_user() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let code = initiate(&mut client, "charlie@example.com", REGION_GLOBAL).await.expect("initiate");

    let resp = verify(&mut client, "charlie@example.com", &code, REGION_GLOBAL)
        .await
        .expect("verify should succeed");

    let token = extract_onboarding_token(&resp);
    assert!(!token.is_empty(), "onboarding token should be non-empty");
}

/// Verify with a wrong code returns an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_wrong_code_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    initiate(&mut client, "dave@example.com", REGION_GLOBAL).await.expect("initiate");

    let err = verify(&mut client, "dave@example.com", "ZZZZZZ", REGION_GLOBAL)
        .await
        .expect_err("wrong code should fail");

    // Wrong code results in an error (either NOT_FOUND, PERMISSION_DENIED, or similar)
    assert_ne!(err.code(), tonic::Code::Ok, "wrong code should return error");
}

/// Verify without prior initiate returns an error (no verification record).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_without_initiate_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = verify(&mut client, "nobody@example.com", "ABCDEF", REGION_GLOBAL)
        .await
        .expect_err("verify without initiate should fail");

    assert_ne!(err.code(), tonic::Code::Ok);
}

/// Verify with empty code returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_empty_code_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = verify(&mut client, "empty@example.com", "", REGION_GLOBAL)
        .await
        .expect_err("empty code should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// Tests: Region Validation
// ============================================================================

/// Unknown region value returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_invalid_region_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = initiate(&mut client, "region@example.com", 999)
        .await
        .expect_err("unknown region should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("region"), "error should mention region");
}

/// Unspecified region (0) returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_unspecified_region_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = initiate(&mut client, "unspec@example.com", 0)
        .await
        .expect_err("unspecified region should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// Tests: Input Validation
// ============================================================================

/// Empty email returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_empty_email_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = initiate(&mut client, "", REGION_GLOBAL).await.expect_err("empty email should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Invalid email format returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_initiate_malformed_email_rejected() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = initiate(&mut client, "not-an-email", REGION_GLOBAL)
        .await
        .expect_err("malformed email should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// Tests: Complete Registration (Error Paths)
// ============================================================================

/// Complete registration with a malformed onboarding token returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_malformed_token() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = client
        .complete_registration(proto::CompleteRegistrationRequest {
            onboarding_token: "not-a-valid-token".to_string(),
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            organization_name: "Alice Corp".to_string(),
            region: REGION_GLOBAL,
        })
        .await
        .expect_err("malformed token should fail");

    // Should be InvalidArgument (token decode failure) or similar
    assert!(
        err.code() == tonic::Code::InvalidArgument
            || err.code() == tonic::Code::NotFound
            || err.code() == tonic::Code::PermissionDenied,
        "expected validation error, got {:?}: {}",
        err.code(),
        err.message()
    );
}

/// Complete registration with empty onboarding token returns INVALID_ARGUMENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_empty_token() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    let err = client
        .complete_registration(proto::CompleteRegistrationRequest {
            onboarding_token: String::new(),
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            organization_name: "Alice Corp".to_string(),
            region: REGION_GLOBAL,
        })
        .await
        .expect_err("empty token should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Complete registration without prior verification returns NOT_FOUND.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_without_verify() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    // Generate a valid-format onboarding token that doesn't match any record
    let (token, _hash) = inferadb_ledger_types::onboarding::generate_onboarding_token();

    let err = client
        .complete_registration(proto::CompleteRegistrationRequest {
            onboarding_token: token,
            email: "unverified@example.com".to_string(),
            name: "Nobody".to_string(),
            organization_name: "No Corp".to_string(),
            region: REGION_GLOBAL,
        })
        .await
        .expect_err("complete without verify should fail");

    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "expected NOT_FOUND, got {:?}: {}",
        err.code(),
        err.message()
    );
}

/// Complete registration with wrong token hash returns PERMISSION_DENIED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_registration_wrong_token_hash() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    // Initiate and verify to create an onboarding account
    let code =
        initiate(&mut client, "wrong-hash@example.com", REGION_GLOBAL).await.expect("initiate");
    let resp =
        verify(&mut client, "wrong-hash@example.com", &code, REGION_GLOBAL).await.expect("verify");
    let _token = extract_onboarding_token(&resp);

    // Use a DIFFERENT valid token (not the one from verification)
    let (wrong_token, _) = inferadb_ledger_types::onboarding::generate_onboarding_token();

    let err = client
        .complete_registration(proto::CompleteRegistrationRequest {
            onboarding_token: wrong_token,
            email: "wrong-hash@example.com".to_string(),
            name: "WrongHash".to_string(),
            organization_name: "Wrong Corp".to_string(),
            region: REGION_GLOBAL,
        })
        .await
        .expect_err("wrong token hash should fail");

    assert_eq!(
        err.code(),
        tonic::Code::PermissionDenied,
        "expected PERMISSION_DENIED for wrong token, got {:?}: {}",
        err.code(),
        err.message()
    );
}

// ============================================================================
// Tests: Re-verification
// ============================================================================

/// Re-verifying the same email after a successful verify produces a new onboarding token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reverify_produces_new_token() {
    let cluster = TestCluster::new(1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    // First flow
    let code1 =
        initiate(&mut client, "reverify@example.com", REGION_GLOBAL).await.expect("initiate 1");
    let resp1 =
        verify(&mut client, "reverify@example.com", &code1, REGION_GLOBAL).await.expect("verify 1");
    let token1 = extract_onboarding_token(&resp1).to_string();

    // Second flow (re-initiate and re-verify)
    let code2 =
        initiate(&mut client, "reverify@example.com", REGION_GLOBAL).await.expect("initiate 2");
    let resp2 =
        verify(&mut client, "reverify@example.com", &code2, REGION_GLOBAL).await.expect("verify 2");
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
    let code_hash_hex = code_hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_ne!(
        hmac1, code_hash_hex,
        "email HMAC and code hash should use different derivation domains"
    );
}

// ============================================================================
// Tests: Missing Blinding Key (via RegionTestCluster which doesn't configure it)
// ============================================================================

/// When email_blinding_key is not configured, onboarding RPCs return FAILED_PRECONDITION.
///
/// Uses `RegionTestCluster` which builds `LedgerServer` without the blinding key,
/// unlike `TestCluster` which always configures it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_missing_blinding_key_returns_failed_precondition() {
    let cluster = crate::common::RegionTestCluster::new(1, 1).await;
    let addr = cluster.nodes()[0].addr;
    let mut client = create_user_client(addr).await.expect("connect");

    // Wait for cluster to stabilize
    cluster.wait_for_leaders(Duration::from_secs(10)).await;

    let err = client
        .initiate_email_verification(proto::InitiateEmailVerificationRequest {
            email: "nokey@example.com".to_string(),
            region: 10, // US_EAST_VA — RegionTestCluster has data regions
        })
        .await
        .expect_err("should fail without blinding key");

    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "expected FAILED_PRECONDITION, got {:?}: {}",
        err.code(),
        err.message()
    );
    assert!(
        err.message().contains("blinding key"),
        "error should mention blinding key: {}",
        err.message()
    );
}
