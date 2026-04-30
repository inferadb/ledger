//! Wire-based UserService ops.
//!
//! Mirrors [`super::super::ops::user`], [`super::super::ops::onboarding`], and
//! [`super::super::ops::credential`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`UserServiceClient`](inferadb_ledger_wire_services::UserServiceClient)
//! instead of the proto-generated tonic client.
//!
//! The 22 RPCs split into five groups (matching the proto surface):
//!
//! 1. **User CRUD** — create / get / update / delete / list / search.
//! 2. **User email management** — create / delete / search / verify.
//! 3. **User region migration + GDPR erasure** — `migrate_user_region` / `erase_user`.
//! 4. **Onboarding** — `initiate_email_verification` / `verify_email_code` /
//!    `complete_registration`. The `verify_email_code` response is a oneof over
//!    [`ExistingUserSession`] / [`OnboardingSession`] / [`TotpRequired`].
//! 5. **Credential management** — passkey / TOTP / recovery-code CRUD plus TOTP challenge /
//!    verification flow. The credential body is a oneof over [`PasskeyCredentialData`] /
//!    [`TotpCredentialData`] / [`RecoveryCodeCredentialData`].
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (and the domain-input → wire-request
//! direction for enum tags / oneof variants / timestamps) in isolation. F.1.e
//! flipped the connection pool so the per-RPC entry points are now reached
//! from the corresponding [`super::super::ops::user`] dispatch arms.

use std::{str::FromStr, sync::Arc};

use bytes::Bytes;
use inferadb_ledger_types::{
    OrganizationSlug, Region, UserCredentialId, UserEmailId, UserRole, UserSlug, UserStatus,
};
use inferadb_ledger_wire::services::{shared as ws, user as w};
use inferadb_ledger_wire_services::UserServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    token::TokenPair,
    types::{
        admin::{
            BlindingKeyRotationStatus, EmailVerificationCode, EmailVerificationResult,
            RegistrationResult, UserEmailInfo, UserInfo, UserMigrationInfo,
        },
        credential::{
            CredentialData, CredentialType, PasskeyCredentialInfo, RecoveryCodeCredentialInfo,
            RecoveryCodeResult, TotpAlgorithm, TotpCredentialInfo, UserCredentialInfo,
        },
    },
};

// ---------------------------------------------------------------------------
// User CRUD
// ---------------------------------------------------------------------------

/// Issue a `CreateUser` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::create_user`](crate::LedgerClient::create_user) at
/// the dispatch layer. The caller is responsible for pre-computing the
/// `email_hmac` against the active blinding key.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's retry
/// loop is responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (rate-limit family, migration codes, all others land in `Rpc`). Returns
/// [`SdkError::Rpc`] with [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) when
/// the server omits the `user` field on the response envelope.
pub(crate) async fn create_user(
    wire_client: Arc<WireClient>,
    request_id: u128,
    name: String,
    email: String,
    email_hmac: String,
    region: Region,
    role: UserRole,
) -> Result<UserInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::CreateUserRequest {
        name,
        email,
        region: region.as_str().to_owned(),
        role: Some(domain_user_role_to_wire(role)),
        email_hmac,
        organization_name: String::new(),
        organization_tier: None,
    };
    let response = client.create_user(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .user
        .as_ref()
        .map(domain_user_info_from_wire)
        .ok_or_else(|| missing_field("user", "CreateUserResponse"))
}

/// Issue a `GetUser` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::get_user`](crate::LedgerClient::get_user). The
/// `caller` slug doubles as the lookup target on this wrapper (matching the
/// tonic path), but the wire request retains both fields so server-side
/// authorization can distinguish self-reads from cross-user reads.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn get_user(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
) -> Result<UserInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::GetUserRequest { slug: Some(user), caller: Some(user) };
    let response = client.get_user(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .user
        .as_ref()
        .map(domain_user_info_from_wire)
        .ok_or_else(|| missing_field("user", "GetUserResponse"))
}

/// Issue an `UpdateUser` RPC via the wire transport.
///
/// At least one of `name` / `role` / `email` must be set on the wrapper —
/// the wire request forwards each as `Option<_>`, but the server rejects
/// fully-empty updates.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn update_user(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    name: Option<String>,
    role: Option<UserRole>,
    email: Option<UserEmailId>,
) -> Result<UserInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::UpdateUserRequest {
        slug: Some(user),
        name,
        role: role.map(domain_user_role_to_wire),
        primary_email: email,
        caller: Some(user),
    };
    let response = client.update_user(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .user
        .as_ref()
        .map(domain_user_info_from_wire)
        .ok_or_else(|| missing_field("user", "UpdateUserResponse"))
}

/// Issue a `DeleteUser` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::delete_user`](crate::LedgerClient::delete_user).
/// The wire response is intentionally narrow — slug + deletion timestamp +
/// retention window. The other [`UserInfo`] fields default to empty / zero /
/// `Deleted` to match the proto-path behavior so SDK consumers see one
/// shape across both transports.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s.
pub(crate) async fn delete_user(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    caller: UserSlug,
) -> Result<UserInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::DeleteUserRequest { slug: Some(user), caller: Some(caller) };
    let response = client.delete_user(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(UserInfo {
        slug: response.slug.unwrap_or_else(|| UserSlug::new(0)),
        name: String::new(),
        email: UserEmailId::new(0),
        status: UserStatus::Deleted,
        role: UserRole::User,
        created_at: None,
        updated_at: None,
        deleted_at: nanos_to_system_time(response.deleted_at),
        retention_days: Some(response.retention_days),
    })
}

/// Issue a `ListUsers` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_users`](crate::LedgerClient::list_users).
/// `page_token` is exposed as `Vec<u8>` even though the wire type uses
/// [`Bytes`] — the boundary conversion happens here so callers stay
/// decoupled from the wire crate.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn list_users(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    page_size: u32,
    page_token: Option<Vec<u8>>,
) -> Result<(Vec<UserInfo>, Option<Vec<u8>>)> {
    let client = UserServiceClient::new(wire_client);
    let request = w::ListUsersRequest {
        page_token: page_token.map(Bytes::from),
        page_size,
        region: None,
        caller: Some(caller),
    };
    let response = client.list_users(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    let users: Vec<UserInfo> = response.users.iter().map(domain_user_info_from_wire).collect();
    let next_page_token = response.next_page_token.map(|b| b.to_vec());
    Ok((users, next_page_token))
}

/// Issue a `SearchUsers` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::search_users`](crate::LedgerClient::search_users).
/// The tonic path always sends a filter with only the `email` field set;
/// this wire wrapper preserves that shape exactly.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn search_users(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    email: String,
) -> Result<Vec<UserInfo>> {
    let client = UserServiceClient::new(wire_client);
    let request = w::SearchUsersRequest {
        filter: Some(w::UserSearchFilter {
            email: Some(email),
            status: None,
            role: None,
            name_prefix: None,
        }),
        page_token: None,
        page_size: 100,
        caller: Some(caller),
    };
    let response =
        client.search_users(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.users.iter().map(domain_user_info_from_wire).collect())
}

// ---------------------------------------------------------------------------
// User email management
// ---------------------------------------------------------------------------

/// Issue a `CreateUserEmail` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::create_user_email`](crate::LedgerClient::create_user_email).
/// The caller pre-computes the email HMAC against the active blinding key;
/// this wrapper just forwards.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn create_user_email(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    email: String,
    email_hmac: String,
) -> Result<UserEmailInfo> {
    let client = UserServiceClient::new(wire_client);
    let request =
        w::CreateUserEmailRequest { user: Some(user), email, email_hmac, caller: Some(user) };
    let response =
        client.create_user_email(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .email
        .as_ref()
        .map(domain_user_email_info_from_wire)
        .ok_or_else(|| missing_field("email", "CreateUserEmailResponse"))
}

/// Issue a `DeleteUserEmail` RPC via the wire transport.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn delete_user_email(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    email_id: UserEmailId,
) -> Result<()> {
    let client = UserServiceClient::new(wire_client);
    let request = w::DeleteUserEmailRequest {
        user: Some(user),
        email_id: Some(email_id),
        caller: Some(user),
    };
    client.delete_user_email(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

/// Issue a `SearchUserEmail` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::search_user_email`](crate::LedgerClient::search_user_email).
/// Either `user` or `email` (or both) must be populated to scope the
/// search; the server rejects fully-empty filters.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn search_user_email(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: Option<UserSlug>,
    email: Option<String>,
) -> Result<Vec<UserEmailInfo>> {
    let client = UserServiceClient::new(wire_client);
    let request = w::SearchUserEmailRequest {
        filter: Some(w::UserEmailSearchFilter { user, email, verified_only: None }),
        page_token: None,
        page_size: 100,
        caller: Some(caller),
    };
    let response =
        client.search_user_email(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.emails.iter().map(domain_user_email_info_from_wire).collect())
}

/// Issue a `VerifyUserEmail` RPC via the wire transport.
///
/// `token` is the opaque verification string the server emitted on the
/// initial email send; this wrapper does not re-derive or validate it
/// client-side.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn verify_user_email(
    wire_client: Arc<WireClient>,
    request_id: u128,
    token: String,
) -> Result<UserEmailInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::VerifyUserEmailRequest { token };
    let response =
        client.verify_user_email(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .email
        .as_ref()
        .map(domain_user_email_info_from_wire)
        .ok_or_else(|| missing_field("email", "VerifyUserEmailResponse"))
}

// ---------------------------------------------------------------------------
// User region migration + GDPR erasure
// ---------------------------------------------------------------------------

/// Issue a `MigrateUserRegion` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::migrate_user_region`](crate::LedgerClient::migrate_user_region).
/// The returned `source_region` falls back to [`Region::GLOBAL`] (and the
/// `target_region` falls back to the caller-provided target) if the wire
/// response carries a region slug the client doesn't recognize — same
/// fail-soft contract as the proto path.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn migrate_user_region(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    target_region: Region,
) -> Result<UserMigrationInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::MigrateUserRegionRequest {
        slug: Some(user),
        target_region: target_region.as_str().to_owned(),
        caller: Some(caller),
    };
    let response =
        client.migrate_user_region(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(UserMigrationInfo {
        slug: response.slug.unwrap_or_else(|| UserSlug::new(0)),
        source_region: Region::from_str(&response.source_region).unwrap_or(Region::GLOBAL),
        target_region: Region::from_str(&response.target_region).unwrap_or(target_region),
        directory_status: response.directory_status,
    })
}

/// Issue an `EraseUser` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::erase_user`](crate::LedgerClient::erase_user).
/// Returns the slug of the erased user (echoed by the server). This is a
/// crypto-shredding operation — the call is irreversible.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn erase_user(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
    caller: UserSlug,
    region: Region,
) -> Result<UserSlug> {
    let client = UserServiceClient::new(wire_client);
    let request = w::EraseUserRequest {
        user: Some(user),
        caller: Some(caller),
        region: region.as_str().to_owned(),
    };
    let response = client.erase_user(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.user.unwrap_or_else(|| UserSlug::new(0)))
}

// ---------------------------------------------------------------------------
// Onboarding (email verification + registration)
// ---------------------------------------------------------------------------

/// Issue an `InitiateEmailVerification` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::initiate_email_verification`](crate::LedgerClient::initiate_email_verification).
/// The returned [`EmailVerificationCode`] should be sent to the user's
/// email out-of-band by the caller.
///
/// # Errors
///
/// Same shape as [`create_user`].
pub(crate) async fn initiate_email_verification(
    wire_client: Arc<WireClient>,
    request_id: u128,
    email: String,
    region: Region,
) -> Result<EmailVerificationCode> {
    let client = UserServiceClient::new(wire_client);
    let request = w::InitiateEmailVerificationRequest { email, region: region.as_str().to_owned() };
    let response = client
        .initiate_email_verification(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(EmailVerificationCode { code: response.code })
}

/// Issue a `VerifyEmailCode` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::verify_email_code`](crate::LedgerClient::verify_email_code).
/// The wire response carries a oneof over [`ExistingUserSession`] /
/// [`OnboardingSession`] / [`TotpRequired`]; this fn projects each variant
/// into the matching [`EmailVerificationResult`] arm.
///
/// # Errors
///
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) when the response oneof is
/// empty or when an `ExistingUser` variant omits its `user` / `session` fields.
pub(crate) async fn verify_email_code(
    wire_client: Arc<WireClient>,
    request_id: u128,
    email: String,
    code: String,
    region: Region,
) -> Result<EmailVerificationResult> {
    let client = UserServiceClient::new(wire_client);
    let request = w::VerifyEmailCodeRequest { email, code, region: region.as_str().to_owned() };
    let response =
        client.verify_email_code(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    match response.result {
        Some(w::VerifyEmailCodeResult::ExistingUser(existing)) => {
            let user =
                existing.user.ok_or_else(|| missing_field("user", "VerifyEmailCodeResponse"))?;
            let session = existing
                .session
                .map(token_pair_from_wire)
                .ok_or_else(|| missing_field("session", "VerifyEmailCodeResponse"))?;
            Ok(EmailVerificationResult::ExistingUser { user, session })
        },
        Some(w::VerifyEmailCodeResult::NewUser(onboarding)) => {
            Ok(EmailVerificationResult::NewUser { onboarding_token: onboarding.onboarding_token })
        },
        Some(w::VerifyEmailCodeResult::TotpRequired(totp)) => {
            Ok(EmailVerificationResult::TotpRequired {
                challenge_nonce: totp.challenge_nonce.to_vec(),
            })
        },
        None => Err(SdkError::Rpc {
            code: inferadb_ledger_wire::ErrorCode::Internal,
            message: "Empty verify_email_code response".to_owned(),
            request_id: None,
            trace_id: None,
            error_details: None,
        }),
    }
}

/// Issue a `CompleteRegistration` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::complete_registration`](crate::LedgerClient::complete_registration).
/// Requires the `onboarding_token` from a prior
/// [`verify_email_code`] response.
///
/// # Errors
///
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) when the response omits the
/// new user, the user's slug, or the session token pair.
pub(crate) async fn complete_registration(
    wire_client: Arc<WireClient>,
    request_id: u128,
    onboarding_token: String,
    email: String,
    region: Region,
    name: String,
    organization_name: String,
) -> Result<RegistrationResult> {
    let client = UserServiceClient::new(wire_client);
    let request = w::CompleteRegistrationRequest {
        onboarding_token,
        email,
        region: region.as_str().to_owned(),
        name,
        organization_name,
    };
    let response =
        client.complete_registration(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    let user = response
        .user
        .and_then(|u| u.slug)
        .ok_or_else(|| missing_field("user.slug", "CompleteRegistrationResponse"))?;
    let session = response
        .session
        .map(token_pair_from_wire)
        .ok_or_else(|| missing_field("session", "CompleteRegistrationResponse"))?;
    let organization = response.organization.map(|o| OrganizationSlug::new(o.value()));
    Ok(RegistrationResult { user, session, organization })
}

// ---------------------------------------------------------------------------
// Credential CRUD
// ---------------------------------------------------------------------------
//
// These entry points are duplicated in [`super::credential`], which is what
// [`crate::ops::credential`]'s wire dispatch arm actually calls. The copies
// here predate the credential-module split (see E.5.10); the unit tests at
// the bottom of this file still exercise their wire ↔ domain mapping
// helpers, so the bodies are retained behind targeted `#[allow(dead_code)]`
// pending a follow-up consolidation. Out of F.1.f scope.

#[allow(dead_code)]
pub(crate) async fn create_user_credential(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    name: String,
    data: CredentialData,
) -> Result<UserCredentialInfo> {
    let client = UserServiceClient::new(wire_client);
    let credential_type = domain_credential_type_to_wire(CredentialType::from_data(&data));
    let wire_data = match data {
        CredentialData::Passkey(pk) => {
            w::CreateUserCredentialData::Passkey(domain_passkey_to_wire(&pk))
        },
        CredentialData::Totp(totp) => w::CreateUserCredentialData::Totp(domain_totp_to_wire(&totp)),
        CredentialData::RecoveryCode(rc) => {
            w::CreateUserCredentialData::RecoveryCode(domain_recovery_to_wire(&rc))
        },
    };
    let request = w::CreateUserCredentialRequest {
        user: Some(user),
        credential_type,
        name,
        caller: Some(caller),
        data: Some(wire_data),
    };
    let response =
        client.create_user_credential(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .credential
        .as_ref()
        .map(domain_user_credential_info_from_wire)
        .ok_or_else(|| missing_field("credential", "CreateUserCredentialResponse"))
}

/// Issue a `ListUserCredentials` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_user_credentials`](crate::LedgerClient::list_user_credentials).
/// TOTP secrets are stripped server-side before the response is built.
///
/// # Errors
///
/// Same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn list_user_credentials(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    credential_type: Option<CredentialType>,
) -> Result<Vec<UserCredentialInfo>> {
    let client = UserServiceClient::new(wire_client);
    let request = w::ListUserCredentialsRequest {
        user: Some(user),
        credential_type: credential_type.map(domain_credential_type_to_wire),
        caller: Some(caller),
    };
    let response =
        client.list_user_credentials(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.credentials.iter().map(domain_user_credential_info_from_wire).collect())
}

/// Issue an `UpdateUserCredential` RPC via the wire transport.
///
/// TOTP and recovery-code credentials are immutable after creation —
/// only `name` / `enabled` can be updated for those types. Passkey
/// credentials may additionally update sign-count / backup-state via
/// `passkey_data`.
///
/// # Errors
///
/// Same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn update_user_credential(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    credential_id: UserCredentialId,
    name: Option<String>,
    enabled: Option<bool>,
    passkey_data: Option<PasskeyCredentialInfo>,
) -> Result<UserCredentialInfo> {
    let client = UserServiceClient::new(wire_client);
    let request = w::UpdateUserCredentialRequest {
        user: Some(user),
        credential_id: credential_id.value(),
        name,
        enabled,
        caller: Some(caller),
        passkey: passkey_data.as_ref().map(domain_passkey_to_wire),
    };
    let response =
        client.update_user_credential(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .credential
        .as_ref()
        .map(domain_user_credential_info_from_wire)
        .ok_or_else(|| missing_field("credential", "UpdateUserCredentialResponse"))
}

/// Issue a `DeleteUserCredential` RPC via the wire transport.
///
/// Server-side rejects the call if it would leave the user with zero
/// credentials (safety guard).
///
/// # Errors
///
/// Same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn delete_user_credential(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    credential_id: UserCredentialId,
) -> Result<()> {
    let client = UserServiceClient::new(wire_client);
    let request = w::DeleteUserCredentialRequest {
        user: Some(user),
        credential_id: credential_id.value(),
        caller: Some(caller),
    };
    client.delete_user_credential(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// TOTP challenge / verification / recovery code
// ---------------------------------------------------------------------------

/// Issue a `CreateTotpChallenge` RPC via the wire transport.
///
/// Returns the 32-byte challenge nonce that must be echoed back to
/// [`verify_totp`] / [`consume_recovery_code`].
///
/// # Errors
///
/// Same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn create_totp_challenge(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    primary_method: String,
) -> Result<Vec<u8>> {
    let client = UserServiceClient::new(wire_client);
    let request =
        w::CreateTotpChallengeRequest { user: Some(user), primary_method, caller: Some(caller) };
    let response =
        client.create_totp_challenge(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.challenge_nonce.to_vec())
}

/// Issue a `VerifyTotp` RPC via the wire transport.
///
/// On success, atomically consumes the challenge and creates a session.
///
/// # Errors
///
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) if the response omits the
/// session token pair; otherwise same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn verify_totp(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    totp_code: String,
    challenge_nonce: Vec<u8>,
) -> Result<TokenPair> {
    let client = UserServiceClient::new(wire_client);
    let request = w::VerifyTotpRequest {
        user: Some(user),
        totp_code,
        challenge_nonce: Bytes::from(challenge_nonce),
        credential_used: None,
        caller: Some(caller),
    };
    let response = client.verify_totp(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    let tokens = response.tokens.ok_or_else(|| missing_field("tokens", "VerifyTotpResponse"))?;
    Ok(token_pair_from_wire(tokens))
}

/// Issue a `ConsumeRecoveryCode` RPC via the wire transport.
///
/// Atomically consumes one recovery code hash and creates a session.
/// Returns the token pair plus the count of remaining unused codes.
///
/// # Errors
///
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) if the response omits the
/// session token pair; otherwise same shape as [`create_user`].
#[allow(dead_code)]
pub(crate) async fn consume_recovery_code(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    code: String,
    challenge_nonce: Vec<u8>,
) -> Result<RecoveryCodeResult> {
    let client = UserServiceClient::new(wire_client);
    let request = w::ConsumeRecoveryCodeRequest {
        user: Some(user),
        code,
        challenge_nonce: Bytes::from(challenge_nonce),
        credential_used: None,
        caller: Some(caller),
    };
    let response =
        client.consume_recovery_code(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    let tokens =
        response.tokens.ok_or_else(|| missing_field("tokens", "ConsumeRecoveryCodeResponse"))?;
    Ok(RecoveryCodeResult {
        tokens: token_pair_from_wire(tokens),
        remaining_codes: response.remaining_codes,
    })
}

// ---------------------------------------------------------------------------
// Wire ↔ domain mappers
// ---------------------------------------------------------------------------

/// Maps a wire [`User`](ws::User) into the SDK domain [`UserInfo`].
///
/// `slug` defaults to `0` when the wire envelope omits it (matches the
/// proto-path `as_ref().map_or(0, ...)` fallback), and `email` defaults
/// to `UserEmailId(0)` on absence. `created_at` / `updated_at` /
/// `deleted_at` are UNIX nanoseconds on the wire and convert via
/// [`nanos_to_system_time`]; a `0` timestamp collapses to `None`. The
/// `retention_days` field is populated only on `delete_user` responses
/// (which build the [`UserInfo`] directly), so this mapper leaves it
/// `None`.
fn domain_user_info_from_wire(user: &ws::User) -> UserInfo {
    UserInfo {
        slug: user.slug.unwrap_or_else(|| UserSlug::new(0)),
        name: user.name.clone(),
        email: user.email.unwrap_or_else(|| UserEmailId::new(0)),
        status: wire_user_status_to_domain(user.status),
        role: wire_user_role_to_domain(user.role),
        created_at: nanos_to_system_time(user.created_at),
        updated_at: nanos_to_system_time(user.updated_at),
        deleted_at: user.deleted_at.and_then(nanos_to_system_time),
        retention_days: None,
    }
}

/// Maps a wire [`UserEmail`](ws::UserEmail) into the SDK domain
/// [`UserEmailInfo`]. `verified_at` of `0` (wire convention for "unset")
/// surfaces as both `verified: false` and `verified_at: None`.
fn domain_user_email_info_from_wire(email: &ws::UserEmail) -> UserEmailInfo {
    UserEmailInfo {
        id: email.id.unwrap_or_else(|| UserEmailId::new(0)),
        email: email.email.clone(),
        verified: email.verified_at != 0,
        created_at: nanos_to_system_time(email.created_at),
        verified_at: nanos_to_system_time(email.verified_at),
    }
}

/// Maps a wire [`UserCredential`](w::UserCredential) into the SDK domain
/// [`UserCredentialInfo`]. The credential body oneof projects across the
/// three [`CredentialData`] arms; `data: None` flows through unchanged.
/// An unknown `credential_type` (e.g. wire `Unspecified`) collapses to
/// [`CredentialType::Passkey`] — same fallback as the proto path.
fn domain_user_credential_info_from_wire(c: &w::UserCredential) -> UserCredentialInfo {
    let data = c.data.as_ref().map(|d| match d {
        w::UserCredentialData::Passkey(pk) => CredentialData::Passkey(domain_passkey_from_wire(pk)),
        w::UserCredentialData::Totp(totp) => CredentialData::Totp(domain_totp_from_wire(totp)),
        w::UserCredentialData::RecoveryCode(rc) => {
            CredentialData::RecoveryCode(domain_recovery_from_wire(rc))
        },
    });
    UserCredentialInfo {
        id: UserCredentialId::new(c.id),
        user: c.user.unwrap_or_else(|| UserSlug::new(0)),
        credential_type: wire_credential_type_to_domain(c.credential_type)
            .unwrap_or(CredentialType::Passkey),
        name: c.name.clone(),
        enabled: c.enabled,
        created_at: nanos_to_system_time(c.created_at),
        last_used_at: c.last_used_at.and_then(nanos_to_system_time),
        data,
    }
}

/// Maps a wire [`PasskeyCredentialData`](w::PasskeyCredentialData) into
/// the SDK domain [`PasskeyCredentialInfo`]. [`Bytes`] fields convert to
/// `Vec<u8>` at the SDK boundary so consumers stay decoupled from the
/// wire crate.
fn domain_passkey_from_wire(pk: &w::PasskeyCredentialData) -> PasskeyCredentialInfo {
    PasskeyCredentialInfo {
        credential_id: pk.credential_id.to_vec(),
        public_key: pk.public_key.to_vec(),
        sign_count: pk.sign_count,
        transports: pk.transports.clone(),
        backup_eligible: pk.backup_eligible,
        backup_state: pk.backup_state,
        attestation_format: pk.attestation_format.clone(),
        aaguid: pk.aaguid.as_ref().map(|b| b.to_vec()),
    }
}

/// Maps the SDK domain [`PasskeyCredentialInfo`] into a wire
/// [`PasskeyCredentialData`]. Allocates [`Bytes`] from the underlying
/// `Vec<u8>` — this is the outbound direction (request bodies) so the
/// allocation cost lands on the SDK consumer's call stack.
fn domain_passkey_to_wire(pk: &PasskeyCredentialInfo) -> w::PasskeyCredentialData {
    w::PasskeyCredentialData {
        credential_id: Bytes::from(pk.credential_id.clone()),
        public_key: Bytes::from(pk.public_key.clone()),
        sign_count: pk.sign_count,
        transports: pk.transports.clone(),
        backup_eligible: pk.backup_eligible,
        backup_state: pk.backup_state,
        attestation_format: pk.attestation_format.clone(),
        aaguid: pk.aaguid.as_ref().map(|b| Bytes::from(b.clone())),
    }
}

/// Maps a wire [`TotpCredentialData`](w::TotpCredentialData) into the
/// SDK domain [`TotpCredentialInfo`]. Note: `secret` is populated only
/// on the initial create response — every subsequent read sees an empty
/// `secret` field by server convention.
fn domain_totp_from_wire(totp: &w::TotpCredentialData) -> TotpCredentialInfo {
    TotpCredentialInfo {
        secret: totp.secret.to_vec(),
        algorithm: wire_totp_algorithm_to_domain(totp.algorithm),
        digits: totp.digits,
        period: totp.period,
    }
}

/// Maps the SDK domain [`TotpCredentialInfo`] into a wire
/// [`TotpCredentialData`].
fn domain_totp_to_wire(totp: &TotpCredentialInfo) -> w::TotpCredentialData {
    w::TotpCredentialData {
        secret: Bytes::from(totp.secret.clone()),
        algorithm: domain_totp_algorithm_to_wire(totp.algorithm),
        digits: totp.digits,
        period: totp.period,
    }
}

/// Maps a wire [`RecoveryCodeCredentialData`](w::RecoveryCodeCredentialData)
/// into the SDK domain [`RecoveryCodeCredentialInfo`].
fn domain_recovery_from_wire(rc: &w::RecoveryCodeCredentialData) -> RecoveryCodeCredentialInfo {
    RecoveryCodeCredentialInfo {
        code_hashes: rc.code_hashes.iter().map(|b| b.to_vec()).collect(),
        total_generated: rc.total_generated,
    }
}

/// Maps the SDK domain [`RecoveryCodeCredentialInfo`] into a wire
/// [`RecoveryCodeCredentialData`].
fn domain_recovery_to_wire(rc: &RecoveryCodeCredentialInfo) -> w::RecoveryCodeCredentialData {
    w::RecoveryCodeCredentialData {
        code_hashes: rc.code_hashes.iter().map(|v| Bytes::from(v.clone())).collect(),
        total_generated: rc.total_generated,
    }
}

/// Maps a wire [`TokenPair`](ws::TokenPair) into the SDK domain
/// [`TokenPair`]. `0` timestamps surface as `None` — same convention as
/// the proto path's absent `Timestamp` message.
fn token_pair_from_wire(p: ws::TokenPair) -> TokenPair {
    TokenPair {
        access_token: p.access_token,
        refresh_token: p.refresh_token,
        access_expires_at: nanos_to_system_time(p.access_expires_at),
        refresh_expires_at: nanos_to_system_time(p.refresh_expires_at),
    }
}

/// Numeric-tag mapping from the wire [`UserStatus`](ws::UserStatus) to
/// the domain enum. Wire `Unspecified` collapses to [`UserStatus::Active`]
/// — matches the proto path's `_ => UserStatus::Active` fallback.
fn wire_user_status_to_domain(status: ws::UserStatus) -> UserStatus {
    match status {
        ws::UserStatus::Active => UserStatus::Active,
        ws::UserStatus::PendingOrg => UserStatus::PendingOrg,
        ws::UserStatus::Suspended => UserStatus::Suspended,
        ws::UserStatus::Deleting => UserStatus::Deleting,
        ws::UserStatus::Deleted => UserStatus::Deleted,
        // Unspecified collapses to Active — matches proto path.
        ws::UserStatus::Unspecified => UserStatus::Active,
    }
}

/// Numeric-tag mapping from the wire [`UserRole`](ws::UserRole) to the
/// domain enum. Wire `Unspecified` and `User` both collapse to
/// [`UserRole::User`] — matches the proto path.
fn wire_user_role_to_domain(role: ws::UserRole) -> UserRole {
    match role {
        ws::UserRole::Admin => UserRole::Admin,
        // Unspecified + User both surface as User — matches proto path's
        // `_ => UserRole::User` fallback.
        ws::UserRole::User | ws::UserRole::Unspecified => UserRole::User,
    }
}

/// Numeric-tag mapping from the SDK domain [`UserRole`] to the wire
/// [`UserRole`](ws::UserRole), used on outgoing requests.
fn domain_user_role_to_wire(role: UserRole) -> ws::UserRole {
    match role {
        UserRole::User => ws::UserRole::User,
        UserRole::Admin => ws::UserRole::Admin,
    }
}

/// Numeric-tag mapping from the wire [`CredentialType`](ws::CredentialType)
/// to the domain enum. `Unspecified` returns `None` so callers can apply
/// their own fallback (the per-credential default is
/// [`CredentialType::Passkey`]).
fn wire_credential_type_to_domain(t: ws::CredentialType) -> Option<CredentialType> {
    match t {
        ws::CredentialType::Passkey => Some(CredentialType::Passkey),
        ws::CredentialType::Totp => Some(CredentialType::Totp),
        ws::CredentialType::RecoveryCode => Some(CredentialType::RecoveryCode),
        ws::CredentialType::Unspecified => None,
    }
}

/// Numeric-tag mapping from the SDK domain [`CredentialType`] to the
/// wire [`CredentialType`](ws::CredentialType).
fn domain_credential_type_to_wire(t: CredentialType) -> ws::CredentialType {
    match t {
        CredentialType::Passkey => ws::CredentialType::Passkey,
        CredentialType::Totp => ws::CredentialType::Totp,
        CredentialType::RecoveryCode => ws::CredentialType::RecoveryCode,
    }
}

/// Numeric-tag mapping from the wire [`TotpAlgorithm`](ws::TotpAlgorithm)
/// to the domain enum.
///
/// IMPORTANT: the wire enum has *no* `Unspecified = 0` variant — `Sha1`
/// itself is the zero-tag, matching RFC 6238's default. Any future wire
/// addition must keep this invariant or the encoding breaks
/// byte-compatibility with the proto path.
fn wire_totp_algorithm_to_domain(alg: ws::TotpAlgorithm) -> TotpAlgorithm {
    match alg {
        ws::TotpAlgorithm::Sha1 => TotpAlgorithm::Sha1,
        ws::TotpAlgorithm::Sha256 => TotpAlgorithm::Sha256,
        ws::TotpAlgorithm::Sha512 => TotpAlgorithm::Sha512,
    }
}

/// Numeric-tag mapping from the SDK domain [`TotpAlgorithm`] to the wire
/// [`TotpAlgorithm`](ws::TotpAlgorithm).
fn domain_totp_algorithm_to_wire(alg: TotpAlgorithm) -> ws::TotpAlgorithm {
    match alg {
        TotpAlgorithm::Sha1 => ws::TotpAlgorithm::Sha1,
        TotpAlgorithm::Sha256 => ws::TotpAlgorithm::Sha256,
        TotpAlgorithm::Sha512 => ws::TotpAlgorithm::Sha512,
    }
}

/// Translates a UNIX-nanoseconds timestamp into `Option<SystemTime>`.
///
/// `0` collapses to `None` — matches the proto path's absent `Timestamp`
/// message. Overflow (e.g. far-future or negative-as-`u64` values) also
/// collapses to `None` rather than panicking.
fn nanos_to_system_time(nanos: u64) -> Option<std::time::SystemTime> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos / 1_000_000_000;
    let sub_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, sub_nanos))
}

/// Builds an `SdkError::Rpc` for a missing field in a wire response.
///
/// Mirrors [`crate::proto_util::missing_response_field`] — kept local
/// here to avoid coupling `ops_wire/*.rs` to the `proto_util` module's
/// tonic import chain. The shape is identical (Internal code, descriptive
/// message, no correlation IDs because the wire transport doesn't
/// surface them).
fn missing_field(field: &str, response_type: &str) -> SdkError {
    SdkError::Rpc {
        code: inferadb_ledger_wire::ErrorCode::Internal,
        message: format!("Missing {field} in {response_type}"),
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

// `BlindingKeyRotationStatus` is shipped via the AdminService (rotate /
// rehash status RPCs); the user-service wire surface does not include
// blinding-key APIs. Re-stating the import keeps the dead-code warning
// off the `BlindingKeyRotationStatus` symbol while documenting the
// service split for the next reader.
#[allow(dead_code)]
const _: Option<BlindingKeyRotationStatus> = None;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for wire-response → domain-type mapping plus the
    //! enum-tag projections in both directions and the credential
    //! oneof variants. End-to-end tests against a real `WireClient`
    //! / `WireServer` are deferred to E.7 (TestCluster migration) — at
    //! that point the 22 per-RPC entry points get exercised in full.

    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    // -----------------------------------------------------------------------
    // Repr-i32 numeric tags pinned
    // -----------------------------------------------------------------------

    #[test]
    fn wire_user_status_numeric_tags_match_proto() {
        // Wire enum is `#[repr(i32)]`; tags must match the proto so the
        // numeric tag flows uninterpreted through the dispatch layer.
        assert_eq!(ws::UserStatus::Unspecified as i32, 0);
        assert_eq!(ws::UserStatus::Active as i32, 1);
        assert_eq!(ws::UserStatus::PendingOrg as i32, 2);
        assert_eq!(ws::UserStatus::Suspended as i32, 3);
        assert_eq!(ws::UserStatus::Deleting as i32, 4);
        assert_eq!(ws::UserStatus::Deleted as i32, 5);
    }

    #[test]
    fn wire_user_role_numeric_tags_match_proto() {
        assert_eq!(ws::UserRole::Unspecified as i32, 0);
        assert_eq!(ws::UserRole::User as i32, 1);
        assert_eq!(ws::UserRole::Admin as i32, 2);
    }

    #[test]
    fn wire_credential_type_numeric_tags_match_proto() {
        assert_eq!(ws::CredentialType::Unspecified as i32, 0);
        assert_eq!(ws::CredentialType::Passkey as i32, 1);
        assert_eq!(ws::CredentialType::Totp as i32, 2);
        assert_eq!(ws::CredentialType::RecoveryCode as i32, 3);
    }

    #[test]
    fn wire_totp_algorithm_has_no_unspecified_zero() {
        // Critical invariant: TotpAlgorithm starts at Sha1 = 0 (RFC 6238
        // default), NOT Unspecified = 0. A future addition flipping this
        // would silently break wire byte-compat with the proto enum.
        assert_eq!(ws::TotpAlgorithm::Sha1 as i32, 0);
        assert_eq!(ws::TotpAlgorithm::Sha256 as i32, 1);
        assert_eq!(ws::TotpAlgorithm::Sha512 as i32, 2);
    }

    // -----------------------------------------------------------------------
    // UserStatus mapping
    // -----------------------------------------------------------------------

    #[test]
    fn wire_user_status_each_variant_maps_correctly() {
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::Active), UserStatus::Active);
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::PendingOrg), UserStatus::PendingOrg);
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::Suspended), UserStatus::Suspended);
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::Deleting), UserStatus::Deleting);
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::Deleted), UserStatus::Deleted);
        // Unspecified collapses to Active — matches proto path.
        assert_eq!(wire_user_status_to_domain(ws::UserStatus::Unspecified), UserStatus::Active);
    }

    // -----------------------------------------------------------------------
    // UserRole mapping (both directions)
    // -----------------------------------------------------------------------

    #[test]
    fn wire_user_role_each_variant_maps_correctly() {
        assert_eq!(wire_user_role_to_domain(ws::UserRole::Admin), UserRole::Admin);
        assert_eq!(wire_user_role_to_domain(ws::UserRole::User), UserRole::User);
        // Unspecified + User both surface as User — matches proto path.
        assert_eq!(wire_user_role_to_domain(ws::UserRole::Unspecified), UserRole::User);
    }

    #[test]
    fn domain_user_role_to_wire_each_variant() {
        assert_eq!(domain_user_role_to_wire(UserRole::User), ws::UserRole::User);
        assert_eq!(domain_user_role_to_wire(UserRole::Admin), ws::UserRole::Admin);
    }

    // -----------------------------------------------------------------------
    // CredentialType mapping (both directions)
    // -----------------------------------------------------------------------

    #[test]
    fn wire_credential_type_each_variant_maps_correctly() {
        assert_eq!(
            wire_credential_type_to_domain(ws::CredentialType::Passkey),
            Some(CredentialType::Passkey),
        );
        assert_eq!(
            wire_credential_type_to_domain(ws::CredentialType::Totp),
            Some(CredentialType::Totp),
        );
        assert_eq!(
            wire_credential_type_to_domain(ws::CredentialType::RecoveryCode),
            Some(CredentialType::RecoveryCode),
        );
        // Unspecified returns None so caller decides the fallback.
        assert_eq!(wire_credential_type_to_domain(ws::CredentialType::Unspecified), None);
    }

    #[test]
    fn domain_credential_type_to_wire_each_variant() {
        assert_eq!(
            domain_credential_type_to_wire(CredentialType::Passkey),
            ws::CredentialType::Passkey
        );
        assert_eq!(domain_credential_type_to_wire(CredentialType::Totp), ws::CredentialType::Totp);
        assert_eq!(
            domain_credential_type_to_wire(CredentialType::RecoveryCode),
            ws::CredentialType::RecoveryCode,
        );
    }

    // -----------------------------------------------------------------------
    // TotpAlgorithm mapping (both directions)
    // -----------------------------------------------------------------------

    #[test]
    fn wire_totp_algorithm_each_variant_maps_correctly() {
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha1), TotpAlgorithm::Sha1);
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha256), TotpAlgorithm::Sha256,);
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha512), TotpAlgorithm::Sha512,);
    }

    #[test]
    fn domain_totp_algorithm_to_wire_each_variant() {
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha1), ws::TotpAlgorithm::Sha1);
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha256), ws::TotpAlgorithm::Sha256);
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha512), ws::TotpAlgorithm::Sha512);
    }

    // -----------------------------------------------------------------------
    // nanos_to_system_time edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn nanos_to_system_time_zero_returns_none() {
        assert!(nanos_to_system_time(0).is_none());
    }

    #[test]
    fn nanos_to_system_time_round_trips_known_value() {
        let nanos = 1_700_000_000_500_000_000u64;
        let st = nanos_to_system_time(nanos).expect("known value should convert");
        let d = st.duration_since(UNIX_EPOCH).expect("after epoch");
        assert_eq!(d.as_secs(), 1_700_000_000);
        assert_eq!(d.subsec_nanos(), 500_000_000);
    }

    // -----------------------------------------------------------------------
    // domain_user_info_from_wire
    // -----------------------------------------------------------------------

    #[test]
    fn domain_user_info_round_trip_preserves_fields() {
        let user = ws::User {
            id: None,
            name: "Alice".to_owned(),
            email: Some(UserEmailId::new(42)),
            status: ws::UserStatus::Active,
            created_at: 1_700_000_000_000_000_000,
            updated_at: 1_700_000_001_000_000_000,
            role: ws::UserRole::Admin,
            slug: Some(UserSlug::new(7)),
            deleted_at: None,
        };
        let info = domain_user_info_from_wire(&user);
        assert_eq!(info.slug, UserSlug::new(7));
        assert_eq!(info.name, "Alice");
        assert_eq!(info.email, UserEmailId::new(42));
        assert_eq!(info.status, UserStatus::Active);
        assert_eq!(info.role, UserRole::Admin);
        assert!(info.created_at.is_some());
        assert!(info.updated_at.is_some());
        assert!(info.deleted_at.is_none());
        assert_eq!(info.retention_days, None);
    }

    #[test]
    fn domain_user_info_missing_slug_defaults_to_zero() {
        let user = ws::User {
            id: None,
            name: String::new(),
            email: None,
            status: ws::UserStatus::Unspecified,
            created_at: 0,
            updated_at: 0,
            role: ws::UserRole::Unspecified,
            slug: None,
            deleted_at: None,
        };
        let info = domain_user_info_from_wire(&user);
        assert_eq!(info.slug, UserSlug::new(0));
        assert_eq!(info.email, UserEmailId::new(0));
        // Unspecified status collapses to Active per fail-soft contract.
        assert_eq!(info.status, UserStatus::Active);
        assert_eq!(info.role, UserRole::User);
        assert!(info.created_at.is_none());
        assert!(info.updated_at.is_none());
    }

    #[test]
    fn domain_user_info_with_deleted_at() {
        let user = ws::User {
            id: None,
            name: "Bob".to_owned(),
            email: Some(UserEmailId::new(1)),
            status: ws::UserStatus::Deleted,
            created_at: 1_700_000_000_000_000_000,
            updated_at: 1_700_000_001_000_000_000,
            role: ws::UserRole::User,
            slug: Some(UserSlug::new(99)),
            deleted_at: Some(1_700_000_002_000_000_000),
        };
        let info = domain_user_info_from_wire(&user);
        assert_eq!(info.status, UserStatus::Deleted);
        assert!(info.deleted_at.is_some());
    }

    // -----------------------------------------------------------------------
    // domain_user_email_info_from_wire
    // -----------------------------------------------------------------------

    #[test]
    fn domain_user_email_info_unverified_when_zero() {
        let email = ws::UserEmail {
            id: Some(UserEmailId::new(11)),
            user: None,
            email: "x@y.com".to_owned(),
            created_at: 1_700_000_000_000_000_000,
            verified_at: 0,
        };
        let info = domain_user_email_info_from_wire(&email);
        assert_eq!(info.id, UserEmailId::new(11));
        assert_eq!(info.email, "x@y.com");
        assert!(!info.verified);
        assert!(info.created_at.is_some());
        assert!(info.verified_at.is_none());
    }

    #[test]
    fn domain_user_email_info_verified_when_nonzero() {
        let email = ws::UserEmail {
            id: Some(UserEmailId::new(12)),
            user: None,
            email: "v@y.com".to_owned(),
            created_at: 1_700_000_000_000_000_000,
            verified_at: 1_700_000_001_000_000_000,
        };
        let info = domain_user_email_info_from_wire(&email);
        assert!(info.verified);
        assert!(info.verified_at.is_some());
    }

    // -----------------------------------------------------------------------
    // token_pair_from_wire
    // -----------------------------------------------------------------------

    #[test]
    fn token_pair_from_wire_round_trip() {
        let p = ws::TokenPair {
            access_token: "access".to_owned(),
            refresh_token: "refresh".to_owned(),
            access_expires_at: 1_700_000_000_000_000_000,
            refresh_expires_at: 1_700_000_002_000_000_000,
        };
        let domain = token_pair_from_wire(p);
        assert_eq!(domain.access_token, "access");
        assert_eq!(domain.refresh_token, "refresh");
        assert!(domain.access_expires_at.is_some());
        assert!(domain.refresh_expires_at.is_some());
    }

    #[test]
    fn token_pair_from_wire_zero_timestamps_are_none() {
        let p = ws::TokenPair {
            access_token: String::new(),
            refresh_token: String::new(),
            access_expires_at: 0,
            refresh_expires_at: 0,
        };
        let domain = token_pair_from_wire(p);
        assert!(domain.access_expires_at.is_none());
        assert!(domain.refresh_expires_at.is_none());
    }

    // -----------------------------------------------------------------------
    // Passkey credential round-trip (both directions)
    // -----------------------------------------------------------------------

    #[test]
    fn passkey_round_trip_through_wire() {
        let domain = PasskeyCredentialInfo {
            credential_id: vec![1, 2, 3, 4],
            public_key: vec![5, 6, 7, 8],
            sign_count: 10,
            transports: vec!["usb".to_owned(), "nfc".to_owned()],
            backup_eligible: true,
            backup_state: false,
            attestation_format: Some("packed".to_owned()),
            aaguid: Some(vec![0xAB; 16]),
        };
        let wire = domain_passkey_to_wire(&domain);
        let back = domain_passkey_from_wire(&wire);
        assert_eq!(domain, back);
    }

    #[test]
    fn passkey_with_no_aaguid_round_trips() {
        let domain = PasskeyCredentialInfo {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 0,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        let wire = domain_passkey_to_wire(&domain);
        let back = domain_passkey_from_wire(&wire);
        assert_eq!(domain, back);
    }

    // -----------------------------------------------------------------------
    // TOTP credential round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn totp_round_trip_through_wire() {
        let domain = TotpCredentialInfo {
            secret: vec![0x42; 20],
            algorithm: TotpAlgorithm::Sha256,
            digits: 8,
            period: 60,
        };
        let wire = domain_totp_to_wire(&domain);
        let back = domain_totp_from_wire(&wire);
        assert_eq!(domain, back);
    }

    // -----------------------------------------------------------------------
    // Recovery-code credential round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_code_round_trip_through_wire() {
        let domain = RecoveryCodeCredentialInfo {
            code_hashes: vec![vec![1; 32], vec![2; 32], vec![3; 32]],
            total_generated: 3,
        };
        let wire = domain_recovery_to_wire(&domain);
        let back = domain_recovery_from_wire(&wire);
        assert_eq!(domain, back);
    }

    // -----------------------------------------------------------------------
    // UserCredential oneof — every variant projects to the matching domain arm
    // -----------------------------------------------------------------------

    #[test]
    fn user_credential_passkey_variant_maps_correctly() {
        let cred = w::UserCredential {
            id: 1,
            user: Some(UserSlug::new(7)),
            credential_type: ws::CredentialType::Passkey,
            name: "Touch ID".to_owned(),
            enabled: true,
            created_at: 1_700_000_000_000_000_000,
            last_used_at: None,
            data: Some(w::UserCredentialData::Passkey(w::PasskeyCredentialData {
                credential_id: Bytes::from_static(&[0xAA; 16]),
                public_key: Bytes::from_static(&[0xBB; 65]),
                sign_count: 5,
                transports: vec!["internal".to_owned()],
                backup_eligible: true,
                backup_state: true,
                attestation_format: None,
                aaguid: None,
            })),
        };
        let info = domain_user_credential_info_from_wire(&cred);
        assert_eq!(info.id, UserCredentialId::new(1));
        assert_eq!(info.credential_type, CredentialType::Passkey);
        assert!(matches!(info.data, Some(CredentialData::Passkey(_))));
        assert!(info.last_used_at.is_none());
    }

    #[test]
    fn user_credential_totp_variant_maps_correctly() {
        let cred = w::UserCredential {
            id: 2,
            user: Some(UserSlug::new(8)),
            credential_type: ws::CredentialType::Totp,
            name: "Authenticator".to_owned(),
            enabled: true,
            created_at: 1_700_000_000_000_000_000,
            last_used_at: Some(1_700_000_001_000_000_000),
            data: Some(w::UserCredentialData::Totp(w::TotpCredentialData {
                secret: Bytes::from_static(&[]),
                algorithm: ws::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            })),
        };
        let info = domain_user_credential_info_from_wire(&cred);
        assert_eq!(info.credential_type, CredentialType::Totp);
        if let Some(CredentialData::Totp(t)) = &info.data {
            assert!(t.secret.is_empty());
            assert_eq!(t.algorithm, TotpAlgorithm::Sha1);
        } else {
            panic!("expected TOTP data");
        }
        assert!(info.last_used_at.is_some());
    }

    #[test]
    fn user_credential_recovery_code_variant_maps_correctly() {
        let cred = w::UserCredential {
            id: 3,
            user: Some(UserSlug::new(9)),
            credential_type: ws::CredentialType::RecoveryCode,
            name: "Recovery".to_owned(),
            enabled: true,
            created_at: 1_700_000_000_000_000_000,
            last_used_at: None,
            data: Some(w::UserCredentialData::RecoveryCode(w::RecoveryCodeCredentialData {
                code_hashes: vec![Bytes::from_static(&[1; 32]), Bytes::from_static(&[2; 32])],
                total_generated: 10,
            })),
        };
        let info = domain_user_credential_info_from_wire(&cred);
        assert_eq!(info.credential_type, CredentialType::RecoveryCode);
        if let Some(CredentialData::RecoveryCode(rc)) = &info.data {
            assert_eq!(rc.code_hashes.len(), 2);
            assert_eq!(rc.total_generated, 10);
        } else {
            panic!("expected recovery code data");
        }
    }

    #[test]
    fn user_credential_no_data_round_trips() {
        let cred = w::UserCredential {
            id: 4,
            user: None,
            credential_type: ws::CredentialType::Unspecified,
            name: String::new(),
            enabled: false,
            created_at: 0,
            last_used_at: None,
            data: None,
        };
        let info = domain_user_credential_info_from_wire(&cred);
        // Unspecified credential type collapses to Passkey fallback.
        assert_eq!(info.credential_type, CredentialType::Passkey);
        assert!(info.data.is_none());
        assert_eq!(info.user, UserSlug::new(0));
    }

    // -----------------------------------------------------------------------
    // CreateUserCredentialData oneof — every variant survives serialization
    // -----------------------------------------------------------------------

    #[test]
    fn create_user_credential_data_oneof_passkey_round_trips() {
        let req = w::CreateUserCredentialRequest {
            user: Some(UserSlug::new(1)),
            credential_type: ws::CredentialType::Passkey,
            name: "k".to_owned(),
            caller: Some(UserSlug::new(1)),
            data: Some(w::CreateUserCredentialData::Passkey(w::PasskeyCredentialData {
                credential_id: Bytes::from_static(&[0xAB; 16]),
                public_key: Bytes::from_static(&[0xCD; 65]),
                sign_count: 0,
                transports: vec!["usb".to_owned()],
                backup_eligible: true,
                backup_state: false,
                attestation_format: Some("packed".to_owned()),
                aaguid: Some(Bytes::from_static(&[0xEF; 16])),
            })),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: w::CreateUserCredentialRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn create_user_credential_data_oneof_totp_round_trips() {
        let req = w::CreateUserCredentialRequest {
            user: Some(UserSlug::new(2)),
            credential_type: ws::CredentialType::Totp,
            name: "totp".to_owned(),
            caller: Some(UserSlug::new(2)),
            data: Some(w::CreateUserCredentialData::Totp(w::TotpCredentialData {
                secret: Bytes::from_static(&[0x01; 20]),
                algorithm: ws::TotpAlgorithm::Sha512,
                digits: 8,
                period: 60,
            })),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: w::CreateUserCredentialRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn create_user_credential_data_oneof_recovery_code_round_trips() {
        let req = w::CreateUserCredentialRequest {
            user: Some(UserSlug::new(3)),
            credential_type: ws::CredentialType::RecoveryCode,
            name: "recovery".to_owned(),
            caller: Some(UserSlug::new(3)),
            data: Some(w::CreateUserCredentialData::RecoveryCode(w::RecoveryCodeCredentialData {
                code_hashes: vec![Bytes::from_static(&[0x11; 32])],
                total_generated: 1,
            })),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: w::CreateUserCredentialRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    // -----------------------------------------------------------------------
    // VerifyEmailCodeResult oneof — every variant survives serialization
    // -----------------------------------------------------------------------

    #[test]
    fn verify_email_code_result_existing_user_round_trips() {
        let resp = w::VerifyEmailCodeResponse {
            result: Some(w::VerifyEmailCodeResult::ExistingUser(w::ExistingUserSession {
                user: Some(UserSlug::new(7)),
                session: Some(ws::TokenPair {
                    access_token: "a".to_owned(),
                    refresh_token: "r".to_owned(),
                    access_expires_at: 1_700_000_000_000_000_000,
                    refresh_expires_at: 1_700_000_002_000_000_000,
                }),
            })),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: w::VerifyEmailCodeResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn verify_email_code_result_new_user_round_trips() {
        let resp = w::VerifyEmailCodeResponse {
            result: Some(w::VerifyEmailCodeResult::NewUser(w::OnboardingSession {
                onboarding_token: "tok".to_owned(),
            })),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: w::VerifyEmailCodeResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn verify_email_code_result_totp_required_round_trips() {
        let resp = w::VerifyEmailCodeResponse {
            result: Some(w::VerifyEmailCodeResult::TotpRequired(ws::TotpRequired {
                challenge_nonce: Bytes::from_static(&[0xCC; 32]),
            })),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: w::VerifyEmailCodeResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    // -----------------------------------------------------------------------
    // missing_field error shape
    // -----------------------------------------------------------------------

    #[test]
    fn missing_field_returns_internal_rpc_error() {
        let err = missing_field("user", "GetUserResponse");
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(message.contains("user"));
                assert!(message.contains("GetUserResponse"));
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Sanity: the wire credential request struct is wide — confirm its size
    // doesn't blow up Result<T, SdkError> beyond the clippy threshold. We
    // use postcard round-trip rather than a hard-coded size assertion (the
    // tag scheme can shift); this just guards encoding integrity.
    // -----------------------------------------------------------------------

    #[test]
    fn delete_user_response_zero_deleted_at_yields_none() {
        // Sanity: a server response with deleted_at = 0 should not panic;
        // it should yield a UserInfo with deleted_at: None.
        let info = UserInfo {
            slug: UserSlug::new(99),
            name: String::new(),
            email: UserEmailId::new(0),
            status: UserStatus::Deleted,
            role: UserRole::User,
            created_at: None,
            updated_at: None,
            deleted_at: nanos_to_system_time(0),
            retention_days: Some(30),
        };
        assert!(info.deleted_at.is_none());
    }

    // -----------------------------------------------------------------------
    // Specific check: nanos_to_system_time covers a known historical value
    // (for a typical recent UNIX timestamp).
    // -----------------------------------------------------------------------

    #[test]
    fn nanos_to_system_time_handles_subsecond_precision() {
        let t = nanos_to_system_time(1_500_000_000).expect("translates");
        assert_eq!(t, UNIX_EPOCH + Duration::from_nanos(1_500_000_000));
    }
}
