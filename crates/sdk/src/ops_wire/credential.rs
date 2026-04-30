//! Wire-based user credential ops.
//!
//! Mirrors [`super::super::ops::credential`]: same returned domain types
//! ([`UserCredentialInfo`], [`RecoveryCodeResult`]), same `Result<_, SdkError>`
//! shape on the consumer side. Internal dispatch goes through the
//! wire-services-generated
//! [`UserServiceClient`](inferadb_ledger_wire_services::UserServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (credential type / TOTP algorithm /
//! credential data variants, optional `UserCredential` unwrap, page-less
//! list envelope) in isolation. The seven per-RPC entry points themselves
//! are not exercised yet — [`LedgerClient`](crate::LedgerClient) keeps
//! dispatching through the tonic `ops::credential` path until F.1 flips
//! the connection pool.
//!
//! This file overlaps the `UserService` opcode block with the parallel
//! `ops_wire::user` migration (each file is a thin wrapper over the same
//! generated client). The split is deliberate: domain wrappers cluster by
//! consumer concern (credentials vs. user CRUD), not by gRPC service.

use std::sync::Arc;

use bytes::Bytes;
use inferadb_ledger_types::{UserCredentialId, UserSlug};
use inferadb_ledger_wire::services::{shared as ws, user as w};
use inferadb_ledger_wire_services::UserServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    token::TokenPair,
    types::credential::{
        CredentialData, CredentialType, PasskeyCredentialInfo, RecoveryCodeCredentialInfo,
        RecoveryCodeResult, TotpAlgorithm, TotpCredentialInfo, UserCredentialInfo,
    },
};

// ---------------------------------------------------------------------------
// Credential CRUD
// ---------------------------------------------------------------------------

/// Issue a `CreateUserCredential` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::create_user_credential`](crate::LedgerClient::create_user_credential)
/// at the dispatch layer. The credential type is derived from the `data`
/// variant; the wire request enum is populated from the same domain
/// payload. For TOTP credentials, the response carries the secret exactly
/// once — subsequent reads return an empty secret.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's
/// retry loop is responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s.
/// Returns [`SdkError::Rpc`] with
/// [`ErrorCode::Internal`](inferadb_ledger_wire::ErrorCode::Internal) when the response envelope
/// omits the `credential` field.
pub(crate) async fn create_user_credential(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    user: UserSlug,
    name: String,
    data: CredentialData,
) -> Result<UserCredentialInfo> {
    let credential_type = domain_credential_type_from_data(&data);
    let wire_data = domain_credential_data_to_wire(data);

    let client = UserServiceClient::new(wire_client);
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
        .map(domain_user_credential_info_from_wire)
        .ok_or_else(|| missing_field("credential", "CreateUserCredentialResponse"))
}

/// Issue a `ListUserCredentials` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::list_user_credentials`](crate::LedgerClient::list_user_credentials).
/// TOTP secrets are stripped server-side; the `secret` field on a returned
/// TOTP credential is always empty.
///
/// # Errors
///
/// Same shape as [`create_user_credential`] minus the missing-field path
/// (the list envelope is always populated, even when empty).
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
    Ok(response.credentials.into_iter().map(domain_user_credential_info_from_wire).collect())
}

/// Issue an `UpdateUserCredential` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::update_user_credential`](crate::LedgerClient::update_user_credential).
/// Only `name` / `enabled` are mutable for TOTP and recovery code
/// credentials; passkey credentials additionally accept the full
/// `PasskeyCredentialInfo` payload (e.g. updated sign count after WebAuthn
/// authentication).
///
/// # Errors
///
/// Same shape as [`create_user_credential`].
#[allow(clippy::too_many_arguments)]
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
        passkey: passkey_data.map(domain_passkey_to_wire),
    };
    let response =
        client.update_user_credential(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .credential
        .map(domain_user_credential_info_from_wire)
        .ok_or_else(|| missing_field("credential", "UpdateUserCredentialResponse"))
}

/// Issue a `DeleteUserCredential` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::delete_user_credential`](crate::LedgerClient::delete_user_credential).
/// The server rejects deletes that would leave the user with no credentials
/// — that surfaces as a server-side [`WireError`] flowing through
/// [`rpc_error_to_sdk_error`].
///
/// # Errors
///
/// Same shape as [`create_user_credential`] minus the missing-field path
/// (the response envelope is empty by design).
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
// TOTP challenge / verification
// ---------------------------------------------------------------------------

/// Issue a `CreateTotpChallenge` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::create_totp_challenge`](crate::LedgerClient::create_totp_challenge).
/// Returns a 32-byte challenge nonce that the caller threads into the
/// subsequent [`verify_totp`] / [`consume_recovery_code`] call. The wire
/// type carries `Bytes`; the SDK exposes `Vec<u8>` so callers stay
/// decoupled from the wire crate.
///
/// # Errors
///
/// Same shape as [`create_user_credential`] minus the missing-field path.
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
/// Mirrors [`LedgerClient::verify_totp`](crate::LedgerClient::verify_totp).
/// On success, the server atomically consumes the challenge and creates a
/// user session; the response carries the access + refresh token pair.
///
/// # Errors
///
/// Same shape as [`create_user_credential`].
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
    response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "VerifyTotpResponse"))
}

/// Issue a `ConsumeRecoveryCode` RPC via the wire transport.
///
/// Mirrors [`LedgerClient::consume_recovery_code`](crate::LedgerClient::consume_recovery_code).
/// On success, the server atomically removes the matching code hash and
/// creates a user session; the response carries the token pair plus the
/// number of unused codes remaining (so callers can warn the user when
/// recovery codes are running low).
///
/// # Errors
///
/// Same shape as [`create_user_credential`].
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
    let tokens = response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "ConsumeRecoveryCodeResponse"))?;
    Ok(RecoveryCodeResult { tokens, remaining_codes: response.remaining_codes })
}

// ---------------------------------------------------------------------------
// Wire → domain mappers
// ---------------------------------------------------------------------------

/// Maps a wire `UserCredential` into the SDK's [`UserCredentialInfo`].
///
/// Optional fields fall back to the same defaults as the proto path's
/// `UserCredentialInfo::from_proto` — missing user slug becomes `0`,
/// missing `data` stays `None`, the credential type collapses to
/// [`CredentialType::Passkey`] when the wire enum is `Unspecified` (so a
/// future server bug doesn't trip the call site).
fn domain_user_credential_info_from_wire(c: w::UserCredential) -> UserCredentialInfo {
    UserCredentialInfo {
        id: UserCredentialId::new(c.id),
        user: c.user.unwrap_or_else(|| UserSlug::new(0)),
        credential_type: wire_credential_type_to_domain(c.credential_type)
            .unwrap_or(CredentialType::Passkey),
        name: c.name,
        enabled: c.enabled,
        created_at: nanos_to_system_time(c.created_at),
        last_used_at: c.last_used_at.and_then(nanos_to_system_time),
        data: c.data.map(domain_credential_data_from_wire),
    }
}

/// Maps a wire `UserCredentialData` into the SDK's [`CredentialData`].
fn domain_credential_data_from_wire(d: w::UserCredentialData) -> CredentialData {
    match d {
        w::UserCredentialData::Passkey(pk) => CredentialData::Passkey(domain_passkey_from_wire(pk)),
        w::UserCredentialData::Totp(totp) => CredentialData::Totp(domain_totp_from_wire(totp)),
        w::UserCredentialData::RecoveryCode(rc) => {
            CredentialData::RecoveryCode(domain_recovery_code_from_wire(rc))
        },
    }
}

/// Maps a wire `PasskeyCredentialData` into the SDK's
/// [`PasskeyCredentialInfo`]. `Bytes` fields collapse to `Vec<u8>` so the
/// public type stays decoupled from the wire crate.
fn domain_passkey_from_wire(p: w::PasskeyCredentialData) -> PasskeyCredentialInfo {
    PasskeyCredentialInfo {
        credential_id: p.credential_id.to_vec(),
        public_key: p.public_key.to_vec(),
        sign_count: p.sign_count,
        transports: p.transports,
        backup_eligible: p.backup_eligible,
        backup_state: p.backup_state,
        attestation_format: p.attestation_format,
        aaguid: p.aaguid.map(|b| b.to_vec()),
    }
}

/// Maps a wire `TotpCredentialData` into the SDK's [`TotpCredentialInfo`].
/// The wire `algorithm` enum has no `Unspecified` variant — `0` is `Sha1`
/// per the wire definition (matches RFC 6238).
fn domain_totp_from_wire(t: w::TotpCredentialData) -> TotpCredentialInfo {
    TotpCredentialInfo {
        secret: t.secret.to_vec(),
        algorithm: wire_totp_algorithm_to_domain(t.algorithm),
        digits: t.digits,
        period: t.period,
    }
}

/// Maps a wire `RecoveryCodeCredentialData` into the SDK's
/// [`RecoveryCodeCredentialInfo`].
fn domain_recovery_code_from_wire(r: w::RecoveryCodeCredentialData) -> RecoveryCodeCredentialInfo {
    RecoveryCodeCredentialInfo {
        code_hashes: r.code_hashes.into_iter().map(|b| b.to_vec()).collect(),
        total_generated: r.total_generated,
    }
}

/// Maps a wire `TokenPair` into the SDK's domain [`TokenPair`].
///
/// The wire payload uses UNIX nanoseconds; the domain type uses
/// `Option<SystemTime>`. `0` is the wire convention for "unset" and
/// surfaces as `None` here (mirrors the proto path's
/// `proto_timestamp_to_system_time` which returns `None` on absence).
fn domain_token_pair_from_wire(t: ws::TokenPair) -> TokenPair {
    TokenPair {
        access_token: t.access_token,
        refresh_token: t.refresh_token,
        access_expires_at: nanos_to_system_time(t.access_expires_at),
        refresh_expires_at: nanos_to_system_time(t.refresh_expires_at),
    }
}

// ---------------------------------------------------------------------------
// Domain → wire mappers
// ---------------------------------------------------------------------------

/// Derives the wire `CredentialType` tag from a domain
/// [`CredentialData`] payload, matching
/// [`CredentialType::from_data`](crate::types::credential::CredentialType::from_data).
fn domain_credential_type_from_data(data: &CredentialData) -> ws::CredentialType {
    match data {
        CredentialData::Passkey(_) => ws::CredentialType::Passkey,
        CredentialData::Totp(_) => ws::CredentialType::Totp,
        CredentialData::RecoveryCode(_) => ws::CredentialType::RecoveryCode,
    }
}

/// Maps a domain [`CredentialData`] payload into the wire request enum.
fn domain_credential_data_to_wire(data: CredentialData) -> w::CreateUserCredentialData {
    match data {
        CredentialData::Passkey(pk) => {
            w::CreateUserCredentialData::Passkey(domain_passkey_to_wire(pk))
        },
        CredentialData::Totp(totp) => w::CreateUserCredentialData::Totp(domain_totp_to_wire(totp)),
        CredentialData::RecoveryCode(rc) => {
            w::CreateUserCredentialData::RecoveryCode(domain_recovery_code_to_wire(rc))
        },
    }
}

/// Maps a domain [`PasskeyCredentialInfo`] into the wire payload.
fn domain_passkey_to_wire(p: PasskeyCredentialInfo) -> w::PasskeyCredentialData {
    w::PasskeyCredentialData {
        credential_id: Bytes::from(p.credential_id),
        public_key: Bytes::from(p.public_key),
        sign_count: p.sign_count,
        transports: p.transports,
        backup_eligible: p.backup_eligible,
        backup_state: p.backup_state,
        attestation_format: p.attestation_format,
        aaguid: p.aaguid.map(Bytes::from),
    }
}

/// Maps a domain [`TotpCredentialInfo`] into the wire payload.
fn domain_totp_to_wire(t: TotpCredentialInfo) -> w::TotpCredentialData {
    w::TotpCredentialData {
        secret: Bytes::from(t.secret),
        algorithm: domain_totp_algorithm_to_wire(t.algorithm),
        digits: t.digits,
        period: t.period,
    }
}

/// Maps a domain [`RecoveryCodeCredentialInfo`] into the wire payload.
fn domain_recovery_code_to_wire(r: RecoveryCodeCredentialInfo) -> w::RecoveryCodeCredentialData {
    w::RecoveryCodeCredentialData {
        code_hashes: r.code_hashes.into_iter().map(Bytes::from).collect(),
        total_generated: r.total_generated,
    }
}

// ---------------------------------------------------------------------------
// Enum tag projections
// ---------------------------------------------------------------------------

/// Maps a wire `CredentialType` into the SDK's [`CredentialType`] enum.
///
/// The wire `Unspecified` variant collapses to `None` — mirrors the proto
/// path's `CredentialType::from_proto_i32` which returns `None` for the
/// zero tag. Call sites must decide on a fallback (the credential-info
/// helper picks `Passkey`, matching the proto path).
fn wire_credential_type_to_domain(ct: ws::CredentialType) -> Option<CredentialType> {
    match ct {
        ws::CredentialType::Passkey => Some(CredentialType::Passkey),
        ws::CredentialType::Totp => Some(CredentialType::Totp),
        ws::CredentialType::RecoveryCode => Some(CredentialType::RecoveryCode),
        ws::CredentialType::Unspecified => None,
    }
}

/// Maps a domain [`CredentialType`] into the wire enum, used on outgoing
/// requests. The domain side has no `Unspecified` variant by design.
fn domain_credential_type_to_wire(ct: CredentialType) -> ws::CredentialType {
    match ct {
        CredentialType::Passkey => ws::CredentialType::Passkey,
        CredentialType::Totp => ws::CredentialType::Totp,
        CredentialType::RecoveryCode => ws::CredentialType::RecoveryCode,
    }
}

/// Maps a wire `TotpAlgorithm` into the SDK's [`TotpAlgorithm`] enum.
///
/// The wire enum has no `Unspecified` variant — `Sha1` is the zero tag by
/// design (matches RFC 6238 default and the proto path's
/// `TotpAlgorithm::from_proto_i32` fallback).
fn wire_totp_algorithm_to_domain(alg: ws::TotpAlgorithm) -> TotpAlgorithm {
    match alg {
        ws::TotpAlgorithm::Sha1 => TotpAlgorithm::Sha1,
        ws::TotpAlgorithm::Sha256 => TotpAlgorithm::Sha256,
        ws::TotpAlgorithm::Sha512 => TotpAlgorithm::Sha512,
    }
}

/// Maps a domain [`TotpAlgorithm`] into the wire enum, used on outgoing
/// requests.
fn domain_totp_algorithm_to_wire(alg: TotpAlgorithm) -> ws::TotpAlgorithm {
    match alg {
        TotpAlgorithm::Sha1 => ws::TotpAlgorithm::Sha1,
        TotpAlgorithm::Sha256 => ws::TotpAlgorithm::Sha256,
        TotpAlgorithm::Sha512 => ws::TotpAlgorithm::Sha512,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Converts a UNIX-nanoseconds wire timestamp into `SystemTime`.
///
/// Returns `None` for `0` (the wire convention for "unset") and for values
/// that overflow `Duration`. Mirrors the proto path's
/// `proto_timestamp_to_system_time` which returns `None` on absence /
/// invalid inputs.
fn nanos_to_system_time(nanos: u64) -> Option<std::time::SystemTime> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, subsec_nanos))
}

/// Builds an `SdkError::Rpc` for a missing field in a wire response.
///
/// Mirrors [`crate::proto_util::missing_response_field`] — kept local here
/// to avoid coupling `ops_wire/*.rs` to the `proto_util` module's tonic
/// import chain. The shape is identical (Internal code, descriptive
/// message, no correlation IDs because the wire transport doesn't surface
/// them).
fn missing_field(field: &str, response_type: &str) -> SdkError {
    SdkError::Rpc {
        code: inferadb_ledger_wire::ErrorCode::Internal,
        message: format!("Missing {field} in {response_type}"),
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the wire-response → domain-type mapping plus the
    //! enum-tag projections in both directions. End-to-end tests against a
    //! real `WireClient` / `WireServer` are deferred to E.7 (TestCluster
    //! migration) — at that point the seven per-RPC entry points get
    //! exercised in full.

    use super::*;

    // ----- CredentialType: wire → domain -----

    #[test]
    fn wire_credential_type_each_variant_maps_correctly() {
        // Pin per-variant mapping so a future addition to the wire enum
        // doesn't silently fall through without a matching domain variant.
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
        // Unspecified → None — matches proto path.
        assert_eq!(wire_credential_type_to_domain(ws::CredentialType::Unspecified), None);
    }

    // ----- CredentialType: domain → wire -----

    #[test]
    fn domain_credential_type_to_wire_each_variant() {
        assert_eq!(
            domain_credential_type_to_wire(CredentialType::Passkey),
            ws::CredentialType::Passkey,
        );
        assert_eq!(domain_credential_type_to_wire(CredentialType::Totp), ws::CredentialType::Totp);
        assert_eq!(
            domain_credential_type_to_wire(CredentialType::RecoveryCode),
            ws::CredentialType::RecoveryCode,
        );
    }

    // ----- domain_credential_type_from_data discriminator -----

    #[test]
    fn credential_type_from_data_each_variant() {
        let pk = CredentialData::Passkey(PasskeyCredentialInfo {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 0,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        });
        let totp = CredentialData::Totp(TotpCredentialInfo {
            secret: vec![],
            algorithm: TotpAlgorithm::Sha1,
            digits: 6,
            period: 30,
        });
        let rc = CredentialData::RecoveryCode(RecoveryCodeCredentialInfo {
            code_hashes: vec![],
            total_generated: 0,
        });
        assert_eq!(domain_credential_type_from_data(&pk), ws::CredentialType::Passkey);
        assert_eq!(domain_credential_type_from_data(&totp), ws::CredentialType::Totp);
        assert_eq!(domain_credential_type_from_data(&rc), ws::CredentialType::RecoveryCode);
    }

    // ----- TotpAlgorithm: wire → domain -----

    #[test]
    fn wire_totp_algorithm_each_variant_maps_correctly() {
        // Wire enum has NO Unspecified variant — `0` is `Sha1` by design,
        // matching RFC 6238's default and the proto path's i32 fallback.
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha1), TotpAlgorithm::Sha1);
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha256), TotpAlgorithm::Sha256);
        assert_eq!(wire_totp_algorithm_to_domain(ws::TotpAlgorithm::Sha512), TotpAlgorithm::Sha512);
    }

    // ----- TotpAlgorithm: domain → wire -----

    #[test]
    fn domain_totp_algorithm_to_wire_each_variant() {
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha1), ws::TotpAlgorithm::Sha1);
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha256), ws::TotpAlgorithm::Sha256);
        assert_eq!(domain_totp_algorithm_to_wire(TotpAlgorithm::Sha512), ws::TotpAlgorithm::Sha512);
    }

    // ----- Repr-i32 numeric tags pinned -----

    #[test]
    fn wire_credential_type_numeric_tags_match_proto() {
        // Wire enum is `#[repr(i32)]`; tags must match proto so the
        // numeric tag flows uninterpreted through the dispatch layer.
        assert_eq!(ws::CredentialType::Unspecified as i32, 0);
        assert_eq!(ws::CredentialType::Passkey as i32, 1);
        assert_eq!(ws::CredentialType::Totp as i32, 2);
        assert_eq!(ws::CredentialType::RecoveryCode as i32, 3);
    }

    #[test]
    fn wire_totp_algorithm_numeric_tags_match_proto() {
        // The wire `TotpAlgorithm` enum is `#[repr(i32)]` with `Sha1 = 0`
        // (NO Unspecified) — keeps RFC 6238's default at the zero tag and
        // matches the proto path's i32 fallback in
        // `TotpAlgorithm::from_proto_i32`.
        assert_eq!(ws::TotpAlgorithm::Sha1 as i32, 0);
        assert_eq!(ws::TotpAlgorithm::Sha256 as i32, 1);
        assert_eq!(ws::TotpAlgorithm::Sha512 as i32, 2);
    }

    // ----- nanos_to_system_time -----

    #[test]
    fn nanos_to_system_time_zero_returns_none() {
        // 0 ns is the wire convention for "unset"; matches proto's absent
        // `Timestamp` message.
        assert!(nanos_to_system_time(0).is_none());
    }

    #[test]
    fn nanos_to_system_time_round_trips_known_value() {
        let nanos = 1_700_000_000_500_000_000u64;
        let st = nanos_to_system_time(nanos).expect("known value should convert");
        let d = st.duration_since(std::time::UNIX_EPOCH).expect("after epoch");
        assert_eq!(d.as_secs(), 1_700_000_000);
        assert_eq!(d.subsec_nanos(), 500_000_000);
    }

    // ----- Passkey wire ↔ domain round-trip -----

    #[test]
    fn passkey_wire_round_trip_preserves_fields() {
        let domain = PasskeyCredentialInfo {
            credential_id: vec![1, 2, 3, 4],
            public_key: vec![10, 20, 30],
            sign_count: 7,
            transports: vec!["usb".to_owned(), "nfc".to_owned()],
            backup_eligible: true,
            backup_state: false,
            attestation_format: Some("packed".to_owned()),
            aaguid: Some(vec![0xAB; 16]),
        };
        let wire = domain_passkey_to_wire(domain.clone());
        let back = domain_passkey_from_wire(wire);
        assert_eq!(domain, back);
    }

    #[test]
    fn passkey_wire_optional_fields_round_trip_none() {
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
        let wire = domain_passkey_to_wire(domain.clone());
        let back = domain_passkey_from_wire(wire);
        assert_eq!(domain, back);
    }

    // ----- TOTP wire ↔ domain round-trip -----

    #[test]
    fn totp_wire_round_trip_preserves_fields() {
        let domain = TotpCredentialInfo {
            secret: vec![42; 20],
            algorithm: TotpAlgorithm::Sha256,
            digits: 8,
            period: 60,
        };
        let wire = domain_totp_to_wire(domain.clone());
        let back = domain_totp_from_wire(wire);
        assert_eq!(domain, back);
    }

    // ----- RecoveryCode wire ↔ domain round-trip -----

    #[test]
    fn recovery_code_wire_round_trip_preserves_fields() {
        let domain = RecoveryCodeCredentialInfo {
            code_hashes: vec![vec![1u8; 32], vec![2u8; 32], vec![3u8; 32]],
            total_generated: 10,
        };
        let wire = domain_recovery_code_to_wire(domain.clone());
        let back = domain_recovery_code_from_wire(wire);
        assert_eq!(domain, back);
    }

    // ----- CredentialData enum round-trip via Create wire payload -----

    #[test]
    fn credential_data_passkey_round_trips_through_wire() {
        let pk = PasskeyCredentialInfo {
            credential_id: vec![1, 2, 3],
            public_key: vec![4, 5, 6],
            sign_count: 1,
            transports: vec!["internal".to_owned()],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        let domain = CredentialData::Passkey(pk);
        let wire = domain_credential_data_to_wire(domain.clone());
        // Re-shape the Create-side enum into the read-side enum so the
        // wire→domain helper accepts it; the Create / read variants share
        // payload types but live on separate enums.
        let read_side = match wire {
            w::CreateUserCredentialData::Passkey(p) => w::UserCredentialData::Passkey(p),
            w::CreateUserCredentialData::Totp(t) => w::UserCredentialData::Totp(t),
            w::CreateUserCredentialData::RecoveryCode(r) => w::UserCredentialData::RecoveryCode(r),
        };
        let back = domain_credential_data_from_wire(read_side);
        assert_eq!(domain, back);
    }

    #[test]
    fn credential_data_totp_round_trips_through_wire() {
        let totp = TotpCredentialInfo {
            secret: vec![0xCD; 20],
            algorithm: TotpAlgorithm::Sha512,
            digits: 6,
            period: 30,
        };
        let domain = CredentialData::Totp(totp);
        let wire = domain_credential_data_to_wire(domain.clone());
        let read_side = match wire {
            w::CreateUserCredentialData::Passkey(p) => w::UserCredentialData::Passkey(p),
            w::CreateUserCredentialData::Totp(t) => w::UserCredentialData::Totp(t),
            w::CreateUserCredentialData::RecoveryCode(r) => w::UserCredentialData::RecoveryCode(r),
        };
        let back = domain_credential_data_from_wire(read_side);
        assert_eq!(domain, back);
    }

    #[test]
    fn credential_data_recovery_code_round_trips_through_wire() {
        let rc =
            RecoveryCodeCredentialInfo { code_hashes: vec![vec![7u8; 32]], total_generated: 5 };
        let domain = CredentialData::RecoveryCode(rc);
        let wire = domain_credential_data_to_wire(domain.clone());
        let read_side = match wire {
            w::CreateUserCredentialData::Passkey(p) => w::UserCredentialData::Passkey(p),
            w::CreateUserCredentialData::Totp(t) => w::UserCredentialData::Totp(t),
            w::CreateUserCredentialData::RecoveryCode(r) => w::UserCredentialData::RecoveryCode(r),
        };
        let back = domain_credential_data_from_wire(read_side);
        assert_eq!(domain, back);
    }

    // ----- UserCredential envelope -----

    #[test]
    fn user_credential_info_from_wire_passkey() {
        let cred = w::UserCredential {
            id: 42,
            user: Some(UserSlug::new(999)),
            credential_type: ws::CredentialType::Passkey,
            name: "Touch ID".to_owned(),
            enabled: true,
            created_at: 1_700_000_000_000_000_000,
            last_used_at: None,
            data: Some(w::UserCredentialData::Passkey(w::PasskeyCredentialData {
                credential_id: Bytes::from_static(&[1, 2, 3]),
                public_key: Bytes::from_static(&[4, 5, 6]),
                sign_count: 5,
                transports: vec!["internal".to_owned()],
                backup_eligible: false,
                backup_state: false,
                attestation_format: None,
                aaguid: None,
            })),
        };
        let info = domain_user_credential_info_from_wire(cred);
        assert_eq!(info.id, UserCredentialId::new(42));
        assert_eq!(info.user, UserSlug::new(999));
        assert_eq!(info.credential_type, CredentialType::Passkey);
        assert_eq!(info.name, "Touch ID");
        assert!(info.enabled);
        assert!(info.created_at.is_some());
        assert!(info.last_used_at.is_none());
        assert!(matches!(info.data, Some(CredentialData::Passkey(_))));
    }

    #[test]
    fn user_credential_info_from_wire_totp_with_last_used() {
        let cred = w::UserCredential {
            id: 10,
            user: Some(UserSlug::new(555)),
            credential_type: ws::CredentialType::Totp,
            name: "Authenticator".to_owned(),
            enabled: true,
            created_at: 1_700_000_000_000_000_000,
            last_used_at: Some(1_700_000_001_000_000_000),
            data: Some(w::UserCredentialData::Totp(w::TotpCredentialData {
                secret: Bytes::new(), // server-stripped
                algorithm: ws::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            })),
        };
        let info = domain_user_credential_info_from_wire(cred);
        assert_eq!(info.credential_type, CredentialType::Totp);
        assert!(info.last_used_at.is_some());
        match info.data {
            Some(CredentialData::Totp(totp)) => {
                assert!(totp.secret.is_empty());
                assert_eq!(totp.algorithm, TotpAlgorithm::Sha1);
            },
            other => panic!("expected TOTP data, got {other:?}"),
        }
    }

    #[test]
    fn user_credential_info_from_wire_no_data_no_user_no_timestamps() {
        // Mirrors the proto path's tolerant defaults: missing user → 0,
        // missing data → None, zero timestamp → None, Unspecified type
        // collapses to Passkey (matches proto path's
        // `unwrap_or(CredentialType::Passkey)` in the from_proto helper).
        let cred = w::UserCredential {
            id: 1,
            user: None,
            credential_type: ws::CredentialType::Unspecified,
            name: String::new(),
            enabled: false,
            created_at: 0,
            last_used_at: None,
            data: None,
        };
        let info = domain_user_credential_info_from_wire(cred);
        assert_eq!(info.user, UserSlug::new(0));
        assert_eq!(info.credential_type, CredentialType::Passkey);
        assert!(info.created_at.is_none());
        assert!(info.last_used_at.is_none());
        assert!(info.data.is_none());
    }

    // ----- TokenPair wire → domain -----

    #[test]
    fn token_pair_from_wire_preserves_fields() {
        let wire = ws::TokenPair {
            access_token: "access.jwt".to_owned(),
            refresh_token: "refresh.opaque".to_owned(),
            access_expires_at: 1_700_000_000_000_000_000,
            refresh_expires_at: 1_800_000_000_000_000_000,
        };
        let domain = domain_token_pair_from_wire(wire);
        assert_eq!(domain.access_token, "access.jwt");
        assert_eq!(domain.refresh_token, "refresh.opaque");
        assert!(domain.access_expires_at.is_some());
        assert!(domain.refresh_expires_at.is_some());
    }

    #[test]
    fn token_pair_from_wire_zero_expiry_round_trips_none() {
        // Mirrors the proto path's `proto_timestamp_to_system_time`
        // returning `None` for absent timestamps. Wire convention: 0 ns
        // means "unset".
        let wire = ws::TokenPair {
            access_token: String::new(),
            refresh_token: String::new(),
            access_expires_at: 0,
            refresh_expires_at: 0,
        };
        let domain = domain_token_pair_from_wire(wire);
        assert!(domain.access_expires_at.is_none());
        assert!(domain.refresh_expires_at.is_none());
    }

    // ----- missing_field error shape -----

    #[test]
    fn missing_field_returns_internal_rpc_error() {
        let err = missing_field("credential", "CreateUserCredentialResponse");
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(message.contains("CreateUserCredentialResponse"), "got: {message}");
                assert!(message.contains("Missing credential"), "got: {message}");
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    // ----- postcard round-trip on the Create request -----

    #[test]
    fn create_user_credential_request_postcard_round_trip() {
        // Sanity check: the wire request type round-trips through postcard
        // unchanged. Catches a regression where Bytes / Option<Bytes>
        // tag-collide on the codec boundary.
        let req = w::CreateUserCredentialRequest {
            user: Some(UserSlug::new(42)),
            credential_type: ws::CredentialType::Passkey,
            name: "yubikey".to_owned(),
            caller: Some(UserSlug::new(42)),
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
}
