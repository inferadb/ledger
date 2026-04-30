//! Wire-based TokenService ops.
//!
//! Mirrors [`super::super::ops::token`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`TokenServiceClient`](inferadb_ledger_wire_services::TokenServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (oneof variants, `SigningKeyScope`
//! enum tag, `Bytes` → `Vec<u8>` for the public-key byte field, UNIX-nanos
//! timestamps → `Option<SystemTime>`) in isolation. F.1.e flipped the
//! connection pool so each entry point is now reached from the corresponding
//! [`super::super::ops::token`] dispatch arm.

use std::sync::Arc;

use inferadb_ledger_types::{AppSlug, OrganizationSlug, UserSlug, VaultSlug};
use inferadb_ledger_wire::services::{shared as ws, token as w};
use inferadb_ledger_wire_services::TokenServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    token::{PublicKeyInfo, TokenPair, ValidatedToken},
};

// ---------------------------------------------------------------------------
// Sessions, validation, refresh, revoke
// ---------------------------------------------------------------------------

/// Creates a user session (access + refresh token pair) via the wire
/// transport.
///
/// Mirrors [`LedgerClient::create_user_session`](crate::LedgerClient::create_user_session)
/// at the dispatch layer. `caller` is currently equal to `user` (matching
/// the tonic op); `credential_used` is always `None` here — the dedicated
/// credential-validation entry points populate it on the server side.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (rate-limit family, migration codes, all others land in `Rpc`).
pub(crate) async fn create_user_session(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
) -> Result<TokenPair> {
    let client = TokenServiceClient::new(wire_client);
    let request =
        w::CreateUserSessionRequest { user: Some(user), credential_used: None, caller: Some(user) };
    let response =
        client.create_user_session(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "CreateUserSessionResponse"))
}

/// Validates an access token and returns parsed claims via the wire
/// transport.
///
/// Mirrors [`LedgerClient::validate_token`](crate::LedgerClient::validate_token).
/// The wire `ValidateTokenResponse.claims` is a `oneof` modeled as a Rust
/// enum; an absent oneof collapses to `Internal`-coded
/// [`SdkError::Rpc`] — same shape the tonic path produces via
/// [`missing_response_field`](crate::proto_util::missing_response_field).
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn validate_token(
    wire_client: Arc<WireClient>,
    request_id: u128,
    token: String,
    expected_audience: String,
) -> Result<ValidatedToken> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::ValidateTokenRequest { token, expected_audience };
    let response =
        client.validate_token(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    domain_validated_token_from_wire(response)
        .ok_or_else(|| missing_field("claims", "ValidateTokenResponse"))
}

/// Revokes all sessions for a user via the wire transport.
///
/// Mirrors [`LedgerClient::revoke_all_user_sessions`](crate::LedgerClient::revoke_all_user_sessions).
/// Returns the number of sessions revoked.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn revoke_all_user_sessions(
    wire_client: Arc<WireClient>,
    request_id: u128,
    user: UserSlug,
) -> Result<u64> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RevokeAllUserSessionsRequest { user: Some(user), caller: Some(user) };
    let response = client
        .revoke_all_user_sessions(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(response.revoked_count)
}

/// Revokes all sessions for an app via the wire transport.
///
/// Mirrors [`LedgerClient::revoke_all_app_sessions`](crate::LedgerClient::revoke_all_app_sessions).
/// Returns the number of sessions revoked.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn revoke_all_app_sessions(
    wire_client: Arc<WireClient>,
    request_id: u128,
    app: AppSlug,
) -> Result<u64> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RevokeAllAppSessionsRequest { organization: None, app: Some(app) };
    let response = client
        .revoke_all_app_sessions(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(response.revoked_count)
}

/// Refreshes a token pair using a refresh token via the wire transport.
///
/// Mirrors [`LedgerClient::refresh_token`](crate::LedgerClient::refresh_token).
/// The old refresh token is invalidated server-side (rotate-on-use).
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn refresh_token(
    wire_client: Arc<WireClient>,
    request_id: u128,
    refresh_token: String,
) -> Result<TokenPair> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RefreshTokenRequest { refresh_token };
    let response =
        client.refresh_token(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "RefreshTokenResponse"))
}

/// Revokes a token (and its entire family) via the wire transport.
///
/// Mirrors [`LedgerClient::revoke_token`](crate::LedgerClient::revoke_token).
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn revoke_token(
    wire_client: Arc<WireClient>,
    request_id: u128,
    refresh_token: String,
) -> Result<()> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RevokeTokenRequest { refresh_token };
    client.revoke_token(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Vault-token authentication
// ---------------------------------------------------------------------------

/// Creates a vault access token for an app via the wire transport.
///
/// Mirrors [`LedgerClient::create_vault_token`](crate::LedgerClient::create_vault_token).
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn create_vault_token(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    app: AppSlug,
    vault: VaultSlug,
    scopes: Vec<String>,
) -> Result<TokenPair> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::CreateVaultTokenRequest {
        organization: Some(organization),
        app: Some(app),
        vault: Some(vault),
        scopes,
    };
    let response =
        client.create_vault_token(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "CreateVaultTokenResponse"))
}

/// Authenticates a client assertion JWT and returns a vault access token
/// via the wire transport.
///
/// Mirrors [`LedgerClient::authenticate_client_assertion`](crate::LedgerClient::authenticate_client_assertion).
/// The server verifies the JWT signature against the app's registered
/// client-assertion public keys, validates claims (iss, sub, exp, aud), and
/// issues a scoped vault token if the app is authorized.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn authenticate_client_assertion(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    vault: VaultSlug,
    assertion_jwt: String,
    scopes: Vec<String>,
) -> Result<TokenPair> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::AuthenticateClientAssertionRequest {
        organization: Some(organization),
        vault: Some(vault),
        assertion_jwt,
        scopes,
    };
    let response = client
        .authenticate_client_assertion(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    response
        .tokens
        .map(domain_token_pair_from_wire)
        .ok_or_else(|| missing_field("tokens", "AuthenticateClientAssertionResponse"))
}

// ---------------------------------------------------------------------------
// Signing-key management
// ---------------------------------------------------------------------------

/// Creates a new signing key for the given scope via the wire transport.
///
/// Mirrors [`LedgerClient::create_signing_key`](crate::LedgerClient::create_signing_key).
/// `scope` accepts `"global"` or `"organization"`; anything else collapses
/// to [`w::SigningKeyScope::Unspecified`] (server-side rejected). `organization`
/// is required for `"organization"` scope and ignored for `"global"`.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn create_signing_key(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    scope: &str,
    organization: Option<OrganizationSlug>,
) -> Result<PublicKeyInfo> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::CreateSigningKeyRequest {
        scope: domain_signing_key_scope_to_wire(scope),
        organization,
        caller: Some(caller),
    };
    let response =
        client.create_signing_key(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .key
        .map(domain_public_key_info_from_wire)
        .ok_or_else(|| missing_field("key", "CreateSigningKeyResponse"))
}

/// Rotates a signing key via the wire transport.
///
/// Mirrors [`LedgerClient::rotate_signing_key`](crate::LedgerClient::rotate_signing_key).
/// `kid` identifies the key to rotate. `grace_period_secs` controls how
/// long the old key remains valid for verification after rotation (0 =
/// immediate). `force_revoke` revokes the old key immediately regardless
/// of grace period.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn rotate_signing_key(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    kid: String,
    grace_period_secs: Option<u64>,
    force_revoke: bool,
) -> Result<PublicKeyInfo> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RotateSigningKeyRequest {
        kid,
        grace_period_secs: grace_period_secs.unwrap_or(0),
        force_revoke,
        caller: Some(caller),
    };
    let response =
        client.rotate_signing_key(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .new_key
        .map(domain_public_key_info_from_wire)
        .ok_or_else(|| missing_field("new_key", "RotateSigningKeyResponse"))
}

/// Revokes a signing key by its `kid` via the wire transport.
///
/// Mirrors [`LedgerClient::revoke_signing_key`](crate::LedgerClient::revoke_signing_key).
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn revoke_signing_key(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    kid: String,
) -> Result<()> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::RevokeSigningKeyRequest { kid, caller: Some(caller) };
    client.revoke_signing_key(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

/// Gets active public keys for token verification via the wire transport.
///
/// Mirrors [`LedgerClient::get_public_keys`](crate::LedgerClient::get_public_keys).
/// Supply `organization` to retrieve organization-scoped keys; pass `None`
/// for global keys.
///
/// # Errors
///
/// Same shape as [`create_user_session`].
pub(crate) async fn get_public_keys(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: Option<OrganizationSlug>,
) -> Result<Vec<PublicKeyInfo>> {
    let client = TokenServiceClient::new(wire_client);
    let request = w::GetPublicKeysRequest { organization, caller: Some(caller) };
    let response =
        client.get_public_keys(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.keys.into_iter().map(domain_public_key_info_from_wire).collect())
}

// ---------------------------------------------------------------------------
// Domain-mapping helpers
// ---------------------------------------------------------------------------

/// Maps a wire `TokenPair` to the domain [`TokenPair`].
///
/// `access_expires_at` / `refresh_expires_at` are UNIX nanoseconds in the
/// wire shape; the domain type uses `Option<SystemTime>` (None when the
/// timestamp is `0`). This matches the proto-side shape where an absent
/// `Timestamp` field translates to `None` upstream.
fn domain_token_pair_from_wire(p: ws::TokenPair) -> TokenPair {
    TokenPair {
        access_token: p.access_token,
        refresh_token: p.refresh_token,
        access_expires_at: unix_nanos_to_system_time(p.access_expires_at),
        refresh_expires_at: unix_nanos_to_system_time(p.refresh_expires_at),
    }
}

/// Maps a wire `ValidateTokenResponse` to the domain [`ValidatedToken`].
///
/// The wire response carries a `oneof claims` modeled as the Rust enum
/// [`ValidateTokenClaims`](w::ValidateTokenClaims); an absent oneof
/// collapses to `None` so the call site can surface a "missing field"
/// [`SdkError::Rpc`] — same fail-soft contract as the proto path.
fn domain_validated_token_from_wire(r: w::ValidateTokenResponse) -> Option<ValidatedToken> {
    match r.claims? {
        w::ValidateTokenClaims::UserSession(c) => {
            Some(ValidatedToken::UserSession { user: UserSlug::new(c.user_slug), role: c.role })
        },
        w::ValidateTokenClaims::VaultAccess(c) => Some(ValidatedToken::VaultAccess {
            organization: c.org_slug,
            app: AppSlug::new(c.app_slug),
            vault: VaultSlug::new(c.vault_slug),
            scopes: c.scopes,
        }),
    }
}

/// Maps a wire `PublicKeyInfo` to the domain [`PublicKeyInfo`].
///
/// `public_key` is a [`bytes::Bytes`] on the wire (32-byte Ed25519 key)
/// and a `Vec<u8>` on the domain type. `valid_from` / `valid_until` /
/// `created_at` are UNIX nanoseconds; `0` collapses to `None`.
fn domain_public_key_info_from_wire(k: w::PublicKeyInfo) -> PublicKeyInfo {
    PublicKeyInfo {
        kid: k.kid,
        public_key: k.public_key.to_vec(),
        status: k.status,
        valid_from: unix_nanos_to_system_time(k.valid_from),
        valid_until: unix_nanos_to_system_time(k.valid_until),
        created_at: unix_nanos_to_system_time(k.created_at),
    }
}

/// Maps a domain scope string (`"global"` / `"organization"`) into the
/// wire `SigningKeyScope` enum tag.
///
/// Anything outside the two known values collapses to `Unspecified` so
/// the server can reject it with a structured error rather than the SDK
/// panicking on an invalid input.
fn domain_signing_key_scope_to_wire(scope: &str) -> w::SigningKeyScope {
    match scope {
        "global" => w::SigningKeyScope::Global,
        "organization" => w::SigningKeyScope::Organization,
        _ => w::SigningKeyScope::Unspecified,
    }
}

/// Translates a UNIX-nanoseconds timestamp into `Option<SystemTime>`.
///
/// `0` collapses to `None` (matches the proto-side behavior, where an
/// absent `Timestamp` field deserializes as `None` upstream of
/// `proto_timestamp_to_system_time`). Overflow is also `None`.
fn unix_nanos_to_system_time(nanos: u64) -> Option<std::time::SystemTime> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos / 1_000_000_000;
    let sub_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, sub_nanos))
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
    //! Unit coverage for the wire-response → domain-type mapping. End-to-end
    //! tests against a real `WireClient` / `WireServer` are deferred to
    //! E.7 (TestCluster migration) — at that point the 12 entry points
    //! get exercised in full.

    use std::time::{Duration, UNIX_EPOCH};

    use bytes::Bytes;

    use super::*;

    fn sample_wire_token_pair() -> ws::TokenPair {
        ws::TokenPair {
            access_token: "access.jwt".to_owned(),
            refresh_token: "refresh.opaque".to_owned(),
            access_expires_at: 1_700_000_000_000_000_000,
            refresh_expires_at: 1_700_000_086_400_000_000,
        }
    }

    fn sample_wire_public_key() -> w::PublicKeyInfo {
        w::PublicKeyInfo {
            kid: "kid-1".to_owned(),
            public_key: Bytes::from(vec![0xAB; 32]),
            status: "active".to_owned(),
            valid_from: 1_700_000_000_000_000_000,
            valid_until: 1_800_000_000_000_000_000,
            created_at: 1_700_000_000_000_000_000,
        }
    }

    // -----------------------------------------------------------------
    // TokenPair mapping
    // -----------------------------------------------------------------

    #[test]
    fn token_pair_round_trips_all_fields() {
        let wire = sample_wire_token_pair();
        let domain = domain_token_pair_from_wire(wire);
        assert_eq!(domain.access_token, "access.jwt");
        assert_eq!(domain.refresh_token, "refresh.opaque");
        assert_eq!(
            domain.access_expires_at.expect("access_expires_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000)
        );
        assert_eq!(
            domain.refresh_expires_at.expect("refresh_expires_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_086_400_000_000)
        );
    }

    #[test]
    fn token_pair_zero_timestamps_default_to_none() {
        // Mirror the proto-side behavior where an absent `Timestamp` field
        // deserializes as `None`. A `0` UNIX-nanos value is the wire
        // equivalent — domain type must surface `None`, not 1970-01-01.
        let wire = ws::TokenPair {
            access_token: "a".to_owned(),
            refresh_token: "r".to_owned(),
            access_expires_at: 0,
            refresh_expires_at: 0,
        };
        let domain = domain_token_pair_from_wire(wire);
        assert!(domain.access_expires_at.is_none());
        assert!(domain.refresh_expires_at.is_none());
    }

    // -----------------------------------------------------------------
    // ValidateTokenClaims oneof variants
    // -----------------------------------------------------------------

    #[test]
    fn validate_token_user_session_variant_maps_correctly() {
        let resp = w::ValidateTokenResponse {
            subject: "user:42".to_owned(),
            token_type: "user_session".to_owned(),
            expires_at: 1_700_000_000_000_000_000,
            claims: Some(w::ValidateTokenClaims::UserSession(w::UserSessionClaims {
                user_slug: 42,
                role: "admin".to_owned(),
            })),
        };
        let domain = domain_validated_token_from_wire(resp).expect("claims present");
        match domain {
            ValidatedToken::UserSession { user, role } => {
                assert_eq!(user, UserSlug::new(42));
                assert_eq!(role, "admin");
            },
            other => panic!("expected UserSession, got {other:?}"),
        }
    }

    #[test]
    fn validate_token_vault_access_variant_maps_correctly() {
        let resp = w::ValidateTokenResponse {
            subject: "app:9".to_owned(),
            token_type: "vault_access".to_owned(),
            expires_at: 1_700_000_000_000_000_000,
            claims: Some(w::ValidateTokenClaims::VaultAccess(w::VaultAccessClaims {
                org_slug: 7,
                app_slug: 9,
                vault_slug: 11,
                scopes: vec!["read".to_owned(), "write".to_owned()],
            })),
        };
        let domain = domain_validated_token_from_wire(resp).expect("claims present");
        match domain {
            ValidatedToken::VaultAccess { organization, app, vault, scopes } => {
                assert_eq!(organization, 7);
                assert_eq!(app, AppSlug::new(9));
                assert_eq!(vault, VaultSlug::new(11));
                assert_eq!(scopes, vec!["read".to_owned(), "write".to_owned()]);
            },
            other => panic!("expected VaultAccess, got {other:?}"),
        }
    }

    #[test]
    fn validate_token_missing_claims_oneof_returns_none() {
        // When the server response carries no `claims` oneof, the helper
        // returns `None` so the call site can surface a missing-field
        // `SdkError::Rpc` — same fail-soft contract as the proto path.
        let resp = w::ValidateTokenResponse {
            subject: String::new(),
            token_type: String::new(),
            expires_at: 0,
            claims: None,
        };
        assert!(domain_validated_token_from_wire(resp).is_none());
    }

    // -----------------------------------------------------------------
    // PublicKeyInfo mapping
    // -----------------------------------------------------------------

    #[test]
    fn public_key_info_round_trips_all_fields() {
        let wire = sample_wire_public_key();
        let domain = domain_public_key_info_from_wire(wire);
        assert_eq!(domain.kid, "kid-1");
        assert_eq!(domain.public_key, vec![0xAB; 32]);
        assert_eq!(domain.status, "active");
        assert_eq!(
            domain.valid_from.expect("valid_from present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000)
        );
        assert_eq!(
            domain.valid_until.expect("valid_until present"),
            UNIX_EPOCH + Duration::from_nanos(1_800_000_000_000_000_000)
        );
        assert_eq!(
            domain.created_at.expect("created_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000)
        );
    }

    #[test]
    fn public_key_info_zero_timestamps_default_to_none() {
        let wire = w::PublicKeyInfo {
            kid: "kid-2".to_owned(),
            public_key: Bytes::from(vec![0u8; 32]),
            status: "rotated".to_owned(),
            valid_from: 0,
            valid_until: 0,
            created_at: 0,
        };
        let domain = domain_public_key_info_from_wire(wire);
        assert!(domain.valid_from.is_none());
        assert!(domain.valid_until.is_none());
        assert!(domain.created_at.is_none());
    }

    #[test]
    fn public_key_info_byte_field_preserves_full_32_bytes() {
        // Bytes -> Vec<u8> conversion must not truncate. Build a key with
        // a non-uniform pattern so a partial copy would compare unequal.
        let mut key_bytes = Vec::with_capacity(32);
        for i in 0..32u8 {
            key_bytes.push(i);
        }
        let wire = w::PublicKeyInfo {
            kid: "kid-3".to_owned(),
            public_key: Bytes::from(key_bytes.clone()),
            status: "active".to_owned(),
            valid_from: 0,
            valid_until: 0,
            created_at: 0,
        };
        let domain = domain_public_key_info_from_wire(wire);
        assert_eq!(domain.public_key.len(), 32);
        assert_eq!(domain.public_key, key_bytes);
    }

    // -----------------------------------------------------------------
    // SigningKeyScope enum tag
    // -----------------------------------------------------------------

    #[test]
    fn signing_key_scope_repr_values_match_proto() {
        // Lock in the integer tags so a cross-protocol audit (proto vs
        // wire) can grep for the same values. The wire enum is
        // #[repr(i32)] = 0/1/2 by design, mirroring the proto enum.
        assert_eq!(w::SigningKeyScope::Unspecified as i32, 0);
        assert_eq!(w::SigningKeyScope::Global as i32, 1);
        assert_eq!(w::SigningKeyScope::Organization as i32, 2);
    }

    #[test]
    fn signing_key_scope_global_string_maps_to_global_tag() {
        assert_eq!(domain_signing_key_scope_to_wire("global"), w::SigningKeyScope::Global);
    }

    #[test]
    fn signing_key_scope_organization_string_maps_to_organization_tag() {
        assert_eq!(
            domain_signing_key_scope_to_wire("organization"),
            w::SigningKeyScope::Organization
        );
    }

    #[test]
    fn signing_key_scope_unknown_string_collapses_to_unspecified() {
        // Mirror the proto-side fallback — anything outside the two known
        // values lands in `Unspecified` so the server can reject with a
        // structured error rather than the SDK panicking on bad input.
        assert_eq!(domain_signing_key_scope_to_wire("vault"), w::SigningKeyScope::Unspecified);
        assert_eq!(domain_signing_key_scope_to_wire(""), w::SigningKeyScope::Unspecified);
        assert_eq!(domain_signing_key_scope_to_wire("GLOBAL"), w::SigningKeyScope::Unspecified);
    }

    // -----------------------------------------------------------------
    // unix_nanos_to_system_time edge cases
    // -----------------------------------------------------------------

    #[test]
    fn unix_nanos_to_system_time_zero_is_none() {
        assert!(unix_nanos_to_system_time(0).is_none());
    }

    #[test]
    fn unix_nanos_to_system_time_handles_subsecond_precision() {
        let t = unix_nanos_to_system_time(1_500_000_000).expect("translates");
        assert_eq!(t, UNIX_EPOCH + Duration::new(1, 500_000_000));
    }

    #[test]
    fn unix_nanos_to_system_time_handles_recent_realistic_value() {
        let nanos: u64 = 1_700_123_456_789_000_000;
        let t = unix_nanos_to_system_time(nanos).expect("translates");
        let dur = t.duration_since(UNIX_EPOCH).expect("post-epoch");
        assert_eq!(u64::try_from(dur.as_nanos()).expect("fits in u64"), nanos);
    }

    // -----------------------------------------------------------------
    // missing_field shape
    // -----------------------------------------------------------------

    #[test]
    fn missing_field_produces_internal_rpc_error() {
        let err = missing_field("tokens", "CreateUserSessionResponse");
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(message.contains("tokens"));
                assert!(message.contains("CreateUserSessionResponse"));
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }
}
