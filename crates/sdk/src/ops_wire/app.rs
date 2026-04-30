//! Wire-based AppService ops.
//!
//! Mirrors [`super::super::ops::app`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`AppServiceClient`](inferadb_ledger_wire_services::AppServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! wire-response → domain-type mapping (and the domain-input → wire-request
//! direction for enum tags / timestamps) in isolation. F.1.e flipped the
//! connection pool so each entry point is now reached from the
//! corresponding [`super::super::ops::app`] dispatch arm.

use std::sync::Arc;

use inferadb_ledger_types::{
    AppSlug, ClientAssertionId as DomainClientAssertionId, OrganizationSlug, UserSlug, VaultSlug,
};
use inferadb_ledger_wire::services::app as w;
use inferadb_ledger_wire_services::AppServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    types::app::{
        AppClientAssertionInfo, AppClientSecretStatus, AppCredentialType, AppCredentialsInfo,
        AppInfo, AppVaultConnectionInfo, CreateAppClientAssertionResult,
    },
};

// ---------------------------------------------------------------------------
// App CRUD
// ---------------------------------------------------------------------------

/// Creates a new app via the wire transport.
///
/// Mirrors [`LedgerClient::create_app`](crate::LedgerClient::create_app) at
/// the dispatch layer. The caller owns slug generation (so retries reuse the
/// same slug for idempotent apply); this fn just forwards.
///
/// # Errors
///
/// Same shape as the tonic path — transport / decode failures land in
/// [`SdkError::Connection`]; server-returned [`WireError`]s flow through
/// [`from_wire_error_with_correlation`].
pub(crate) async fn create_app(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    name: String,
    description: Option<String>,
    app_slug: AppSlug,
) -> Result<AppInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::CreateAppRequest {
        organization: Some(organization),
        caller: Some(user),
        name,
        description,
        slug: Some(app_slug),
    };
    let response = client.create_app(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .app
        .map(domain_app_info_from_wire)
        .ok_or_else(|| missing_field("app", "CreateAppResponse"))
}

/// Gets an app by slug via the wire transport.
pub(crate) async fn get_app(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
) -> Result<AppInfo> {
    let client = AppServiceClient::new(wire_client);
    let request =
        w::GetAppRequest { organization: Some(organization), caller: Some(user), app: Some(app) };
    let response = client.get_app(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .app
        .map(domain_app_info_from_wire)
        .ok_or_else(|| missing_field("app", "GetAppResponse"))
}

/// Lists all apps in an organization via the wire transport.
pub(crate) async fn list_apps(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
) -> Result<Vec<AppInfo>> {
    let client = AppServiceClient::new(wire_client);
    let request = w::ListAppsRequest { organization: Some(organization), caller: Some(user) };
    let response = client.list_apps(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.apps.into_iter().map(domain_app_info_from_wire).collect())
}

/// Updates an app's name and/or description via the wire transport.
pub(crate) async fn update_app(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    name: Option<String>,
    description: Option<String>,
) -> Result<AppInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::UpdateAppRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        name,
        description,
    };
    let response = client.update_app(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .app
        .map(domain_app_info_from_wire)
        .ok_or_else(|| missing_field("app", "UpdateAppResponse"))
}

/// Deletes an app via the wire transport.
pub(crate) async fn delete_app(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
) -> Result<()> {
    let client = AppServiceClient::new(wire_client);
    let request = w::DeleteAppRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
    };
    client.delete_app(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

/// Enables / disables an app via the wire transport.
pub(crate) async fn set_app_enabled(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    enabled: bool,
) -> Result<AppInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::SetAppEnabledRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        enabled,
    };
    let response =
        client.set_app_enabled(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .app
        .map(domain_app_info_from_wire)
        .ok_or_else(|| missing_field("app", "SetAppEnabledResponse"))
}

// ---------------------------------------------------------------------------
// App credentials
// ---------------------------------------------------------------------------

/// Toggles an individual credential type on an app via the wire transport.
pub(crate) async fn set_app_credential_enabled(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    credential_type: AppCredentialType,
    enabled: bool,
) -> Result<AppInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::SetAppCredentialEnabledRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        credential_type: domain_credential_type_to_wire(credential_type),
        enabled,
    };
    let response = client
        .set_app_credential_enabled(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    response
        .app
        .map(domain_app_info_from_wire)
        .ok_or_else(|| missing_field("app", "SetAppCredentialEnabledResponse"))
}

/// Reads the client-secret status for an app via the wire transport.
pub(crate) async fn get_app_client_secret(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
) -> Result<AppClientSecretStatus> {
    let client = AppServiceClient::new(wire_client);
    let request = w::GetAppClientSecretRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
    };
    let response =
        client.get_app_client_secret(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(AppClientSecretStatus { enabled: response.enabled, has_secret: response.has_secret })
}

/// Rotates the app's client secret via the wire transport.
///
/// Caller supplies the 16-byte idempotency key so retries return the same
/// secret instead of generating a new one.
pub(crate) async fn rotate_app_client_secret(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    idempotency_key: [u8; 16],
) -> Result<String> {
    let client = AppServiceClient::new(wire_client);
    let request = w::RotateAppClientSecretRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        idempotency_key: bytes::Bytes::copy_from_slice(&idempotency_key),
    };
    let response = client
        .rotate_app_client_secret(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(response.secret)
}

// ---------------------------------------------------------------------------
// Client assertions
// ---------------------------------------------------------------------------

/// Lists client assertions for an app via the wire transport.
pub(crate) async fn list_app_client_assertions(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
) -> Result<Vec<AppClientAssertionInfo>> {
    let client = AppServiceClient::new(wire_client);
    let request = w::ListAppClientAssertionsRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
    };
    let response = client
        .list_app_client_assertions(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(response.assertions.into_iter().map(domain_assertion_info_from_wire).collect())
}

/// Fetches a single client assertion by ID via the wire transport.
pub(crate) async fn get_app_client_assertion(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    assertion: DomainClientAssertionId,
) -> Result<AppClientAssertionInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::GetAppClientAssertionRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        assertion: Some(assertion),
    };
    let response = client
        .get_app_client_assertion(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    response
        .assertion
        .map(domain_assertion_info_from_wire)
        .ok_or_else(|| missing_field("assertion", "GetAppClientAssertionResponse"))
}

/// Creates a client assertion for an app via the wire transport.
///
/// `expires_at` is supplied as UNIX nanoseconds — translation from
/// `SystemTime` happens at the [`LedgerClient`](crate::LedgerClient) call
/// site so this fn stays straight-through.
pub(crate) async fn create_app_client_assertion(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    name: String,
    expires_at_unix_nanos: u64,
    idempotency_key: [u8; 16],
) -> Result<CreateAppClientAssertionResult> {
    let client = AppServiceClient::new(wire_client);
    let request = w::CreateAppClientAssertionRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        name,
        expires_at: expires_at_unix_nanos,
        idempotency_key: bytes::Bytes::copy_from_slice(&idempotency_key),
    };
    let response = client
        .create_app_client_assertion(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    let assertion = response
        .assertion
        .map(domain_assertion_info_from_wire)
        .ok_or_else(|| missing_field("assertion", "CreateAppClientAssertionResponse"))?;
    Ok(CreateAppClientAssertionResult { assertion, private_key_pem: response.private_key_pem })
}

/// Deletes a client assertion via the wire transport.
pub(crate) async fn delete_app_client_assertion(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    assertion: DomainClientAssertionId,
) -> Result<()> {
    let client = AppServiceClient::new(wire_client);
    let request = w::DeleteAppClientAssertionRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        assertion: Some(assertion),
    };
    client
        .delete_app_client_assertion(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

/// Toggles a single client assertion's enabled flag via the wire transport.
pub(crate) async fn set_app_client_assertion_enabled(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    assertion: DomainClientAssertionId,
    enabled: bool,
) -> Result<()> {
    let client = AppServiceClient::new(wire_client);
    let request = w::SetAppClientAssertionEnabledRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        assertion: Some(assertion),
        enabled,
    };
    client
        .set_app_client_assertion_enabled(request, request_id)
        .await
        .map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Vault connections
// ---------------------------------------------------------------------------

/// Lists vault connections for an app via the wire transport.
pub(crate) async fn list_app_vaults(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
) -> Result<Vec<AppVaultConnectionInfo>> {
    let client = AppServiceClient::new(wire_client);
    let request = w::ListAppVaultsRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
    };
    let response =
        client.list_app_vaults(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.vaults.into_iter().map(domain_vault_connection_from_wire).collect())
}

/// Adds a vault connection to an app via the wire transport.
pub(crate) async fn add_app_vault(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    vault: VaultSlug,
    allowed_scopes: Vec<String>,
) -> Result<AppVaultConnectionInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::AddAppVaultRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        vault: Some(vault),
        allowed_scopes,
    };
    let response =
        client.add_app_vault(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .vault
        .map(domain_vault_connection_from_wire)
        .ok_or_else(|| missing_field("vault", "AddAppVaultResponse"))
}

/// Updates the allowed scopes for an app's vault connection via the wire transport.
pub(crate) async fn update_app_vault(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    vault: VaultSlug,
    allowed_scopes: Vec<String>,
) -> Result<AppVaultConnectionInfo> {
    let client = AppServiceClient::new(wire_client);
    let request = w::UpdateAppVaultRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        vault: Some(vault),
        allowed_scopes,
    };
    let response =
        client.update_app_vault(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    response
        .vault
        .map(domain_vault_connection_from_wire)
        .ok_or_else(|| missing_field("vault", "UpdateAppVaultResponse"))
}

/// Removes a vault connection from an app via the wire transport.
pub(crate) async fn remove_app_vault(
    wire_client: Arc<WireClient>,
    request_id: u128,
    organization: OrganizationSlug,
    user: UserSlug,
    app: AppSlug,
    vault: VaultSlug,
) -> Result<()> {
    let client = AppServiceClient::new(wire_client);
    let request = w::RemoveAppVaultRequest {
        organization: Some(organization),
        caller: Some(user),
        app: Some(app),
        vault: Some(vault),
    };
    client.remove_app_vault(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire → domain mappers
// ---------------------------------------------------------------------------

/// Maps a wire `AppInfo` into the SDK domain [`AppInfo`].
///
/// Mirrors `app_info_from_proto`: an absent `slug` collapses to
/// `AppSlug::new(0)` (matching the proto helper's behavior), and the
/// `created_at` / `updated_at` UNIX-nanos values translate to
/// `Option<SystemTime>` with `0` meaning "unset".
fn domain_app_info_from_wire(w: w::AppInfo) -> AppInfo {
    AppInfo {
        slug: w.slug.unwrap_or_else(|| AppSlug::new(0)),
        name: w.name,
        description: w.description,
        enabled: w.enabled,
        credentials: w.credentials.map(domain_credentials_from_wire),
        created_at: unix_nanos_to_system_time(w.created_at),
        updated_at: unix_nanos_to_system_time(w.updated_at),
    }
}

/// Maps a wire `AppCredentialsInfo` into the SDK domain
/// [`AppCredentialsInfo`].
fn domain_credentials_from_wire(w: w::AppCredentialsInfo) -> AppCredentialsInfo {
    AppCredentialsInfo {
        client_secret_enabled: w.client_secret_enabled,
        mtls_ca_enabled: w.mtls_ca_enabled,
        mtls_self_signed_enabled: w.mtls_self_signed_enabled,
        client_assertion_enabled: w.client_assertion_enabled,
    }
}

/// Maps a wire `AppClientAssertionInfo` into the SDK domain
/// [`AppClientAssertionInfo`].
fn domain_assertion_info_from_wire(w: w::AppClientAssertionInfo) -> AppClientAssertionInfo {
    AppClientAssertionInfo {
        id: w.id.unwrap_or_else(|| DomainClientAssertionId::new(0)),
        name: w.name,
        enabled: w.enabled,
        expires_at: unix_nanos_to_system_time(w.expires_at),
        created_at: unix_nanos_to_system_time(w.created_at),
    }
}

/// Maps a wire `AppVaultConnectionInfo` into the SDK domain
/// [`AppVaultConnectionInfo`].
///
/// The wire type carries an `updated_at` field that the domain type does not
/// expose; it is dropped here, matching the proto-side conversion.
fn domain_vault_connection_from_wire(w: w::AppVaultConnectionInfo) -> AppVaultConnectionInfo {
    AppVaultConnectionInfo {
        vault_slug: w.vault.unwrap_or_else(|| VaultSlug::new(0)),
        allowed_scopes: w.allowed_scopes,
        created_at: unix_nanos_to_system_time(w.created_at),
    }
}

/// Maps the SDK domain [`AppCredentialType`] into the wire enum.
///
/// Only the four "real" variants are reachable — the wire enum's
/// `Unspecified` is the proto unspecified placeholder and is never produced
/// by the SDK.
fn domain_credential_type_to_wire(t: AppCredentialType) -> w::AppCredentialType {
    match t {
        AppCredentialType::ClientSecret => w::AppCredentialType::ClientSecret,
        AppCredentialType::MtlsCa => w::AppCredentialType::MtlsCa,
        AppCredentialType::MtlsSelfSigned => w::AppCredentialType::MtlsSelfSigned,
        AppCredentialType::ClientAssertion => w::AppCredentialType::ClientAssertion,
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
    //! Unit coverage for the wire-response → domain-type mapping and the
    //! domain enum → wire enum tag mapping. End-to-end tests against a real
    //! `WireClient` / `WireServer` are deferred to E.7 (TestCluster
    //! migration); the per-RPC entry points themselves get exercised in
    //! full at that point.

    use std::time::{Duration, UNIX_EPOCH};

    use inferadb_ledger_types::{AppSlug, ClientAssertionId, VaultSlug};

    use super::*;

    // -----------------------------------------------------------------------
    // Domain mapping round-trips
    // -----------------------------------------------------------------------

    #[test]
    fn domain_app_info_round_trip_with_all_fields() {
        let wire = w::AppInfo {
            slug: Some(AppSlug::new(0xAABBCCDD)),
            name: "billing".to_owned(),
            description: Some("internal billing pipeline".to_owned()),
            enabled: true,
            credentials: Some(w::AppCredentialsInfo {
                client_secret_enabled: true,
                mtls_ca_enabled: false,
                mtls_self_signed_enabled: false,
                client_assertion_enabled: true,
            }),
            created_at: 1_700_000_000_000_000_000,
            updated_at: 1_700_000_001_500_000_000,
        };
        let domain = domain_app_info_from_wire(wire);
        assert_eq!(domain.slug.value(), 0xAABBCCDD);
        assert_eq!(domain.name, "billing");
        assert_eq!(domain.description.as_deref(), Some("internal billing pipeline"));
        assert!(domain.enabled);
        let creds = domain.credentials.expect("credentials present");
        assert!(creds.client_secret_enabled);
        assert!(!creds.mtls_ca_enabled);
        assert!(!creds.mtls_self_signed_enabled);
        assert!(creds.client_assertion_enabled);
        assert_eq!(
            domain.created_at.expect("created_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000),
        );
        assert_eq!(
            domain.updated_at.expect("updated_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_001_500_000_000),
        );
    }

    #[test]
    fn domain_app_info_handles_absent_optional_fields() {
        // Server is permitted to omit slug, description, credentials, and
        // both timestamps — mirror the proto helper's defaults exactly so a
        // stray `None` doesn't bubble into the SDK as a panic.
        let wire = w::AppInfo {
            slug: None,
            name: "pending".to_owned(),
            description: None,
            enabled: false,
            credentials: None,
            created_at: 0,
            updated_at: 0,
        };
        let domain = domain_app_info_from_wire(wire);
        assert_eq!(domain.slug.value(), 0);
        assert_eq!(domain.description, None);
        assert!(domain.credentials.is_none());
        assert!(domain.created_at.is_none());
        assert!(domain.updated_at.is_none());
    }

    #[test]
    fn domain_assertion_info_round_trip() {
        let wire = w::AppClientAssertionInfo {
            id: Some(ClientAssertionId::new(99)),
            name: "ci-key".to_owned(),
            enabled: true,
            expires_at: 1_800_000_000_000_000_000,
            created_at: 1_700_000_000_000_000_000,
        };
        let domain = domain_assertion_info_from_wire(wire);
        assert_eq!(domain.id.value(), 99);
        assert_eq!(domain.name, "ci-key");
        assert!(domain.enabled);
        assert_eq!(
            domain.expires_at.expect("expires_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_800_000_000_000_000_000),
        );
        assert_eq!(
            domain.created_at.expect("created_at present"),
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_000),
        );
    }

    #[test]
    fn domain_assertion_info_handles_absent_id_and_zero_timestamps() {
        let wire = w::AppClientAssertionInfo {
            id: None,
            name: String::new(),
            enabled: false,
            expires_at: 0,
            created_at: 0,
        };
        let domain = domain_assertion_info_from_wire(wire);
        assert_eq!(domain.id.value(), 0);
        assert!(domain.expires_at.is_none());
        assert!(domain.created_at.is_none());
    }

    #[test]
    fn domain_vault_connection_round_trip() {
        let wire = w::AppVaultConnectionInfo {
            vault: Some(VaultSlug::new(0xDEADBEEF)),
            allowed_scopes: vec!["read".to_owned(), "write".to_owned()],
            created_at: 1_700_000_000_000_000_000,
            // Domain type intentionally drops updated_at — assert the
            // mapper ignores it cleanly rather than panicking on present /
            // absent.
            updated_at: 1_700_000_500_000_000_000,
        };
        let domain = domain_vault_connection_from_wire(wire);
        assert_eq!(domain.vault_slug.value(), 0xDEADBEEF);
        assert_eq!(domain.allowed_scopes, vec!["read".to_owned(), "write".to_owned()]);
        assert!(domain.created_at.is_some());
    }

    #[test]
    fn domain_vault_connection_handles_absent_vault() {
        let wire = w::AppVaultConnectionInfo {
            vault: None,
            allowed_scopes: Vec::new(),
            created_at: 0,
            updated_at: 0,
        };
        let domain = domain_vault_connection_from_wire(wire);
        assert_eq!(domain.vault_slug.value(), 0);
        assert!(domain.allowed_scopes.is_empty());
        assert!(domain.created_at.is_none());
    }

    // -----------------------------------------------------------------------
    // Enum mapping
    // -----------------------------------------------------------------------

    #[test]
    fn credential_type_each_variant_maps_to_wire() {
        // Pin the per-variant mapping so a future wire-side rename or
        // reorder doesn't silently mismatch the i32 tag the server sees.
        assert_eq!(
            domain_credential_type_to_wire(AppCredentialType::ClientSecret),
            w::AppCredentialType::ClientSecret
        );
        assert_eq!(
            domain_credential_type_to_wire(AppCredentialType::MtlsCa),
            w::AppCredentialType::MtlsCa
        );
        assert_eq!(
            domain_credential_type_to_wire(AppCredentialType::MtlsSelfSigned),
            w::AppCredentialType::MtlsSelfSigned
        );
        assert_eq!(
            domain_credential_type_to_wire(AppCredentialType::ClientAssertion),
            w::AppCredentialType::ClientAssertion
        );
    }

    #[test]
    fn credential_type_wire_tag_values_match_proto_repr() {
        // Tags must line up with the proto enum (`AppCredentialType::ClientSecret == 1`,
        // ...) so the postcard-encoded `i32` round-trips identically through
        // both transports.
        assert_eq!(w::AppCredentialType::Unspecified as i32, 0);
        assert_eq!(w::AppCredentialType::ClientSecret as i32, 1);
        assert_eq!(w::AppCredentialType::MtlsCa as i32, 2);
        assert_eq!(w::AppCredentialType::MtlsSelfSigned as i32, 3);
        assert_eq!(w::AppCredentialType::ClientAssertion as i32, 4);
    }

    // -----------------------------------------------------------------------
    // unix_nanos_to_system_time edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn unix_nanos_to_system_time_zero_is_none() {
        assert!(unix_nanos_to_system_time(0).is_none());
    }

    #[test]
    fn unix_nanos_to_system_time_handles_subsecond_precision() {
        // 1.5s past the epoch — the conversion must split secs and
        // sub-nanos correctly, not round to whole seconds.
        let t = unix_nanos_to_system_time(1_500_000_000).expect("translates");
        assert_eq!(t, UNIX_EPOCH + Duration::new(1, 500_000_000));
    }

    #[test]
    fn unix_nanos_to_system_time_handles_recent_realistic_value() {
        let nanos: u64 = 1_700_123_456_789_000_000;
        let t = unix_nanos_to_system_time(nanos).expect("translates");
        let dur = t.duration_since(UNIX_EPOCH).expect("post-epoch");
        assert_eq!(dur.as_nanos() as u64, nanos);
    }

    // -----------------------------------------------------------------------
    // missing_field
    // -----------------------------------------------------------------------

    #[test]
    fn missing_field_produces_internal_rpc_error() {
        let err = missing_field("app", "GetAppResponse");
        match err {
            SdkError::Rpc { code, message, request_id, trace_id, error_details } => {
                assert_eq!(code, inferadb_ledger_wire::ErrorCode::Internal);
                assert!(message.contains("app"));
                assert!(message.contains("GetAppResponse"));
                assert!(request_id.is_none());
                assert!(trace_id.is_none());
                assert!(error_details.is_none());
            },
            other => panic!("expected Rpc, got {other:?}"),
        }
    }
}
