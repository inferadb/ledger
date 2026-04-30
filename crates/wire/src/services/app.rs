//! Wire types for `AppService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{AppSlug, ClientAssertionId, OrganizationSlug, UserSlug, VaultSlug};

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AppCredentialType {
    Unspecified = 0,
    ClientSecret = 1,
    MtlsCa = 2,
    MtlsSelfSigned = 3,
    ClientAssertion = 4,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppInfo {
    pub slug: Option<AppSlug>,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub credentials: Option<AppCredentialsInfo>,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppCredentialsInfo {
    pub client_secret_enabled: bool,
    pub mtls_ca_enabled: bool,
    pub mtls_self_signed_enabled: bool,
    pub client_assertion_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppClientAssertionInfo {
    pub id: Option<ClientAssertionId>,
    pub name: String,
    pub enabled: bool,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppVaultConnectionInfo {
    pub vault: Option<VaultSlug>,
    pub allowed_scopes: Vec<String>,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

// ---------------------------------------------------------------------------
// App CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateAppRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub name: String,
    pub description: Option<String>,
    pub slug: Option<AppSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateAppResponse {
    pub app: Option<AppInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppResponse {
    pub app: Option<AppInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppsRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppsResponse {
    pub apps: Vec<AppInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateAppRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateAppResponse {
    pub app: Option<AppInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteAppRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteAppResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppEnabledRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppEnabledResponse {
    pub app: Option<AppInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppCredentialEnabledRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub credential_type: AppCredentialType,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppCredentialEnabledResponse {
    pub app: Option<AppInfo>,
}

// ---------------------------------------------------------------------------
// Client secret
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppClientSecretRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppClientSecretResponse {
    pub enabled: bool,
    pub has_secret: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RotateAppClientSecretRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    /// 16-byte UUID for idempotent retry.
    pub idempotency_key: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateAppClientSecretResponse {
    pub secret: String,
}

// ---------------------------------------------------------------------------
// Client assertion
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppClientAssertionsRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppClientAssertionsResponse {
    pub assertions: Vec<AppClientAssertionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppClientAssertionRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub assertion: Option<ClientAssertionId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetAppClientAssertionResponse {
    pub assertion: Option<AppClientAssertionInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateAppClientAssertionRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub name: String,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    /// 16-byte UUID for idempotent retry.
    pub idempotency_key: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateAppClientAssertionResponse {
    pub assertion: Option<AppClientAssertionInfo>,
    pub private_key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteAppClientAssertionRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub assertion: Option<ClientAssertionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteAppClientAssertionResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppClientAssertionEnabledRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub assertion: Option<ClientAssertionId>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetAppClientAssertionEnabledResponse {}

// ---------------------------------------------------------------------------
// Vault connections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppVaultsRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListAppVaultsResponse {
    pub vaults: Vec<AppVaultConnectionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddAppVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub vault: Option<VaultSlug>,
    pub allowed_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddAppVaultResponse {
    pub vault: Option<AppVaultConnectionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateAppVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub vault: Option<VaultSlug>,
    pub allowed_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateAppVaultResponse {
    pub vault: Option<AppVaultConnectionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveAppVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub caller: Option<UserSlug>,
    pub app: Option<AppSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoveAppVaultResponse {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn create_app_request_roundtrips_via_postcard() {
        let req = CreateAppRequest {
            organization: Some(OrganizationSlug::new(1)),
            caller: Some(UserSlug::new(42)),
            name: "my-service".to_owned(),
            description: Some("internal microservice".to_owned()),
            slug: Some(AppSlug::new(0xABCDEF)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: CreateAppRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
