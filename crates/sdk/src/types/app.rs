//! Application domain types: apps, credentials, assertions, vault connections.

use inferadb_ledger_types::{AppSlug, ClientAssertionId as DomainClientAssertionId, VaultSlug};

/// SDK representation of a client application.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppInfo {
    /// External Snowflake slug.
    pub slug: AppSlug,
    /// Human-readable name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Whether the app is enabled.
    pub enabled: bool,
    /// Credentials configuration (absent in list responses).
    pub credentials: Option<AppCredentialsInfo>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
    /// Last update timestamp.
    pub updated_at: Option<std::time::SystemTime>,
}

/// Credential configuration for an app.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppCredentialsInfo {
    /// Client secret credential.
    pub client_secret_enabled: bool,
    /// mTLS CA credential.
    pub mtls_ca_enabled: bool,
    /// mTLS self-signed credential.
    pub mtls_self_signed_enabled: bool,
    /// Client assertion (private key JWT) credential.
    pub client_assertion_enabled: bool,
}

/// A client assertion entry (public metadata only — private key is never returned after creation).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppClientAssertionInfo {
    /// Server-assigned assertion ID.
    pub id: DomainClientAssertionId,
    /// User-provided name.
    pub name: String,
    /// Whether this assertion is individually enabled.
    pub enabled: bool,
    /// Expiration timestamp.
    pub expires_at: Option<std::time::SystemTime>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
}

/// A vault connection for an app.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppVaultConnectionInfo {
    /// Vault slug (external identifier).
    pub vault_slug: VaultSlug,
    /// Allowed scopes for this vault connection.
    pub allowed_scopes: Vec<String>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
}

/// Result of creating a client assertion — includes the private key PEM (returned only once).
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CreateAppClientAssertionResult {
    /// The created assertion metadata.
    pub assertion: AppClientAssertionInfo,
    /// Private key PEM (Ed25519). Only returned on creation.
    pub private_key_pem: String,
}

impl std::fmt::Debug for CreateAppClientAssertionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateAppClientAssertionResult")
            .field("assertion", &self.assertion)
            .field("private_key_pem", &"<redacted>")
            .finish()
    }
}

/// Result of getting a client secret's status.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppClientSecretStatus {
    /// Whether the client secret credential is enabled.
    pub enabled: bool,
    /// Whether a secret has been generated.
    pub has_secret: bool,
}

/// Credential type for enable/disable operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AppCredentialType {
    /// Client secret (shared secret).
    ClientSecret,
    /// mTLS with CA-signed certificate.
    MtlsCa,
    /// mTLS with self-signed certificate.
    MtlsSelfSigned,
    /// Client assertion (private key JWT).
    ClientAssertion,
}
