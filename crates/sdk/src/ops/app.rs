//! App CRUD, credentials, client assertions, and vault connection operations.

use inferadb_ledger_types::{
    AppSlug, ClientAssertionId as DomainClientAssertionId, OrganizationSlug, UserSlug, VaultSlug,
};

use crate::{
    LedgerClient,
    error::Result,
    types::app::{
        AppClientAssertionInfo, AppClientSecretStatus, AppCredentialType, AppInfo,
        AppVaultConnectionInfo, CreateAppClientAssertionResult,
    },
};

impl LedgerClient {
    // =========================================================================
    // App CRUD
    // =========================================================================

    /// Creates a new app in an organization.
    pub async fn create_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<AppInfo> {
        let name = name.into();
        // Generate the app slug once, outside the retry loop. Every retry
        // for this logical call reuses it so the per-org apply
        // idempotency-by-slug path returns the same AppId instead of
        // creating a duplicate directory entry on
        // response-lost-in-flight.
        let app_slug = inferadb_ledger_types::snowflake::generate_app_slug().map_err(|e| {
            crate::error::SdkError::Config { message: format!("generate app slug: {e}") }
        })?;
        let pool = self.pool.clone();
        self.call_with_retry("create_app", || {
            let pool = pool.clone();
            let description = description.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::create_app(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    name,
                    description,
                    app_slug,
                )
                .await
            }
        })
        .await
    }

    /// Gets an app by slug.
    pub async fn get_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_app", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::get_app(wire_client, request_id, organization, user, app)
                    .await
            }
        })
        .await
    }

    /// Lists all apps in an organization.
    pub async fn list_apps(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
    ) -> Result<Vec<AppInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("list_apps", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::list_apps(wire_client, request_id, organization, user).await
            }
        })
        .await
    }

    /// Updates an app's name and/or description.
    pub async fn update_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        name: Option<String>,
        description: Option<String>,
    ) -> Result<AppInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_app", || {
            let pool = pool.clone();
            let description = description.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::update_app(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    name,
                    description,
                )
                .await
            }
        })
        .await
    }

    /// Deletes an app.
    pub async fn delete_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_app", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::delete_app(wire_client, request_id, organization, user, app)
                    .await
            }
        })
        .await
    }

    /// Enables an app.
    pub async fn enable_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.set_app_enabled(organization, user, app, true).await
    }

    /// Disables an app.
    pub async fn disable_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.set_app_enabled(organization, user, app, false).await
    }

    async fn set_app_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        enabled: bool,
    ) -> Result<AppInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("set_app_enabled", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::set_app_enabled(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    enabled,
                )
                .await
            }
        })
        .await
    }

    // =========================================================================
    // App Credentials
    // =========================================================================

    /// Enables or disables a credential type for an app.
    pub async fn set_app_credential_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        credential_type: AppCredentialType,
        enabled: bool,
    ) -> Result<AppInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("set_app_credential_enabled", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::set_app_credential_enabled(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    credential_type,
                    enabled,
                )
                .await
            }
        })
        .await
    }

    /// Gets the client secret status for an app (enabled flag and whether a secret exists).
    pub async fn get_app_client_secret(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppClientSecretStatus> {
        let pool = self.pool.clone();
        self.call_with_retry("get_app_client_secret", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::get_app_client_secret(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                )
                .await
            }
        })
        .await
    }

    /// Rotates the client secret for an app. Returns the new plaintext secret (base64-encoded).
    ///
    /// An idempotency key is generated per call so retries (including automatic
    /// retries) return the same secret instead of creating a new one.
    pub async fn rotate_app_client_secret(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<String> {
        let pool = self.pool.clone();
        let idempotency_key: [u8; 16] = rand::random();

        self.call_with_retry("rotate_app_client_secret", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::rotate_app_client_secret(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    idempotency_key,
                )
                .await
            }
        })
        .await
    }

    // =========================================================================
    // App Client Assertions
    // =========================================================================

    /// Lists client assertions for an app.
    pub async fn list_app_client_assertions(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<Vec<AppClientAssertionInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("list_app_client_assertions", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::list_app_client_assertions(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                )
                .await
            }
        })
        .await
    }

    /// Fetches a single client assertion by ID.
    ///
    /// Returns [`SdkError::Rpc`] with `code = NotFound` when the assertion does
    /// not exist, belongs to a different app, or its enclosing app belongs to a
    /// different organization than the one supplied.
    ///
    /// [`SdkError::Rpc`]: crate::SdkError::Rpc
    pub async fn get_app_client_assertion(
        &self,
        user: UserSlug,
        organization: OrganizationSlug,
        app: AppSlug,
        assertion: DomainClientAssertionId,
    ) -> Result<AppClientAssertionInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_app_client_assertion", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::get_app_client_assertion(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    assertion,
                )
                .await
            }
        })
        .await
    }

    /// Creates a client assertion for an app. Returns the assertion metadata and private key PEM.
    ///
    /// The private key PEM is only returned on creation — it cannot be retrieved again.
    /// An idempotency key is generated per call so retries return the same keypair.
    pub async fn create_app_client_assertion(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        name: impl Into<String>,
        expires_at: std::time::SystemTime,
    ) -> Result<CreateAppClientAssertionResult> {
        let name = name.into();
        let pool = self.pool.clone();
        let idempotency_key: [u8; 16] = rand::random();
        // Wire transport uses UNIX-nanos `u64` directly. Pre-clamp negative
        // (pre-epoch) values to 0 to mirror the proto path's behavior on
        // malformed inputs.
        let expires_at_unix_nanos: u64 = expires_at
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0);

        self.call_with_retry("create_app_client_assertion", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::create_app_client_assertion(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    name,
                    expires_at_unix_nanos,
                    idempotency_key,
                )
                .await
            }
        })
        .await
    }

    /// Deletes a client assertion for an app.
    pub async fn delete_app_client_assertion(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        assertion: DomainClientAssertionId,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_app_client_assertion", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::delete_app_client_assertion(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    assertion,
                )
                .await
            }
        })
        .await
    }

    /// Enables or disables a specific client assertion.
    pub async fn set_app_client_assertion_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        assertion: DomainClientAssertionId,
        enabled: bool,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("set_app_client_assertion_enabled", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::set_app_client_assertion_enabled(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    assertion,
                    enabled,
                )
                .await
            }
        })
        .await
    }

    // =========================================================================
    // App Vault Connections
    // =========================================================================

    /// Lists vault connections for an app.
    pub async fn list_app_vaults(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<Vec<AppVaultConnectionInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("list_app_vaults", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::list_app_vaults(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                )
                .await
            }
        })
        .await
    }

    /// Adds a vault connection to an app.
    pub async fn add_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
        allowed_scopes: Vec<String>,
    ) -> Result<AppVaultConnectionInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("add_app_vault", || {
            let pool = pool.clone();
            let allowed_scopes = allowed_scopes.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::add_app_vault(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    vault,
                    allowed_scopes,
                )
                .await
            }
        })
        .await
    }

    /// Updates the allowed scopes for a vault connection.
    pub async fn update_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
        allowed_scopes: Vec<String>,
    ) -> Result<AppVaultConnectionInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_app_vault", || {
            let pool = pool.clone();
            let allowed_scopes = allowed_scopes.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::update_app_vault(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    vault,
                    allowed_scopes,
                )
                .await
            }
        })
        .await
    }

    /// Removes a vault connection from an app.
    pub async fn remove_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("remove_app_vault", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::app::remove_app_vault(
                    wire_client,
                    request_id,
                    organization,
                    user,
                    app,
                    vault,
                )
                .await
            }
        })
        .await
    }
}
