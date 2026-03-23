//! App CRUD, credentials, client assertions, and vault connection operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{
    AppSlug, ClientAssertionId as DomainClientAssertionId, OrganizationSlug, UserSlug, VaultSlug,
};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::{missing_response_field, system_time_to_proto_timestamp},
    retry::with_retry_cancellable,
    types::app::{
        AppClientAssertionInfo, AppClientSecretStatus, AppCredentialType, AppInfo,
        AppVaultConnectionInfo, CreateAppClientAssertionResult, app_info_from_proto,
        assertion_info_from_proto, vault_connection_from_proto,
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
        self.check_shutdown(None)?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_app",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::CreateAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                        description: description.clone(),
                    };

                    let response =
                        client.create_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "CreateAppResponse"))
                },
            ),
        )
        .await
    }

    /// Gets an app by slug.
    pub async fn get_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_app",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::GetAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client.get_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "GetAppResponse"))
                },
            ),
        )
        .await
    }

    /// Lists all apps in an organization.
    pub async fn list_apps(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
    ) -> Result<Vec<AppInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_apps",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_apps",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::ListAppsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.list_apps(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.apps.iter().map(app_info_from_proto).collect())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_app",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::UpdateAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        name: name.clone(),
                        description: description.clone(),
                    };

                    let response =
                        client.update_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "UpdateAppResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes an app.
    pub async fn delete_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_app",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::DeleteAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    client.delete_app(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_enabled",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::SetAppEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        enabled,
                    };

                    let response =
                        client.set_app_enabled(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "SetAppEnabledResponse"))
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let credential_type_i32 = credential_type.to_proto();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_credential_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_credential_enabled",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::SetAppCredentialEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        credential_type: credential_type_i32,
                        enabled,
                    };

                    let response = client
                        .set_app_credential_enabled(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.app.map(|a| app_info_from_proto(&a)).ok_or_else(|| {
                        missing_response_field("app", "SetAppCredentialEnabledResponse")
                    })
                },
            ),
        )
        .await
    }

    /// Gets the client secret status for an app (enabled flag and whether a secret exists).
    pub async fn get_app_client_secret(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppClientSecretStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_app_client_secret",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_app_client_secret",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::GetAppClientSecretRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client
                        .get_app_client_secret(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(AppClientSecretStatus {
                        enabled: response.enabled,
                        has_secret: response.has_secret,
                    })
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let idempotency_key: [u8; 16] = rand::random();

        self.with_metrics(
            "rotate_app_client_secret",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_app_client_secret",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::RotateAppClientSecretRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        idempotency_key: idempotency_key.to_vec(),
                    };

                    let response = client
                        .rotate_app_client_secret(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.secret)
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_app_client_assertions",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_app_client_assertions",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::ListAppClientAssertionsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client
                        .list_app_client_assertions(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.assertions.iter().map(assertion_info_from_proto).collect())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let idempotency_key: [u8; 16] = rand::random();
        let expires_at_proto = system_time_to_proto_timestamp(&expires_at);

        self.with_metrics(
            "create_app_client_assertion",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_app_client_assertion",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::CreateAppClientAssertionRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        name: name.clone(),
                        expires_at: Some(expires_at_proto),
                        idempotency_key: idempotency_key.to_vec(),
                    };

                    let response = client
                        .create_app_client_assertion(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let assertion = response
                        .assertion
                        .map(|a| assertion_info_from_proto(&a))
                        .ok_or_else(|| {
                            missing_response_field("assertion", "CreateAppClientAssertionResponse")
                        })?;

                    Ok(CreateAppClientAssertionResult {
                        assertion,
                        private_key_pem: response.private_key_pem,
                    })
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_app_client_assertion",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_app_client_assertion",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::DeleteAppClientAssertionRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        assertion: Some(proto::ClientAssertionId { id: assertion.value() }),
                    };

                    client.delete_app_client_assertion(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_client_assertion_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_client_assertion_enabled",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::SetAppClientAssertionEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        assertion: Some(proto::ClientAssertionId { id: assertion.value() }),
                        enabled,
                    };

                    client.set_app_client_assertion_enabled(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_app_vaults",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_app_vaults",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::ListAppVaultsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response =
                        client.list_app_vaults(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.vaults.iter().map(vault_connection_from_proto).collect())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "add_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "add_app_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::AddAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        allowed_scopes: allowed_scopes.clone(),
                    };

                    let response =
                        client.add_app_vault(tonic::Request::new(request)).await?.into_inner();

                    response
                        .vault
                        .map(|v| vault_connection_from_proto(&v))
                        .ok_or_else(|| missing_response_field("vault", "AddAppVaultResponse"))
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_app_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::UpdateAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        allowed_scopes: allowed_scopes.clone(),
                    };

                    let response =
                        client.update_app_vault(tonic::Request::new(request)).await?.into_inner();

                    response
                        .vault
                        .map(|v| vault_connection_from_proto(&v))
                        .ok_or_else(|| missing_response_field("vault", "UpdateAppVaultResponse"))
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "remove_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "remove_app_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_app_client);

                    let request = proto::RemoveAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    client.remove_app_vault(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }
}
