//! Vault CRUD operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
    retry::with_retry_cancellable,
    types::admin::{VaultInfo, VaultStatus},
};

impl LedgerClient {
    /// Creates a new vault in an organization.
    ///
    /// Creates a vault within the specified organization. The vault slug
    /// (external identifier) is assigned by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing the new vault's metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let vault = client.create_vault(organization).await?;
    /// println!("Created vault with slug: {}", vault.vault);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_vault(&self, organization: OrganizationSlug) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_vault_client);

                    let request = proto::CreateVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        replication_factor: 0,  // Use default
                        initial_nodes: vec![],  // Auto-assigned
                        retention_policy: None, // Default: FULL
                    };

                    let response =
                        client.create_vault(tonic::Request::new(request)).await?.into_inner();

                    let genesis = response.genesis.unwrap_or_default();
                    Ok(VaultInfo {
                        organization,
                        vault: VaultSlug::new(response.vault.map_or(0, |v| v.slug)),
                        height: genesis.height,
                        state_root: genesis.state_root.map(|h| h.value).unwrap_or_default(),
                        nodes: vec![],
                        leader: None,
                        status: VaultStatus::Active,
                    })
                },
            ),
        )
        .await
    }

    /// Returns information about a vault.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing vault metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization or vault does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let info = client.get_vault(organization, vault).await?;
    /// println!("Vault height: {}, status: {:?}", info.height, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_vault_client);

                    let request = proto::GetVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response =
                        client.get_vault(tonic::Request::new(request)).await?.into_inner();

                    Ok(VaultInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Lists vaults on this node.
    ///
    /// Returns a paginated list of vaults that this node is hosting or
    /// participating in. Pass the returned `next_page_token` into subsequent
    /// calls to retrieve further pages.
    ///
    /// # Arguments
    ///
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(vaults, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let (vaults, _next) = client.list_vaults(100, None).await?;
    /// for v in vaults {
    ///     println!("Vault {} in {}", v.vault, v.organization);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_vaults(
        &self,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<VaultInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_vaults",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_vaults",
                || async {
                    let mut client = crate::connected_client!(pool, create_vault_client);

                    let request =
                        proto::ListVaultsRequest { page_token: page_token.clone(), page_size };

                    let response =
                        client.list_vaults(tonic::Request::new(request)).await?.into_inner();

                    let vaults = response.vaults.into_iter().map(VaultInfo::from_proto).collect();

                    Ok((vaults, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Updates vault metadata (retention policy).
    pub async fn update_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        retention_policy: Option<proto::BlockRetentionPolicy>,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_vault_client);

                    let request = proto::UpdateVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        retention_policy,
                    };

                    client.update_vault(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }
}
