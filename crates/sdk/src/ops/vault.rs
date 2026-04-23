//! Vault CRUD operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
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
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let vault = client.create_vault(UserSlug::new(42), organization).await?;
    /// println!("Created vault with slug: {}", vault.vault);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_vault(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
    ) -> Result<VaultInfo> {
        // Generate the vault slug once, outside the retry loop. Every
        // retry for this logical call reuses it so the server's
        // per-org CreateVault idempotency (keyed on slug) returns the
        // same VaultId instead of creating a duplicate body on
        // response-lost-in-flight.
        let vault_slug = inferadb_ledger_types::snowflake::generate_vault_slug().map_err(|e| {
            crate::error::SdkError::Config { message: format!("generate vault slug: {e}") }
        })?;
        let pool = self.pool.clone();
        self.call_with_retry("create_vault", || {
            let pool = pool.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_vault_client);

                let request = proto::CreateVaultRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    replication_factor: 0,  // Use default
                    initial_nodes: vec![],  // Auto-assigned
                    retention_policy: None, // Default: FULL
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                    slug: Some(proto::VaultSlug { slug: vault_slug.value() }),
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
            }
        })
        .await
    }

    /// Returns information about a vault.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let info = client.get_vault(UserSlug::new(42), organization, vault).await?;
    /// println!("Vault height: {}, status: {:?}", info.height, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_vault(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<VaultInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_vault", || {
            let pool = pool.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_vault_client);

                let request = proto::GetVaultRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.get_vault(tonic::Request::new(request)).await?.into_inner();

                Ok(VaultInfo::from_proto(response))
            }
        })
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
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    /// * `organization` - Optional organization filter.
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
    /// # use inferadb_ledger_sdk::{LedgerClient, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let (vaults, _next) = client.list_vaults(UserSlug::new(42), 100, None, None).await?;
    /// for v in vaults {
    ///     println!("Vault {} in {}", v.vault, v.organization);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_vaults(
        &self,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
        organization: Option<OrganizationSlug>,
    ) -> Result<(Vec<VaultInfo>, Option<Vec<u8>>)> {
        let pool = self.pool.clone();
        self.call_with_retry("list_vaults", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_vault_client);

                let request = proto::ListVaultsRequest {
                    page_token: page_token.clone(),
                    page_size,
                    organization: organization.map(|o| proto::OrganizationSlug { slug: o.value() }),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.list_vaults(tonic::Request::new(request)).await?.into_inner();

                let vaults = response.vaults.into_iter().map(VaultInfo::from_proto).collect();

                Ok((vaults, response.next_page_token))
            }
        })
        .await
    }

    /// Deletes a vault from an organization.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization or vault does not exist
    /// - The vault has active data preventing deletion
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// client.delete_vault(UserSlug::new(42), organization, vault).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_vault(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_vault", || {
            let pool = pool.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_vault_client);

                let request = proto::DeleteVaultRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                client.delete_vault(tonic::Request::new(request)).await?;

                Ok(())
            }
        })
        .await
    }

    /// Updates vault metadata (retention policy).
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    pub async fn update_vault(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        retention_policy: Option<proto::BlockRetentionPolicy>,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("update_vault", || {
            let pool = pool.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_vault_client);

                let request = proto::UpdateVaultRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    retention_policy,
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                client.update_vault(tonic::Request::new(request)).await?;

                Ok(())
            }
        })
        .await
    }
}
