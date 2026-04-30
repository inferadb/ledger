//! Vault CRUD operations.

use inferadb_ledger_types::{
    BlockRetentionMode, BlockRetentionPolicy, OrganizationSlug, UserSlug, VaultSlug,
};

use crate::{LedgerClient, error::Result, types::admin::VaultInfo};

impl LedgerClient {
    /// Creates a new vault in an organization.
    ///
    /// Creates a vault within `organization` and returns [`VaultInfo`]
    /// containing the new vault's metadata. The vault slug is assigned by
    /// the leader and included in the response.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts or the
    /// organization does not exist.
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
        let pool = self.pool.clone();
        // The wire op generates its own vault slug internally — same
        // idempotency contract (one slug per logical call, reused across
        // retries) — so we simply forward.
        self.call_with_retry("create_vault", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::vault::create_vault(wire_client, request_id, caller, organization)
                    .await
            }
        })
        .await
    }

    /// Returns information about a vault.
    ///
    /// Returns [`VaultInfo`] containing vault metadata for `vault` in
    /// `organization`.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts or the
    /// organization or vault does not exist.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::vault::get_vault(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                )
                .await
            }
        })
        .await
    }

    /// Lists vaults on this node.
    ///
    /// Returns a paginated list of vaults this node hosts or participates in.
    /// `page_size` is the maximum items per page (0 = server default). Pass
    /// `page_token` from a previous response to fetch the next page; pass
    /// `None` for the first page. Supply `organization` to filter by a
    /// specific organization. Returns a tuple `(vaults, next_page_token)`
    /// where `next_page_token` is `None` when there are no more pages.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::vault::list_vaults(
                    wire_client,
                    request_id,
                    caller,
                    page_size,
                    page_token,
                    organization,
                )
                .await
            }
        })
        .await
    }

    /// Deletes a vault from an organization.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts, the
    /// organization or vault does not exist, or the vault has active data
    /// preventing deletion.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::vault::delete_vault(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                )
                .await
            }
        })
        .await
    }

    /// Updates vault metadata (retention policy).
    pub async fn update_vault(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        retention_policy: Option<BlockRetentionPolicy>,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("update_vault", move || {
            let pool = pool.clone();
            async move {
                // Translate domain BlockRetentionPolicy → wire equivalent.
                // The wire `mode` is `Option<BlockRetentionMode>`; the
                // domain enum has only `Full` / `Compacted` (no
                // `Unspecified`), so we always emit `Some(...)`.
                let wire_retention = retention_policy.as_ref().map(|p| {
                    use inferadb_ledger_wire::services::shared as ws;
                    let mode = match p.mode {
                        BlockRetentionMode::Full => ws::BlockRetentionMode::Full,
                        BlockRetentionMode::Compacted => ws::BlockRetentionMode::Compacted,
                    };
                    ws::BlockRetentionPolicy {
                        mode: Some(mode),
                        retention_blocks: Some(p.retention_blocks),
                    }
                });
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::vault::update_vault(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    wire_retention,
                )
                .await
            }
        })
        .await
    }
}
