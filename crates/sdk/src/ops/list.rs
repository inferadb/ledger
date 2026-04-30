//! Entity, relationship, and resource listing operations.

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
    types::query::{
        Entity, ListEntitiesOpts, ListRelationshipsOpts, ListResourcesOpts, PagedResult,
        Relationship,
    },
};

impl LedgerClient {
    // =========================================================================
    // Query Operations
    // =========================================================================

    /// Lists entities matching a key prefix.
    ///
    /// Returns a paginated list of entities with keys starting with the prefix
    /// specified in `opts`. Use the `next_page_token` in the result to fetch
    /// additional pages.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Shutdown`] if the client has been shut down,
    /// or [`crate::SdkError::Rpc`] if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListEntitiesOpts, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// // List all users
    /// let result = client.list_entities(UserSlug::new(42), organization, ListEntitiesOpts::with_prefix("user:")).await?;
    /// for entity in result.items {
    ///     println!("Key: {}, Version: {}", entity.key, entity.version);
    /// }
    ///
    /// // Fetch next page if available
    /// if let Some(token) = result.next_page_token {
    ///     let next_page = client.list_entities(
    ///         UserSlug::new(42),
    ///         organization,
    ///         ListEntitiesOpts::with_prefix("user:").page_token(token)
    ///     ).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_entities(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        opts: ListEntitiesOpts,
    ) -> Result<PagedResult<Entity>> {
        let pool = self.pool.clone();

        self.call_with_retry("list_entities", || {
            let pool = pool.clone();
            let opts = opts.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::list::list_entities(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    opts,
                )
                .await
            }
        })
        .await
    }

    /// Lists relationships in a vault with optional filters.
    ///
    /// Returns a paginated list of relationships matching the filter criteria
    /// in `opts`. All filter fields are optional; omitting a filter matches all
    /// values. Use the `next_page_token` in the result to fetch additional pages.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Shutdown`] if the client has been shut down,
    /// or [`crate::SdkError::Rpc`] if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListRelationshipsOpts, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all relationships for a document
    /// let result = client.list_relationships(
    ///     UserSlug::new(42),
    ///     organization,
    ///     vault,
    ///     ListRelationshipsOpts::new().resource("document:123")
    /// ).await?;
    ///
    /// for rel in result.items {
    ///     println!("{} -> {} -> {}", rel.resource, rel.relation, rel.subject);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_relationships(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListRelationshipsOpts,
    ) -> Result<PagedResult<Relationship>> {
        let pool = self.pool.clone();

        self.call_with_retry("list_relationships", || {
            let pool = pool.clone();
            let opts = opts.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::list::list_relationships(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    opts,
                )
                .await
            }
        })
        .await
    }

    /// Lists distinct resource IDs matching a type prefix.
    ///
    /// Returns a paginated list of unique resource identifiers that match the
    /// type prefix specified in `opts` (e.g., `"document"` matches
    /// `"document:1"`, `"document:2"`, etc.). Use the `next_page_token` in
    /// the result to fetch additional pages.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Shutdown`] if the client has been shut down,
    /// or [`crate::SdkError::Rpc`] if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListResourcesOpts, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all document resources
    /// let result = client.list_resources(
    ///     UserSlug::new(42),
    ///     organization,
    ///     vault,
    ///     ListResourcesOpts::with_type("document")
    /// ).await?;
    ///
    /// for resource_id in result.items {
    ///     println!("Resource: {}", resource_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_resources(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListResourcesOpts,
    ) -> Result<PagedResult<String>> {
        let pool = self.pool.clone();

        self.call_with_retry("list_resources", || {
            let pool = pool.clone();
            let opts = opts.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::list::list_resources(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    opts,
                )
                .await
            }
        })
        .await
    }
}
