//! Entity, relationship, and resource listing operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::non_empty,
    retry::with_retry_cancellable,
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
    /// Returns a paginated list of entities with keys starting with the given prefix.
    /// Use the `next_page_token` to fetch additional pages.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `opts` - Query options including prefix filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListEntitiesOpts, OrganizationSlug, VaultSlug};
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_entities",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_entities",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::ListEntitiesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        key_prefix: opts.key_prefix.clone(),
                        at_height: opts.at_height,
                        include_expired: opts.include_expired,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                        vault: opts.vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_entities(tonic::Request::new(request)).await?.into_inner();

                    let items = response.entities.into_iter().map(Entity::from_proto).collect();

                    let next_page_token = non_empty(response.next_page_token);

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists relationships in a vault with optional filters.
    ///
    /// Returns a paginated list of relationships matching the filter criteria.
    /// All filter fields are optional; omitting a filter matches all values.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including filters, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListRelationshipsOpts, OrganizationSlug, VaultSlug};
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_relationships",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_relationships",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::ListRelationshipsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource: opts.resource.clone(),
                        relation: opts.relation.clone(),
                        subject: opts.subject.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_relationships(tonic::Request::new(request)).await?.into_inner();

                    let items =
                        response.relationships.into_iter().map(Relationship::from_proto).collect();

                    let next_page_token = non_empty(response.next_page_token);

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists distinct resource IDs matching a type prefix.
    ///
    /// Returns a paginated list of unique resource identifiers that match the given
    /// type prefix (e.g., "document" matches "document:1", "document:2", etc.).
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including type filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListResourcesOpts, OrganizationSlug, VaultSlug};
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_resources",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_resources",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::ListResourcesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource_type: opts.resource_type.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_resources(tonic::Request::new(request)).await?.into_inner();

                    let next_page_token = non_empty(response.next_page_token);

                    Ok(PagedResult {
                        items: response.resources,
                        next_page_token,
                        block_height: response.block_height,
                    })
                },
            ),
        )
        .await
    }
}
