//! Fluent builder for relationship list queries.
//!
//! Provides a chainable API for constructing filtered relationship queries
//! with pagination and consistency options.
//!
//! # Example
//!
//! ```no_run
//! # use inferadb_ledger_sdk::LedgerClient;
//! # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
//! let page = client
//!     .relationship_query(1, 1)
//!     .resource("document:report-q4")
//!     .relation("viewer")
//!     .limit(50)
//!     .linearizable()
//!     .execute()
//!     .await?;
//!
//! for rel in &page.items {
//!     println!("{} --{}-> {}", rel.resource, rel.relation, rel.subject);
//! }
//!
//! if let Some(token) = &page.next_page_token {
//!     let _next = client
//!         .relationship_query(1, 1)
//!         .resource("document:report-q4")
//!         .relation("viewer")
//!         .page_token(token.clone())
//!         .execute()
//!         .await?;
//! }
//! # Ok(())
//! # }
//! ```

use crate::{
    LedgerClient,
    client::{ListRelationshipsOpts, PagedResult, ReadConsistency, Relationship},
    error::Result,
};

/// Fluent builder for relationship list queries.
///
/// All filter fields are optional — omitting a filter matches all values
/// for that field. Chain filters, then call `.execute()`.
pub struct RelationshipQueryBuilder<'a> {
    client: &'a LedgerClient,
    namespace_id: i64,
    vault_id: i64,
    resource: Option<String>,
    relation: Option<String>,
    subject: Option<String>,
    at_height: Option<u64>,
    limit: u32,
    page_token: Option<String>,
    consistency: ReadConsistency,
}

impl<'a> RelationshipQueryBuilder<'a> {
    /// Creates a new relationship query builder.
    pub(crate) fn new(client: &'a LedgerClient, namespace_id: i64, vault_id: i64) -> Self {
        Self {
            client,
            namespace_id,
            vault_id,
            resource: None,
            relation: None,
            subject: None,
            at_height: None,
            limit: 0,
            page_token: None,
            consistency: ReadConsistency::Eventual,
        }
    }

    /// Filter by resource identifier (exact match).
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Filter by relation name (exact match).
    pub fn relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Filter by subject identifier (exact match).
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Reads at a specific block height for point-in-time queries.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Maximum number of results per page.
    ///
    /// Zero (default) uses the server's default page size.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continue from a previous query's pagination token.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Use linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }

    /// Use eventual consistency (default).
    pub fn eventual(mut self) -> Self {
        self.consistency = ReadConsistency::Eventual;
        self
    }

    /// Execute the relationship query and return a paginated result.
    ///
    /// # Errors
    ///
    /// Returns an error if the query RPC fails after retry attempts are
    /// exhausted or the client has been shut down.
    pub async fn execute(self) -> Result<PagedResult<Relationship>> {
        let opts = ListRelationshipsOpts {
            resource: self.resource,
            relation: self.relation,
            subject: self.subject,
            at_height: self.at_height,
            limit: self.limit,
            page_token: self.page_token,
            consistency: self.consistency,
        };
        self.client.list_relationships(self.namespace_id, self.vault_id, opts).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::{super::test_helpers::test_client, *};

    #[tokio::test]
    async fn relationship_query_default_state() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2);
        assert_eq!(builder.namespace_id, 1);
        assert_eq!(builder.vault_id, 2);
        assert!(builder.resource.is_none());
        assert!(builder.relation.is_none());
        assert!(builder.subject.is_none());
        assert!(builder.at_height.is_none());
        assert_eq!(builder.limit, 0);
        assert!(builder.page_token.is_none());
        assert!(matches!(builder.consistency, ReadConsistency::Eventual));
    }

    #[tokio::test]
    async fn relationship_query_with_resource_filter() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2).resource("document:report-q4");
        assert_eq!(builder.resource.as_deref(), Some("document:report-q4"));
    }

    #[tokio::test]
    async fn relationship_query_with_relation_filter() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2).relation("viewer");
        assert_eq!(builder.relation.as_deref(), Some("viewer"));
    }

    #[tokio::test]
    async fn relationship_query_with_subject_filter() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2).subject("user:alice");
        assert_eq!(builder.subject.as_deref(), Some("user:alice"));
    }

    #[tokio::test]
    async fn relationship_query_chained_filters() {
        let client = test_client().await;
        let builder = client
            .relationship_query(1, 2)
            .resource("doc:1")
            .relation("editor")
            .subject("user:bob")
            .limit(25)
            .at_height(100)
            .linearizable();
        assert_eq!(builder.resource.as_deref(), Some("doc:1"));
        assert_eq!(builder.relation.as_deref(), Some("editor"));
        assert_eq!(builder.subject.as_deref(), Some("user:bob"));
        assert_eq!(builder.limit, 25);
        assert_eq!(builder.at_height, Some(100));
        assert!(matches!(builder.consistency, ReadConsistency::Linearizable));
    }

    #[tokio::test]
    async fn relationship_query_page_token() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2).page_token("next-page-abc");
        assert_eq!(builder.page_token.as_deref(), Some("next-page-abc"));
    }

    #[tokio::test]
    async fn relationship_query_consistency_override() {
        let client = test_client().await;
        let builder = client.relationship_query(1, 2).linearizable().eventual();
        assert!(matches!(builder.consistency, ReadConsistency::Eventual));
    }

    #[tokio::test]
    async fn relationship_query_accepts_string_and_str() {
        let client = test_client().await;
        let owned = String::from("doc:owned");
        let builder = client.relationship_query(1, 2).resource(owned).relation("viewer");
        assert_eq!(builder.resource.as_deref(), Some("doc:owned"));
        assert_eq!(builder.relation.as_deref(), Some("viewer"));
    }
}
