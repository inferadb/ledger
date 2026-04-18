//! Relationship check operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};
use tokio_util::sync::CancellationToken;

use crate::{
    LedgerClient,
    error::Result,
    retry::with_retry_cancellable,
    types::{query::CheckRelationshipOutcome, read::ReadConsistency},
};

impl LedgerClient {
    /// Checks whether a direct relationship tuple `(resource, relation, subject)`
    /// exists in the vault.
    ///
    /// This is the storage-primitive existence check. It does NOT evaluate
    /// authorization policy, userset rewrites, or schema-driven inheritance.
    /// Policy evaluation happens in the Engine layer above Ledger; Ledger only
    /// answers "is this tuple stored?"
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing the check (external slug).
    /// * `organization` - Organization slug.
    /// * `vault` - Vault slug.
    /// * `resource` - Resource identifier (format `"type:id"`).
    /// * `relation` - Relation name.
    /// * `subject` - Subject identifier (format `"type:id"` or `"type:id#relation"`).
    /// * `consistency` - `Eventual` (any replica) or `Linearizable` (leader).
    /// * `at_height` - Historical check at the given block height; `None` for current.
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// A [`CheckRelationshipOutcome`] with `exists` and `checked_at_height`.
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails after retry attempts are exhausted,
    /// the client has been shut down, or the cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, ReadConsistency, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (org, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let outcome = client.check_relationship(
    ///     UserSlug::new(42),
    ///     org,
    ///     vault,
    ///     "doc:123",
    ///     "viewer",
    ///     "user:7",
    ///     ReadConsistency::Eventual,
    ///     None,
    ///     None,
    /// ).await?;
    /// if outcome.exists {
    ///     println!("user:7 is a viewer of doc:123 at height {}", outcome.checked_at_height);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn check_relationship(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
        consistency: ReadConsistency,
        at_height: Option<u64>,
        token: Option<CancellationToken>,
    ) -> Result<CheckRelationshipOutcome> {
        self.check_shutdown(token.as_ref())?;

        let resource = resource.into();
        let relation = relation.into();
        let subject = subject.into();
        let token = self.effective_token(token.as_ref());
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "check_relationship",
            with_retry_cancellable(
                &retry_policy,
                &token,
                Some(pool),
                "check_relationship",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::CheckRelationshipRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource: resource.clone(),
                        relation: relation.clone(),
                        subject: subject.clone(),
                        consistency: consistency.to_proto() as i32,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        at_height,
                    };

                    let response =
                        client.check_relationship(tonic::Request::new(request)).await?.into_inner();

                    Ok(CheckRelationshipOutcome {
                        exists: response.exists,
                        checked_at_height: response.checked_at_height,
                    })
                },
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

    use std::time::Duration;

    use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

    use crate::{
        ClientConfig, LedgerClient, ReadConsistency, ServerSource, UserSlug, mock::MockLedgerServer,
    };

    const ORG: OrganizationSlug = OrganizationSlug::new(1);
    const VAULT: VaultSlug = VaultSlug::new(1);
    const CALLER: UserSlug = UserSlug::new(42);

    async fn create_client(server: &MockLedgerServer) -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([server.endpoint().to_string()]))
            .client_id("test-client")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("valid config");

        LedgerClient::new(config).await.expect("client creation")
    }

    #[tokio::test]
    async fn check_relationship_returns_true_when_tuple_exists() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "doc:123", "viewer", "user:7");
        let client = create_client(&server).await;

        let outcome = client
            .check_relationship(
                CALLER,
                ORG,
                VAULT,
                "doc:123",
                "viewer",
                "user:7",
                ReadConsistency::Eventual,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(outcome.exists);
        assert!(outcome.checked_at_height >= 1);
    }

    #[tokio::test]
    async fn check_relationship_returns_false_when_tuple_absent() {
        let server = MockLedgerServer::start().await.unwrap();
        let client = create_client(&server).await;

        let outcome = client
            .check_relationship(
                CALLER,
                ORG,
                VAULT,
                "doc:123",
                "viewer",
                "user:7",
                ReadConsistency::Eventual,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!outcome.exists);
    }

    #[tokio::test]
    async fn check_relationship_with_linearizable_consistency() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "folder:1", "editor", "user:99");
        let client = create_client(&server).await;

        let outcome = client
            .check_relationship(
                CALLER,
                ORG,
                VAULT,
                "folder:1",
                "editor",
                "user:99",
                ReadConsistency::Linearizable,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(outcome.exists);
    }

    #[tokio::test]
    async fn check_relationship_with_at_height() {
        let server = MockLedgerServer::start().await.unwrap();
        server.add_relationship(ORG, VAULT, "doc:10", "owner", "user:5");
        let client = create_client(&server).await;

        // The mock ignores at_height but the field is forwarded correctly.
        let outcome = client
            .check_relationship(
                CALLER,
                ORG,
                VAULT,
                "doc:10",
                "owner",
                "user:5",
                ReadConsistency::Eventual,
                Some(100),
                None,
            )
            .await
            .unwrap();

        // exists is determined by mock state regardless of height in the mock
        assert!(outcome.exists);
    }

    #[tokio::test]
    async fn check_relationship_partial_match_returns_false() {
        let server = MockLedgerServer::start().await.unwrap();
        // Only "viewer" relation exists, not "editor"
        server.add_relationship(ORG, VAULT, "doc:123", "viewer", "user:7");
        let client = create_client(&server).await;

        let outcome = client
            .check_relationship(
                CALLER,
                ORG,
                VAULT,
                "doc:123",
                "editor",
                "user:7",
                ReadConsistency::Eventual,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!outcome.exists);
    }
}
