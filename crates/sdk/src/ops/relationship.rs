//! Relationship check operations.

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
    /// answers "is this tuple stored?" `resource` uses the format `"type:id"`;
    /// `subject` uses `"type:id"` or `"type:id#relation"`. Pass
    /// [`ReadConsistency::Linearizable`](crate::ReadConsistency::Linearizable)
    /// to read from the leader; `Eventual` serves any replica. Returns a
    /// [`CheckRelationshipOutcome`](crate::CheckRelationshipOutcome) with
    /// `exists` and the block height at which the check was evaluated.
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
                    let wire_client = crate::connected_wire_client!(pool);
                    let request_id: u128 = rand::random();
                    crate::ops_wire::relationship::check_relationship(
                        wire_client,
                        request_id,
                        caller,
                        organization,
                        vault,
                        resource.clone(),
                        relation.clone(),
                        subject.clone(),
                        consistency,
                    )
                    .await
                },
            ),
        )
        .await
    }
}
