//! Cryptographically verified read operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
    retry::with_retry_cancellable,
    types::{query::VerifiedValue, verified_read::VerifyOpts},
};

impl LedgerClient {
    // =========================================================================
    // Verified Read Operations
    // =========================================================================

    /// Reads a value with cryptographic proof for client-side verification.
    ///
    /// Returns the value along with a Merkle proof that can be used to verify
    /// the value is authentic without trusting the server. The proof links
    /// the entity value to the state root in the block header.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - Entity key to read.
    /// * `opts` - Verification options (height, chain proof).
    ///
    /// # Returns
    ///
    /// `VerifiedValue` containing the value and proofs, or `None` if key not found.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug, VerifyOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.verified_read(UserSlug::new(42), organization, Some(vault), "user:123", VerifyOpts::new()).await?;
    /// if let Some(verified) = result {
    ///     // Verify the proof before using the value
    ///     verified.verify()?;
    ///     println!("Value: {:?}", verified.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verified_read(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        opts: VerifyOpts,
    ) -> Result<Option<VerifiedValue>> {
        self.check_shutdown(None)?;

        let key = key.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verified_read",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verified_read",
                || async {
                    let mut client = crate::connected_client!(pool, create_read_client);

                    let request = proto::VerifiedReadRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        key: key.clone(),
                        at_height: opts.at_height,
                        include_chain_proof: opts.include_chain_proof,
                        trusted_height: opts.trusted_height,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.verified_read(tonic::Request::new(request)).await?.into_inner();

                    // If no value and no block header, key was not found
                    if response.value.is_none() && response.block_header.is_none() {
                        return Ok(None);
                    }

                    Ok(VerifiedValue::from_proto(response))
                },
            ),
        )
        .await
    }
}
