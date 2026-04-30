//! Cryptographically verified read operations.

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::Result,
    types::{query::VerifiedValue, verified_read::VerifyOpts},
};

impl LedgerClient {
    // =========================================================================
    // Verified Read Operations
    // =========================================================================

    /// Reads a value with cryptographic proof for client-side verification.
    ///
    /// Returns `Some(`[`VerifiedValue`](crate::VerifiedValue)`)` containing
    /// the value and a Merkle proof linking the entity to the state root in
    /// the block header, or `None` if the key was not found. The proof can be
    /// verified client-side without trusting the server. Omit `vault` for
    /// organization-level entities. Pass [`VerifyOpts`](crate::VerifyOpts) to
    /// request a specific block height or include a chain proof.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Shutdown`] if the client has been shut down,
    /// or [`crate::SdkError::Rpc`] if the read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug, VerifyOpts};
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
        let key = key.into();
        let pool = self.pool.clone();
        self.call_with_retry("verified_read", || {
            let pool = pool.clone();
            let key = key.clone();
            let opts = opts.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::verified_read::verified_read(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    vault,
                    key,
                    opts,
                )
                .await
            }
        })
        .await
    }
}
