//! Node and vault health check operations.

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    types::admin::{HealthCheckResult, HealthStatus},
};

impl LedgerClient {
    // =========================================================================
    // Health Operations
    // =========================================================================

    /// Checks node-level health.
    ///
    /// Returns `true` if the node is healthy and has a leader elected, or
    /// `false` if degraded. This is a simple health check suitable for load
    /// balancer probes. An unavailable node returns an error, not `false`.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unavailable or connection fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.health_check().await? {
    ///     println!("Node is healthy");
    /// } else {
    ///     println!("Node is degraded but available");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self) -> Result<bool> {
        self.check_shutdown(None)?;

        let result = self.health_check_detailed().await?;
        match result.status {
            HealthStatus::Healthy => Ok(true),
            HealthStatus::Degraded => Ok(false),
            HealthStatus::Unavailable => {
                Err(error::SdkError::Unavailable { message: result.message })
            },
            HealthStatus::Unspecified => Ok(false),
        }
    }

    /// Returns detailed node-level health information.
    ///
    /// Returns a [`HealthCheckResult`](crate::HealthCheckResult) with full
    /// status, message, and details map. Use this for monitoring and diagnostics
    /// that need more than a simple boolean.
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
    /// let health = client.health_check_detailed().await?;
    /// println!("Status: {:?}, Message: {}", health.status, health.message);
    /// if let Some(term) = health.details.get("current_term") {
    ///     println!("Current Raft term: {}", term);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_detailed(&self) -> Result<HealthCheckResult> {
        let pool = self.pool.clone();

        self.call_with_retry("health_check_detailed", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::health::check_health(wire_client, request_id).await
            }
        })
        .await
    }

    /// Checks health of a specific vault.
    ///
    /// Returns a [`HealthCheckResult`](crate::HealthCheckResult) with
    /// vault-specific health information including block height, status, and
    /// any divergence data.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let health = client.health_check_vault(organization, VaultSlug::new(0)).await?;
    /// println!("Vault status: {:?}", health.status);
    /// if let Some(height) = health.details.get("block_height") {
    ///     println!("Current height: {}", height);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<HealthCheckResult> {
        let pool = self.pool.clone();

        self.call_with_retry("health_check_vault", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::health::check_health_vault(
                    wire_client,
                    request_id,
                    organization,
                    vault,
                )
                .await
            }
        })
        .await
    }
}
