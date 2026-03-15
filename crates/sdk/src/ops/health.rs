//! Node and vault health check operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    retry::with_retry_cancellable,
    types::admin::{HealthCheckResult, HealthStatus},
};

impl LedgerClient {
    // =========================================================================
    // Health Operations
    // =========================================================================

    /// Checks node-level health.
    ///
    /// Returns `true` if the node is healthy and has a leader elected.
    /// This is a simple health check suitable for load balancer probes.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node is healthy, `false` if degraded.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unavailable or connection fails.
    /// Note: An unavailable node returns an error, not `false`.
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
    /// Returns full health check result including status, message, and details.
    /// Use this for monitoring and diagnostics that need more than a simple boolean.
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with status, message, and details.
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_detailed",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_detailed",
                || async {
                    let mut client = crate::connected_client!(pool, create_health_client);

                    let request = proto::HealthCheckRequest { organization: None, vault: None };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Checks health of a specific vault.
    ///
    /// Returns detailed health information for a specific vault, including
    /// block height, health status, and any divergence information.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with vault-specific health information.
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
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_vault",
                || async {
                    let mut client = crate::connected_client!(pool, create_health_client);

                    let request = proto::HealthCheckRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }
}
