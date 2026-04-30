//! [`LedgerClient`] struct, construction, lifecycle, and gRPC client factories.
//!
//! Domain operation methods (read, write, events, admin, etc.) are implemented
//! in the `ops/` submodules and delegated from `LedgerClient`. Domain types are
//! in the `types/` submodules. Shared proto conversion helpers are in
//! `proto_util`.

use std::sync::Arc;

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

// Re-export domain types for the test module's `use super::*`.
#[cfg(test)]
pub use crate::types::{admin::*, query::*, read::*, verified_read::*};
use crate::{
    config::ClientConfig,
    connection::ConnectionPool,
    error::{self, Result},
    server::{ServerResolver, ServerSource},
};

/// Acquires a wire client from the pool's wire-transport vec.
///
/// Returns an [`Arc<WireClient>`] round-robin-selected from the pool. The
/// QUIC + auth handshake itself is deferred to the first
/// [`WireClient::acquire`](inferadb_ledger_wire_transport::WireClient::acquire)
/// inside the dispatched op. Returns an [`SdkError::Config`] when the
/// wire-client construction failed at pool-build time (TLS configuration,
/// endpoint URLs, etc.).
#[doc(hidden)]
#[macro_export]
macro_rules! connected_wire_client {
    ($pool:expr) => {{
        $pool.cached_wire_client().ok_or_else(|| $crate::SdkError::Config {
            message: "wire client not configured: wire-client construction failed (check \
                      TLS configuration and endpoint URLs)"
                .to_owned(),
        })?
    }};
}

/// High-level client for interacting with the Ledger service.
///
/// `LedgerClient` orchestrates:
/// - Connection pool for efficient channel management
/// - Sequence tracker for client-side idempotency
/// - Retry logic for transient failure recovery
/// - Server discovery (DNS, file, or static endpoints)
/// - Graceful shutdown with request cancellation
///
/// # Server Discovery
///
/// The client supports three server discovery modes:
/// - **Static**: Fixed list of endpoint URLs
/// - **DNS**: Resolve A records from a domain (for Kubernetes headless services)
/// - **File**: Load servers from a JSON manifest file
///
/// For DNS and file sources, the client performs initial resolution during
/// construction and starts a background refresh task.
///
/// # Shutdown Behavior
///
/// When [`shutdown()`](Self::shutdown) is called:
/// 1. All pending requests are cancelled with `SdkError::Shutdown`
/// 2. New requests immediately fail with `SdkError::Shutdown`
/// 3. Server resolver refresh task is stopped
/// 4. Pending operations are cancelled
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::builder()
///     .servers(ServerSource::from_static(["http://localhost:50051"]))
///     .client_id("my-app-001")
///     .build()?;
///
/// let client = LedgerClient::new(config).await?;
///
/// // ... use the client ...
///
/// // Graceful shutdown
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// # Cancellation
///
/// The client supports two levels of cancellation:
///
/// **Client-level** — [`shutdown()`](Self::shutdown) cancels all in-flight
/// requests and rejects new ones with `SdkError::Shutdown`.
///
/// **Per-request** — Methods like [`read`](Self::read) and
/// [`write`](Self::write) accept an optional
/// [`CancellationToken`](tokio_util::sync::CancellationToken) that cancels
/// a single request with `SdkError::Cancelled`.
///
/// Both mechanisms interrupt in-flight RPCs and backoff sleeps via
/// `tokio::select!`. Access the client's token via
/// [`cancellation_token()`](Self::cancellation_token) to create child
/// tokens or integrate with application-level shutdown.
#[derive(Clone)]
pub struct LedgerClient {
    pub(crate) pool: ConnectionPool,
    /// Server resolver for DNS/file discovery.
    resolver: Option<Arc<ServerResolver>>,
    /// Cancellation token for coordinated shutdown.
    pub(crate) cancellation: tokio_util::sync::CancellationToken,
}

impl LedgerClient {
    /// Creates a new `LedgerClient` with the given configuration.
    ///
    /// This constructor validates the configuration and performs initial server
    /// resolution for DNS/file sources. Connections are established lazily on
    /// first use.
    ///
    /// For DNS and file server sources, a background refresh task is started
    /// to periodically re-resolve servers.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration is invalid
    /// - DNS resolution fails (for DNS sources)
    /// - File read/parse fails (for file sources)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Static endpoints
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::from_static(["http://localhost:50051"]))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    ///
    /// // DNS discovery (Kubernetes)
    /// use inferadb_ledger_sdk::DnsConfig;
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::dns(DnsConfig::builder().domain("ledger.default.svc").build()))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let cancellation = tokio_util::sync::CancellationToken::new();

        // Create resolver for DNS/file sources
        let (resolver, initial_endpoints) = match config.servers() {
            ServerSource::Static(_) => (None, None),
            source @ (ServerSource::Dns(_) | ServerSource::File(_)) => {
                let resolver = Arc::new(ServerResolver::new(source.clone()));

                // Perform initial resolution
                let servers = resolver.resolve().await.map_err(|e| error::SdkError::Config {
                    message: format!("Server discovery failed: {e}"),
                })?;

                // Convert to endpoint URLs
                let endpoints: Vec<String> = servers.iter().map(|s| s.url()).collect();

                // Start background refresh task
                resolver.start_refresh_task();

                (Some(resolver), Some(endpoints))
            },
        };

        let pool = ConnectionPool::new(config);

        // Set initial endpoints for DNS/file sources
        if let Some(endpoints) = initial_endpoints {
            pool.update_endpoints(endpoints);
        }

        Ok(Self { pool, resolver, cancellation })
    }

    /// Convenience constructor for connecting to a single endpoint.
    ///
    /// Creates a client with default configuration, connecting to the specified
    /// endpoint with the given client ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([endpoint.into()]))
            .client_id(client_id)
            .build()?;

        Self::new(config).await
    }

    /// Returns the client ID used for idempotency tracking.
    #[inline]
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.pool.config().client_id()
    }

    /// Returns a reference to the client configuration.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        self.pool.config()
    }

    /// Returns a reference to the connection pool.
    #[inline]
    #[must_use]
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    // =========================================================================
    // Fluent Builders
    // =========================================================================

    /// Creates a fluent write builder for the given organization and optional vault.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let result = client
    ///     .write_builder(UserSlug::new(42), organization, Some(VaultSlug::new(1)))
    ///     .set("user:123", b"data".to_vec())
    ///     .create_relationship("doc:1", "viewer", "user:123")
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn write_builder(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::WriteBuilder<'_> {
        crate::builders::WriteBuilder::new(self, caller, organization, vault)
    }

    /// Creates a fluent batch read builder for the given organization and optional vault.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let results = client
    ///     .batch_read_builder(UserSlug::new(42), organization, Some(VaultSlug::new(1)))
    ///     .key("user:123")
    ///     .key("user:456")
    ///     .linearizable()
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn batch_read_builder(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::BatchReadBuilder<'_> {
        crate::builders::BatchReadBuilder::new(self, caller, organization, vault)
    }

    /// Creates a fluent relationship query builder for the given organization and vault.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let page = client
    ///     .relationship_query(UserSlug::new(42), organization, VaultSlug::new(1))
    ///     .resource("document:report")
    ///     .relation("viewer")
    ///     .limit(50)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn relationship_query(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> crate::builders::RelationshipQueryBuilder<'_> {
        crate::builders::RelationshipQueryBuilder::new(self, caller, organization, vault)
    }

    /// Returns the client's cancellation token.
    ///
    /// The token can be used to:
    /// - Monitor shutdown state via `CancellationToken::cancelled()`
    /// - Create child tokens for per-request cancellation
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "svc").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let token = client.cancellation_token().child_token();
    ///
    /// // Cancel after 100ms
    /// let cancel_token = token.clone();
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ///     cancel_token.cancel();
    /// });
    ///
    /// // This read will be cancelled if it takes longer than 100ms
    /// let result = client.read(UserSlug::new(42), organization, None, "key", None, Some(token)).await;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

    /// Initiates graceful shutdown of the client.
    ///
    /// This method:
    /// 1. Cancels all pending requests (they will return `SdkError::Shutdown`)
    /// 2. Prevents new requests from being accepted
    /// 3. Stops the server resolver refresh task (if using DNS/file discovery)
    /// 4. Resets the connection pool
    ///
    /// After calling `shutdown()`, all operations will immediately return
    /// `SdkError::Shutdown`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// # let operations = vec![];
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    ///
    /// // Perform operations...
    /// client.write(UserSlug::new(42), organization, Some(vault), operations, None).await?;
    ///
    /// // Graceful shutdown before application exit
    /// client.shutdown().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&self) {
        // Cancel all pending and future operations
        self.cancellation.cancel();

        // Stop server resolver refresh task
        if let Some(ref resolver) = self.resolver {
            resolver.shutdown();
        }

        tracing::debug!("Client shutdown initiated");

        // Reset connection pool to close connections
        self.pool.reset();
    }

    /// Returns `true` if the client has been shut down.
    ///
    /// After shutdown, all operations will fail with `SdkError::Shutdown`.
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns an error if the client has been shut down or the request token
    /// has been cancelled.
    #[inline]
    pub(crate) fn check_shutdown(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(error::SdkError::Shutdown);
        }
        if let Some(token) = request_token
            && token.is_cancelled()
        {
            return Err(error::SdkError::Cancelled);
        }
        Ok(())
    }

    /// Creates a token that fires when either the client shuts down or
    /// the per-request token is cancelled.
    pub(crate) fn effective_token(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> tokio_util::sync::CancellationToken {
        match request_token {
            Some(req_token) => {
                let child = self.cancellation.child_token();
                let child_clone = child.clone();
                let req_clone = req_token.clone();
                tokio::spawn(async move {
                    req_clone.cancelled().await;
                    child_clone.cancel();
                });
                child
            },
            None => self.cancellation.clone(),
        }
    }

    /// Executes a future and records request metrics (latency + success/error).
    pub(crate) async fn with_metrics<T>(
        &self,
        method: &str,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let start = std::time::Instant::now();
        let result = fut.await;
        let duration = start.elapsed();
        self.pool.metrics().record_request(method, duration, result.is_ok());
        result
    }

    /// Wraps an RPC operation with shutdown check, retry policy, and metrics.
    ///
    /// Combines the three-step pattern (check shutdown, retry with cancellation,
    /// record metrics) into a single call. The `operation` closure is an `FnMut`
    /// because it may be invoked multiple times on transient failures.
    pub(crate) async fn call_with_retry<T, F, Fut>(&self, method: &str, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.check_shutdown(None)?;
        let retry_policy = self.pool.config().retry_policy().clone();
        self.with_metrics(
            method,
            crate::retry::with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&self.pool),
                method,
                operation,
            ),
        )
        .await
    }

    /// Creates a discovery service that shares this client's connection pool.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, DiscoveryConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-client").await?;
    /// let discovery = client.create_discovery_service(DiscoveryConfig::enabled());
    ///
    /// // Start background endpoint refresh
    /// discovery.start_background_refresh();
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create_discovery_service(
        &self,
        config: crate::config::DiscoveryConfig,
    ) -> crate::discovery::DiscoveryService {
        crate::discovery::DiscoveryService::new(self.pool.clone(), config)
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use inferadb_ledger_types::{Region, UserSlug};

    use super::*;
    use crate::config::RetryPolicy;

    const ORG: OrganizationSlug = OrganizationSlug::new(1);

    #[tokio::test]
    async fn test_new_with_valid_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.client_id(), "test-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_convenience_constructor() {
        let client = LedgerClient::connect("http://localhost:50051", "quick-client")
            .await
            .expect("client creation");

        assert_eq!(client.client_id(), "quick-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_with_invalid_endpoint() {
        let result = LedgerClient::connect("not-a-url", "test-client").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_accessor_returns_full_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("accessor-test")
            .timeout(Duration::from_secs(30))
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().timeout(), Duration::from_secs(30));
        assert!(client.config().compression());
    }

    #[tokio::test]
    async fn test_pool_accessor_returns_pool() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("pool-test")
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert!(client.pool().compression_enabled());
    }

    #[tokio::test]
    async fn test_create_discovery_service() {
        use crate::config::DiscoveryConfig;

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("discovery-test")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let discovery = client.create_discovery_service(DiscoveryConfig::enabled());

        assert!(discovery.config().is_enabled());
    }

    #[tokio::test]
    async fn test_new_preserves_retry_policy() {
        let retry_policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(100))
            .build();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("retry-test")
            .retry_policy(retry_policy)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().retry_policy().max_attempts, 5);
    }

    // =========================================================================
    // ReadConsistency Tests
    // =========================================================================

    #[test]
    fn test_read_consistency_default_is_eventual() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Eventual);
    }

    // =========================================================================
    // Connection Failure Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable
    // endpoints. They don't require a running server — they test the
    // retry/error paths.

    /// Creates a client configured for fast failure against an unreachable
    /// endpoint.
    ///
    /// Wire transport's [`WireClient::acquire`] retries indefinitely on
    /// connect failure (each per-attempt handshake is bounded by
    /// `connect_timeout`, but the loop wraps a `wait().await` between
    /// attempts and never gives up). To make sure the SDK's outer retry
    /// loop returns within the test's timeout window, we set
    /// `total_timeout` on the retry policy: that bounds the whole retry
    /// cycle (including in-flight `acquire`) via `tokio::time::timeout`
    /// and surfaces as [`SdkError::Timeout`](crate::error::SdkError::Timeout).
    async fn make_unreachable_client() -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("conn-failure-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .total_timeout(Duration::from_millis(500))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");
        LedgerClient::new(config).await.expect("client creation")
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_all_operations_return_error_on_connection_failure() {
        use std::{future::Future, pin::Pin};

        let cases: Vec<(
            &str,
            Box<dyn FnOnce(LedgerClient) -> Pin<Box<dyn Future<Output = bool>>>>,
        )> = vec![
            (
                "read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.read(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            "test-key",
                            None,
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "batch_read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.batch_read(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            vec!["key1", "key2", "key3"],
                            None,
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "write",
                Box::new(|c| {
                    Box::pin(async move {
                        let ops = vec![Operation::set_entity("key", b"value".to_vec(), None, None)];
                        c.write(UserSlug::new(42), ORG, Some(VaultSlug::new(0)), ops, None)
                            .await
                            .is_err()
                    })
                }),
            ),
            (
                "health_check",
                Box::new(|c| Box::pin(async move { c.health_check().await.is_err() })),
            ),
            (
                "health_check_detailed",
                Box::new(|c| Box::pin(async move { c.health_check_detailed().await.is_err() })),
            ),
        ];

        for (label, call_fn) in cases {
            let client = make_unreachable_client().await;
            assert!(call_fn(client).await, "{label}: expected connection error");
        }
    }

    // =========================================================================
    // Operation Builder Tests
    // =========================================================================

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_operation_construction() {
        // Each case: (label, operation, validation fn)
        let cases: Vec<(&str, Operation, Box<dyn Fn(&Operation)>)> = vec![
            (
                "set_entity basic",
                Operation::set_entity("user:123", b"data".to_vec(), None, None),
                Box::new(|op| match op {
                    Operation::SetEntity { key, value, expires_at, condition } => {
                        assert_eq!(key, "user:123");
                        assert_eq!(value, b"data");
                        assert!(expires_at.is_none());
                        assert!(condition.is_none());
                    },
                    _ => panic!("Expected SetEntity"),
                }),
            ),
            (
                "set_entity with expiry",
                Operation::set_entity("session:abc", b"token".to_vec(), Some(1700000000), None),
                Box::new(|op| match op {
                    Operation::SetEntity { key, value, expires_at, condition } => {
                        assert_eq!(key, "session:abc");
                        assert_eq!(value, b"token");
                        assert_eq!(*expires_at, Some(1700000000));
                        assert!(condition.is_none());
                    },
                    _ => panic!("Expected SetEntity"),
                }),
            ),
            (
                "set_entity if not exists",
                Operation::set_entity(
                    "lock:xyz",
                    b"owner".to_vec(),
                    None,
                    Some(SetCondition::NotExists),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity {
                        key, condition: Some(SetCondition::NotExists), ..
                    } => {
                        assert_eq!(key, "lock:xyz");
                    },
                    _ => panic!("Expected SetEntity with NotExists condition"),
                }),
            ),
            (
                "set_entity if version",
                Operation::set_entity(
                    "counter",
                    b"42".to_vec(),
                    None,
                    Some(SetCondition::Version(100)),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity { condition: Some(SetCondition::Version(v)), .. } => {
                        assert_eq!(*v, 100);
                    },
                    _ => panic!("Expected SetEntity with Version condition"),
                }),
            ),
            (
                "set_entity if value equals",
                Operation::set_entity(
                    "data",
                    b"new".to_vec(),
                    None,
                    Some(SetCondition::ValueEquals(b"old".to_vec())),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity {
                        condition: Some(SetCondition::ValueEquals(v)), ..
                    } => {
                        assert_eq!(v, b"old");
                    },
                    _ => panic!("Expected SetEntity with ValueEquals condition"),
                }),
            ),
            (
                "delete_entity",
                Operation::delete_entity("obsolete:key"),
                Box::new(|op| match op {
                    Operation::DeleteEntity { key } => {
                        assert_eq!(key, "obsolete:key");
                    },
                    _ => panic!("Expected DeleteEntity"),
                }),
            ),
            (
                "create_relationship",
                Operation::create_relationship("doc:456", "viewer", "user:123"),
                Box::new(|op| match op {
                    Operation::CreateRelationship { resource, relation, subject } => {
                        assert_eq!(resource, "doc:456");
                        assert_eq!(relation, "viewer");
                        assert_eq!(subject, "user:123");
                    },
                    _ => panic!("Expected CreateRelationship"),
                }),
            ),
            (
                "delete_relationship",
                Operation::delete_relationship("doc:456", "editor", "team:admins#member"),
                Box::new(|op| match op {
                    Operation::DeleteRelationship { resource, relation, subject } => {
                        assert_eq!(resource, "doc:456");
                        assert_eq!(relation, "editor");
                        assert_eq!(subject, "team:admins#member");
                    },
                    _ => panic!("Expected DeleteRelationship"),
                }),
            ),
        ];

        for (label, op, validate) in &cases {
            validate(op);
            assert!(!label.is_empty());
        }
    }

    #[test]
    fn test_set_condition_from_expected_none() {
        let cond = SetCondition::from_expected(None::<Vec<u8>>);
        assert!(matches!(cond, SetCondition::NotExists));
    }

    #[test]
    fn test_set_condition_from_expected_some_vec() {
        let cond = SetCondition::from_expected(Some(b"old-value".to_vec()));
        match cond {
            SetCondition::ValueEquals(v) => assert_eq!(v, b"old-value"),
            other => panic!("Expected ValueEquals, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_condition_from_expected_some_slice() {
        let slice: &[u8] = b"expected";
        let cond = SetCondition::from_expected(Some(slice.to_vec()));
        match cond {
            SetCondition::ValueEquals(v) => assert_eq!(v, b"expected"),
            other => panic!("Expected ValueEquals, got: {other:?}"),
        }
    }

    // =========================================================================
    // Default trait tests for status enums
    // =========================================================================

    #[test]
    fn test_organization_status_default() {
        let status: OrganizationStatus = Default::default();
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_default() {
        let status: VaultStatus = Default::default();
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_health_status_default() {
        let status: HealthStatus = Default::default();
        assert_eq!(status, HealthStatus::Unspecified);
    }

    // =========================================================================
    // HealthCheckResult tests (domain-only)
    // =========================================================================

    #[test]
    fn test_health_check_result_is_healthy() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(result.is_healthy());
        assert!(!result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_degraded() {
        let result = HealthCheckResult {
            status: HealthStatus::Degraded,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_unavailable() {
        let result = HealthCheckResult {
            status: HealthStatus::Unavailable,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(!result.is_degraded());
        assert!(result.is_unavailable());
    }

    // =========================================================================
    // Merkle proof verify tests (domain-only)
    // =========================================================================

    #[test]
    fn test_merkle_proof_verify_single_element_tree() {
        let proof = MerkleProof {
            leaf_hash: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            siblings: vec![],
        };
        let expected_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree_mismatch() {
        let proof = MerkleProof { leaf_hash: vec![1, 2, 3, 4], siblings: vec![] };
        let wrong_root = vec![5, 6, 7, 8];
        assert!(!proof.verify(&wrong_root));
    }

    #[test]
    fn test_merkle_proof_verify_with_siblings() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Right }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_left_sibling() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        let mut hasher = Sha256::new();
        hasher.update(&sibling_hash);
        hasher.update(&leaf_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Left }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_tampered_proof_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let correct_root = hasher.finalize().to_vec();

        let tampered_sibling = vec![2u8; 32];
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: tampered_sibling, direction: Direction::Right }],
        };

        assert!(!proof.verify(&correct_root));
    }

    #[test]
    fn test_merkle_proof_verify_wrong_direction_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Left }],
        };

        assert!(!proof.verify(&expected_root));
    }

    // =========================================================================
    // ChainProof verify tests (domain-only)
    // =========================================================================

    #[test]
    fn test_chain_proof_verify_empty() {
        let chain = ChainProof { headers: vec![] };
        let trusted_hash = vec![0; 32];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_first_links_to_trusted() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![1, 2, 3, 4],
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
                block_hash: vec![],
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_fails_if_first_not_linked() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0, 0, 0, 0],
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
                block_hash: vec![],
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_multi_header_server_hash() {
        let block_hash_0 = vec![42; 32];

        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: block_hash_0.clone(),
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: block_hash_0,
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![99; 32],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_multi_header_wrong_hash_fails() {
        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: vec![42; 32],
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0xFF; 32],
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![99; 32],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_empty_block_hash_fails() {
        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: vec![],
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![],
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32];
        assert!(!chain.verify(&trusted_hash));
    }

    // =========================================================================
    // VerifyOpts tests
    // =========================================================================

    #[test]
    fn test_verify_opts_default() {
        let opts = VerifyOpts::new();
        assert!(opts.at_height.is_none());
        assert!(!opts.include_chain_proof);
        assert!(opts.trusted_height.is_none());
    }

    #[test]
    fn test_verify_opts_at_height() {
        let opts = VerifyOpts::new().at_height(100);
        assert_eq!(opts.at_height, Some(100));
        assert!(!opts.include_chain_proof);
    }

    #[test]
    fn test_verify_opts_with_chain_proof() {
        let opts = VerifyOpts::new().with_chain_proof(50);
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verify_opts_builder_chain() {
        let opts = VerifyOpts::new().at_height(100).with_chain_proof(50);
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    // =========================================================================
    // VerifiedValue verify tests (domain-only)
    // =========================================================================

    #[test]
    fn test_verified_value_verify_succeeds_with_matching_root() {
        let state_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: state_root.clone(),
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
                block_hash: vec![],
            },
            merkle_proof: MerkleProof { leaf_hash: state_root, siblings: vec![] },
            chain_proof: None,
        };

        assert!(verified.verify().is_ok());
    }

    #[test]
    fn test_verified_value_verify_fails_with_mismatched_root() {
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: vec![1, 2, 3, 4],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
                block_hash: vec![],
            },
            merkle_proof: MerkleProof { leaf_hash: vec![5, 6, 7, 8], siblings: vec![] },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_err());
    }

    // =========================================================================
    // Query Types Tests (domain-only)
    // =========================================================================

    #[test]
    fn test_entity_is_expired_at() {
        let entity =
            Entity { key: "key".to_string(), value: vec![], expires_at: Some(1000), version: 1 };

        assert!(!entity.is_expired_at(999));
        assert!(entity.is_expired_at(1000));
        assert!(entity.is_expired_at(1001));
    }

    #[test]
    fn test_entity_is_expired_at_no_expiration() {
        let entity = Entity { key: "key".to_string(), value: vec![], expires_at: None, version: 1 };
        assert!(!entity.is_expired_at(u64::MAX));
    }

    #[test]
    fn test_relationship_new() {
        let rel = Relationship::new("document:1", "viewer", "user:alice");
        assert_eq!(rel.resource, "document:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:alice");
    }

    #[test]
    fn test_relationship_equality_and_hash() {
        use std::collections::HashSet;

        let rel1 = Relationship::new("doc:1", "editor", "user:bob");
        let rel2 = Relationship::new("doc:1", "editor", "user:bob");
        let rel3 = Relationship::new("doc:1", "viewer", "user:bob");

        assert_eq!(rel1, rel2);
        assert_ne!(rel1, rel3);

        let mut set = HashSet::new();
        set.insert(rel1.clone());
        assert!(set.contains(&rel2));
        assert!(!set.contains(&rel3));
    }

    #[test]
    fn test_paged_result_has_next_page() {
        let with_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: Some("token".to_string()),
            block_height: 100,
        };

        let without_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: None,
            block_height: 100,
        };

        assert!(with_next.has_next_page());
        assert!(!without_next.has_next_page());
    }

    #[test]
    fn test_list_entities_opts_builder() {
        let opts = ListEntitiesOpts::with_prefix("user:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .page_token("abc123")
            .linearizable();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_defaults() {
        let opts = ListEntitiesOpts::with_prefix("session:");

        assert_eq!(opts.key_prefix, "session:");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_builder() {
        let opts = ListRelationshipsOpts::new()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable);

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_defaults() {
        let opts = ListRelationshipsOpts::new();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_builder() {
        let opts = ListResourcesOpts::with_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .linearizable();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_defaults() {
        let opts = ListResourcesOpts::with_type("folder");

        assert_eq!(opts.resource_type, "folder");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    // =========================================================================
    // Shutdown Tests
    // =========================================================================

    #[tokio::test]
    async fn test_is_shutdown_false_initially() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        assert!(!client.is_shutdown(), "client should not be shutdown initially");
    }

    #[tokio::test]
    async fn test_is_shutdown_true_after_shutdown() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        client.shutdown().await;

        assert!(client.is_shutdown(), "client should be shutdown after calling shutdown()");
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        client.shutdown().await;
        client.shutdown().await;
        client.shutdown().await;

        assert!(client.is_shutdown());
    }

    #[tokio::test]
    async fn test_cloned_client_shares_shutdown_state() {
        let client1 = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        let client2 = client1.clone();

        assert!(!client1.is_shutdown());
        assert!(!client2.is_shutdown());

        client1.shutdown().await;

        assert!(client1.is_shutdown());
        assert!(client2.is_shutdown(), "cloned client should share shutdown state");
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_all_operations_return_shutdown_error() {
        use std::{future::Future, pin::Pin};

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        client.shutdown().await;

        let cases: Vec<(&str, Pin<Box<dyn Future<Output = bool> + '_>>)> = vec![
            (
                "read",
                Box::pin(async {
                    matches!(
                        client
                            .read(
                                UserSlug::new(42),
                                ORG,
                                Some(VaultSlug::new(0)),
                                "key",
                                None,
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "write",
                Box::pin(async {
                    matches!(
                        client
                            .write(
                                UserSlug::new(42),
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec![Operation::set_entity("key", vec![1, 2, 3], None, None)],
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "health_check",
                Box::pin(async {
                    matches!(client.health_check().await, Err(crate::error::SdkError::Shutdown))
                }),
            ),
        ];

        for (label, fut) in cases {
            assert!(fut.await, "{label}: expected Shutdown error");
        }
    }

    #[tokio::test]
    async fn test_shutdown_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Shutdown.is_retryable());
    }

    // =========================================================================
    // Cancellation tests
    // =========================================================================

    #[tokio::test]
    async fn test_cancellation_token_accessor() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = client.cancellation_token();

        assert!(!token.is_cancelled());

        client.shutdown().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_child_token_cancelled_on_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let child = client.cancellation_token().child_token();

        assert!(!child.is_cancelled());

        client.shutdown().await;
        assert!(child.is_cancelled());
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_pre_cancelled_token_returns_cancelled() {
        use std::{future::Future, pin::Pin};

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let cases: Vec<(
            &str,
            Box<
                dyn FnOnce(
                    LedgerClient,
                    tokio_util::sync::CancellationToken,
                ) -> Pin<Box<dyn Future<Output = bool>>>,
            >,
        )> = vec![
            (
                "read",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.read(UserSlug::new(42), ORG, None, "key", None, Some(t)).await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
            (
                "write",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.write(
                                UserSlug::new(42),
                                ORG,
                                None,
                                vec![Operation::set_entity("key", b"val".to_vec(), None, None)],
                                Some(t),
                            )
                            .await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
        ];

        for (label, call_fn) in cases {
            let token = tokio_util::sync::CancellationToken::new();
            token.cancel();
            assert!(call_fn(client.clone(), token).await, "{label}: expected Cancelled error");
        }
    }

    #[tokio::test]
    async fn test_cancelled_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Cancelled.is_retryable());
    }

    #[tokio::test]
    async fn test_cancelled_differs_from_shutdown() {
        let cancelled = crate::error::SdkError::Cancelled;
        let shutdown = crate::error::SdkError::Shutdown;

        assert!(!matches!(cancelled, crate::error::SdkError::Shutdown));
        assert!(!matches!(shutdown, crate::error::SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_read_with_cancellation_token_returns_cancelled_during_backoff() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        let token_clone = token.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            token_clone.cancel();
        });

        let start = std::time::Instant::now();
        let result = client.read(UserSlug::new(42), ORG, None, "key", None, Some(token)).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "should fail when cancellation token is triggered during retries");
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_shutdown_cancels_inflight_retries() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let client_clone = client.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            client_clone.shutdown().await;
        });

        let start = std::time::Instant::now();
        let result = client.read(UserSlug::new(42), ORG, None, "key", None, None).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "should fail when shutdown is triggered during retries");
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    // =========================================================================
    // Operation validation tests
    // =========================================================================

    #[test]
    fn test_operation_validate_set_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user:123", b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_set_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::SetEntity {
            key: String::new(),
            value: b"data".to_vec(),
            expires_at: None,
            condition: None,
        };
        let err = op.validate(&config).unwrap_err();
        assert!(err.to_string().contains("key"), "Error should mention key: {err}");
    }

    #[test]
    fn test_operation_validate_set_entity_invalid_key_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user 123", b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_key_too_long() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_key_bytes(10)
            .build()
            .unwrap();
        let op = Operation::set_entity("a".repeat(11), b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_value_too_large() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_value_bytes(4)
            .build()
            .unwrap();
        let op = Operation::set_entity("key", vec![0u8; 5], None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_entity("user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_delete_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::DeleteEntity { key: String::new() };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_create_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_with_hash() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123#member");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_empty_resource() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::CreateRelationship {
            resource: String::new(),
            relation: "viewer".to_string(),
            subject: "user:123".to_string(),
        };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_relationship_invalid_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc 456", "viewer", "user:123");
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    // =========================================================================
    // estimated_size_bytes tests
    // =========================================================================

    #[test]
    fn test_estimated_size_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec(), None, None);
        assert_eq!(op.estimated_size_bytes(), 3 + 5);
    }

    #[test]
    fn test_estimated_size_delete_entity() {
        let op = Operation::delete_entity("user:123");
        assert_eq!(op.estimated_size_bytes(), 8);
    }

    #[test]
    fn test_estimated_size_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert_eq!(op.estimated_size_bytes(), 7 + 6 + 8);
    }

    // =========================================================================
    // SdkError::Validation tests
    // =========================================================================

    #[test]
    fn test_sdk_validation_error_not_retryable() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sdk_validation_error_display() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(err.to_string().contains("key too long"));
    }

    // Region import sanity-check (avoid unused-import warning when no Region
    // tests are present here).
    #[test]
    fn region_import_used() {
        let _ = Region::US_EAST_VA;
    }
}
