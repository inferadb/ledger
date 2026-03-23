//! Core [`LedgerClient`] struct, lifecycle, and gRPC client factories.
//!
//! Domain-specific methods are in the [`ops`] module. Domain types are
//! in the [`types`] module. Shared proto conversion helpers are in
//! [`proto_util`].

use std::sync::Arc;

use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

// Re-export domain types for the test module's `use super::*`.
#[cfg(test)]
pub use crate::types::admin::*;
#[cfg(test)]
pub use crate::types::events::*;
#[cfg(test)]
pub use crate::types::query::*;
#[cfg(test)]
pub use crate::types::read::*;
#[cfg(test)]
pub use crate::types::streaming::BlockAnnouncement;
#[cfg(test)]
pub use crate::types::verified_read::*;
use crate::{
    config::ClientConfig,
    connection::ConnectionPool,
    error::{self, Result},
    server::{ServerResolver, ServerSource},
    tracing::TraceContextInterceptor,
};

/// Generates a `LedgerClient` method that creates a gRPC service client with
/// optional compression and tracing. All service clients follow the same pattern:
/// attach the tracing interceptor, then conditionally enable gzip compression.
#[doc(hidden)]
#[macro_export]
macro_rules! create_grpc_client {
    ($fn_name:ident, $mod:ident, $client:ident) => {
        pub(crate) fn $fn_name(
            channel: tonic::transport::Channel,
            compression_enabled: bool,
            interceptor: $crate::tracing::TraceContextInterceptor,
        ) -> inferadb_ledger_proto::proto::$mod::$client<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                $crate::tracing::TraceContextInterceptor,
            >,
        > {
            let client =
                inferadb_ledger_proto::proto::$mod::$client::with_interceptor(channel, interceptor);
            if compression_enabled {
                client
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            } else {
                client
            }
        }
    };
}

/// Acquires a channel from the connection pool and constructs a typed gRPC client.
///
/// Replaces the 5-line boilerplate of `pool.get_channel()` + `Self::create_xxx_client(...)`.
#[doc(hidden)]
#[macro_export]
macro_rules! connected_client {
    ($pool:expr, $create_fn:ident) => {{
        let channel = $pool.get_channel().await?;
        Self::$create_fn(
            channel,
            $pool.compression_enabled(),
            $crate::tracing::TraceContextInterceptor::with_timeout(
                $pool.config().trace(),
                $pool.config().timeout(),
            ),
        )
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
/// 4. Sequence tracker state is flushed to disk (if using persistence)
/// 5. Connections are closed
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
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
    /// let result = client.read(organization, None, "key", None, Some(token)).await;
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
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

    /// Creates a trace context interceptor based on the client's configuration.
    #[inline]
    pub(crate) fn trace_interceptor(&self) -> TraceContextInterceptor {
        TraceContextInterceptor::with_timeout(
            self.pool.config().trace(),
            self.pool.config().timeout(),
        )
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

    // =========================================================================
    // gRPC Client Factories
    // =========================================================================

    create_grpc_client!(create_read_client, read_service_client, ReadServiceClient);
    create_grpc_client!(create_write_client, write_service_client, WriteServiceClient);
    create_grpc_client!(create_admin_client, admin_service_client, AdminServiceClient);
    create_grpc_client!(
        create_organization_client,
        organization_service_client,
        OrganizationServiceClient
    );
    create_grpc_client!(create_vault_client, vault_service_client, VaultServiceClient);
    create_grpc_client!(create_user_client, user_service_client, UserServiceClient);
    create_grpc_client!(create_app_client, app_service_client, AppServiceClient);
    create_grpc_client!(create_events_client, events_service_client, EventsServiceClient);
    create_grpc_client!(create_health_client, health_service_client, HealthServiceClient);
    create_grpc_client!(create_token_client, token_service_client, TokenServiceClient);
    create_grpc_client!(
        create_invitation_client,
        invitation_service_client,
        InvitationServiceClient
    );
}
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use inferadb_ledger_proto::proto;
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

    #[test]
    fn test_read_consistency_to_proto_eventual() {
        let consistency = ReadConsistency::Eventual;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Eventual as i32);
    }

    #[test]
    fn test_read_consistency_to_proto_linearizable() {
        let consistency = ReadConsistency::Linearizable;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Linearizable as i32);
    }

    // =========================================================================
    // Connection Failure Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    /// Creates a client configured for fast failure against an unreachable endpoint.
    async fn make_unreachable_client() -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("conn-failure-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
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
                "read (linearizable)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.read(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            "test-key",
                            Some(ReadConsistency::Linearizable),
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "read (none vault)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.read(UserSlug::new(42), ORG, None, "user:123", None, None).await.is_err()
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
                "batch_read (linearizable)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.batch_read(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            vec!["key1", "key2"],
                            Some(ReadConsistency::Linearizable),
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
                "write (multiple ops)",
                Box::new(|c| {
                    Box::pin(async move {
                        let ops = vec![
                            Operation::set_entity("user:1", b"alice".to_vec(), None, None),
                            Operation::set_entity("user:2", b"bob".to_vec(), None, None),
                            Operation::create_relationship("doc:1", "viewer", "user:1"),
                            Operation::create_relationship("doc:1", "editor", "user:2"),
                        ];
                        c.write(UserSlug::new(42), ORG, Some(VaultSlug::new(0)), ops, None)
                            .await
                            .is_err()
                    })
                }),
            ),
            (
                "batch_write",
                Box::new(|c| {
                    Box::pin(async move {
                        let batches =
                            vec![vec![Operation::set_entity("key", b"value".to_vec(), None, None)]];
                        c.batch_write(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            batches,
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "batch_write (multiple groups)",
                Box::new(|c| {
                    Box::pin(async move {
                        let batches = vec![
                            vec![Operation::set_entity("user:123", b"alice".to_vec(), None, None)],
                            vec![
                                Operation::create_relationship("doc:456", "viewer", "user:123"),
                                Operation::create_relationship("folder:789", "editor", "user:123"),
                            ],
                        ];
                        c.batch_write(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            batches,
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "watch_blocks",
                Box::new(|c| {
                    Box::pin(async move {
                        c.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(0), 1).await.is_err()
                    })
                }),
            ),
            (
                "watch_blocks (different vaults)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(1), 1).await.is_err()
                            && c.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(2), 1)
                                .await
                                .is_err()
                    })
                }),
            ),
            (
                "watch_blocks (start heights)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(0), 1).await.is_err()
                            && c.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(0), 100)
                                .await
                                .is_err()
                    })
                }),
            ),
            (
                "create_organization",
                Box::new(|c| {
                    Box::pin(async move {
                        c.create_organization(
                            "test-ns",
                            Region::US_EAST_VA,
                            UserSlug::new(0),
                            OrganizationTier::Free,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "get_organization",
                Box::new(|c| {
                    Box::pin(
                        async move { c.get_organization(ORG, UserSlug::new(0)).await.is_err() },
                    )
                }),
            ),
            (
                "list_organizations",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_organizations(UserSlug::new(0), 0, None).await.is_err()
                    })
                }),
            ),
            (
                "create_vault",
                Box::new(|c| {
                    Box::pin(async move { c.create_vault(UserSlug::new(42), ORG).await.is_err() })
                }),
            ),
            (
                "get_vault",
                Box::new(|c| {
                    Box::pin(async move {
                        c.get_vault(UserSlug::new(42), ORG, VaultSlug::new(1)).await.is_err()
                    })
                }),
            ),
            (
                "list_vaults",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_vaults(UserSlug::new(42), 0, None, None).await.is_err()
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
            (
                "health_check_vault",
                Box::new(|c| {
                    Box::pin(
                        async move { c.health_check_vault(ORG, VaultSlug::new(0)).await.is_err() },
                    )
                }),
            ),
            (
                "verified_read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.verified_read(
                            UserSlug::new(42),
                            ORG,
                            Some(VaultSlug::new(0)),
                            "key",
                            VerifyOpts::new(),
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "list_entities",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_entities(
                            UserSlug::new(42),
                            ORG,
                            ListEntitiesOpts::with_prefix("user:"),
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "list_entities (with options)",
                Box::new(|c| {
                    Box::pin(async move {
                        let opts = ListEntitiesOpts::with_prefix("session:")
                            .at_height(100)
                            .include_expired()
                            .limit(50)
                            .linearizable();
                        c.list_entities(UserSlug::new(42), ORG, opts).await.is_err()
                    })
                }),
            ),
            (
                "list_relationships",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_relationships(
                            UserSlug::new(42),
                            ORG,
                            VaultSlug::new(0),
                            ListRelationshipsOpts::new(),
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "list_relationships (with filters)",
                Box::new(|c| {
                    Box::pin(async move {
                        let opts = ListRelationshipsOpts::new()
                            .resource("document:1")
                            .relation("viewer")
                            .limit(100);
                        c.list_relationships(UserSlug::new(42), ORG, VaultSlug::new(0), opts)
                            .await
                            .is_err()
                    })
                }),
            ),
            (
                "list_resources",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_resources(
                            UserSlug::new(42),
                            ORG,
                            VaultSlug::new(0),
                            ListResourcesOpts::with_type("document"),
                        )
                        .await
                        .is_err()
                    })
                }),
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
            // Verify the label is non-empty (forces use of label binding)
            assert!(!label.is_empty());
        }
    }

    #[test]
    fn test_operation_to_proto_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec(), None, None);
        let proto_op = op.to_proto();

        assert!(proto_op.op.is_some());
        match proto_op.op.unwrap() {
            proto::operation::Op::SetEntity(set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, b"value");
            },
            _ => panic!("Expected SetEntity proto"),
        }
    }

    #[test]
    fn test_operation_to_proto_create_relationship() {
        let op = Operation::create_relationship("res", "rel", "sub");
        let proto_op = op.to_proto();

        match proto_op.op.unwrap() {
            proto::operation::Op::CreateRelationship(rel) => {
                assert_eq!(rel.resource, "res");
                assert_eq!(rel.relation, "rel");
                assert_eq!(rel.subject, "sub");
            },
            _ => panic!("Expected CreateRelationship proto"),
        }
    }

    #[test]
    fn test_set_condition_to_proto() {
        let not_exists = SetCondition::NotExists;
        let proto_cond = not_exists.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::NotExists(true))
        ));

        let must_exist = SetCondition::MustExist;
        let proto_cond = must_exist.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::MustExists(true))
        ));

        let version = SetCondition::Version(42);
        let proto_cond = version.to_proto();
        assert!(matches!(proto_cond.condition, Some(proto::set_condition::Condition::Version(42))));

        let value_eq = SetCondition::ValueEquals(b"test".to_vec());
        let proto_cond = value_eq.to_proto();
        match proto_cond.condition {
            Some(proto::set_condition::Condition::ValueEquals(v)) => {
                assert_eq!(v, b"test");
            },
            _ => panic!("Expected ValueEquals"),
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
    // WriteSuccess Tests
    // =========================================================================

    #[test]
    fn test_write_success_fields() {
        let success =
            WriteSuccess { tx_id: "abc123".to_string(), block_height: 42, assigned_sequence: 5 };

        assert_eq!(success.tx_id, "abc123");
        assert_eq!(success.block_height, 42);
        assert_eq!(success.assigned_sequence, 5);
    }

    #[test]
    fn test_tx_id_to_hex() {
        // Test with Some(TxId)
        let tx_id = proto::TxId { id: vec![0x12, 0x34, 0xab, 0xcd] };
        let hex = LedgerClient::tx_id_to_hex(Some(tx_id));
        assert_eq!(hex, "1234abcd");

        // Test with None
        let hex = LedgerClient::tx_id_to_hex(None);
        assert_eq!(hex, "");
    }

    // (write/batch_write connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // BlockAnnouncement Tests
    // =========================================================================

    #[test]
    fn test_block_announcement_from_proto_with_all_fields() {
        use prost_types::Timestamp;

        let proto_announcement = proto::BlockAnnouncement {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            height: 100,
            block_hash: Some(proto::Hash { value: vec![0x12, 0x34] }),
            state_root: Some(proto::Hash { value: vec![0xab, 0xcd] }),
            timestamp: Some(Timestamp { seconds: 1700000000, nanos: 123_456_789 }),
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, ORG);
        assert_eq!(announcement.vault, VaultSlug::new(2));
        assert_eq!(announcement.height, 100);
        assert_eq!(announcement.block_hash, vec![0x12, 0x34]);
        assert_eq!(announcement.state_root, vec![0xab, 0xcd]);
        assert!(announcement.timestamp.is_some());
    }

    #[test]
    fn test_block_announcement_from_proto_with_missing_optional_fields() {
        let proto_announcement = proto::BlockAnnouncement {
            organization: None,
            vault: None,
            height: 50,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, OrganizationSlug::new(0));
        assert_eq!(announcement.vault, VaultSlug::new(0));
        assert_eq!(announcement.height, 50);
        assert!(announcement.block_hash.is_empty());
        assert!(announcement.state_root.is_empty());
        assert!(announcement.timestamp.is_none());
    }

    #[test]
    fn test_block_announcement_equality() {
        let a = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        let b = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        assert_eq!(a, b);
    }

    #[test]
    fn test_block_announcement_clone() {
        let original = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12, 0x34],
            state_root: vec![0xab, 0xcd],
            timestamp: None,
        };

        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    // (watch_blocks connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Admin Operation Tests
    // =========================================================================

    #[test]
    fn test_organization_status_from_proto_active() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Active as i32);
        assert_eq!(status, OrganizationStatus::Active);
    }

    #[test]
    fn test_organization_status_from_proto_deleted() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Deleted as i32);
        assert_eq!(status, OrganizationStatus::Deleted);
    }

    #[test]
    fn test_organization_status_from_proto_unspecified() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Unspecified as i32);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_from_proto_invalid() {
        let status = OrganizationStatus::from_proto(999);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_default() {
        let status: OrganizationStatus = Default::default();
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_active() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Active as i32);
        assert_eq!(status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_status_from_proto_read_only() {
        let status = VaultStatus::from_proto(proto::VaultStatus::ReadOnly as i32);
        assert_eq!(status, VaultStatus::ReadOnly);
    }

    #[test]
    fn test_vault_status_from_proto_deleted() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Deleted as i32);
        assert_eq!(status, VaultStatus::Deleted);
    }

    #[test]
    fn test_vault_status_from_proto_unspecified() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Unspecified as i32);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_invalid() {
        let status = VaultStatus::from_proto(999);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_default() {
        let status: VaultStatus = Default::default();
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_from_proto() {
        let proto = proto::GetOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: 42 }),
            name: "test-organization".to_string(),
            region: proto::Region::Global.into(),
            member_nodes: vec![
                proto::NodeId { id: "node-100".to_string() },
                proto::NodeId { id: "node-101".to_string() },
            ],
            status: proto::OrganizationStatus::Active as i32,
            config_version: 5,
            created_at: None,
            tier: 0,
            members: vec![
                proto::OrganizationMember {
                    user: Some(proto::UserSlug { slug: 100 }),
                    role: proto::OrganizationMemberRole::Admin.into(),
                    joined_at: None,
                },
                proto::OrganizationMember {
                    user: Some(proto::UserSlug { slug: 200 }),
                    role: proto::OrganizationMemberRole::Member.into(),
                    joined_at: None,
                },
            ],
            updated_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(42));
        assert_eq!(info.name, "test-organization");
        assert_eq!(info.region, Region::GLOBAL);
        assert_eq!(info.member_nodes, vec!["node-100", "node-101"]);
        assert_eq!(info.config_version, 5);
        assert_eq!(info.status, OrganizationStatus::Active);
        assert_eq!(info.members.len(), 2);
        assert_eq!(info.members[0].user, UserSlug::new(100));
        assert_eq!(info.members[0].role, OrganizationMemberRole::Admin);
        assert_eq!(info.members[1].user, UserSlug::new(200));
        assert_eq!(info.members[1].role, OrganizationMemberRole::Member);
    }

    #[test]
    fn test_organization_info_from_proto_with_missing_fields() {
        let proto = proto::GetOrganizationResponse {
            slug: None,
            name: "minimal".to_string(),
            region: proto::Region::Unspecified.into(),
            member_nodes: vec![],
            status: proto::OrganizationStatus::Unspecified as i32,
            config_version: 0,
            created_at: None,
            tier: 0,
            members: vec![],
            updated_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(0));
        assert_eq!(info.name, "minimal");
        // Unspecified (0) falls back to GLOBAL
        assert_eq!(info.region, Region::GLOBAL);
        assert!(info.member_nodes.is_empty());
        assert_eq!(info.config_version, 0);
        assert_eq!(info.status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_info_from_proto() {
        let proto = proto::GetVaultResponse {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 10 }),
            height: 1000,
            state_root: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            nodes: vec![
                proto::NodeId { id: "node-200".to_string() },
                proto::NodeId { id: "node-201".to_string() },
            ],
            leader: Some(proto::NodeId { id: "node-200".to_string() }),
            status: proto::VaultStatus::Active as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, ORG);
        assert_eq!(info.vault, VaultSlug::new(10));
        assert_eq!(info.height, 1000);
        assert_eq!(info.state_root, vec![1, 2, 3, 4]);
        assert_eq!(info.nodes, vec!["node-200", "node-201"]);
        assert_eq!(info.leader, Some("node-200".to_string()));
        assert_eq!(info.status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_info_from_proto_with_missing_fields() {
        let proto = proto::GetVaultResponse {
            organization: None,
            vault: None,
            height: 0,
            state_root: None,
            nodes: vec![],
            leader: None,
            status: proto::VaultStatus::Unspecified as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, OrganizationSlug::new(0));
        assert_eq!(info.vault, VaultSlug::new(0));
        assert_eq!(info.height, 0);
        assert!(info.state_root.is_empty());
        assert!(info.nodes.is_empty());
        assert_eq!(info.leader, None);
        assert_eq!(info.status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_equality() {
        let info1 = OrganizationInfo {
            slug: ORG,
            name: "test".to_string(),
            region: Region::GLOBAL,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            config_version: 1,
            status: OrganizationStatus::Active,
            tier: OrganizationTier::Free,
            members: vec![OrganizationMemberInfo {
                user: UserSlug::new(42),
                role: OrganizationMemberRole::Admin,
                joined_at: None,
            }],
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[test]
    fn test_vault_info_equality() {
        let info1 = VaultInfo {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            state_root: vec![1, 2, 3],
            nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader: Some("node-1".to_string()),
            status: VaultStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    // (admin connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // HealthStatus Tests
    // =========================================================================

    #[test]
    fn test_health_status_from_proto_healthy() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Healthy as i32);
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_status_from_proto_degraded() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Degraded as i32);
        assert_eq!(status, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_status_from_proto_unavailable() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unavailable as i32);
        assert_eq!(status, HealthStatus::Unavailable);
    }

    #[test]
    fn test_health_status_from_proto_unspecified() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unspecified as i32);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_from_proto_invalid() {
        let status = HealthStatus::from_proto(999);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_default() {
        let status: HealthStatus = Default::default();
        assert_eq!(status, HealthStatus::Unspecified);
    }

    // =========================================================================
    // HealthCheckResult Tests
    // =========================================================================

    #[test]
    fn test_health_check_result_from_proto() {
        let mut details = std::collections::HashMap::new();
        details.insert("current_term".to_string(), "5".to_string());
        details.insert("leader_id".to_string(), "node-1".to_string());

        let proto = proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Node is healthy".to_string(),
            details: details.clone(),
        };

        let result = HealthCheckResult::from_proto(proto);

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.message, "Node is healthy");
        assert_eq!(result.details, details);
    }

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
    // Health Check Integration Tests
    // =========================================================================

    // (health connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Verified Read Tests
    // =========================================================================

    #[test]
    fn test_direction_from_proto_left() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Left as i32);
        assert_eq!(direction, Direction::Left);
    }

    #[test]
    fn test_direction_from_proto_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Right as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_direction_from_proto_unspecified_defaults_to_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Unspecified as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_merkle_sibling_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_sibling = proto::MerkleSibling {
            hash: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            direction: proto::Direction::Left as i32,
        };
        let sibling = MerkleSibling::from_proto(proto_sibling);
        assert_eq!(sibling.hash, vec![1, 2, 3, 4]);
        assert_eq!(sibling.direction, Direction::Left);
    }

    #[test]
    fn test_merkle_proof_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: vec![0; 32] }),
            siblings: vec![
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![1; 32] }),
                    direction: proto::Direction::Left as i32,
                },
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![2; 32] }),
                    direction: proto::Direction::Right as i32,
                },
            ],
        };
        let proof = MerkleProof::from_proto(proto_proof);
        assert_eq!(proof.leaf_hash, vec![0; 32]);
        assert_eq!(proof.siblings.len(), 2);
        assert_eq!(proof.siblings[0].direction, Direction::Left);
        assert_eq!(proof.siblings[1].direction, Direction::Right);
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree() {
        // Single element tree: leaf hash equals root
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

        // Create a simple two-leaf tree
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(leaf || sibling) since sibling is on right
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

        // Create a proof where sibling is on the left
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(sibling || leaf) since sibling is on left
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

        // Compute correct root
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let correct_root = hasher.finalize().to_vec();

        // Tamper with the sibling hash
        let tampered_sibling = vec![2u8; 32];
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: tampered_sibling, direction: Direction::Right }],
        };

        // Should not verify against correct root
        assert!(!proof.verify(&correct_root));
    }

    #[test]
    fn test_merkle_proof_verify_wrong_direction_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute root with sibling on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        // Create proof with wrong direction (Left instead of Right)
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling {
                hash: sibling_hash,
                direction: Direction::Left, // Wrong!
            }],
        };

        // Should fail verification
        assert!(!proof.verify(&expected_root));
    }

    #[test]
    fn test_block_header_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 100,
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            previous_hash: Some(proto::Hash { value: vec![1; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
            state_root: Some(proto::Hash { value: vec![3; 32] }),
            timestamp: Some(prost_types::Timestamp { seconds: 1704067200, nanos: 0 }),
            leader_id: Some(proto::NodeId { id: "node-1".to_string() }),
            term: 5,
            committed_index: 99,
            block_hash: Some(proto::Hash { value: vec![10; 32] }),
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 100);
        assert_eq!(header.organization, ORG);
        assert_eq!(header.vault, VaultSlug::new(2));
        assert_eq!(header.previous_hash, vec![1; 32]);
        assert_eq!(header.tx_merkle_root, vec![2; 32]);
        assert_eq!(header.state_root, vec![3; 32]);
        assert!(header.timestamp.is_some());
        assert_eq!(header.leader_id, "node-1");
        assert_eq!(header.term, 5);
        assert_eq!(header.committed_index, 99);
    }

    #[test]
    fn test_block_header_from_proto_with_missing_fields() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 1,
            organization: None,
            vault: None,
            previous_hash: None,
            tx_merkle_root: None,
            state_root: None,
            timestamp: None,
            leader_id: None,
            term: 0,
            committed_index: 0,
            block_hash: None,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 1);
        assert_eq!(header.organization, OrganizationSlug::new(0));
        assert_eq!(header.vault, VaultSlug::new(0));
        assert!(header.previous_hash.is_empty());
        assert!(header.tx_merkle_root.is_empty());
        assert!(header.state_root.is_empty());
        assert!(header.timestamp.is_none());
        assert!(header.leader_id.is_empty());
    }

    #[test]
    fn test_chain_proof_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_chain = proto::ChainProof {
            headers: vec![
                proto::BlockHeader {
                    height: 101,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![0; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![1; 32] }),
                    state_root: Some(proto::Hash { value: vec![2; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 100,
                    block_hash: None,
                },
                proto::BlockHeader {
                    height: 102,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![3; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![4; 32] }),
                    state_root: Some(proto::Hash { value: vec![5; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 101,
                    block_hash: None,
                },
            ],
        };

        let chain = ChainProof::from_proto(proto_chain);
        assert_eq!(chain.headers.len(), 2);
        assert_eq!(chain.headers[0].height, 101);
        assert_eq!(chain.headers[1].height, 102);
    }

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
                previous_hash: vec![1, 2, 3, 4], // Must match trusted_hash
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
                previous_hash: vec![0, 0, 0, 0], // Wrong hash
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
        // Build a two-header chain where header[1].previous_hash matches
        // header[0].block_hash (provided by server).
        let block_hash_0 = vec![42; 32]; // Simulated server-computed hash

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
            previous_hash: block_hash_0, // Links to header0.block_hash
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![99; 32],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32]; // header[0].previous_hash
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
            previous_hash: vec![0xFF; 32], // Wrong — doesn't match header0.block_hash
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
        // If server didn't provide block_hash, verification should fail
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
            block_hash: vec![], // Empty — server didn't provide
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![], // Matches empty block_hash, but should still fail
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

    #[test]
    fn test_verified_value_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
                block_hash: None,
            }),
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_some());
        let v = verified.unwrap();
        assert_eq!(v.value, Some(b"test-value".to_vec()));
        assert_eq!(v.block_height, 100);
        assert_eq!(v.block_header.height, 100);
        assert_eq!(v.merkle_proof.leaf_hash, vec![4; 32]);
        assert!(v.chain_proof.is_none());
    }

    #[test]
    fn test_verified_value_from_proto_missing_header() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: None, // Missing
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if header missing
    }

    #[test]
    fn test_verified_value_from_proto_missing_proof() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
                block_hash: None,
            }),
            merkle_proof: None, // Missing
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if proof missing
    }

    #[test]
    fn test_verified_value_verify_succeeds_with_matching_root() {
        // Create a verified value where the merkle proof matches the state root
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
            merkle_proof: MerkleProof {
                leaf_hash: state_root, // Single element tree: leaf == root
                siblings: vec![],
            },
            chain_proof: None,
        };

        assert!(verified.verify().is_ok());
    }

    #[test]
    fn test_verified_value_verify_fails_with_mismatched_root() {
        // Create a verified value where the merkle proof does NOT match the state root
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: vec![1, 2, 3, 4], // Expected root
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
                block_hash: vec![],
            },
            merkle_proof: MerkleProof {
                leaf_hash: vec![5, 6, 7, 8], // Different hash!
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_err());
    }

    // (verified_read connection failure test consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Query Types Tests
    // =========================================================================

    #[test]
    fn test_entity_from_proto() {
        let proto_entity = proto::Entity {
            key: "user:123".to_string(),
            value: b"data".to_vec(),
            expires_at: Some(1700000000),
            version: 42,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.key, "user:123");
        assert_eq!(entity.value, b"data");
        assert_eq!(entity.expires_at, Some(1700000000));
        assert_eq!(entity.version, 42);
    }

    #[test]
    fn test_entity_from_proto_no_expiration() {
        let proto_entity = proto::Entity {
            key: "session:abc".to_string(),
            value: vec![],
            expires_at: None,
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_from_proto_zero_expiration_treated_as_none() {
        let proto_entity = proto::Entity {
            key: "key".to_string(),
            value: vec![],
            expires_at: Some(0),
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        // Zero expiration is treated as "no expiration"
        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_is_expired_at() {
        let entity =
            Entity { key: "key".to_string(), value: vec![], expires_at: Some(1000), version: 1 };

        // Before expiration
        assert!(!entity.is_expired_at(999));
        // At expiration
        assert!(entity.is_expired_at(1000));
        // After expiration
        assert!(entity.is_expired_at(1001));
    }

    #[test]
    fn test_entity_is_expired_at_no_expiration() {
        let entity = Entity { key: "key".to_string(), value: vec![], expires_at: None, version: 1 };

        // Never expires
        assert!(!entity.is_expired_at(u64::MAX));
    }

    #[test]
    fn test_entity_equality() {
        let entity1 = Entity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            expires_at: Some(1000),
            version: 1,
        };
        let entity2 = entity1.clone();

        assert_eq!(entity1, entity2);
    }

    #[test]
    fn test_relationship_new() {
        let rel = Relationship::new("document:1", "viewer", "user:alice");

        assert_eq!(rel.resource, "document:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:alice");
    }

    #[test]
    fn test_relationship_from_proto() {
        let proto_rel = proto::Relationship {
            resource: "folder:root".to_string(),
            relation: "owner".to_string(),
            subject: "user:admin".to_string(),
        };

        let rel = Relationship::from_proto(proto_rel);

        assert_eq!(rel.resource, "folder:root");
        assert_eq!(rel.relation, "owner");
        assert_eq!(rel.subject, "user:admin");
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

    #[test]
    fn test_list_entities_opts_bon_builder() {
        let opts = ListEntitiesOpts::builder()
            .key_prefix("user:")
            .at_height(100)
            .include_expired(true)
            .limit(50)
            .page_token("abc123")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_defaults() {
        let opts = ListEntitiesOpts::builder().build();

        assert_eq!(opts.key_prefix, "");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_matches_default() {
        let from_builder = ListEntitiesOpts::builder().build();
        let from_default = ListEntitiesOpts::default();

        assert_eq!(from_builder.key_prefix, from_default.key_prefix);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.include_expired, from_default.include_expired);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder() {
        let opts = ListRelationshipsOpts::builder()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_defaults() {
        let opts = ListRelationshipsOpts::builder().build();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_matches_default() {
        let from_builder = ListRelationshipsOpts::builder().build();
        let from_default = ListRelationshipsOpts::default();

        assert_eq!(from_builder.resource, from_default.resource);
        assert_eq!(from_builder.relation, from_default.relation);
        assert_eq!(from_builder.subject, from_default.subject);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_resources_opts_bon_builder() {
        let opts = ListResourcesOpts::builder()
            .resource_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_defaults() {
        let opts = ListResourcesOpts::builder().build();

        assert_eq!(opts.resource_type, "");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_matches_default() {
        let from_builder = ListResourcesOpts::builder().build();
        let from_default = ListResourcesOpts::default();

        assert_eq!(from_builder.resource_type, from_default.resource_type);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    // (query operation connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

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

        // Multiple shutdown calls should not panic
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

        // Shutdown through client1
        client1.shutdown().await;

        // Both should reflect shutdown state
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
                "batch_write",
                Box::pin(async {
                    matches!(
                        client
                            .batch_write(
                                UserSlug::new(42),
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec![vec![Operation::set_entity("key", vec![1, 2, 3], None, None)]],
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "batch_read",
                Box::pin(async {
                    matches!(
                        client
                            .batch_read(
                                UserSlug::new(42),
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec!["key1".to_string(), "key2".to_string()],
                                None,
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "watch_blocks",
                Box::pin(async {
                    matches!(
                        client.watch_blocks(UserSlug::new(42), ORG, VaultSlug::new(0), 1).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "create_organization",
                Box::pin(async {
                    matches!(
                        client
                            .create_organization(
                                "test",
                                Region::US_EAST_VA,
                                UserSlug::new(0),
                                OrganizationTier::Free
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "get_organization",
                Box::pin(async {
                    matches!(
                        client.get_organization(ORG, UserSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_organizations",
                Box::pin(async {
                    matches!(
                        client.list_organizations(UserSlug::new(0), 0, None).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "create_vault",
                Box::pin(async {
                    matches!(
                        client.create_vault(UserSlug::new(42), ORG).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "get_vault",
                Box::pin(async {
                    matches!(
                        client.get_vault(UserSlug::new(42), ORG, VaultSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_vaults",
                Box::pin(async {
                    matches!(
                        client.list_vaults(UserSlug::new(42), 0, None, None).await,
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
            (
                "health_check_detailed",
                Box::pin(async {
                    matches!(
                        client.health_check_detailed().await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "health_check_vault",
                Box::pin(async {
                    matches!(
                        client.health_check_vault(ORG, VaultSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "verified_read",
                Box::pin(async {
                    matches!(
                        client
                            .verified_read(
                                UserSlug::new(42),
                                ORG,
                                Some(VaultSlug::new(0)),
                                "key",
                                VerifyOpts::new()
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_entities",
                Box::pin(async {
                    matches!(
                        client
                            .list_entities(
                                UserSlug::new(42),
                                ORG,
                                ListEntitiesOpts::with_prefix("key")
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_relationships",
                Box::pin(async {
                    matches!(
                        client
                            .list_relationships(
                                UserSlug::new(42),
                                ORG,
                                VaultSlug::new(0),
                                ListRelationshipsOpts::new()
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_resources",
                Box::pin(async {
                    matches!(
                        client
                            .list_resources(
                                UserSlug::new(42),
                                ORG,
                                VaultSlug::new(0),
                                ListResourcesOpts::with_type("doc")
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
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

        // Token should not be cancelled initially
        assert!(!token.is_cancelled());

        // After shutdown, the token should be cancelled
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
            (
                "batch_read",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.batch_read(
                                UserSlug::new(42),
                                ORG,
                                None,
                                vec!["key1", "key2"],
                                None,
                                Some(t)
                            )
                            .await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
            (
                "batch_write",
                Box::new(|c, t| {
                    Box::pin(async move {
                        let ops =
                            vec![vec![Operation::set_entity("key", b"val".to_vec(), None, None)]];
                        matches!(
                            c.batch_write(UserSlug::new(42), ORG, None, ops, Some(t)).await,
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
        // Cancelled and Shutdown are distinct error types
        let cancelled = crate::error::SdkError::Cancelled;
        let shutdown = crate::error::SdkError::Shutdown;

        assert!(!matches!(cancelled, crate::error::SdkError::Shutdown));
        assert!(!matches!(shutdown, crate::error::SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_read_with_cancellation_token_returns_cancelled_during_backoff() {
        // Set many retries with long backoff so cancellation fires during backoff
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

        // Cancel after 200ms — the first attempt fails quickly,
        // then the 30s backoff starts, and cancellation fires during it
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            token_clone.cancel();
        });

        let start = std::time::Instant::now();
        let result = client.read(UserSlug::new(42), ORG, None, "key", None, Some(token)).await;
        let elapsed = start.elapsed();

        // Should be cancelled during the backoff sleep (or get a transport error
        // if the connection fails before the cancellation token fires).
        assert!(result.is_err(), "should fail when cancellation token is triggered during retries");
        // Should return quickly, not wait for the 30s backoff
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

        // Shutdown after 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            client_clone.shutdown().await;
        });

        let start = std::time::Instant::now();
        let result = client.read(UserSlug::new(42), ORG, None, "key", None, None).await;
        let elapsed = start.elapsed();

        // Should receive a shutdown/cancellation/transport error (the exact variant
        // depends on the race between connection failure and shutdown signal).
        assert!(result.is_err(), "should fail when shutdown is triggered during retries");
        // Should not wait for the full 10 attempts × 30s backoff
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
        assert_eq!(op.estimated_size_bytes(), 3 + 5); // "key" + "value"
    }

    #[test]
    fn test_estimated_size_delete_entity() {
        let op = Operation::delete_entity("user:123");
        assert_eq!(op.estimated_size_bytes(), 8); // "user:123"
    }

    #[test]
    fn test_estimated_size_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert_eq!(op.estimated_size_bytes(), 7 + 6 + 8); // "doc:456" + "viewer" + "user:123"
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

    // =========================================================================
    // Events type conversion tests
    // =========================================================================

    fn make_proto_event_entry() -> proto::EventEntry {
        proto::EventEntry {
            event_id: uuid::Uuid::nil().into_bytes().to_vec(),
            source_service: "ledger".to_string(),
            event_type: "ledger.vault.created".to_string(),
            timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 500_000 }),
            scope: proto::EventScope::Organization as i32,
            action: "vault_created".to_string(),
            emission_path: proto::EventEmissionPath::EmissionPathApplyPhase as i32,
            principal: "user:alice".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 12345 }),
            vault: Some(proto::VaultSlug { slug: 67890 }),
            outcome: proto::EventOutcome::Success as i32,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details: [("vault_name".to_string(), "my-vault".to_string())].into_iter().collect(),
            block_height: Some(42),
            node_id: None,
            trace_id: Some("abc-trace".to_string()),
            correlation_id: Some("batch-123".to_string()),
            operations_count: None,
            expires_at: 0,
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_success() {
        let proto = make_proto_event_entry();
        let entry = SdkEventEntry::from_proto(proto);

        assert_eq!(entry.source_service, "ledger");
        assert_eq!(entry.event_type, "ledger.vault.created");
        assert_eq!(entry.action, "vault_created");
        assert_eq!(entry.principal, "user:alice");
        assert_eq!(entry.organization, OrganizationSlug::new(12345));
        assert_eq!(entry.vault, Some(VaultSlug::new(67890)));
        assert_eq!(entry.scope, EventScope::Organization);
        assert_eq!(entry.emission_path, EventEmissionPath::ApplyPhase);
        assert!(matches!(entry.outcome, EventOutcome::Success));
        assert_eq!(entry.details.get("vault_name").unwrap(), "my-vault");
        assert_eq!(entry.block_height, Some(42));
        assert_eq!(entry.trace_id.as_deref(), Some("abc-trace"));
        assert_eq!(entry.correlation_id.as_deref(), Some("batch-123"));
        assert_eq!(entry.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_sdk_event_entry_from_proto_failed_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Failed as i32;
        proto.error_code = Some("STORAGE_FULL".to_string());
        proto.error_detail = Some("disk quota exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "STORAGE_FULL");
                assert_eq!(detail, "disk quota exceeded");
            },
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_denied_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Denied as i32;
        proto.denial_reason = Some("rate limit exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limit exceeded");
            },
            _ => panic!("expected Denied outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_handler_phase() {
        let mut proto = make_proto_event_entry();
        proto.emission_path = proto::EventEmissionPath::EmissionPathHandlerPhase as i32;
        proto.node_id = Some(7);

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.emission_path, EventEmissionPath::HandlerPhase);
        assert_eq!(entry.node_id, Some(7));
    }

    #[test]
    fn test_sdk_event_entry_from_proto_system_scope() {
        let mut proto = make_proto_event_entry();
        proto.scope = proto::EventScope::System as i32;
        proto.organization = Some(proto::OrganizationSlug { slug: 0 });

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.scope, EventScope::System);
        assert_eq!(entry.organization, OrganizationSlug::new(0));
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_uuid() {
        let entry = SdkEventEntry::from_proto(make_proto_event_entry());
        assert_eq!(entry.event_id_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_non_uuid() {
        let mut proto = make_proto_event_entry();
        proto.event_id = vec![0xab, 0xcd, 0xef];
        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.event_id_string(), "abcdef");
    }

    #[test]
    fn test_event_filter_default_is_all_pass() {
        let filter = EventFilter::new();
        let proto = filter.to_proto();

        assert!(proto.actions.is_empty());
        assert!(proto.start_time.is_none());
        assert!(proto.end_time.is_none());
        assert!(proto.event_type_prefix.is_none());
        assert!(proto.principal.is_none());
        assert_eq!(proto.outcome, 0);
        assert_eq!(proto.emission_path, 0);
        assert!(proto.correlation_id.is_none());
    }

    #[test]
    fn test_event_filter_with_all_options() {
        let start = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let end = chrono::DateTime::from_timestamp(2_000_000, 0).unwrap();

        let filter = EventFilter::new()
            .start_time(start)
            .end_time(end)
            .actions(["vault_created", "vault_deleted"])
            .event_type_prefix("ledger.vault")
            .principal("user:bob")
            .outcome_denied()
            .apply_phase_only()
            .correlation_id("job-99");

        let proto = filter.to_proto();
        assert_eq!(proto.start_time.unwrap().seconds, 1_000_000);
        assert_eq!(proto.end_time.unwrap().seconds, 2_000_000);
        assert_eq!(proto.actions, vec!["vault_created", "vault_deleted"]);
        assert_eq!(proto.event_type_prefix.as_deref(), Some("ledger.vault"));
        assert_eq!(proto.principal.as_deref(), Some("user:bob"));
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.emission_path, proto::EventEmissionPath::EmissionPathApplyPhase as i32);
        assert_eq!(proto.correlation_id.as_deref(), Some("job-99"));
    }

    #[test]
    fn test_event_filter_outcome_variants() {
        assert_eq!(
            EventFilter::new().outcome_success().to_proto().outcome,
            proto::EventOutcome::Success as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_failed().to_proto().outcome,
            proto::EventOutcome::Failed as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_denied().to_proto().outcome,
            proto::EventOutcome::Denied as i32,
        );
    }

    #[test]
    fn test_event_filter_emission_path_variants() {
        assert_eq!(
            EventFilter::new().apply_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathApplyPhase as i32,
        );
        assert_eq!(
            EventFilter::new().handler_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
        );
    }

    #[test]
    fn test_ingest_event_entry_required_fields() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Success,
        );
        let proto = entry.into_proto();

        assert_eq!(proto.event_type, "engine.authorization.checked");
        assert_eq!(proto.principal, "user:alice");
        assert_eq!(proto.outcome, proto::EventOutcome::Success as i32);
        assert!(proto.details.is_empty());
        assert!(proto.trace_id.is_none());
        assert!(proto.correlation_id.is_none());
        assert!(proto.vault.is_none());
        assert!(proto.timestamp.is_none());
    }

    #[test]
    fn test_ingest_event_entry_with_all_optional_fields() {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let entry = SdkIngestEventEntry::new(
            "control.member.invited",
            "admin:bob",
            EventOutcome::Failed { code: "LIMIT".to_string(), detail: "max members".to_string() },
        )
        .detail("email", "new@example.com")
        .trace_id("trace-xyz")
        .correlation_id("import-batch-7")
        .vault(VaultSlug::new(999))
        .timestamp(ts);

        let proto = entry.into_proto();
        assert_eq!(proto.event_type, "control.member.invited");
        assert_eq!(proto.principal, "admin:bob");
        assert_eq!(proto.outcome, proto::EventOutcome::Failed as i32);
        assert_eq!(proto.error_code.as_deref(), Some("LIMIT"));
        assert_eq!(proto.error_detail.as_deref(), Some("max members"));
        assert_eq!(proto.details.get("email").unwrap(), "new@example.com");
        assert_eq!(proto.trace_id.as_deref(), Some("trace-xyz"));
        assert_eq!(proto.correlation_id.as_deref(), Some("import-batch-7"));
        assert_eq!(proto.vault.unwrap().slug, 999);
        assert_eq!(proto.timestamp.unwrap().seconds, 1_700_000_000);
    }

    #[test]
    fn test_ingest_event_entry_denied_outcome() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:charlie",
            EventOutcome::Denied { reason: "no permission".to_string() },
        );
        let proto = entry.into_proto();
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.denial_reason.as_deref(), Some("no permission"));
        assert!(proto.error_code.is_none());
    }

    #[test]
    fn test_event_page_has_next_page() {
        let page = EventPage {
            entries: vec![],
            next_page_token: Some("cursor-abc".to_string()),
            total_estimate: None,
        };
        assert!(page.has_next_page());

        let page = EventPage { entries: vec![], next_page_token: None, total_estimate: None };
        assert!(!page.has_next_page());
    }

    #[test]
    fn test_event_outcome_roundtrip_success() {
        let outcome = EventOutcome::Success;
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        assert!(matches!(restored, EventOutcome::Success));
    }

    #[test]
    fn test_event_outcome_roundtrip_failed() {
        let outcome = EventOutcome::Failed {
            code: "ERR_001".to_string(),
            detail: "something broke".to_string(),
        };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "ERR_001");
                assert_eq!(detail, "something broke");
            },
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_event_outcome_roundtrip_denied() {
        let outcome = EventOutcome::Denied { reason: "rate limited".to_string() };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limited");
            },
            _ => panic!("expected Denied"),
        }
    }

    #[test]
    fn test_event_scope_from_proto() {
        assert_eq!(EventScope::from_proto(proto::EventScope::System as i32), EventScope::System);
        assert_eq!(
            EventScope::from_proto(proto::EventScope::Organization as i32),
            EventScope::Organization,
        );
        // Unknown falls back to System
        assert_eq!(EventScope::from_proto(99), EventScope::System);
    }

    #[test]
    fn test_event_emission_path_from_proto() {
        assert_eq!(
            EventEmissionPath::from_proto(proto::EventEmissionPath::EmissionPathApplyPhase as i32),
            EventEmissionPath::ApplyPhase,
        );
        assert_eq!(
            EventEmissionPath::from_proto(
                proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
            ),
            EventEmissionPath::HandlerPhase,
        );
        // Unknown falls back to ApplyPhase
        assert_eq!(EventEmissionPath::from_proto(99), EventEmissionPath::ApplyPhase);
    }

    #[test]
    fn test_ingest_event_entry_detail_builder() {
        let entry = SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success)
            .detail("key1", "val1")
            .detail("key2", "val2");
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
        assert_eq!(proto.details.get("key1").unwrap(), "val1");
        assert_eq!(proto.details.get("key2").unwrap(), "val2");
    }

    #[test]
    fn test_ingest_event_entry_details_bulk() {
        let map: std::collections::HashMap<String, String> =
            [("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
                .into_iter()
                .collect();
        let entry =
            SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success).details(map);
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
    }

    // =========================================================================
    // Migration Status Tests
    // =========================================================================

    #[test]
    fn test_organization_status_from_proto_migrating() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Migrating as i32);
        assert_eq!(status, OrganizationStatus::Migrating);
    }

    #[test]
    fn test_organization_status_from_proto_suspended() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Suspended as i32);
        assert_eq!(status, OrganizationStatus::Suspended);
    }

    #[test]
    fn test_migration_info_construction() {
        let info = MigrationInfo {
            slug: OrganizationSlug::new(42),
            source_region: Region::US_EAST_VA,
            target_region: Region::IE_EAST_DUBLIN,
            status: OrganizationStatus::Migrating,
        };

        assert_eq!(info.slug, OrganizationSlug::new(42));
        assert_eq!(info.source_region, Region::US_EAST_VA);
        assert_eq!(info.target_region, Region::IE_EAST_DUBLIN);
        assert_eq!(info.status, OrganizationStatus::Migrating);
    }

    #[test]
    fn test_email_verification_code_from_proto() {
        let code = EmailVerificationCode { code: "ABC123".to_string() };
        assert_eq!(code.code, "ABC123");
    }

    #[test]
    fn test_email_verification_result_existing_user() {
        let result = EmailVerificationResult::ExistingUser {
            user: UserSlug::new(42),
            session: crate::token::TokenPair {
                access_token: "at".to_string(),
                refresh_token: "rt".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
        };
        match result {
            EmailVerificationResult::ExistingUser { user, session } => {
                assert_eq!(user, UserSlug::new(42));
                assert_eq!(session.access_token, "at");
                assert_eq!(session.refresh_token, "rt");
            },
            _ => panic!("Expected ExistingUser"),
        }
    }

    #[test]
    fn test_email_verification_result_new_user() {
        let result = EmailVerificationResult::NewUser { onboarding_token: "ilobt_abc".to_string() };
        match result {
            EmailVerificationResult::NewUser { onboarding_token } => {
                assert_eq!(onboarding_token, "ilobt_abc");
            },
            _ => panic!("Expected NewUser"),
        }
    }

    #[test]
    fn test_registration_result_fields() {
        let result = RegistrationResult {
            user: UserSlug::new(99),
            session: crate::token::TokenPair {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
            organization: Some(OrganizationSlug::new(1001)),
        };
        assert_eq!(result.user, UserSlug::new(99));
        assert_eq!(result.session.access_token, "access");
        assert_eq!(result.organization, Some(OrganizationSlug::new(1001)));
    }

    #[test]
    fn test_registration_result_without_organization() {
        let result = RegistrationResult {
            user: UserSlug::new(100),
            session: crate::token::TokenPair {
                access_token: "a".to_string(),
                refresh_token: "r".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
            organization: None,
        };
        assert!(result.organization.is_none());
    }
}
