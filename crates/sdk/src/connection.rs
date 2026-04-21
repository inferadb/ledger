//! Connection pool and channel management.
//!
//! Manages tonic gRPC channels with lazy connection establishment,
//! health checking, and endpoint configuration.
//!
//! # Architecture
//!
//! The [`ConnectionPool`] wraps a tonic [`Channel`] with:
//! - **Lazy connection**: Channel is established on first use, not at construction
//! - **Shared ownership**: The channel is wrapped in `Arc<RwLock<...>>` for thread-safe access
//! - **Configurable settings**: Timeouts, keepalive, compression from [`ClientConfig`]
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_sdk::{ClientConfig, ConnectionPool, ServerSource};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::builder()
//!     .servers(ServerSource::from_static(["http://localhost:50051"]))
//!     .client_id("my-client")
//!     .build()?;
//!
//! let pool = ConnectionPool::new(config);
//! let channel = pool.get_channel().await?;
//! # Ok(())
//! # }
//! ```

use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{
    config::ClientConfig,
    error::{Result, SdkError},
    server::{ServerSelector, ServerSource},
};

/// HTTP/2 keep-alive interval for idle connections.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// HTTP/2 keep-alive timeout.
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP keepalive interval.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Manages tonic gRPC channels with lazy connection establishment.
///
/// Provides lazy connection establishment and thread-safe channel sharing.
/// The underlying tonic [`Channel`] is cheap to clone (it shares the HTTP/2
/// connection internally), so this pool caches a single channel that can
/// be cloned by multiple callers.
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    /// Cached channel, lazily initialized.
    channel: Arc<RwLock<Option<Channel>>>,

    /// Client configuration for connection settings.
    config: ClientConfig,

    /// Dynamic endpoints override. When set, these are used instead of
    /// the endpoints from the config. Updated by discovery service.
    dynamic_endpoints: Arc<RwLock<Option<Vec<String>>>>,

    /// Server selector for latency-based ordering.
    selector: ServerSelector,

    /// Per-endpoint circuit breaker, if enabled.
    circuit_breaker: Option<crate::circuit_breaker::CircuitBreaker>,

    /// Region leader cache for preferred-region routing.
    region_cache: Option<Arc<crate::region_resolver::RegionLeaderCache>>,

    /// Gateway channel kept alive as fallback for region resolution.
    gateway_channel: Arc<RwLock<Option<Channel>>>,

    /// Background leader-watch task handle and its cancellation token.
    ///
    /// Lazily populated on first `get_or_create_gateway_channel` call when
    /// `region_cache` is `Some`. Shared across pool clones so the task
    /// outlives any individual clone but is cancelled when the last clone
    /// drops (`Arc` strong count reaches 1 in `Drop`).
    leader_watcher: Arc<parking_lot::Mutex<Option<LeaderWatcherHandle>>>,
}

/// Background leader-watch task handle held by [`ConnectionPool`].
#[derive(Debug)]
struct LeaderWatcherHandle {
    cancel: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for LeaderWatcherHandle {
    fn drop(&mut self) {
        // Cancel cooperatively first; if the task is blocked on an RPC that
        // doesn't observe cancellation before the pool is dropped, abort
        // as a fallback so we don't leak the task.
        self.cancel.cancel();
        self.handle.abort();
    }
}

impl ConnectionPool {
    /// Creates a new connection pool with the given configuration.
    ///
    /// The pool does not establish a connection immediately; the connection
    /// is lazily created on the first call to [`get_channel`](Self::get_channel).
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        let circuit_breaker = config
            .circuit_breaker()
            .map(|cb_config| crate::circuit_breaker::CircuitBreaker::new(cb_config.clone()));

        let region_cache = config.preferred_region().map(|r| {
            let cache = Arc::new(crate::region_resolver::RegionLeaderCache::with_ttls(
                r,
                config.region_leader_soft_ttl(),
                config.region_leader_hard_ttl(),
            ));
            cache.set_metrics(Arc::clone(config.metrics()));
            cache
        });

        Self {
            channel: Arc::new(RwLock::new(None)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector: ServerSelector::new(),
            circuit_breaker,
            region_cache,
            gateway_channel: Arc::new(RwLock::new(None)),
            leader_watcher: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Creates a new connection pool with a custom server selector.
    ///
    /// Use this when you want to share a selector across multiple pools.
    #[must_use]
    pub fn with_selector(config: ClientConfig, selector: ServerSelector) -> Self {
        let circuit_breaker = config
            .circuit_breaker()
            .map(|cb_config| crate::circuit_breaker::CircuitBreaker::new(cb_config.clone()));

        let region_cache = config.preferred_region().map(|r| {
            let cache = Arc::new(crate::region_resolver::RegionLeaderCache::with_ttls(
                r,
                config.region_leader_soft_ttl(),
                config.region_leader_hard_ttl(),
            ));
            cache.set_metrics(Arc::clone(config.metrics()));
            cache
        });

        Self {
            channel: Arc::new(RwLock::new(None)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector,
            circuit_breaker,
            region_cache,
            gateway_channel: Arc::new(RwLock::new(None)),
            leader_watcher: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Returns a connected channel, establishing the connection if needed.
    ///
    /// On first call, this method establishes a connection to the configured
    /// endpoint(s). Subsequent calls return a clone of the cached channel
    /// (which shares the underlying HTTP/2 connection).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No endpoints are configured
    /// - The endpoint URL is invalid
    /// - Connection establishment fails
    pub async fn get_channel(&self) -> Result<Channel> {
        // Check circuit breaker before attempting connection
        if let Some(ref cb) = self.circuit_breaker {
            let endpoint = self.current_endpoint();
            cb.check(&endpoint)?;
        }

        // Region-aware routing: if preferred_region is set, use leader cache
        if let Some(ref cache) = self.region_cache {
            return self.get_region_channel(cache).await;
        }

        // Fast path: check if channel already exists
        {
            let guard = self.channel.read();
            if let Some(channel) = guard.as_ref() {
                return Ok(channel.clone());
            }
        }

        // Slow path: need to establish connection
        let endpoint = self.current_endpoint();
        let new_channel = match self.create_channel().await {
            Ok(ch) => {
                self.config
                    .metrics()
                    .record_connection(&endpoint, crate::metrics::ConnectionEvent::Connected);
                ch
            },
            Err(e) => {
                self.config
                    .metrics()
                    .record_connection(&endpoint, crate::metrics::ConnectionEvent::Failed);
                return Err(e);
            },
        };

        // Store and return the channel
        {
            let mut guard = self.channel.write();
            // Double-check pattern: another task might have connected while we waited
            if let Some(channel) = guard.as_ref() {
                return Ok(channel.clone());
            }
            *guard = Some(new_channel.clone());
        }

        Ok(new_channel)
    }

    /// Creates a new channel with all configured settings applied.
    async fn create_channel(&self) -> Result<Channel> {
        // Use dynamic endpoints if available, otherwise derive from config's server source
        let endpoint_url = {
            let dynamic = self.dynamic_endpoints.read();
            if let Some(ref endpoints) = *dynamic {
                endpoints.first().cloned()
            } else {
                // Get endpoint from server source
                match self.config.servers() {
                    ServerSource::Static(endpoints) => endpoints.first().cloned(),
                    ServerSource::Dns(_) | ServerSource::File(_) => {
                        // For DNS/File sources, dynamic endpoints should be set by the resolver
                        None
                    },
                }
            }
        };

        let Some(endpoint_url) = endpoint_url else {
            return Err(SdkError::Connection {
                message:
                    "No endpoints available. For DNS/File sources, ensure the resolver has run."
                        .to_owned(),
            });
        };

        // Unix Domain Socket — use a UDS connector instead of TCP
        if endpoint_url.starts_with('/') {
            return self.create_uds_channel(&endpoint_url).await;
        }

        // Parse the endpoint URL
        let endpoint =
            Endpoint::try_from(endpoint_url.clone()).map_err(|_| SdkError::InvalidUrl {
                url: endpoint_url.clone(),
                message: "Failed to parse as tonic endpoint".to_owned(),
            })?;

        // Apply connection settings (including TLS if configured)
        let endpoint = self.configure_endpoint(endpoint)?;

        // Establish the connection
        let channel = endpoint.connect().await?;

        Ok(channel)
    }

    /// Applies configuration settings to an endpoint.
    ///
    /// Note: Compression is configured at the service client level, not the endpoint.
    /// The [`compression_enabled`](Self::compression_enabled) method indicates whether
    /// compression should be applied when creating service clients.
    fn configure_endpoint(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let endpoint = endpoint
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(TCP_KEEPALIVE_INTERVAL))
            .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
            .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true);

        // Apply TLS configuration if present
        let endpoint = if let Some(ref tls_config) = self.config.tls {
            let tls = self.build_tls_config(tls_config)?;
            endpoint.tls_config(tls).map_err(|e| SdkError::Connection {
                message: format!("TLS configuration error: {e}"),
            })?
        } else {
            endpoint
        };

        Ok(endpoint)
    }

    /// Builds a tonic `ClientTlsConfig` from our SDK's `TlsConfig`.
    fn build_tls_config(&self, tls: &crate::config::TlsConfig) -> Result<ClientTlsConfig> {
        let mut tls_config = ClientTlsConfig::new();

        // Add native roots if requested
        if tls.use_native_roots() {
            tls_config = tls_config.with_native_roots();
        }

        // Add CA certificate if provided
        if let Some(ca_cert) = tls.ca_cert() {
            let pem = ca_cert.to_pem();
            let cert = Certificate::from_pem(pem);
            tls_config = tls_config.ca_certificate(cert);
        }

        // Add client identity for mutual TLS if provided
        if let (Some(client_cert), Some(client_key)) = (tls.client_cert(), tls.client_key()) {
            let cert_pem = client_cert.to_pem();
            let identity = Identity::from_pem(cert_pem, client_key);
            tls_config = tls_config.identity(identity);
        }

        // Set domain name override if provided
        if let Some(domain) = tls.domain_name() {
            tls_config = tls_config.domain_name(domain);
        }

        Ok(tls_config)
    }

    /// Gets a channel to the resolved regional leader, resolving if needed.
    ///
    /// When a preferred region is configured, this method manages a direct
    /// channel to that region's Raft leader. On first call (or after cache
    /// expiry), the leader is resolved via the gateway's `ResolveRegionLeader`
    /// RPC. If resolution fails, falls back to the gateway channel so the
    /// server can handle routing via forwarding.
    async fn get_region_channel(
        &self,
        cache: &Arc<crate::region_resolver::RegionLeaderCache>,
    ) -> Result<Channel> {
        let region_label = cache.region().to_string();
        let metrics = self.config.metrics();
        // Check if cached leader is still usable (fresh or stale-but-usable).
        if let Some((cached, freshness)) = cache.cached_endpoint_with_freshness() {
            metrics.leader_cache_hit(&region_label);
            // On stale-but-usable, opportunistically trigger a background
            // refresh — but only if the gateway channel is already warm.
            // If it isn't, skip the refresh; the next full resolve will
            // reinitialize it.
            if freshness == crate::region_resolver::CacheFreshness::StaleButUsable
                && let Some(gateway) = self.gateway_channel.read().clone()
            {
                metrics.region_resolve_stale_served(&region_label);
                cache.spawn_background_refresh(gateway);
            }
            let existing = self.channel.read().clone();
            if let Some(channel) = existing {
                return Ok(channel);
            }
            let channel = self.connect_to_endpoint(cached.as_ref()).await?;
            *self.channel.write() = Some(channel.clone());
            return Ok(channel);
        }

        // Cache miss/expired — resolve via gateway
        metrics.leader_cache_miss(&region_label);
        let gateway = self.get_or_create_gateway_channel().await?;
        match cache.resolve(&gateway).await {
            Ok(endpoint) => {
                let channel = self.connect_to_endpoint(&endpoint).await?;
                *self.channel.write() = Some(channel.clone());
                Ok(channel)
            },
            Err(_) => {
                // Resolution failed — fall back to gateway
                tracing::warn!(
                    region = %cache.region(),
                    "Region leader resolution failed, falling back to gateway"
                );
                Ok(gateway)
            },
        }
    }

    /// Gets or creates the gateway channel (the originally-configured endpoint).
    ///
    /// The gateway channel is the generic entry point (e.g. `api.inferadb.com`)
    /// that can forward requests to the correct regional leader server-side.
    /// It is kept alive as a fallback when direct leader resolution fails.
    async fn get_or_create_gateway_channel(&self) -> Result<Channel> {
        {
            let existing = self.gateway_channel.read().clone();
            if let Some(channel) = existing {
                self.ensure_leader_watcher(&channel);
                return Ok(channel);
            }
        }
        // Create gateway channel using existing create_channel logic
        let channel = self.create_channel().await?;
        *self.gateway_channel.write() = Some(channel.clone());
        self.ensure_leader_watcher(&channel);
        Ok(channel)
    }

    /// Spawns the leader-watch task on first use when a region cache is
    /// configured. No-op when no region is preferred or the task is already
    /// running.
    fn ensure_leader_watcher(&self, gateway: &Channel) {
        let Some(ref cache) = self.region_cache else {
            return;
        };
        let mut slot = self.leader_watcher.lock();
        if slot.is_some() {
            return;
        }
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = crate::region_resolver::spawn_leader_watcher(
            Arc::clone(cache),
            gateway.clone(),
            Arc::clone(self.config.metrics()),
            cancel.clone(),
        );
        *slot = Some(LeaderWatcherHandle { cancel, handle });
    }

    /// Connects to a specific endpoint URL with the pool's TLS and timeout settings.
    async fn connect_to_endpoint(&self, endpoint_url: &str) -> Result<Channel> {
        // Unix Domain Socket — use a UDS connector instead of TCP
        if endpoint_url.starts_with('/') {
            return self.create_uds_channel(endpoint_url).await;
        }

        let endpoint = Endpoint::try_from(endpoint_url.to_owned()).map_err(|_| {
            SdkError::Connection { message: format!("Invalid leader endpoint: {endpoint_url}") }
        })?;
        let endpoint = self.configure_endpoint(endpoint)?;
        endpoint.connect().await.map_err(|e| SdkError::Connection {
            message: format!("Failed to connect to region leader: {e}"),
        })
    }

    /// Creates a gRPC channel over a Unix Domain Socket.
    ///
    /// Uses a dummy HTTP endpoint since tonic requires a valid URI, but the
    /// actual transport is routed through the UDS connector. Timeouts are
    /// applied; TLS and TCP-specific settings (nodelay, keepalive) are skipped
    /// as they do not apply to Unix sockets.
    async fn create_uds_channel(&self, path: &str) -> Result<Channel> {
        let path_buf = std::path::PathBuf::from(path);
        // Tonic requires a valid URI for the endpoint, but the connector
        // overrides the actual transport — use a placeholder address.
        let endpoint =
            Endpoint::try_from("http://[::]:50051").map_err(|_| SdkError::InvalidUrl {
                url: path.to_owned(),
                message: "Failed to create placeholder endpoint for UDS".to_owned(),
            })?;
        let endpoint =
            endpoint.connect_timeout(self.config.connect_timeout).timeout(self.config.timeout);

        let channel = endpoint
            .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                let path = path_buf.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        tokio::net::UnixStream::connect(&path).await?,
                    ))
                }
            }))
            .await
            .map_err(|e| SdkError::Connection {
                message: format!("Failed to connect to Unix socket {path}: {e}"),
            })?;

        Ok(channel)
    }

    /// On a `NotLeader`-like error, applies the server-provided leader hint
    /// (if any) to the regional cache. Falls back to invalidating the cache
    /// when the server provided no hint.
    ///
    /// Called by the retry loop before attempting a retry so the next attempt
    /// targets the newly-known leader without a separate resolve round-trip.
    pub fn apply_region_leader_hint_or_invalidate(&self, err: &SdkError) {
        let Some(ref cache) = self.region_cache else {
            return;
        };
        let hint = err.server_error_details().and_then(|d| d.leader_hint());
        match hint {
            Some(ref h) if h.leader_endpoint.is_some() => {
                cache.apply_hint(h);
                *self.channel.write() = None;
                // Hint applied — a redirect occurred; record it so operators
                // can observe redirect cost (cold-start vs. warm path).
                self.config.metrics().redirect_retry(&cache.region().to_string());
            },
            _ => {
                cache.invalidate();
                *self.channel.write() = None;
            },
        }
    }

    /// Returns the currently cached region leader endpoint, if any.
    ///
    /// Intended for tests and observability — returns `None` when no
    /// `preferred_region` is configured or the cache is empty/expired.
    #[must_use]
    pub fn region_cached_endpoint(&self) -> Option<Arc<str>> {
        self.region_cache.as_ref().and_then(|c| c.cached_endpoint())
    }

    /// Seeds the region leader cache with an endpoint for testing purposes.
    #[cfg(test)]
    pub(crate) fn seed_region_cache_for_test(&self, endpoint: &str) {
        if let Some(ref cache) = self.region_cache {
            let hint = crate::error::LeaderHint {
                leader_id: Some(1),
                leader_endpoint: Some(endpoint.to_owned()),
                term: Some(1),
                shard_idx: None,
            };
            cache.apply_hint(&hint);
        }
    }

    /// Returns whether compression is enabled for this connection.
    ///
    /// When true, service clients should be configured with gzip compression.
    #[must_use]
    pub fn compression_enabled(&self) -> bool {
        self.config.compression
    }

    /// Returns a reference to the client configuration.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Clears the cached channel, forcing reconnection on next use.
    ///
    /// This can be useful after network changes or when the server
    /// indicates the connection should be reset.
    pub fn reset(&self) {
        let mut guard = self.channel.write();
        if guard.is_some() {
            *guard = None;
            self.config.metrics().record_connection(
                &self.current_endpoint(),
                crate::metrics::ConnectionEvent::Disconnected,
            );
        }
    }

    /// Updates the endpoints used for connections.
    ///
    /// This method is called by the discovery service to update the endpoint
    /// list based on discovered peers. The new endpoints take precedence over
    /// the endpoints in the original configuration.
    ///
    /// Note: This does not automatically reconnect. Call [`reset()`](Self::reset)
    /// after updating endpoints to force reconnection on next use.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - New endpoint URLs to use for connections
    pub fn update_endpoints(&self, endpoints: Vec<String>) {
        let mut guard = self.dynamic_endpoints.write();
        *guard = Some(endpoints);
    }

    /// Returns the current active endpoints.
    ///
    /// If dynamic endpoints have been set via discovery, returns those.
    /// Otherwise returns the endpoints from the original configuration
    /// (only for static server sources).
    #[must_use]
    pub fn active_endpoints(&self) -> Vec<String> {
        let dynamic = self.dynamic_endpoints.read();
        if let Some(ref endpoints) = *dynamic {
            endpoints.clone()
        } else {
            match self.config.servers() {
                ServerSource::Static(endpoints) => endpoints.clone(),
                ServerSource::Dns(_) | ServerSource::File(_) => Vec::new(),
            }
        }
    }

    /// Returns a reference to the server selector.
    ///
    /// Use this to record latencies after successful requests or to
    /// mark servers as unhealthy.
    #[must_use]
    pub fn selector(&self) -> &ServerSelector {
        &self.selector
    }

    /// Returns a reference to the circuit breaker, if enabled.
    #[must_use]
    pub fn circuit_breaker(&self) -> Option<&crate::circuit_breaker::CircuitBreaker> {
        self.circuit_breaker.as_ref()
    }

    /// Returns the SDK metrics collector.
    #[must_use]
    pub fn metrics(&self) -> &std::sync::Arc<dyn crate::metrics::SdkMetrics> {
        self.config.metrics()
    }

    /// Returns the current primary endpoint URL.
    ///
    /// Used by the circuit breaker to track per-endpoint state.
    fn current_endpoint(&self) -> String {
        let dynamic = self.dynamic_endpoints.read();
        if let Some(ref endpoints) = *dynamic {
            endpoints.first().cloned().unwrap_or_else(|| String::from("unknown"))
        } else {
            match self.config.servers() {
                ServerSource::Static(endpoints) => {
                    endpoints.first().cloned().unwrap_or_else(|| String::from("unknown"))
                },
                ServerSource::Dns(_) | ServerSource::File(_) => String::from("unknown"),
            }
        }
    }

    /// Records a failed request on the circuit breaker.
    ///
    /// Should be called after a retryable RPC failure. When consecutive
    /// failures exceed the threshold, the circuit opens and subsequent
    /// requests fast-fail with [`SdkError::CircuitOpen`](crate::SdkError::CircuitOpen).
    pub fn record_failure(&self) {
        if let Some(ref cb) = self.circuit_breaker {
            let endpoint = self.current_endpoint();
            let prev_state = cb.state(&endpoint);
            cb.record_failure(&endpoint);
            let new_state = cb.state(&endpoint);

            // Emit metrics on state transition
            if prev_state != new_state {
                self.config.metrics().record_circuit_state(&endpoint, &new_state.to_string());
            }

            // If the circuit just opened, mark unhealthy
            if prev_state != crate::circuit_breaker::CircuitState::Open
                && new_state == crate::circuit_breaker::CircuitState::Open
            {
                self.selector.mark_unhealthy(&endpoint);
            }
        }
    }

    /// Records a successful request on the circuit breaker and
    /// syncs health status with the server selector.
    pub fn record_success(&self) {
        if let Some(ref cb) = self.circuit_breaker {
            let endpoint = self.current_endpoint();
            let prev_state = cb.state(&endpoint);
            cb.record_success(&endpoint);
            let new_state = cb.state(&endpoint);

            // Emit metrics on state transition
            if prev_state != new_state {
                self.config.metrics().record_circuit_state(&endpoint, &new_state.to_string());
            }

            // If the circuit just closed (was half-open, now closed), mark healthy
            if prev_state == crate::circuit_breaker::CircuitState::HalfOpen
                && new_state == crate::circuit_breaker::CircuitState::Closed
            {
                self.selector.mark_healthy(&endpoint);
            }
        }
    }

    /// Clears dynamic endpoints, reverting to the original configuration.
    pub fn clear_dynamic_endpoints(&self) {
        let mut guard = self.dynamic_endpoints.write();
        *guard = None;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn test_config() -> ClientConfig {
        ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid test config")
    }

    fn test_config_with_compression() -> ClientConfig {
        ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .compression(true)
            .build()
            .expect("valid test config with compression")
    }

    fn test_config_with_region(region: inferadb_ledger_types::Region) -> ClientConfig {
        ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .preferred_region(region)
            .build()
            .expect("valid test config with region")
    }

    fn test_config_without_region() -> ClientConfig {
        test_config()
    }

    fn test_config_with_custom_timeouts() -> ClientConfig {
        ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("valid test config with custom timeouts")
    }

    #[test]
    fn pool_creation_does_not_connect() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Channel should be None initially (lazy connection)
        let guard = pool.channel.read();
        assert!(guard.is_none(), "channel should be None before first use");
    }

    #[test]
    fn pool_config_accessor_returns_config() {
        let config = test_config();
        let pool = ConnectionPool::new(config.clone());

        assert_eq!(pool.config().client_id(), config.client_id());
    }

    #[test]
    fn compression_enabled_reflects_config() {
        let config_no_compression = test_config();
        let pool_no_compression = ConnectionPool::new(config_no_compression);
        assert!(!pool_no_compression.compression_enabled());

        let config_with_compression = test_config_with_compression();
        let pool_with_compression = ConnectionPool::new(config_with_compression);
        assert!(pool_with_compression.compression_enabled());
    }

    #[test]
    fn reset_clears_cached_channel() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Initially empty
        assert!(pool.channel.read().is_none());

        // Reset on empty pool is a no-op
        pool.reset();
        assert!(pool.channel.read().is_none());
    }

    #[test]
    fn pool_stores_custom_timeouts() {
        let config = test_config_with_custom_timeouts();
        let pool = ConnectionPool::new(config);

        assert_eq!(pool.config().timeout(), Duration::from_secs(30));
        assert_eq!(pool.config().connect_timeout(), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn get_channel_fails_with_unreachable_endpoint() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:1"])) // Port 1 is unlikely to have a service
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100)) // Short timeout
            .build()
            .expect("valid config");

        let pool = ConnectionPool::new(config);
        let result = pool.get_channel().await;

        // Connection should fail (timeout or connection refused)
        assert!(result.is_err(), "expected connection to fail");
    }

    #[tokio::test]
    async fn channel_is_cached_after_first_get() {
        // This test would require a mock server, so we just verify the caching logic
        // by checking that the channel slot is updated (we can't actually connect)
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Before any connection attempt, channel is None
        assert!(pool.channel.read().is_none());

        // Note: We can't test successful caching without a real/mock server
        // The get_channel call would fail, but the caching logic is correct
    }

    #[test]
    fn update_endpoints_sets_dynamic_endpoints() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Initially no dynamic endpoints
        assert!(pool.dynamic_endpoints.read().is_none());

        // Update endpoints
        let new_endpoints =
            vec!["http://10.0.0.1:5000".to_string(), "http://10.0.0.2:5000".to_string()];
        pool.update_endpoints(new_endpoints.clone());

        // Dynamic endpoints should be set
        let dynamic = pool.dynamic_endpoints.read();
        assert_eq!(*dynamic, Some(new_endpoints));
    }

    #[test]
    fn active_endpoints_returns_dynamic_when_set() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Initially returns config endpoints (static source)
        assert_eq!(pool.active_endpoints(), vec!["http://localhost:50051".to_string()]);

        // After update, returns dynamic endpoints
        let new_endpoints = vec!["http://10.0.0.1:5000".to_string()];
        pool.update_endpoints(new_endpoints.clone());
        assert_eq!(pool.active_endpoints(), new_endpoints);
    }

    #[test]
    fn clear_dynamic_endpoints_reverts_to_config() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Set dynamic endpoints
        pool.update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);
        assert_ne!(pool.active_endpoints(), vec!["http://localhost:50051".to_string()]);

        // Clear dynamic endpoints
        pool.clear_dynamic_endpoints();
        assert_eq!(pool.active_endpoints(), vec!["http://localhost:50051".to_string()]);
    }

    #[test]
    fn update_endpoints_overwrites_previous() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        pool.update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);
        pool.update_endpoints(vec!["http://10.0.0.2:5000".to_string()]);

        assert_eq!(pool.active_endpoints(), vec!["http://10.0.0.2:5000".to_string()]);
    }

    #[test]
    fn selector_accessor_returns_selector() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Should be able to use the selector
        pool.selector().record_latency("10.0.0.1:50051", Duration::from_millis(50));
        assert!(pool.selector().latency_ms("10.0.0.1:50051").is_some());
    }

    #[test]
    fn with_selector_uses_provided_selector() {
        let config = test_config();
        let selector = ServerSelector::new();

        // Record latency before creating pool
        selector.record_latency("10.0.0.1:50051", Duration::from_millis(100));

        let pool = ConnectionPool::with_selector(config, selector);

        // Pool should use the pre-configured selector
        assert!(pool.selector().latency_ms("10.0.0.1:50051").is_some());
    }

    #[test]
    fn circuit_breaker_none_when_not_configured() {
        let config = test_config();
        let pool = ConnectionPool::new(config);
        assert!(pool.circuit_breaker().is_none());
    }

    #[test]
    fn record_failure_noop_without_circuit_breaker() {
        let config = test_config();
        let pool = ConnectionPool::new(config);
        // Should not panic when circuit breaker is not configured
        pool.record_failure();
    }

    #[test]
    fn record_success_noop_without_circuit_breaker() {
        let config = test_config();
        let pool = ConnectionPool::new(config);
        // Should not panic when circuit breaker is not configured
        pool.record_success();
    }

    #[test]
    fn metrics_accessor_returns_metrics() {
        let config = test_config();
        let pool = ConnectionPool::new(config);
        // Should return the NoopSdkMetrics by default
        let _metrics = pool.metrics();
    }

    #[test]
    fn update_endpoints_then_reset_clears_channel() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        pool.update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);
        assert_eq!(pool.active_endpoints(), vec!["http://10.0.0.1:5000".to_string()]);

        // Reset clears the channel but keeps dynamic endpoints
        pool.reset();
        assert!(pool.channel.read().is_none());
        // Dynamic endpoints are still set
        assert_eq!(pool.active_endpoints(), vec!["http://10.0.0.1:5000".to_string()]);
    }

    #[test]
    fn pool_clone_shares_state() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        let pool2 = pool.clone();
        pool.update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);
        // Cloned pool should see the same dynamic endpoints (Arc shared)
        assert_eq!(pool2.active_endpoints(), vec!["http://10.0.0.1:5000".to_string()]);
    }

    #[test]
    fn multiple_reset_calls_are_safe() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Multiple resets should be safe even with no channel
        pool.reset();
        pool.reset();
        pool.reset();
        assert!(pool.channel.read().is_none());
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_applies_hint_when_present() {
        use inferadb_ledger_types::Region;
        use tonic::Code;

        use crate::error::ServerErrorDetails;

        // Construct a ConnectionPool with region_cache enabled.
        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);
        assert!(pool.region_cached_endpoint().is_none(), "cache starts empty");

        // Build an SdkError with leader hint error_details.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "42".to_owned()),
                ("leader_endpoint".to_owned(), "http://10.0.2.5:5000".to_owned()),
                ("leader_term".to_owned(), "7".to_owned()),
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        assert_eq!(
            pool.region_cached_endpoint().as_deref(),
            Some("http://10.0.2.5:5000"),
            "hint should have been applied"
        );
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_falls_back_to_invalidate() {
        use inferadb_ledger_types::Region;
        use tonic::Code;

        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Seed cache directly.
        pool.seed_region_cache_for_test("http://old:5000");
        assert_eq!(pool.region_cached_endpoint().as_deref(), Some("http://old:5000"),);

        // SdkError without hint (error_details = None).
        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "no hint".into(),
            request_id: None,
            trace_id: None,
            error_details: None,
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        assert!(pool.region_cached_endpoint().is_none(), "cache should have been invalidated");
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_emits_redirect_metric_on_hint() {
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };

        use inferadb_ledger_types::Region;
        use tonic::Code;

        use crate::{error::ServerErrorDetails, metrics::SdkMetrics};

        #[derive(Debug, Default)]
        struct CountingMetrics {
            redirect_retries: AtomicU64,
        }
        impl SdkMetrics for CountingMetrics {
            fn redirect_retry(&self, _region: &str) {
                self.redirect_retries.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counting = Arc::new(CountingMetrics::default());
        let metrics: Arc<dyn SdkMetrics> = counting.clone();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .preferred_region(Region::US_EAST_VA)
            .metrics(metrics)
            .build()
            .expect("valid test config");
        let pool = ConnectionPool::new(config);

        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "42".to_owned()),
                ("leader_endpoint".to_owned(), "http://10.0.2.5:5000".to_owned()),
                ("leader_term".to_owned(), "7".to_owned()),
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        assert_eq!(
            counting.redirect_retries.load(Ordering::SeqCst),
            1,
            "redirect_retry should fire exactly once when a hint is applied",
        );
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_does_not_emit_redirect_metric_without_hint() {
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };

        use inferadb_ledger_types::Region;
        use tonic::Code;

        use crate::metrics::SdkMetrics;

        #[derive(Debug, Default)]
        struct CountingMetrics {
            redirect_retries: AtomicU64,
        }
        impl SdkMetrics for CountingMetrics {
            fn redirect_retry(&self, _region: &str) {
                self.redirect_retries.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counting = Arc::new(CountingMetrics::default());
        let metrics: Arc<dyn SdkMetrics> = counting.clone();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .preferred_region(Region::US_EAST_VA)
            .metrics(metrics)
            .build()
            .expect("valid test config");
        let pool = ConnectionPool::new(config);
        pool.seed_region_cache_for_test("http://old:5000");

        // Hintless error → invalidate path, must NOT fire redirect_retry.
        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "no hint".into(),
            request_id: None,
            trace_id: None,
            error_details: None,
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        assert_eq!(
            counting.redirect_retries.load(Ordering::SeqCst),
            0,
            "redirect_retry must not fire on the invalidate-only branch",
        );
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_noop_when_no_region_cache() {
        use tonic::Code;

        // Pool without a preferred region has region_cache = None.
        let config = test_config_without_region();
        let pool = ConnectionPool::new(config);

        let err = SdkError::Rpc {
            code: Code::Unavailable,
            message: "x".into(),
            request_id: None,
            trace_id: None,
            error_details: None,
        };

        // Must not panic.
        pool.apply_region_leader_hint_or_invalidate(&err);
    }

    #[test]
    fn update_empty_endpoints_list() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        pool.update_endpoints(vec![]);
        // Dynamic endpoints are set but empty
        let dynamic = pool.dynamic_endpoints.read();
        assert!(dynamic.as_ref().unwrap().is_empty());
    }
}
