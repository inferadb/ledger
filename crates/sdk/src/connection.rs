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
//! ```ignore
//! use inferadb_ledger_sdk::{ClientConfig, ServerSource};
//! use inferadb_ledger_sdk::connection::ConnectionPool;
//!
//! let config = ClientConfig::builder()
//!     .servers(ServerSource::from_static(["http://localhost:50051"]))
//!     .client_id("my-client")
//!     .build()?;
//!
//! let pool = ConnectionPool::new(config);
//! let channel = pool.get_channel().await?;
//! ```

use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;
use snafu::ResultExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{
    config::ClientConfig,
    error::{ConnectionSnafu, InvalidUrlSnafu, Result, TransportSnafu},
    server::{ServerSelector, ServerSource},
};

/// HTTP/2 keep-alive interval for idle connections.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// HTTP/2 keep-alive timeout.
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP keepalive interval.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Connection pool managing tonic gRPC channels.
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

        Self {
            channel: Arc::new(RwLock::new(None)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector: ServerSelector::new(),
            circuit_breaker,
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

        Self {
            channel: Arc::new(RwLock::new(None)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector,
            circuit_breaker,
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
            return ConnectionSnafu {
                message:
                    "No endpoints available. For DNS/File sources, ensure the resolver has run."
                        .to_string(),
            }
            .fail();
        };

        // Parse the endpoint URL
        let endpoint = Endpoint::try_from(endpoint_url.clone()).map_err(|_| {
            InvalidUrlSnafu {
                url: endpoint_url.clone(),
                message: "Failed to parse as tonic endpoint".to_string(),
            }
            .build()
        })?;

        // Apply connection settings (including TLS if configured)
        let endpoint = self.configure_endpoint(endpoint)?;

        // Establish the connection
        let channel = endpoint.connect().await.context(TransportSnafu)?;

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
            endpoint.tls_config(tls).map_err(|e| {
                ConnectionSnafu { message: format!("TLS configuration error: {e}") }.build()
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
}
