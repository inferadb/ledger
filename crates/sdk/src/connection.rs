//! Connection pool and wire-client management.
//!
//! [`ConnectionPool`] manages `N` independent
//! [`WireClient`](inferadb_ledger_wire_transport::WireClient) handles
//! (configurable via
//! [`ClientConfig::connection_pool_size`](crate::ClientConfig::connection_pool_size))
//! and round-robins requests across them. Each `WireClient` owns its own
//! QUIC endpoint; the QUIC + auth handshake is deferred to the first
//! [`WireClient::acquire`] inside an op.
//!
//! Connection establishment is lazy. Cache invalidation
//! (leader-hint redirect, [`ConnectionPool::reset`]) is a no-op on the
//! wire-client side — `WireClient` reconnects internally on demand — and
//! gateway-channel state lives behind a single optional slot for region
//! resolution.
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
//! let _wire = pool.cached_wire_client();
//! # Ok(())
//! # }
//! ```

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use inferadb_ledger_wire_transport::WireClient;
use parking_lot::RwLock;

use crate::{
    config::ClientConfig,
    error::{Result, SdkError},
    server::{ServerSelector, ServerSource},
};

/// Manages [`WireClient`] handles with lazy QUIC handshake.
///
/// Provides a fixed-size, round-robin pool of independent
/// [`WireClient`]s. The QUIC + auth handshake itself is deferred to the
/// first [`WireClient::acquire`] inside an op — this struct only
/// materializes the per-endpoint client objects.
#[derive(Clone)]
pub struct ConnectionPool {
    /// Pool of wire-transport [`WireClient`] handles, one per configured
    /// endpoint. Construction at pool-build time ensures every
    /// [`Self::cached_wire_client`] call hits a populated slot or returns
    /// `None` (when wire-client construction failed at build time, the
    /// dispatch site surfaces a clean [`SdkError::Config`]).
    wire_clients: Vec<Arc<WireClient>>,

    /// Round-robin cursor across [`Self::wire_clients`].
    next_client_idx: Arc<AtomicUsize>,

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

    /// Per-`(organization, vault)` leader cache for fine-grained routing.
    ///
    /// Always present, even when no `preferred_region` is configured —
    /// vault-scoped hints carry their own routing information independent
    /// of region selection. Capacity comes from `ClientConfig::vault_cache_capacity`.
    vault_cache: Arc<crate::vault_resolver::VaultLeaderCache>,
}

/// Build one [`WireClient`] per configured endpoint, sharing the same
/// bridged [`rustls::ClientConfig`] across all of them.
///
/// `auth_payload` is materialized from the SDK's existing client-id field
/// for now (no dedicated bearer-token slot exists yet); per-request token
/// rotation is out of scope for F.1.e.1 and would require a separate
/// auth-design pass. UDS endpoints (URL starting with `/`) error out at
/// `parse_wire_endpoint` — the wire transport is QUIC-only and the
/// `ClientConfig` validator rejects this combination at build time, but
/// dynamic-endpoint paths can sneak around that check.
fn build_wire_clients(config: &ClientConfig) -> Result<Vec<Arc<WireClient>>> {
    let endpoints: Vec<String> = match config.servers() {
        ServerSource::Static(endpoints) => endpoints.clone(),
        // For DNS / file sources the resolver populates `dynamic_endpoints`
        // post-construction, after the pool has already been built. Today
        // those paths surface a config error at the dispatch site rather
        // than an empty wire pool — wire+DNS plumbing is a separate task.
        ServerSource::Dns(_) | ServerSource::File(_) => Vec::new(),
    };

    let crypto = crate::tls_bridge::bridge_sdk_tls_to_wire(config.tls())?;

    // Bearer-token plumbing is deferred to a separate auth task; for
    // now every wire client sends an empty auth payload, matching the
    // server-side `PermissiveVerifier` shape used in tests.
    let auth_payload = Bytes::new();

    let mut clients = Vec::with_capacity(endpoints.len());
    for endpoint_url in &endpoints {
        let (server_addr, server_name) = parse_wire_endpoint(endpoint_url)?;
        let quic = inferadb_ledger_wire_transport::tls::client_config(crypto.clone());
        let wire_config = inferadb_ledger_wire_transport::client::ClientConfig {
            server_addr,
            server_name,
            quic,
            auth_payload: auth_payload.clone(),
            connect_timeout: config.connect_timeout(),
        };
        let client = WireClient::new(wire_config).map_err(|e| SdkError::Connection {
            message: format!("failed to build wire client for {endpoint_url}: {e}"),
        })?;
        clients.push(Arc::new(client));
    }
    Ok(clients)
}

/// Parse an SDK endpoint URL into `(SocketAddr, server_name)` as expected
/// by [`inferadb_ledger_wire_transport::client::ClientConfig`].
///
/// Accepts `http://host:port`, `https://host:port`, or bare `host:port`.
/// Hostname-form URLs are resolved synchronously via
/// [`std::net::ToSocketAddrs`] so the resulting `WireClient` has a
/// concrete `SocketAddr` to dial. UDS paths (leading `/`) error out here.
fn parse_wire_endpoint(url: &str) -> Result<(SocketAddr, String)> {
    if url.starts_with('/') {
        return Err(SdkError::Config {
            message: format!(
                "UDS endpoint {url} is not supported on the wire transport (QUIC-only)"
            ),
        });
    }

    // Strip scheme.
    let host_port =
        url.strip_prefix("https://").or_else(|| url.strip_prefix("http://")).unwrap_or(url);

    // Strip a trailing path component if present — `host:port/path` is
    // permissible but the wire transport only consumes `host:port`.
    let host_port = host_port.split('/').next().unwrap_or(host_port);

    // Split host and port. The wire transport always requires an explicit
    // port; URL parsing of the form `https://host` (no port) is rejected
    // here rather than silently defaulting to a wrong-port dial.
    let (host, _port) = host_port.rsplit_once(':').ok_or_else(|| SdkError::InvalidUrl {
        url: url.to_owned(),
        message: "wire endpoint must be host:port".to_owned(),
    })?;

    use std::net::ToSocketAddrs;
    let mut addrs = host_port.to_socket_addrs().map_err(|e| SdkError::InvalidUrl {
        url: url.to_owned(),
        message: format!("failed to resolve {host_port}: {e}"),
    })?;
    let server_addr = addrs.next().ok_or_else(|| SdkError::InvalidUrl {
        url: url.to_owned(),
        message: format!("no socket addresses for {host_port}"),
    })?;
    Ok((server_addr, host.to_owned()))
}

// Manual `Debug` impl: [`WireClient`] does not implement `Debug` (it
// holds a `quinn::Endpoint` and an internal mutex), so we project the
// pool sizes rather than reaching into individual clients. The other
// fields use their default `Debug` projections.
impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("wire_client_count", &self.wire_clients.len())
            .field("config", &self.config)
            .field("dynamic_endpoints", &self.dynamic_endpoints)
            .field("selector", &self.selector)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("region_cache", &self.region_cache)
            .field("vault_cache", &self.vault_cache)
            .finish_non_exhaustive()
    }
}

impl ConnectionPool {
    /// Creates a new connection pool with the given configuration.
    ///
    /// The per-endpoint [`WireClient`] vec is materialized eagerly; the
    /// QUIC + auth handshake itself is deferred to the first
    /// [`WireClient::acquire`] inside an op. A failure during wire-client
    /// construction (e.g. missing CA cert, unparseable endpoint URL) is
    /// captured and surfaced from the first [`Self::cached_wire_client`]
    /// dispatch attempt rather than from the constructor — the pool
    /// itself remains infallible to keep test scaffolding and
    /// [`crate::LedgerClient::new`] simple.
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self::with_selector(config, ServerSelector::new())
    }

    /// Creates a new connection pool with a custom server selector.
    ///
    /// Use this when you want to share a selector across multiple pools.
    /// Same construction semantics as [`Self::new`].
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

        let vault_cache = Arc::new(crate::vault_resolver::VaultLeaderCache::with_capacity(
            config.vault_cache_capacity(),
        ));
        vault_cache.set_metrics(Arc::clone(config.metrics()));

        // Build wire clients eagerly. A construction failure (bad TLS,
        // unparseable endpoint) is logged at debug level and the pool
        // falls back to an empty wire vec; dispatch sites then surface a
        // clean `SdkError::Config` ("wire client not configured") which
        // is far more actionable than blowing up in `ConnectionPool::new`
        // for test fixtures that never exercise the wire path.
        let wire_clients = match build_wire_clients(&config) {
            Ok(clients) => clients,
            Err(err) => {
                tracing::debug!(
                    %err,
                    "wire client pool construction failed; ops will surface SdkError::Config",
                );
                Vec::new()
            },
        };

        Self {
            wire_clients,
            next_client_idx: Arc::new(AtomicUsize::new(0)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector,
            circuit_breaker,
            region_cache,
            vault_cache,
        }
    }

    /// Returns a round-robin-selected [`Arc<WireClient>`] when the wire
    /// pool is non-empty; `None` otherwise (e.g. wire-client construction
    /// failed at build time, or the configured `ServerSource` is DNS/File
    /// and the resolver hasn't run).
    ///
    /// The wire pool is materialized at construction time (one
    /// [`WireClient`] per configured endpoint, in declaration order),
    /// so this never blocks and never lazily reconnects — the lazy
    /// QUIC handshake lives inside [`WireClient::acquire`] and runs on
    /// the first per-RPC call.
    #[must_use]
    pub fn cached_wire_client(&self) -> Option<Arc<WireClient>> {
        if self.wire_clients.is_empty() {
            return None;
        }
        let idx = self.next_client_idx.fetch_add(1, Ordering::Relaxed) % self.wire_clients.len();
        Some(Arc::clone(&self.wire_clients[idx]))
    }

    /// Returns the number of pooled wire clients. Zero when no endpoints
    /// were supplied or wire-client construction failed.
    #[must_use]
    pub fn wire_client_count(&self) -> usize {
        self.wire_clients.len()
    }

    /// On a `NotLeader`-like error, applies the server-provided leader hint
    /// (if any) to the appropriate cache layer.
    ///
    /// Dispatch order:
    ///
    /// - `(organization_slug, vault_slug)` both present on the hint → update the per-vault cache.
    ///   This is the slug-keyed path: the SDK's `VaultLeaderCache` is keyed on `(OrganizationSlug,
    ///   VaultSlug)`, so both slug fields must be known. The internal `*_id` keys also flow through
    ///   and are ignored here — they're informational.
    /// - Either slug absent → fall through to the region cache. Region- and org-scoped rejections,
    ///   plus legacy servers that pre-date the slug fields, route here. Updating the per-vault
    ///   cache from a hint that lacks slugs would write under the wrong key, so we deliberately
    ///   skip rather than guess.
    ///
    /// When the hint carries no usable endpoint, the corresponding cache
    /// entry is invalidated instead so the next attempt cold-resolves.
    pub fn apply_region_leader_hint_or_invalidate(&self, err: &SdkError) {
        let hint = err.server_error_details().and_then(|d| d.leader_hint());

        // Vault-scoped path: the hint identifies a specific (org_slug, vault_slug).
        if let Some(ref h) = hint
            && let (Some(org_slug_raw), Some(vault_slug_raw)) = (h.organization_slug, h.vault_slug)
        {
            let org = inferadb_ledger_types::OrganizationSlug::new(org_slug_raw);
            let vault = inferadb_ledger_types::VaultSlug::new(vault_slug_raw);
            if h.leader_endpoint.is_some() {
                self.vault_cache.apply_hint(org, vault, h);
                let label = self
                    .region_cache
                    .as_ref()
                    .map_or_else(|| format!("vault:{vault_slug_raw}"), |c| c.region().to_string());
                self.config.metrics().redirect_retry(&label);
                return;
            }
            // Hint had vault context but no endpoint — purge the entry.
            self.vault_cache.invalidate(org, vault);
            return;
        }

        // Region-scoped path: existing behavior. A hint that carries
        // `vault_id` but no `vault_slug` (legacy server) lands here too —
        // we can't populate the slug-keyed vault cache without slug values,
        // so the request retries through the region path.
        let Some(ref cache) = self.region_cache else {
            return;
        };
        match hint {
            Some(ref h) if h.leader_endpoint.is_some() => {
                cache.apply_hint(h);
                self.config.metrics().redirect_retry(&cache.region().to_string());
            },
            _ => {
                cache.invalidate();
            },
        }
    }

    /// Returns a reference to the per-vault leader cache.
    #[must_use]
    pub fn vault_cache(&self) -> &Arc<crate::vault_resolver::VaultLeaderCache> {
        &self.vault_cache
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
                organization_id: None,
                vault_id: None,
                organization_slug: None,
                vault_slug: None,
            };
            cache.apply_hint(&hint);
        }
    }

    /// Returns whether compression is enabled for this connection.
    ///
    /// The wire transport does not currently apply payload compression at
    /// the framing layer, but consumers (e.g. discovery service) read this
    /// flag to decide whether to set per-RPC compression hints — preserving
    /// the historical surface so existing consumers compile unchanged.
    #[must_use]
    pub fn compression_enabled(&self) -> bool {
        self.config.compression
    }

    /// Returns a reference to the client configuration.
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Records a disconnect metric event for the current endpoint.
    ///
    /// `WireClient` reconnects internally on demand, so this is mainly a
    /// recovery hook for callers that want to surface a disconnect signal
    /// (e.g. discovery service after refreshing endpoints). The
    /// per-endpoint `Self::wire_clients` vec is left untouched — those
    /// handles are immutable for the pool's lifetime.
    pub fn reset(&self) {
        self.config.metrics().record_connection(
            &self.current_endpoint(),
            crate::metrics::ConnectionEvent::Disconnected,
        );
    }

    /// Updates the endpoints used for connections.
    ///
    /// This method is called by the discovery service to update the endpoint
    /// list based on discovered peers. The new endpoints take precedence over
    /// the endpoints in the original configuration.
    ///
    /// Note: This does not automatically reconnect or rebuild the wire
    /// pool. Wire-client materialization happens at pool construction
    /// only; updating endpoints affects metric labels and active-endpoint
    /// reporting.
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
    fn pool_stores_custom_timeouts() {
        let config = test_config_with_custom_timeouts();
        let pool = ConnectionPool::new(config);

        assert_eq!(pool.config().timeout(), Duration::from_secs(30));
        assert_eq!(pool.config().connect_timeout(), Duration::from_secs(10));
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
    fn pool_clone_shares_state() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        let pool2 = pool.clone();
        pool.update_endpoints(vec!["http://10.0.0.1:5000".to_string()]);
        // Cloned pool should see the same dynamic endpoints (Arc shared)
        assert_eq!(pool2.active_endpoints(), vec!["http://10.0.0.1:5000".to_string()]);
    }

    /// `connection_pool_size = 0` is rejected at config build time.
    #[test]
    fn connection_pool_size_zero_is_rejected() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connection_pool_size(0)
            .build();
        assert!(result.is_err(), "connection_pool_size=0 must fail validation");
    }

    #[test]
    fn apply_region_leader_hint_or_invalidate_applies_hint_when_present() {
        use inferadb_ledger_types::Region;
        use inferadb_ledger_wire::ErrorCode as Code;

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
            code: Code::StaleRouting,
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
        use inferadb_ledger_wire::ErrorCode as Code;

        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Seed cache directly.
        pool.seed_region_cache_for_test("http://old:5000");
        assert_eq!(pool.region_cached_endpoint().as_deref(), Some("http://old:5000"),);

        // SdkError without hint (error_details = None).
        let err = SdkError::Rpc {
            code: Code::StaleRouting,
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
        use inferadb_ledger_wire::ErrorCode as Code;

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
            code: Code::StaleRouting,
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
        use inferadb_ledger_wire::ErrorCode as Code;

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
            code: Code::StaleRouting,
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
        use inferadb_ledger_wire::ErrorCode as Code;

        // Pool without a preferred region has region_cache = None.
        let config = test_config_without_region();
        let pool = ConnectionPool::new(config);

        let err = SdkError::Rpc {
            code: Code::StaleRouting,
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

    // =========================================================================
    // Vault cache dispatch tests
    // =========================================================================

    #[test]
    fn apply_hint_with_vault_slug_routes_to_vault_cache() {
        use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        // Region cache present, vault cache present.
        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Hint carries leader_organization_slug + leader_vault_slug — must
        // route to the vault cache, NOT the region cache.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "11".to_owned()),
                ("leader_endpoint".to_owned(), "http://vault-leader:5000".to_owned()),
                ("leader_term".to_owned(), "9".to_owned()),
                ("leader_shard".to_owned(), "42".to_owned()),
                ("leader_vault".to_owned(), "7".to_owned()),
                ("leader_organization_slug".to_owned(), "1234".to_owned()),
                ("leader_vault_slug".to_owned(), "5678".to_owned()),
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::StaleRouting,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        // The vault cache should now hold the endpoint, keyed by slugs.
        let entry = pool
            .vault_cache()
            .lookup(OrganizationSlug::new(1234), VaultSlug::new(5678))
            .expect("vault cache populated");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://vault-leader:5000"));
        assert_eq!(entry.term, Some(9));

        // The region cache must be UNTOUCHED — vault-scoped hints don't
        // overwrite region cache state.
        assert!(
            pool.region_cached_endpoint().is_none(),
            "region cache must not be populated by a vault-scoped hint",
        );
    }

    #[test]
    fn apply_hint_with_only_legacy_vault_id_falls_through_to_region_cache() {
        use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        // Legacy server compat: hint carries leader_vault (internal id)
        // but no leader_vault_slug. The retry must fall through to the
        // region path instead.
        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "11".to_owned()),
                ("leader_endpoint".to_owned(), "http://legacy-leader:5000".to_owned()),
                ("leader_term".to_owned(), "9".to_owned()),
                ("leader_shard".to_owned(), "42".to_owned()),
                ("leader_vault".to_owned(), "7".to_owned()),
                // No slug fields — legacy server.
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::StaleRouting,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        // Vault cache must remain empty — we don't have slug keys to write under.
        assert!(
            pool.vault_cache().lookup(OrganizationSlug::new(42), VaultSlug::new(7)).is_none(),
            "vault cache must not populate from legacy id-only hint",
        );
        // Region cache catches the fallthrough.
        assert_eq!(
            pool.region_cached_endpoint().as_deref(),
            Some("http://legacy-leader:5000"),
            "region cache should populate for legacy hint",
        );
    }

    #[test]
    fn apply_hint_without_vault_id_routes_to_region_cache() {
        use inferadb_ledger_types::Region;
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Region-scoped hint: no leader_vault key.
        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_id".to_owned(), "11".to_owned()),
                ("leader_endpoint".to_owned(), "http://region-leader:5000".to_owned()),
                ("leader_term".to_owned(), "9".to_owned()),
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::StaleRouting,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        pool.apply_region_leader_hint_or_invalidate(&err);

        assert_eq!(
            pool.region_cached_endpoint().as_deref(),
            Some("http://region-leader:5000"),
            "region cache should populate on hintless vault scope",
        );
        assert!(pool.vault_cache().is_empty(), "vault cache must remain empty");
    }

    #[test]
    fn vault_hint_term_gating_drops_older_hint() {
        use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        let make_err = |endpoint: &str, term: u64| {
            let details = ServerErrorDetails {
                error_code: "2000".into(),
                is_retryable: true,
                retry_after_ms: None,
                context: std::collections::HashMap::from([
                    ("leader_endpoint".to_owned(), endpoint.to_owned()),
                    ("leader_term".to_owned(), term.to_string()),
                    ("leader_shard".to_owned(), "1".to_owned()),
                    ("leader_vault".to_owned(), "1".to_owned()),
                    ("leader_organization_slug".to_owned(), "1".to_owned()),
                    ("leader_vault_slug".to_owned(), "1".to_owned()),
                ]),
                suggested_action: None,
            };
            SdkError::Rpc {
                code: Code::StaleRouting,
                message: "not leader".into(),
                request_id: None,
                trace_id: None,
                error_details: Some(Box::new(details)),
            }
        };

        // Newer term first, then older term — older must be rejected.
        pool.apply_region_leader_hint_or_invalidate(&make_err("http://A:5000", 9));
        pool.apply_region_leader_hint_or_invalidate(&make_err("http://B:5000", 5));

        let entry = pool
            .vault_cache()
            .lookup(OrganizationSlug::new(1), VaultSlug::new(1))
            .expect("populated");
        assert_eq!(
            entry.leader_endpoint.as_deref(),
            Some("http://A:5000"),
            "older-term hint must be rejected (CLAUDE.md SDK rule 6)",
        );
    }

    #[test]
    fn vault_hint_works_without_preferred_region() {
        use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        // Pool without preferred_region — the vault cache is still wired.
        let config = test_config_without_region();
        let pool = ConnectionPool::new(config);

        let details = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_endpoint".to_owned(), "http://vault-leader:5000".to_owned()),
                ("leader_term".to_owned(), "3".to_owned()),
                ("leader_shard".to_owned(), "1".to_owned()),
                ("leader_vault".to_owned(), "2".to_owned()),
                ("leader_organization_slug".to_owned(), "1".to_owned()),
                ("leader_vault_slug".to_owned(), "2".to_owned()),
            ]),
            suggested_action: None,
        };
        let err = SdkError::Rpc {
            code: Code::StaleRouting,
            message: "not leader".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(details)),
        };

        // Must not panic and must populate the vault cache.
        pool.apply_region_leader_hint_or_invalidate(&err);

        let entry = pool
            .vault_cache()
            .lookup(OrganizationSlug::new(1), VaultSlug::new(2))
            .expect("vault cache populated even without region cache");
        assert_eq!(entry.leader_endpoint.as_deref(), Some("http://vault-leader:5000"));
    }

    #[test]
    fn vault_hint_without_endpoint_invalidates_entry() {
        use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
        use inferadb_ledger_wire::ErrorCode as Code;

        use crate::error::ServerErrorDetails;

        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Seed the vault cache.
        let seed = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_endpoint".to_owned(), "http://A:5000".to_owned()),
                ("leader_term".to_owned(), "5".to_owned()),
                ("leader_shard".to_owned(), "1".to_owned()),
                ("leader_vault".to_owned(), "1".to_owned()),
                ("leader_organization_slug".to_owned(), "1".to_owned()),
                ("leader_vault_slug".to_owned(), "1".to_owned()),
            ]),
            suggested_action: None,
        };
        pool.apply_region_leader_hint_or_invalidate(&SdkError::Rpc {
            code: Code::StaleRouting,
            message: "x".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(seed)),
        });
        assert!(pool.vault_cache().lookup(OrganizationSlug::new(1), VaultSlug::new(1)).is_some());

        // Now an empty-endpoint hint with vault context — must invalidate.
        let purge = ServerErrorDetails {
            error_code: "2000".into(),
            is_retryable: true,
            retry_after_ms: None,
            context: std::collections::HashMap::from([
                ("leader_shard".to_owned(), "1".to_owned()),
                ("leader_vault".to_owned(), "1".to_owned()),
                ("leader_organization_slug".to_owned(), "1".to_owned()),
                ("leader_vault_slug".to_owned(), "1".to_owned()),
            ]),
            suggested_action: None,
        };
        pool.apply_region_leader_hint_or_invalidate(&SdkError::Rpc {
            code: Code::StaleRouting,
            message: "x".into(),
            request_id: None,
            trace_id: None,
            error_details: Some(Box::new(purge)),
        });

        assert!(
            pool.vault_cache().lookup(OrganizationSlug::new(1), VaultSlug::new(1)).is_none(),
            "endpoint-less vault hint must invalidate the entry",
        );
    }

    #[test]
    fn vault_cache_is_always_present() {
        // Without preferred_region, the region cache is None but the vault
        // cache must still be wired so vault-scoped routing works.
        let config = test_config_without_region();
        let pool = ConnectionPool::new(config);
        assert!(pool.region_cache.is_none(), "no preferred region → no region cache");
        assert_eq!(
            pool.vault_cache().capacity(),
            crate::vault_resolver::DEFAULT_VAULT_CACHE_CAPACITY,
        );
    }

    #[test]
    fn vault_cache_capacity_respects_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .vault_cache_capacity(42)
            .build()
            .expect("valid config");
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.vault_cache().capacity(), 42);
    }

    /// `parse_wire_endpoint` produces a valid `(SocketAddr, hostname)` for
    /// loopback URLs (which `ToSocketAddrs` can resolve synchronously).
    #[test]
    fn parse_wire_endpoint_loopback() {
        let (addr, name) = parse_wire_endpoint("http://127.0.0.1:50051").expect("parses");
        assert_eq!(addr.port(), 50051);
        assert!(addr.ip().is_loopback());
        assert_eq!(name, "127.0.0.1");
    }

    /// UDS-style URL is rejected with a clear error.
    #[test]
    fn parse_wire_endpoint_rejects_uds() {
        let err = parse_wire_endpoint("/tmp/ledger.sock").expect_err("UDS must error");
        match err {
            SdkError::Config { message } => assert!(message.contains("UDS")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Missing port surfaces a parse-style InvalidUrl.
    #[test]
    fn parse_wire_endpoint_rejects_missing_port() {
        let err = parse_wire_endpoint("http://example.com").expect_err("missing port");
        match err {
            SdkError::InvalidUrl { .. } => {},
            other => panic!("expected InvalidUrl, got {other:?}"),
        }
    }
}
